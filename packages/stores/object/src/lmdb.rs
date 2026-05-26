use {
	crate::{
		CachePointer, DeleteArg, Grant, GrantArg, Object, PutArg, TryGetArg, TryGetBatchArg,
		TryGetOutput,
	},
	bytes::Bytes,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::{FromPrimitive as _, ToPrimitive as _},
	std::borrow::Cow,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub grant_ttl: u64,
	pub map_size: usize,
	pub path: std::path::PathBuf,
}

pub struct Store {
	db: Db,
	env: lmdb::Env,
	#[expect(
		dead_code,
		reason = "The task is held so it stays alive until the store is dropped."
	)]
	grant_clean_task: tangram_futures::task::Task<()>,
	grant_ttl: u64,
	sender: RequestSender,
}

pub type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

enum Request {
	CleanGrants(CleanGrants),
	Delete(DeleteObject),
	DeleteBatch(Vec<DeleteObject>),
	Grant(GrantObject),
	GrantBatch(Vec<GrantObject>),
	Put(PutObject),
	PutBatch(Vec<PutObject>),
}

#[derive(Clone, Copy)]
struct CleanGrants {
	now: i64,
	ttl: u64,
}

struct PutObject {
	bytes: Option<Bytes>,
	cache_pointer: Option<CachePointer>,
	id: tg::object::Id,
	principal: Option<tg::Principal>,
	stored_at: i64,
}

struct DeleteObject {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

struct GrantObject {
	created_at: i64,
	id: tg::object::Id,
	principal: tg::Principal,
	subtree: bool,
}

#[derive(Debug)]
enum Key<'a> {
	Object(&'a tg::object::Id),
	ObjectGrant(&'a tg::object::Id, &'a str),
	ObjectGrantCreatedAt(i64, &'a tg::object::Id, &'a str),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum KeyKind {
	Object = 0,
	ObjectGrant = 1,
	ObjectGrantCreatedAt = 2,
}

impl Store {
	pub fn new(config: &Config) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(
				|error| tg::error!(!error, path = %config.path.display(), "failed to open the lmdb file"),
			)?;
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(
					lmdb::EnvFlags::NO_SUB_DIR
						| lmdb::EnvFlags::WRITE_MAP
						| lmdb::EnvFlags::MAP_ASYNC,
				)
				.open(&config.path)
				.map_err(|error| {
					tg::error!(!error, path = %config.path.display(), "failed to open the lmdb environment")
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|error| tg::error!(!error, "failed to create the database"))?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		// Create the thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});
		let grant_clean_task = Self::spawn_grant_clean_task(&sender, config.grant_ttl);

		Ok(Self {
			db,
			env,
			grant_clean_task,
			grant_ttl: config.grant_ttl,
			sender,
		})
	}

	#[must_use]
	pub fn db(&self) -> Db {
		self.db
	}

	#[must_use]
	pub fn env(&self) -> &lmdb::Env {
		&self.env
	}

	pub fn try_get_sync(&self, arg: &TryGetArg) -> tg::Result<TryGetOutput> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_with_transaction(&transaction, arg)
	}

	pub fn try_get_batch_sync(&self, arg: &TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(arg.ids.len());
		for id in &arg.ids {
			let object = self.object_with_transaction(&transaction, id)?;
			let grants = self.grants_with_transaction(&transaction, id, &arg.principal, arg.now)?;
			outputs.push(TryGetOutput { grants, object });
		}
		Ok(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_object_data_with_transaction(&transaction, id, principal, now)
	}

	pub fn try_get_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		arg: &TryGetArg,
	) -> tg::Result<TryGetOutput> {
		let object = self.object_with_transaction(transaction, &arg.id)?;
		let grants = self.grants_with_transaction(transaction, &arg.id, &arg.principal, arg.now)?;
		Ok(TryGetOutput { grants, object })
	}

	fn object_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object<'static>>> {
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = self
			.db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	pub fn try_get_object_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let arg = TryGetArg {
			id: id.clone(),
			now,
			principal: principal.clone(),
		};
		let output = self.try_get_with_transaction(transaction, &arg)?;
		if !matches!(principal, tg::Principal::Root) && output.grants.is_empty() {
			return Ok(None);
		}
		let Some(value) = output.object else {
			return Ok(None);
		};
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
	}

	fn grants_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Vec<Grant>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		let principal = principal.to_string();
		let key = Key::ObjectGrant(id, &principal);
		let Some(bytes) = self
			.db
			.get(transaction, &key.pack_to_vec())
			.map_err(|error| tg::error!(!error, %id, "failed to get the object grant"))?
		else {
			return Ok(Vec::new());
		};
		let grant = Grant::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object grant"))?;
		Ok((now - grant.created_at < self.grant_ttl.to_i64().unwrap())
			.then_some(grant)
			.into_iter()
			.collect())
	}

	fn grants(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
		grant_ttl: u64,
	) -> tg::Result<Vec<Grant>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		let principal = principal.to_string();
		let key = Key::ObjectGrant(id, &principal);
		let Some(bytes) = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|error| tg::error!(!error, %id, "failed to get the object grant"))?
		else {
			return Ok(Vec::new());
		};
		let grant = Grant::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object grant"))?;
		Ok((now - grant.created_at < grant_ttl.to_i64().unwrap())
			.then_some(grant)
			.into_iter()
			.collect())
	}

	fn task(env: &lmdb::Env, db: &Db, mut receiver: RequestReceiver) {
		while let Some((request, sender)) = receiver.blocking_recv() {
			let mut requests = vec![request];
			let mut senders = vec![sender];
			while let Ok((request, sender)) = receiver.try_recv() {
				requests.push(request);
				senders.push(sender);
			}
			let result = env
				.write_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"));
			let mut transaction = match result {
				Ok(transaction) => transaction,
				Err(error) => {
					for sender in senders {
						sender.send(Err(error.clone())).ok();
					}
					continue;
				},
			};
			let mut responses = vec![];
			for request in requests {
				let result = match request {
					Request::CleanGrants(request) => {
						Self::task_clean_grants(db, &mut transaction, &request)
					},
					Request::Delete(request) => {
						Self::task_delete_object(env, db, &mut transaction, request)
					},
					Request::DeleteBatch(requests) => {
						requests.into_iter().try_for_each(|request| {
							Self::task_delete_object(env, db, &mut transaction, request)
						})
					},
					Request::Grant(request) => Self::task_put_object_grant(
						db,
						&mut transaction,
						&request.id,
						&request.principal,
						request.subtree,
						request.created_at,
					),
					Request::GrantBatch(requests) => requests.into_iter().try_for_each(|request| {
						Self::task_put_object_grant(
							db,
							&mut transaction,
							&request.id,
							&request.principal,
							request.subtree,
							request.created_at,
						)
					}),
					Request::Put(request) => {
						Self::task_put_object(env, db, &mut transaction, request)
					},
					Request::PutBatch(requests) => requests.into_iter().try_for_each(|request| {
						Self::task_put_object(env, db, &mut transaction, request)
					}),
				};
				responses.push(result);
			}
			if let Some(error) = responses.iter().find_map(|result| result.as_ref().err()) {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}
			let result = transaction
				.commit()
				.map_err(|error| tg::error!(!error, "failed to commit the transaction"));
			if let Err(error) = result {
				for sender in senders {
					sender.send(Err(error.clone())).ok();
				}
				continue;
			}
			for (sender, result) in std::iter::zip(senders, responses) {
				sender.send(result).ok();
			}
		}
	}

	fn spawn_grant_clean_task(
		sender: &RequestSender,
		grant_ttl: u64,
	) -> tangram_futures::task::Task<()> {
		let sender = sender.clone();
		tangram_futures::task::Task::spawn(move |stopper| async move {
			let interval = std::time::Duration::from_secs(grant_ttl);
			loop {
				tokio::select! {
					() = tokio::time::sleep(interval) => {},
					() = stopper.wait() => {
						break;
					},
				}
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_secs()
					.to_i64()
					.unwrap();
				let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
				let request = Request::CleanGrants(CleanGrants {
					now,
					ttl: grant_ttl,
				});
				if sender.send((request, response_sender)).await.is_err() {
					break;
				}
				let _ = response_receiver.await;
			}
		})
	}

	fn task_put_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutObject,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		let bytes = existing
			.as_ref()
			.and_then(|entry| entry.bytes.clone())
			.or(request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())));

		let cache_pointer = request
			.cache_pointer
			.or_else(|| existing.and_then(|entry| entry.cache_pointer));

		let value = Object {
			bytes,
			stored_at: request.stored_at,
			cache_pointer,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		if let Some(principal) = request.principal {
			Self::task_put_object_grant(db, transaction, id, &principal, false, request.stored_at)?;
		}

		Ok(())
	}

	fn task_put_object_grant(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		subtree: bool,
		created_at: i64,
	) -> tg::Result<()> {
		let principal = principal.to_string();
		let key = Key::ObjectGrant(id, &principal);
		let key_bytes = key.pack_to_vec();
		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object grant"))?
			.map(Grant::deserialize)
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object grant"))?;

		if let Some(existing) = &existing {
			let index_key = Key::ObjectGrantCreatedAt(existing.created_at, id, &principal);
			db.delete(transaction, &index_key.pack_to_vec()).map_err(
				|error| tg::error!(!error, %id, "failed to delete the object grant index"),
			)?;
		}

		let grant = Grant {
			created_at,
			subtree: subtree || existing.is_some_and(|grant| grant.subtree),
		};
		let value_bytes = grant.serialize()?;
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object grant"))?;
		let index_key = Key::ObjectGrantCreatedAt(created_at, id, &principal);
		db.put(transaction, &index_key.pack_to_vec(), &[])
			.map_err(|error| tg::error!(!error, %id, "failed to put the object grant index"))?;

		Ok(())
	}

	fn task_clean_grants(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: &CleanGrants,
	) -> tg::Result<()> {
		let max_created_at = request.now - request.ttl.to_i64().unwrap();
		let prefix = (KeyKind::ObjectGrantCreatedAt.to_i32().unwrap(),).pack_to_vec();
		let iter = db
			.prefix_iter(&*transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate object grant indexes"))?;
		let mut expired = Vec::new();
		for entry in iter {
			let (key, _) = entry
				.map_err(|error| tg::error!(!error, "failed to read the object grant index"))?;
			let key = Key::unpack_object_grant_created_at(key)?;
			let (created_at, id, principal) = key;
			if created_at > max_created_at {
				break;
			}
			expired.push((created_at, id, principal));
		}
		for (created_at, id, principal) in expired {
			let grant_key = Key::ObjectGrant(&id, &principal);
			db.delete(transaction, &grant_key.pack_to_vec())
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object grant"))?;
			let index_key = Key::ObjectGrantCreatedAt(created_at, &id, &principal);
			db.delete(transaction, &index_key.pack_to_vec()).map_err(
				|error| tg::error!(!error, %id, "failed to delete the object grant index"),
			)?;
		}
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: DeleteObject,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;

		if request.now - value.stored_at >= request.ttl.to_i64().unwrap() {
			db.delete(transaction, &key_bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		}

		Ok(())
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			principal: arg.principal,
			stored_at: arg.stored_at,
		};
		Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = PutObject {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				principal: arg.principal,
				stored_at: arg.stored_at,
			};
			Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn grant_sync(&self, arg: GrantArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		Self::task_put_object_grant(
			&self.db,
			&mut transaction,
			&arg.id,
			&arg.principal,
			arg.subtree,
			arg.created_at,
		)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn grant_batch_sync(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			Self::task_put_object_grant(
				&self.db,
				&mut transaction,
				&arg.id,
				&arg.principal,
				arg.subtree,
				arg.created_at,
			)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = DeleteObject {
				id: arg.id,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::task_delete_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn flush_sync(&self) -> tg::Result<()> {
		self.env
			.force_sync()
			.map_err(|error| tg::error!(!error, "failed to sync"))?;
		Ok(())
	}
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let grant_ttl = self.grant_ttl;
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let key = Key::Object(&arg.id);
				let object = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the object"))?
					.map(Object::deserialize)
					.transpose()
					.map_err(
						|error| tg::error!(!error, id = %arg.id, "failed to deserialize the object"),
					)?;
				let grants = Self::grants(
					&db,
					&transaction,
					&arg.id,
					&arg.principal,
					arg.now,
					grant_ttl,
				)?;
				Ok(TryGetOutput { grants, object })
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		if arg.ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let grant_ttl = self.grant_ttl;
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(arg.ids.len());
				for id in &arg.ids {
					let key = Key::Object(id);
					let object = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
						.map(Object::deserialize)
						.transpose()
						.map_err(
							|error| tg::error!(!error, %id, "failed to deserialize the object"),
						)?;
					let grants =
						Self::grants(&db, &transaction, id, &arg.principal, arg.now, grant_ttl)?;
					outputs.push(TryGetOutput { grants, object });
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			principal: arg.principal,
			stored_at: arg.stored_at,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutBatch(
			args.into_iter()
				.map(|arg| PutObject {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					principal: arg.principal,
					stored_at: arg.stored_at,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Grant(GrantObject {
			created_at: arg.created_at,
			id: arg.id,
			principal: arg.principal,
			subtree: arg.subtree,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::GrantBatch(
			args.into_iter()
				.map(|arg| GrantObject {
					created_at: arg.created_at,
					id: arg.id,
					principal: arg.principal,
					subtree: arg.subtree,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Delete(DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteBatch(
			args.into_iter()
				.map(|arg| DeleteObject {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn flush(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|error| tg::error!(!error, "failed to sync"))
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}
}

impl fdbt::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::Object(id) => {
				(KeyKind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},
			Key::ObjectGrant(id, principal) => (
				KeyKind::ObjectGrant.to_i32().unwrap(),
				id.to_bytes().as_ref(),
				principal,
			)
				.pack(w, tuple_depth),
			Key::ObjectGrantCreatedAt(created_at, id, principal) => (
				KeyKind::ObjectGrantCreatedAt.to_i32().unwrap(),
				created_at,
				id.to_bytes().as_ref(),
				principal,
			)
				.pack(w, tuple_depth),
		}
	}
}

impl Key<'_> {
	fn unpack_object_grant_created_at(bytes: &[u8]) -> tg::Result<(i64, tg::object::Id, String)> {
		let (kind, created_at, id, principal): (i32, i64, Vec<u8>, String) = fdbt::Subspace::all()
			.unpack(bytes)
			.map_err(|error| tg::error!(!error, "failed to unpack the object grant index key"))?;
		let kind = KeyKind::from_i32(kind).ok_or_else(|| tg::error!("invalid key kind"))?;
		if kind != KeyKind::ObjectGrantCreatedAt {
			return Err(tg::error!("unexpected object grant index key"));
		}
		let id = tg::object::Id::from_slice(&id)
			.map_err(|error| tg::error!(!error, "failed to parse the object id"))?;
		Ok((created_at, id, principal))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Store as _;

	#[tokio::test]
	async fn test_put_and_get_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object.
		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: None,
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Get the object.
		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12345,
			principal: tg::Principal::Root,
		};
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_put_object_without_bytes_then_with_bytes() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put without bytes first (should not store anything).
		store
			.put(crate::PutArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				principal: None,
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12345,
			principal: tg::Principal::Root,
		};
		let result = store.try_get(arg).await.unwrap().object;
		assert!(
			result.is_none()
				|| result
					.as_ref()
					.and_then(|object| object.bytes.as_ref())
					.is_none()
		);

		// Put with bytes.
		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: None,
				stored_at: 12346,
			})
			.await
			.unwrap();

		// Verify object now exists.
		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12346,
			principal: tg::Principal::Root,
		};
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_put_and_get_object_sync() {
		// This test mimics what the server does using sync functions.
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		// Create object data and ID similar to server's write.rs.
		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		// Put the object using sync function (like server does).
		store
			.put_sync(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: None,
				stored_at: 12345,
			})
			.unwrap();

		// Get the object using sync function.
		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12345,
			principal: tg::Principal::Root,
		};
		let result = store.try_get_sync(&arg).unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_put_batch_and_get_object() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		store
			.put_batch(vec![crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: None,
				stored_at: 12345,
			}])
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12345,
			principal: tg::Principal::Root,
		};
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_object_grant_authorizes_matching_principal() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let principal = tg::Principal::All;

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: Some(principal.clone()),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12346,
			principal,
		};
		let output = store.try_get(arg).await.unwrap();
		assert!(!output.grants.is_empty());
		assert_eq!(
			output.object.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[tokio::test]
	async fn test_object_grant_can_be_upgraded_to_subtree() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let principal = tg::Principal::All;

		store
			.put(crate::PutArg {
				bytes: Some(bytes),
				cache_pointer: None,
				id: id.clone(),
				principal: Some(principal.clone()),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id: id.clone(),
			now: 12346,
			principal: principal.clone(),
		};
		let output = store.try_get(arg).await.unwrap();
		assert!(output.grants.iter().all(|grant| !grant.subtree));

		store
			.grant(crate::GrantArg {
				created_at: 12347,
				id: id.clone(),
				principal: principal.clone(),
				subtree: true,
			})
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id,
			now: 12348,
			principal,
		};
		let output = store.try_get(arg).await.unwrap();
		assert!(output.grants.iter().any(|grant| grant.subtree));
	}

	#[tokio::test]
	async fn test_object_grant_does_not_authorize_different_principal() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 86_400,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: Some(tg::Principal::All),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id,
			now: 12346,
			principal: tg::Principal::User(tg::user::Id::new()),
		};
		let output = store.try_get(arg).await.unwrap();
		assert!(output.grants.is_empty());
		assert!(output.object.is_some());
	}

	#[tokio::test]
	async fn test_object_grant_expires() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			grant_ttl: 1,
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();

		let content = b"hello world";
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let principal = tg::Principal::All;

		store
			.put(crate::PutArg {
				bytes: Some(bytes),
				cache_pointer: None,
				id: id.clone(),
				principal: Some(principal.clone()),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let arg = crate::TryGetArg {
			id,
			now: 12346,
			principal,
		};
		let output = store.try_get(arg).await.unwrap();
		assert!(output.grants.is_empty());
		assert!(output.object.is_some());
	}
}
