use {
	crate::{CachePointer, DeleteArg, DeleteMembershipArg, Membership, Object, PutArg},
	bytes::Bytes,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub path: std::path::PathBuf,
}

pub struct Store {
	db: Db,
	env: lmdb::Env,
	sender: RequestSender,
}

pub type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

enum Request {
	Put(Put),
	PutBatch(Vec<Put>),
	DeleteMembership(DeleteMembership),
	DeleteMembershipBatch(Vec<DeleteMembership>),
	Delete(Delete),
	DeleteBatch(Vec<Delete>),
}

struct Put {
	bytes: Option<Bytes>,
	cache_pointer: Option<CachePointer>,
	id: tg::object::Id,
	namespace: Option<tg::Namespace>,
	stored_at: i64,
}

struct DeleteMembership {
	id: tg::object::Id,
	namespace: Option<tg::Namespace>,
	now: i64,
	ttl: u64,
}

struct Delete {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

#[derive(Debug)]
enum Key<'a> {
	Object(&'a tg::object::Id),
	Membership(&'a tg::object::Id, Option<&'a tg::Namespace>),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::ToPrimitive)]
#[repr(u8)]
enum KeyKind {
	Object = 0,
	Membership = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::ToPrimitive)]
#[repr(u8)]
enum MembershipKind {
	Public = 0,
	Namespace = 1,
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

		Ok(Self { db, env, sender })
	}

	#[must_use]
	pub fn db(&self) -> Db {
		self.db
	}

	#[must_use]
	pub fn env(&self) -> &lmdb::Env {
		&self.env
	}

	pub fn try_get_sync(
		&self,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Option<Object<'static>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		if !Self::has_membership_with_transaction(&self.db, &transaction, id, namespaces, public)? {
			return Ok(None);
		}
		self.try_get_object_with_transaction(&transaction, id)
	}

	pub fn try_get_batch_sync(
		&self,
		ids: &[tg::object::Id],
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			if !Self::has_membership_with_transaction(
				&self.db,
				&transaction,
				id,
				namespaces,
				public,
			)? {
				outputs.push(None);
				continue;
			}
			let value = self.try_get_object_with_transaction(&transaction, id)?;
			outputs.push(value);
		}
		Ok(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_object_data_with_transaction(&transaction, id, namespaces, public)
	}

	pub fn try_get_object_with_transaction(
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

	pub fn try_get_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Option<Object<'static>>> {
		if !Self::has_membership_with_transaction(&self.db, transaction, id, namespaces, public)? {
			return Ok(None);
		}
		self.try_get_object_with_transaction(transaction, id)
	}

	pub fn try_get_object_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		if !Self::has_membership_with_transaction(&self.db, transaction, id, namespaces, public)? {
			return Ok(None);
		}
		let kind = id.kind();
		let key = Key::Object(id);
		let Some(raw_bytes) = self
			.db
			.get(transaction, &key.pack_to_vec())
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(raw_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
	}

	fn has_membership_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		namespaces: &[tg::Namespace],
		public: bool,
	) -> tg::Result<bool> {
		if public {
			let key = Key::Membership(id, None);
			let value = db
				.get(transaction, &key.pack_to_vec())
				.map_err(|error| tg::error!(!error, %id, "failed to get the object membership"))?;
			if value.is_some() {
				return Ok(true);
			}
		}

		for namespace in namespaces {
			if namespace.is_root() {
				let prefix = Self::membership_namespace_prefix(id);
				if Self::has_membership_with_prefix(db, transaction, id, &prefix)? {
					return Ok(true);
				}
			} else {
				let key = Key::Membership(id, Some(namespace));
				let value = db.get(transaction, &key.pack_to_vec()).map_err(
					|error| tg::error!(!error, %id, "failed to get the object membership"),
				)?;
				if value.is_some() {
					return Ok(true);
				}

				let mut prefix = namespace.to_string();
				prefix.push('/');
				let prefix = Self::membership_namespace_string_prefix(id, &prefix);
				if Self::has_membership_with_prefix(db, transaction, id, &prefix)? {
					return Ok(true);
				}
			}
		}

		Ok(false)
	}

	fn has_membership_with_prefix(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		prefix: &[u8],
	) -> tg::Result<bool> {
		let mut iter = db
			.prefix_iter(transaction, prefix)
			.map_err(|error| tg::error!(!error, %id, "failed to get object memberships"))?;
		let value = iter
			.next()
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to read the object membership"))?;
		let output = value.is_some();
		Ok(output)
	}

	fn membership_namespace_prefix(id: &tg::object::Id) -> Vec<u8> {
		let id_bytes = id.to_bytes();
		(
			KeyKind::Membership.to_i32().unwrap(),
			id_bytes.as_ref(),
			MembershipKind::Namespace.to_i32().unwrap(),
		)
			.pack_to_vec()
	}

	fn membership_namespace_string_prefix(id: &tg::object::Id, prefix: &str) -> Vec<u8> {
		let mut key = Self::membership_namespace_prefix(id);
		key.extend_from_slice(prefix.as_bytes());
		key
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = Put {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			namespace: arg.namespace,
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
			let request = Put {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				namespace: arg.namespace,
				stored_at: arg.stored_at,
			};
			Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_membership_sync(&self, arg: DeleteMembershipArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let request = DeleteMembership {
			id: arg.id,
			namespace: arg.namespace,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete_membership(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_membership_batch_sync(&self, args: Vec<DeleteMembershipArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		for arg in args {
			let request = DeleteMembership {
				id: arg.id,
				namespace: arg.namespace,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::task_delete_membership(&self.env, &self.db, &mut transaction, request)?;
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
		let request = Delete {
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
			let request = Delete {
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
					Request::Put(request) => {
						Self::task_put_object(env, db, &mut transaction, request)
					},
					Request::PutBatch(requests) => requests.into_iter().try_for_each(|request| {
						Self::task_put_object(env, db, &mut transaction, request)
					}),
					Request::DeleteMembership(request) => {
						Self::task_delete_membership(env, db, &mut transaction, request)
					},
					Request::DeleteMembershipBatch(requests) => {
						requests.into_iter().try_for_each(|request| {
							Self::task_delete_membership(env, db, &mut transaction, request)
						})
					},
					Request::Delete(request) => {
						Self::task_delete_object(env, db, &mut transaction, request)
					},
					Request::DeleteBatch(requests) => {
						requests.into_iter().try_for_each(|request| {
							Self::task_delete_object(env, db, &mut transaction, request)
						})
					},
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

	fn task_put_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Put,
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
			cache_pointer,
			stored_at: request.stored_at,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		let key = Key::Membership(id, request.namespace.as_ref());
		let key_bytes = key.pack_to_vec();
		let value = Membership {
			stored_at: request.stored_at,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to put the object membership"))?;

		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_membership(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: DeleteMembership,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Membership(id, request.namespace.as_ref());
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object membership"))?
		else {
			return Ok(());
		};
		let membership = Membership::deserialize(bytes).map_err(
			|error| tg::error!(!error, %id, "failed to deserialize the object membership"),
		)?;
		if request.now - membership.stored_at < request.ttl.to_i64().unwrap() {
			return Ok(());
		}
		db.delete(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to delete the object membership"))?;
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Delete,
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
		let object = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		if request.now - object.stored_at < request.ttl.to_i64().unwrap() {
			return Ok(());
		}
		db.delete(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to delete the object"))?;
		Ok(())
	}
}

impl crate::Store for Store {
	async fn try_get(
		&self,
		id: &tg::object::Id,
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Option<Object<'static>>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				if !Self::has_membership_with_transaction(
					&db,
					&transaction,
					&id,
					&namespaces,
					public,
				)? {
					return Ok(None);
				}
				let key = Key::Object(&id);
				let Some(raw_bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				else {
					return Ok(None);
				};
				let value = Object::deserialize(raw_bytes)
					.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
				Ok(Some(value))
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
		namespaces: Vec<tg::Namespace>,
		public: bool,
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					if !Self::has_membership_with_transaction(
						&db,
						&transaction,
						id,
						&namespaces,
						public,
					)? {
						outputs.push(None);
						continue;
					}
					let key = Key::Object(id);
					let value = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
						.map(Object::deserialize)
						.transpose()
						.map_err(
							|error| tg::error!(!error, %id, "failed to deserialize the object"),
						)?;
					outputs.push(value);
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
		let request = Request::Put(Put {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			namespace: arg.namespace,
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
				.map(|arg| Put {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					namespace: arg.namespace,
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

	async fn delete_membership(&self, arg: DeleteMembershipArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteMembership(DeleteMembership {
			id: arg.id,
			namespace: arg.namespace,
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

	async fn delete_membership_batch(&self, args: Vec<DeleteMembershipArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteMembershipBatch(
			args.into_iter()
				.map(|arg| DeleteMembership {
					id: arg.id,
					namespace: arg.namespace,
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

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Delete(Delete {
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
				.map(|arg| Delete {
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
				let id = id.to_bytes();
				(KeyKind::Object.to_i32().unwrap(), id.as_ref()).pack(w, tuple_depth)
			},
			Key::Membership(id, None) => {
				let id = id.to_bytes();
				(
					KeyKind::Membership.to_i32().unwrap(),
					id.as_ref(),
					MembershipKind::Public.to_i32().unwrap(),
				)
					.pack(w, tuple_depth)
			},
			Key::Membership(id, Some(namespace)) => {
				let id = id.to_bytes();
				let mut offset = (
					KeyKind::Membership.to_i32().unwrap(),
					id.as_ref(),
					MembershipKind::Namespace.to_i32().unwrap(),
				)
					.pack(w, tuple_depth)?;
				let namespace = namespace.to_string();
				w.write_all(namespace.as_bytes())?;
				offset += fdbt::VersionstampOffset::None {
					size: namespace.len().try_into().unwrap(),
				};
				Ok(offset)
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use {super::*, crate::Store as _};

	fn store() -> (tangram_util::fs::Temp, Store) {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
		};
		let store = Store::new(&config).unwrap();
		(temp, store)
	}

	fn object(content: &'static [u8]) -> (tg::object::Id, Bytes) {
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(content),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		(id, bytes)
	}

	fn namespace(value: &str) -> tg::Namespace {
		value.parse().unwrap()
	}

	fn object_bytes(object: Option<Object<'static>>) -> Option<Cow<'static, [u8]>> {
		object.and_then(|object| object.bytes)
	}

	#[tokio::test]
	async fn test_public_membership() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: None,
				stored_at: 12345,
			})
			.await
			.unwrap();

		let result = store.try_get(&id, vec![], true).await.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));

		let result = store.try_get(&id, vec![], false).await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_public_and_root_are_distinct() {
		let (_temp, store) = store();
		let (public_id, public_bytes) = object(b"public");
		let (root_id, root_bytes) = object(b"root");

		store
			.put(crate::PutArg {
				bytes: Some(public_bytes),
				cache_pointer: None,
				id: public_id.clone(),
				namespace: None,
				stored_at: 12345,
			})
			.await
			.unwrap();
		store
			.put(crate::PutArg {
				bytes: Some(root_bytes),
				cache_pointer: None,
				id: root_id.clone(),
				namespace: Some(tg::Namespace::root()),
				stored_at: 12345,
			})
			.await
			.unwrap();

		assert!(
			store
				.try_get(&public_id, vec![], true)
				.await
				.unwrap()
				.is_some()
		);
		assert!(
			store
				.try_get(&public_id, vec![tg::Namespace::root()], false)
				.await
				.unwrap()
				.is_none()
		);
		assert!(
			store
				.try_get(&root_id, vec![], true)
				.await
				.unwrap()
				.is_none()
		);
		assert!(
			store
				.try_get(&root_id, vec![tg::Namespace::root()], false)
				.await
				.unwrap()
				.is_some()
		);
		assert!(
			store
				.try_get_object_data_sync(&public_id, &[tg::Namespace::root()], false)
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test]
	async fn test_namespace_membership_matches_descendants() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo/bar")),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));

		let result = store
			.try_get(&id, vec![namespace("foo/bar")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));
	}

	#[tokio::test]
	async fn test_namespace_membership_uses_slash_boundaries() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foobar")),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_descendant_namespace_does_not_match_parent_membership() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo/bar")], false)
			.await
			.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_put_object_without_bytes_then_with_bytes() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 12345,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert!(
			result
				.as_ref()
				.and_then(|object| object.bytes.as_ref())
				.is_none()
		);

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 12346,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));
	}

	#[test]
	fn test_put_and_get_object_sync() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put_sync(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 12345,
			})
			.unwrap();

		let result = store.try_get_sync(&id, &[namespace("foo")], false).unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));
	}

	#[tokio::test]
	async fn test_put_batch_and_get_object() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put_batch(vec![crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 12345,
			}])
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));
	}

	#[tokio::test]
	async fn test_multiple_memberships_and_delete() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 100,
			})
			.await
			.unwrap();
		store
			.put(crate::PutArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("bar")),
				stored_at: 100,
			})
			.await
			.unwrap();

		store
			.delete_membership(crate::DeleteMembershipArg {
				id: id.clone(),
				namespace: Some(namespace("foo")),
				now: 200,
				ttl: 100,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert!(result.is_none());

		let result = store
			.try_get(&id, vec![namespace("bar")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));

		store
			.delete_membership(crate::DeleteMembershipArg {
				id: id.clone(),
				namespace: Some(namespace("bar")),
				now: 200,
				ttl: 100,
			})
			.await
			.unwrap();
		assert!(
			store
				.try_get_object_data_sync(&id, &[namespace("bar")], false)
				.unwrap()
				.is_none()
		);

		store
			.delete(crate::DeleteArg {
				id: id.clone(),
				now: 200,
				ttl: 100,
			})
			.await
			.unwrap();
		assert!(
			store
				.try_get_object_data_sync(&id, &[namespace("bar")], false)
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test]
	async fn test_delete_membership_uses_membership_stored_at() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 100,
			})
			.await
			.unwrap();

		store
			.delete_membership(crate::DeleteMembershipArg {
				id: id.clone(),
				namespace: Some(namespace("foo")),
				now: 199,
				ttl: 100,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));

		store
			.delete_membership(crate::DeleteMembershipArg {
				id: id.clone(),
				namespace: Some(namespace("foo")),
				now: 200,
				ttl: 100,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert!(result.is_none());
		assert!(
			store
				.try_get_object_data_sync(&id, &[namespace("foo")], false)
				.unwrap()
				.is_none()
		);
	}

	#[tokio::test]
	async fn test_delete_uses_object_stored_at() {
		let (_temp, store) = store();
		let (id, bytes) = object(b"hello world");

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				namespace: Some(namespace("foo")),
				stored_at: 100,
			})
			.await
			.unwrap();

		store
			.delete(crate::DeleteArg {
				id: id.clone(),
				now: 199,
				ttl: 100,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert_eq!(object_bytes(result), Some(Cow::Owned(bytes.to_vec())));

		store
			.delete(crate::DeleteArg {
				id: id.clone(),
				now: 200,
				ttl: 100,
			})
			.await
			.unwrap();

		let result = store
			.try_get(&id, vec![namespace("foo")], false)
			.await
			.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_try_get_batch_filters_by_membership() {
		let (_temp, store) = store();
		let (visible_id, visible_bytes) = object(b"visible");
		let (hidden_id, hidden_bytes) = object(b"hidden");

		store
			.put_batch(vec![
				crate::PutArg {
					bytes: Some(visible_bytes.clone()),
					cache_pointer: None,
					id: visible_id.clone(),
					namespace: Some(namespace("foo")),
					stored_at: 100,
				},
				crate::PutArg {
					bytes: Some(hidden_bytes),
					cache_pointer: None,
					id: hidden_id.clone(),
					namespace: Some(namespace("bar")),
					stored_at: 100,
				},
			])
			.await
			.unwrap();

		let result = store
			.try_get_batch(
				&[visible_id.clone(), hidden_id],
				vec![namespace("foo")],
				false,
			)
			.await
			.unwrap();
		assert_eq!(
			object_bytes(result[0].clone()),
			Some(Cow::Owned(visible_bytes.to_vec()))
		);
		assert!(result[1].is_none());
	}
}
