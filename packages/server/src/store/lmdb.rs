use {
	super::{CacheReference, DeleteArg, PutArg},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client as tg,
};

pub struct Lmdb {
	db: Db,
	env: lmdb::Env,
	sender: RequestSender,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

enum Request {
	Put(Put),
	PutBatch(Vec<Put>),
	Delete(Delete),
	DeleteBatch(Vec<Delete>),
}

struct Put {
	bytes: Option<Bytes>,
	cache_reference: Option<CacheReference>,
	id: tg::object::Id,
	touched_at: i64,
}

struct Delete {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

impl Lmdb {
	pub fn new(config: &crate::config::LmdbStore) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(|source| tg::error!(!source, "failed to create or open the database file"))?;
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(1_099_511_627_776)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(lmdb::EnvFlags::NO_SUB_DIR)
				.open(&config.path)
				.map_err(|source| tg::error!(!source, "failed to open the database"))?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to open the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Create the thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});

		Ok(Self { db, env, sender })
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, tg::Error> {
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = id.to_bytes();
				let key = (0, id.as_ref(), 0);
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the value"))?
				else {
					return Ok(None);
				};
				let bytes = Bytes::copy_from_slice(bytes);
				Ok::<_, tg::Error>(Some(bytes))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		Ok(bytes)
	}

	pub async fn try_get_batch(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in ids {
					let id = id.to_bytes();
					let key = (0, id.as_ref(), 0);
					let bytes = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, "failed to get the value"))?
						.map(Bytes::copy_from_slice);
					outputs.push(bytes);
				}
				Ok::<_, tg::Error>(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to get the objects"))?;
		Ok(bytes)
	}

	pub fn try_get_sync(&self, id: &tg::object::Id) -> Result<Option<Bytes>, tg::Error> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id = id.to_bytes();
		let key = (0, id.as_ref(), 0);
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the value"))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(bytes);
		Ok::<_, tg::Error>(Some(bytes))
	}

	#[allow(dead_code)]
	pub fn try_get_batch_sync(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let id = id.to_bytes();
			let key = (0, id.as_ref(), 0);
			let bytes = self
				.db
				.get(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to get the value"))?
				.map(Bytes::copy_from_slice);
			outputs.push(bytes);
		}
		Ok::<_, tg::Error>(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Data>> {
		let transaction = self.env.read_txn().unwrap();
		let kind = id.kind();
		let id = id.to_bytes();
		let key = (0, id.as_ref(), 0);
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the value"))?
		else {
			return Ok(None);
		};
		let data = tg::object::Data::deserialize(kind, bytes)?;
		Ok(Some(data))
	}

	pub async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, tg::Error> {
		let reference = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id = id.to_bytes();
				let key = (0, id.as_ref(), 2);
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, "failed to get the value"))?
				else {
					return Ok(None);
				};
				let reference = CacheReference::deserialize(bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize the reference"))?;
				Ok::<_, tg::Error>(Some(reference))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		Ok(reference)
	}

	pub fn try_get_cache_reference_sync(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, tg::Error> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id = id.to_bytes();
		let key = (0, id.as_ref(), 2);
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the value"))?
		else {
			return Ok(None);
		};
		let reference = CacheReference::deserialize(bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the reference"))?;
		Ok::<_, tg::Error>(Some(reference))
	}

	pub async fn put(&self, arg: super::PutArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(Put {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			id: arg.id,
			touched_at: arg.touched_at,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutBatch(
			args.into_iter()
				.map(|arg| Put {
					bytes: arg.bytes,
					cache_reference: arg.cache_reference,
					id: arg.id,
					touched_at: arg.touched_at,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
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
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Delete(Delete {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))??;
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
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"));
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
				match request {
					Request::Put(request) => {
						let result = Self::task_put(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::PutBatch(requests) => {
						for request in requests {
							let result = Self::task_put(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
					Request::Delete(request) => {
						let result = Self::task_delete(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::DeleteBatch(requests) => {
						for request in requests {
							let result = Self::task_delete(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
				}
			}
			let result = transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"));
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

	fn task_put(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Put,
	) -> tg::Result<()> {
		if let Some(bytes) = request.bytes {
			let id = request.id.to_bytes();
			let key = (0, id.as_ref(), 0);
			let flags = lmdb::PutFlags::NO_OVERWRITE;
			let result = db.put_with_flags(transaction, flags, &key.pack_to_vec(), &bytes);
			match result {
				Ok(()) | Err(lmdb::Error::Mdb(lmdb::MdbError::KeyExist)) => (),
				Err(error) => {
					return Err(tg::error!(!error, "failed to put the value"));
				},
			}
		}
		let id = request.id.to_bytes();
		let key = (0, id.as_ref(), 1);
		let touched_at = request.touched_at.to_le_bytes();
		db.put(transaction, &key.pack_to_vec(), &touched_at)
			.map_err(|source| tg::error!(!source, "failed to put the value"))?;
		if let Some(cache_reference) = request.cache_reference {
			let id = request.id.to_bytes();
			let key = (0, id.as_ref(), 2);
			let value = cache_reference.serialize().unwrap();
			db.put(transaction, &key.pack_to_vec(), &value)
				.map_err(|source| tg::error!(!source, "failed to put the value"))?;
		}
		Ok(())
	}

	#[allow(clippy::needless_pass_by_value)]
	fn task_delete(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Delete,
	) -> tg::Result<()> {
		let id = request.id.to_bytes();
		let key = (0, id.as_ref(), 1);
		let Some(touched_at) = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, "failed to get the touch time"))?
		else {
			return Ok(());
		};
		let touched_at = touched_at
			.try_into()
			.map_err(|source| tg::error!(!source, "invalid touch time"))?;
		let touched_at = i64::from_le_bytes(touched_at);
		if request.now - touched_at >= request.ttl.to_i64().unwrap() {
			let id = request.id.to_bytes();
			let key = (0, id.as_ref(), 0);
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the object"))?;
			let id = request.id.to_bytes();
			let key = (0, id.as_ref(), 1);
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the object"))?;
			let id = request.id.to_bytes();
			let key = (0, id.as_ref(), 2);
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, "failed to delete the object"))?;
		}
		Ok(())
	}
}
