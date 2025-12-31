use {
	crate::{CachePointer, DeleteArg, Error as _, PutArg},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
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

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;
type _ResponseReceiver = tokio::sync::oneshot::Receiver<tg::Result<()>>;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Lmdb(lmdb::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

enum Request {
	Put(Put),
	PutBatch(Vec<Put>),
	Delete(Delete),
	DeleteBatch(Vec<Delete>),
}

struct Put {
	bytes: Option<Bytes>,
	cache_pointer: Option<CachePointer>,
	id: tg::object::Id,
	touched_at: i64,
}

struct Delete {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

struct Key<'a> {
	id: &'a [u8],
	kind: KeyKind,
}

enum KeyKind {
	Bytes,
	TouchedAt,
	CachePointer,
}

impl Store {
	pub fn new(config: &Config) -> Result<Self, Error> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(Error::other)?;
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
				.map_err(|source| {
					Error::other(
						tg::error!(!source, path = %config.path.display(), "failed to open the lmdb environment"),
					)
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| Error::other(tg::error!(!source, "failed to create the database")))?;
		transaction.commit().map_err(|source| {
			Error::other(tg::error!(!source, "failed to commit the transaction"))
		})?;

		// Create the thread.
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		std::thread::spawn({
			let env = env.clone();
			move || Self::task(&env, &db, receiver)
		});

		Ok(Self { db, env, sender })
	}

	pub fn try_get_sync(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::Bytes,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(bytes);
		Ok::<_, tg::Error>(Some(bytes))
	}

	pub fn try_get_batch_sync(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			let bytes = self
				.db
				.get(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.map(Bytes::copy_from_slice);
			outputs.push(bytes);
		}
		Ok::<_, tg::Error>(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self.env.read_txn().unwrap();
		let kind = id.kind();
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::Bytes,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		Ok(Some((size, data)))
	}

	pub fn try_get_cache_pointer_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<CachePointer>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::CachePointer,
		};
		let Some(bytes) = self
			.db
			.get(&transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the cache pointer"))?
		else {
			return Ok(None);
		};
		let pointer = CachePointer::deserialize(bytes).map_err(
			|source| tg::error!(!source, %id, "failed to deserialize the cache pointer"),
		)?;
		Ok::<_, tg::Error>(Some(pointer))
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
		let id = &request.id;
		if let Some(bytes) = request.bytes {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			let flags = lmdb::PutFlags::NO_OVERWRITE;
			let result = db.put_with_flags(transaction, flags, &key.pack_to_vec(), &bytes);
			match result {
				Ok(()) | Err(lmdb::Error::Mdb(lmdb::MdbError::KeyExist)) => (),
				Err(error) => {
					return Err(tg::error!(!error, %id, "failed to put the object"));
				},
			}
		}
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::TouchedAt,
		};
		let touched_at = request.touched_at.to_le_bytes();
		db.put(transaction, &key.pack_to_vec(), &touched_at)
			.map_err(|source| tg::error!(!source, %id, "failed to put the touched_at"))?;
		if let Some(cache_pointer) = request.cache_pointer {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::CachePointer,
			};
			let value = cache_pointer.serialize().unwrap();
			db.put(transaction, &key.pack_to_vec(), &value)
				.map_err(|source| tg::error!(!source, %id, "failed to put the cache pointer"))?;
		}
		Ok(())
	}

	#[expect(clippy::needless_pass_by_value)]
	fn task_delete(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Delete,
	) -> tg::Result<()> {
		let id = &request.id;
		let id_bytes = id.to_bytes();
		let key = Key {
			id: id_bytes.as_ref(),
			kind: KeyKind::TouchedAt,
		};
		let Some(touched_at) = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the touched_at"))?
		else {
			return Ok(());
		};
		let touched_at = touched_at
			.try_into()
			.map_err(|source| tg::error!(!source, %id, "invalid touched_at"))?;
		let touched_at = i64::from_le_bytes(touched_at);
		if request.now - touched_at >= request.ttl.to_i64().unwrap() {
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::Bytes,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to delete the object"))?;
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::TouchedAt,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to delete the touched_at"))?;
			let id_bytes = id.to_bytes();
			let key = Key {
				id: id_bytes.as_ref(),
				kind: KeyKind::CachePointer,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to delete the cache pointer"))?;
		}
		Ok(())
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = Put {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		};
		Self::task_put(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = Put {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				touched_at: arg.touched_at,
			};
			Self::task_put(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = Delete {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = Delete {
				id: arg.id,
				now: arg.now,
				ttl: arg.ttl,
			};
			Self::task_delete(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn flush_sync(&self) -> tg::Result<()> {
		self.env
			.force_sync()
			.map_err(|source| tg::error!(!source, "failed to sync"))?;
		Ok(())
	}
}

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		let bytes = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id_bytes = id.to_bytes();
				let key = Key {
					id: id_bytes.as_ref(),
					kind: KeyKind::Bytes,
				};
				let Some(bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				else {
					return Ok(None);
				};
				let bytes = Bytes::copy_from_slice(bytes);
				Ok::<_, tg::Error>(Some(bytes))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(bytes)
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
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
					let id_bytes = id.to_bytes();
					let key = Key {
						id: id_bytes.as_ref(),
						kind: KeyKind::Bytes,
					};
					let bytes = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
						.map(Bytes::copy_from_slice);
					outputs.push(bytes);
				}
				Ok::<_, tg::Error>(outputs)
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(bytes)
	}

	async fn try_get_cache_pointer(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CachePointer>, Self::Error> {
		let pointer = tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let id_bytes = id.to_bytes();
				let key = Key {
					id: id_bytes.as_ref(),
					kind: KeyKind::CachePointer,
				};
				let Some(bytes) = db.get(&transaction, &key.pack_to_vec()).map_err(
					|source| tg::error!(!source, %id, "failed to get the cache pointer"),
				)?
				else {
					return Ok(None);
				};
				let pointer = CachePointer::deserialize(bytes).map_err(
					|source| tg::error!(!source, %id, "failed to deserialize the cache pointer"),
				)?;
				Ok::<_, tg::Error>(Some(pointer))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(pointer)
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(Put {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		});
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to send the request"))
			})?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!(%id, "the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
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
					touched_at: arg.touched_at,
				})
				.collect(),
		);
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to send the request")))?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!("the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
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
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to send the request"))
			})?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!(%id, "the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
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
			.map_err(|source| Error::other(tg::error!(!source, "failed to send the request")))?;
		receiver
			.await
			.map_err(|_| Error::other(tg::error!("the task panicked")))?
			.map_err(Error::other)?;
		Ok(())
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| Error::other(tg::error!(!source, "failed to join the task")))?
		.map_err(Error::other)?;
		Ok(())
	}
}

impl foundationdb_tuple::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: foundationdb_tuple::TupleDepth,
	) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
		let column = match self.kind {
			KeyKind::Bytes => 0,
			KeyKind::TouchedAt => 1,
			KeyKind::CachePointer => 2,
		};
		(0, self.id, column).pack(w, tuple_depth)
	}
}
