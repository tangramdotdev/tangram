use {
	crate::{CachePointer, DeleteObjectArg, Object, PutObjectArg},
	bytes::Bytes,
	foundationdb_tuple::TuplePack as _,
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
	Object(ObjectRequest),
}

enum ObjectRequest {
	Put(PutObject),
	PutBatch(Vec<PutObject>),
	Delete(DeleteObject),
	DeleteBatch(Vec<DeleteObject>),
}

struct PutObject {
	bytes: Option<Bytes>,
	cache_pointer: Option<CachePointer>,
	id: tg::object::Id,
	touched_at: i64,
}

struct DeleteObject {
	id: tg::object::Id,
	now: i64,
	ttl: u64,
}

#[derive(Debug)]
enum Key<'a> {
	Object(&'a tg::object::Id),
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
				|source| tg::error!(!source, path = %config.path.display(), "failed to open the lmdb file"),
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
				.map_err(|source| {
					tg::error!(!source, path = %config.path.display(), "failed to open the lmdb environment")
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;
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

	#[must_use]
	pub fn db(&self) -> Db {
		self.db
	}

	#[must_use]
	pub fn env(&self) -> &lmdb::Env {
		&self.env
	}

	pub fn try_get_object_sync(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		self.try_get_object_with_transaction(&transaction, id)
	}

	pub fn try_get_object_batch_sync(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Object<'static>>>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Object(id);
			let value = self
				.db
				.get(&transaction, &key.pack_to_vec())
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				.and_then(|bytes| Object::deserialize(bytes).ok());
			outputs.push(value);
		}
		Ok(outputs)
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		self.try_get_object_data_with_transaction(&transaction, id)
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
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	pub fn try_get_object_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let key = Key::Object(id);
		let Some(raw_bytes) = self
			.db
			.get(transaction, &key.pack_to_vec())
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(raw_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
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
					Request::Object(ObjectRequest::Put(request)) => {
						let result = Self::task_put_object(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::Object(ObjectRequest::PutBatch(requests)) => {
						for request in requests {
							let result = Self::task_put_object(env, db, &mut transaction, request);
							responses.push(result);
						}
					},
					Request::Object(ObjectRequest::Delete(request)) => {
						let result = Self::task_delete_object(env, db, &mut transaction, request);
						responses.push(result);
					},
					Request::Object(ObjectRequest::DeleteBatch(requests)) => {
						for request in requests {
							let result =
								Self::task_delete_object(env, db, &mut transaction, request);
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

	fn task_put_object(
		_env: &lmdb::Env,
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: PutObject,
	) -> tg::Result<()> {
		let id = &request.id;
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();

		// Read existing value if any.
		let existing = db
			.get(transaction, &key_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
			.and_then(|bytes| Object::deserialize(bytes).ok());

		// Determine bytes: use existing if present (NO_OVERWRITE semantics), otherwise use request.
		let bytes = existing
			.as_ref()
			.and_then(|entry| entry.bytes.clone())
			.or(request.bytes.map(|bytes| Cow::Owned(bytes.to_vec())));

		// Merge cache_pointer: use request if provided, otherwise keep existing.
		let cache_pointer = request
			.cache_pointer
			.or_else(|| existing.and_then(|entry| entry.cache_pointer));

		let value = Object {
			bytes,
			touched_at: request.touched_at,
			cache_pointer,
		};
		let value_bytes = value.serialize().unwrap();
		db.put(transaction, &key_bytes, &value_bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;
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
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
		else {
			return Ok(());
		};
		let value = Object::deserialize(bytes)
			.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;

		if request.now - value.touched_at >= request.ttl.to_i64().unwrap() {
			db.delete(transaction, &key_bytes)
				.map_err(|source| tg::error!(!source, %id, "failed to delete the object"))?;
		}
		Ok(())
	}

	pub fn put_object_sync(&self, arg: PutObjectArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		};
		Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn put_object_batch_sync(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		for arg in args {
			let request = PutObject {
				bytes: arg.bytes,
				cache_pointer: arg.cache_pointer,
				id: arg.id,
				touched_at: arg.touched_at,
			};
			Self::task_put_object(&self.env, &self.db, &mut transaction, request)?;
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_sync(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let request = DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		};
		Self::task_delete_object(&self.env, &self.db, &mut transaction, request)?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(())
	}

	pub fn delete_object_batch_sync(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut transaction = self
			.env
			.write_txn()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
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

impl crate::Store for Store {
	async fn try_get_object(&self, id: &tg::object::Id) -> tg::Result<Option<Object<'static>>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let id = id.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let key = Key::Object(&id);
				let Some(raw_bytes) = db
					.get(&transaction, &key.pack_to_vec())
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
				else {
					return Ok(None);
				};
				let value = Object::deserialize(raw_bytes).map_err(
					|source| tg::error!(!source, %id, "failed to deserialize the object"),
				)?;
				Ok(Some(value))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
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
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let key = Key::Object(id);
					let value = db
						.get(&transaction, &key.pack_to_vec())
						.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?
						.and_then(|bytes| Object::deserialize(bytes).ok());
					outputs.push(value);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	async fn put_object(&self, arg: PutObjectArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::Put(PutObject {
			bytes: arg.bytes,
			cache_pointer: arg.cache_pointer,
			id: arg.id,
			touched_at: arg.touched_at,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::PutBatch(
			args.into_iter()
				.map(|arg| PutObject {
					bytes: arg.bytes,
					cache_pointer: arg.cache_pointer,
					id: arg.id,
					touched_at: arg.touched_at,
				})
				.collect(),
		));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		let id = arg.id.clone();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::Delete(DeleteObject {
			id: arg.id,
			now: arg.now,
			ttl: arg.ttl,
		}));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!(%id, "the task panicked"))?
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Object(ObjectRequest::DeleteBatch(
			args.into_iter()
				.map(|arg| DeleteObject {
					id: arg.id,
					now: arg.now,
					ttl: arg.ttl,
				})
				.collect(),
		));
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn flush(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}
}

impl foundationdb_tuple::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: foundationdb_tuple::TupleDepth,
	) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
		match self {
			Key::Object(id) => id.to_bytes().as_ref().pack(w, tuple_depth),
		}
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
			.put_object(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.await
			.unwrap();

		// Get the object.
		let result = store.try_get_object(&id).await.unwrap();
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
			.put_object(crate::PutObjectArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.await
			.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let result = store.try_get_object(&id).await.unwrap();
		assert!(
			result.is_none()
				|| result
					.as_ref()
					.and_then(|object| object.bytes.as_ref())
					.is_none()
		);

		// Put with bytes.
		store
			.put_object(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12346,
			})
			.await
			.unwrap();

		// Verify object now exists.
		let result = store.try_get_object(&id).await.unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	#[test]
	fn test_put_and_get_object_sync() {
		// This test mimics what the server does using sync functions.
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = Config {
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
			.put_object_sync(crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			})
			.unwrap();

		// Get the object using sync function.
		let result = store.try_get_object_sync(&id).unwrap();
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
			.put_object_batch(vec![crate::PutObjectArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				touched_at: 12345,
			}])
			.await
			.unwrap();

		let result = store.try_get_object(&id).await.unwrap();
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}
}
