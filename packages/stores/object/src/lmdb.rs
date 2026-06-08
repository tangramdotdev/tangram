use {
	crate::{DeleteArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod put;
mod task;

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
	Delete(self::delete::Request),
	DeleteBatch(Vec<self::delete::Request>),
	Put(self::put::Request),
	PutBatch(Vec<self::put::Request>),
}

#[derive(Debug)]
enum Key<'a> {
	Object(&'a tg::object::Id),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum KeyKind {
	Object = 0,
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
		let mut flags =
			lmdb::EnvFlags::NO_SUB_DIR | lmdb::EnvFlags::WRITE_MAP | lmdb::EnvFlags::MAP_ASYNC;
		if std::env::var_os("TANGRAM_MACOS_APP_SOCKET").is_some() {
			flags |= lmdb::EnvFlags::NO_LOCK;
		}
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(flags)
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
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		self.try_get(arg).await
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		self.try_get_batch(arg).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		self.put_batch(args).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg).await
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		self.delete_batch(args).await
	}

	async fn flush(&self) -> tg::Result<()> {
		self.flush().await
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
		}
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::borrow::Cow};

	// An object put with bytes can be retrieved with the same bytes.
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
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Get the object.
		let arg = crate::TryGetArg { id: id.clone() };
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object first put without bytes stores no bytes and a later put with bytes makes the bytes retrievable.
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
			.put(crate::PutArg {
				bytes: None,
				cache_pointer: None,
				id: id.clone(),
				stored_at: 12345,
			})
			.await
			.unwrap();

		// Verify object bytes do not exist (object may exist with bytes=None).
		let arg = crate::TryGetArg { id: id.clone() };
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
				stored_at: 12346,
			})
			.await
			.unwrap();

		// Verify object now exists.
		let arg = crate::TryGetArg { id: id.clone() };
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object put and retrieved through the synchronous functions, as the server uses them, round-trips the bytes.
	#[tokio::test]
	async fn test_put_and_get_object_sync() {
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
			.put_sync(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				stored_at: 12345,
			})
			.unwrap();

		// Get the object using sync function.
		let arg = crate::TryGetArg { id: id.clone() };
		let result = store.try_get_sync(&arg).unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// An object put through the batch function can be retrieved with the same bytes.
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
			.put_batch(vec![crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				stored_at: 12345,
			}])
			.await
			.unwrap();

		let arg = crate::TryGetArg { id: id.clone() };
		let result = store.try_get(arg).await.unwrap().object;
		assert_eq!(
			result.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
	}

	// Deleting an object removes the object.
	#[tokio::test]
	async fn test_delete_removes_object() {
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
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				stored_at: 10,
			})
			.await
			.unwrap();

		let output = store
			.try_get(crate::TryGetArg { id: id.clone() })
			.await
			.unwrap();
		assert_eq!(
			output.object.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);

		store
			.delete(crate::DeleteArg {
				id: id.clone(),
				now: 16,
				ttl: 5,
			})
			.await
			.unwrap();

		let output = store.try_get(crate::TryGetArg { id }).await.unwrap();
		assert!(output.object.is_none());
	}
}
