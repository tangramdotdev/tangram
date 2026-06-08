use {
	crate::{DeleteArg, GrantArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput},
	foundationdb_tuple as fdbt, heed as lmdb,
	num::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

mod delete;
mod flush;
mod get;
mod grant;
mod put;
mod task;

#[derive(Clone, Debug)]
pub struct Config {
	pub grant_ttl: u64,
	pub map_size: usize,
	pub path: std::path::PathBuf,
}

pub struct Store {
	db: Db,
	env: lmdb::Env,
	#[expect(dead_code)]
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
	CleanGrants(self::grant::CleanRequest),
	Delete(self::delete::Request),
	DeleteBatch(Vec<self::delete::Request>),
	Grant(self::grant::Request),
	GrantBatch(Vec<self::grant::Request>),
	Put(self::put::Request),
	PutBatch(Vec<self::put::Request>),
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
}

impl crate::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		Store::try_get(self, arg).await
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		Store::try_get_batch(self, arg).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		Store::put(self, arg).await
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		Store::put_batch(self, args).await
	}

	async fn grant(&self, arg: GrantArg) -> tg::Result<()> {
		Store::grant(self, arg).await
	}

	async fn grant_batch(&self, args: Vec<GrantArg>) -> tg::Result<()> {
		Store::grant_batch(self, args).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		Store::delete(self, arg).await
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		Store::delete_batch(self, args).await
	}

	async fn flush(&self) -> tg::Result<()> {
		Store::flush(self).await
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
	fn unpack_object_grant(bytes: &[u8]) -> tg::Result<(tg::object::Id, String)> {
		let (kind, id, principal): (i32, Vec<u8>, String) = fdbt::Subspace::all()
			.unpack(bytes)
			.map_err(|error| tg::error!(!error, "failed to unpack the object grant key"))?;
		let kind = KeyKind::from_i32(kind).ok_or_else(|| tg::error!("invalid key kind"))?;
		if kind != KeyKind::ObjectGrant {
			return Err(tg::error!("unexpected object grant key"));
		}
		let id = tg::object::Id::from_slice(&id)
			.map_err(|error| tg::error!(!error, "failed to parse the object id"))?;
		Ok((id, principal))
	}

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
	use {super::*, bytes::Bytes, std::borrow::Cow};

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
		let principal = tg::Principal::Root;

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
		let principal = tg::Principal::Root;

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
				principal: Some(tg::Principal::Root),
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
		let principal = tg::Principal::Root;

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

	#[tokio::test]
	async fn test_delete_removes_object_grants() {
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
		let principal = tg::Principal::Root;

		store
			.put(crate::PutArg {
				bytes: Some(bytes.clone()),
				cache_pointer: None,
				id: id.clone(),
				principal: Some(principal.clone()),
				stored_at: 10,
			})
			.await
			.unwrap();

		let output = store
			.try_get(crate::TryGetArg {
				id: id.clone(),
				now: 11,
				principal: principal.clone(),
			})
			.await
			.unwrap();
		assert_eq!(
			output.object.and_then(|object| object.bytes),
			Some(Cow::Owned(bytes.to_vec()))
		);
		assert!(!output.grants.is_empty());

		store
			.delete(crate::DeleteArg {
				id: id.clone(),
				now: 16,
				ttl: 5,
			})
			.await
			.unwrap();

		let output = store
			.try_get(crate::TryGetArg {
				id,
				now: 17,
				principal,
			})
			.await
			.unwrap();
		assert!(output.object.is_none());
		assert!(output.grants.is_empty());
	}
}
