use {bytes::Bytes, tangram_client as tg, tangram_store as store};

pub use store::{CacheReference, DeleteArg, PutArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "foundationdb")]
	Fdb(store::fdb::Store),
	Lmdb(store::lmdb::Store),
	Memory(store::memory::Store),
	S3(store::s3::Store),
	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Store),
}

#[derive(Debug, derive_more::Display, derive_more::From, derive_more::Error)]
pub enum Error {
	#[cfg(feature = "foundationdb")]
	Fdb(store::fdb::Error),
	Lmdb(store::lmdb::Error),
	Memory(store::memory::Error),
	S3(store::s3::Error),
	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(config: &crate::config::FdbStore) -> Result<Self, Error> {
		let config = store::fdb::Config {
			path: config.path.clone(),
		};
		let fdb = store::fdb::Store::new(&config)?;
		Ok(Self::Fdb(fdb))
	}

	pub fn new_lmdb(config: &crate::config::LmdbStore) -> Result<Self, Error> {
		let config = store::lmdb::Config {
			path: config.path.clone(),
		};
		let lmdb = store::lmdb::Store::new(&config)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(store::memory::Store::new())
	}

	pub fn new_s3(config: &crate::config::S3Store) -> Self {
		let config = store::s3::Config {
			access_key: config.access_key.clone(),
			bucket: config.bucket.clone(),
			region: config.region.clone(),
			secret_key: config.secret_key.clone(),
			url: config.url.clone(),
		};
		Self::S3(store::s3::Store::new(&config))
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaStore) -> Result<Self, Error> {
		let config = store::scylla::Config {
			addr: config.addr.clone(),
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			username: config.username.clone(),
		};
		let scylla = store::scylla::Store::new(&config)
			.await
			.map_err(Error::Scylla)?;
		Ok(Self::Scylla(scylla))
	}
}

impl store::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get(id).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.try_get(id).await.map_err(Error::Lmdb),
			Self::Memory(memory) => store::Store::try_get(memory, id)
				.await
				.map_err(Error::Memory),
			Self::S3(s3) => s3.try_get(id).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get(id).await.map_err(Error::Scylla),
		}
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_batch(ids).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.try_get_batch(ids).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.try_get_batch(ids).await.map_err(Error::Memory),
			Self::S3(s3) => s3.try_get_batch(ids).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_batch(ids).await.map_err(Error::Scylla),
		}
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_cache_reference(id).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.try_get_cache_reference(id).await.map_err(Error::Lmdb),
			Self::Memory(memory) => store::Store::try_get_cache_reference(memory, id)
				.await
				.map_err(Error::Memory),
			Self::S3(s3) => s3.try_get_cache_reference(id).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.try_get_cache_reference(id)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.put(arg).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.put(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.put(arg).await.map_err(Error::Memory),
			Self::S3(s3) => s3.put(arg).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put(arg).await.map_err(Error::Scylla),
		}
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.put_batch(args).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.put_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.put_batch(args).await.map_err(Error::Memory),
			Self::S3(s3) => s3.put_batch(args).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_batch(args).await.map_err(Error::Scylla),
		}
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.delete(arg).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.delete(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.delete(arg).await.map_err(Error::Memory),
			Self::S3(s3) => s3.delete(arg).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete(arg).await.map_err(Error::Scylla),
		}
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.delete_batch(args).await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.delete_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.delete_batch(args).await.map_err(Error::Memory),
			Self::S3(s3) => s3.delete_batch(args).await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_batch(args).await.map_err(Error::Scylla),
		}
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.flush().await.map_err(Error::Fdb),
			Self::Lmdb(lmdb) => lmdb.flush().await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory.flush().await.map_err(Error::Memory),
			Self::S3(s3) => s3.flush().await.map_err(Error::S3),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.flush().await.map_err(Error::Scylla),
		}
	}
}

impl store::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}
