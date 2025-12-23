use {bytes::Bytes, std::path::Path, tangram_client::prelude::*, tangram_store as store};

pub use store::{CacheReference, DeleteArg, PutArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	Lmdb(store::lmdb::Store),
	Memory(store::memory::Store),
	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Store),
}

#[derive(Debug, derive_more::Display, derive_more::From, derive_more::Error)]
pub enum Error {
	Lmdb(store::lmdb::Error),
	Memory(store::memory::Error),
	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbStore) -> Result<Self, Error> {
		let config = store::lmdb::Config {
			map_size: config.map_size,
			path: directory.join(&config.path),
		};
		let lmdb = store::lmdb::Store::new(&config)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaStore) -> Result<Self, Error> {
		let config = store::scylla::Config {
			addr: config.addr.clone(),
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			speculative_execution: config.speculative_execution.as_ref().map(|se| match se {
				crate::config::ScyllaStoreSpeculativeExecution::Percentile(p) => {
					store::scylla::SpeculativeExecution::Percentile {
						max_retry_count: p.max_retry_count,
						percentile: p.percentile,
					}
				},
				crate::config::ScyllaStoreSpeculativeExecution::Simple(s) => {
					store::scylla::SpeculativeExecution::Simple {
						max_retry_count: s.max_retry_count,
						retry_interval: std::time::Duration::from_millis(s.retry_interval),
					}
				},
			}),
			username: config.username.clone(),
		};
		let scylla = store::scylla::Store::new(&config)
			.await
			.map_err(Error::Scylla)?;
		Ok(Self::Scylla(scylla))
	}

	pub fn try_get_sync(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		if let Store::Lmdb(store) = self {
			store.try_get_sync(id)
		} else if let Store::Memory(store) = self {
			Ok(store.try_get(id))
		} else {
			Err(tg::error!("unimplemented"))
		}
	}

	#[expect(dead_code)]
	pub fn try_get_batch_sync(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		if let Store::Lmdb(store) = self {
			store.try_get_batch_sync(ids)
		} else if let Store::Memory(store) = self {
			Ok(store.try_get_batch(ids))
		} else {
			Err(tg::error!("unimplemented"))
		}
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		if let Store::Lmdb(store) = self {
			store.try_get_object_data_sync(id)
		} else if let Store::Memory(store) = self {
			store.try_get_object_data(id)
		} else {
			Err(tg::error!("unimplemented"))
		}
	}

	pub fn try_get_cache_reference_sync(
		&self,
		id: &tg::blob::Id,
	) -> tg::Result<Option<CacheReference>> {
		if let crate::store::Store::Lmdb(lmdb) = self {
			lmdb.try_get_cache_reference_sync(&id.clone().into())
		} else if let crate::store::Store::Memory(memory) = self {
			Ok(memory.try_get_cache_reference(&id.clone().into()))
		} else {
			Err(tg::error!("invalid store"))
		}
	}

	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		if let Store::Lmdb(store) = self {
			store.put_sync(arg)?;
		} else if let Store::Memory(store) = self {
			store.put(arg);
		} else {
			return Err(tg::error!("unimplemented"));
		}
		Ok(())
	}

	#[expect(dead_code)]
	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if let Store::Lmdb(store) = self {
			store.put_batch_sync(args)?;
		} else if let Store::Memory(store) = self {
			store.put_batch(args);
		} else {
			return Err(tg::error!("unimplemented"));
		}
		Ok(())
	}

	#[expect(dead_code)]
	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		if let Store::Lmdb(store) = self {
			store.delete_sync(arg)?;
		} else if let Store::Memory(store) = self {
			store.delete(arg);
		} else {
			return Err(tg::error!("unimplemented"));
		}
		Ok(())
	}

	#[expect(dead_code)]
	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if let Store::Lmdb(store) = self {
			store.delete_batch_sync(args)?;
		} else if let Store::Memory(store) = self {
			store.delete_batch(args);
		} else {
			return Err(tg::error!("unimplemented"));
		}
		Ok(())
	}

	#[expect(dead_code)]
	pub fn flush_sync(&self) -> tg::Result<()> {
		if let Store::Lmdb(store) = self {
			store.flush_sync()?;
		} else if let Store::Memory(store) = self {
			store.flush();
		} else {
			return Err(tg::error!("unimplemented"));
		}
		Ok(())
	}
}

impl store::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.try_get(id).await.map_err(Error::Lmdb),
			Self::Memory(memory) => store::Store::try_get(memory, id)
				.await
				.map_err(Error::Memory),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get(id).await.map_err(Error::Scylla),
		}
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.try_get_batch(ids).await.map_err(Error::Lmdb),
			Self::Memory(memory) => Ok(memory.try_get_batch(ids)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_batch(ids).await.map_err(Error::Scylla),
		}
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.try_get_cache_reference(id).await.map_err(Error::Lmdb),
			Self::Memory(memory) => store::Store::try_get_cache_reference(memory, id)
				.await
				.map_err(Error::Memory),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.try_get_cache_reference(id)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.put(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.put(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put(arg).await.map_err(Error::Scylla),
		}
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.put_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.put_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_batch(args).await.map_err(Error::Scylla),
		}
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.delete(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.delete(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete(arg).await.map_err(Error::Scylla),
		}
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.delete_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.delete_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_batch(args).await.map_err(Error::Scylla),
		}
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		match self {
			Self::Lmdb(lmdb) => lmdb.flush().await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.flush();
				Ok(())
			},
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
