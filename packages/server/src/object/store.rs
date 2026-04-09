#[cfg(feature = "lmdb")]
use std::path::Path;
use {tangram_client::prelude::*, tangram_object_store as object_store};

pub use object_store::{CachePointer, DeleteObjectArg, PutObjectArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "lmdb")]
	Lmdb(object_store::lmdb::Store),

	Memory(object_store::memory::Store),

	#[cfg(feature = "scylla")]
	Scylla(object_store::scylla::Store),
}

impl Store {
	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::ObjectLmdbStore) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = object_store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
		};
		let lmdb = object_store::lmdb::Store::new(&config).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the lmdb store"),
		)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(object_store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ObjectScyllaStore) -> tg::Result<Self> {
		let config = object_store::scylla::Config {
			addr: config.addr.clone(),
			connections: config.connections,
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			speculative_execution: config.speculative_execution.as_ref().map(|se| match se {
				crate::config::ObjectScyllaStoreSpeculativeExecution::Percentile(p) => {
					object_store::scylla::SpeculativeExecution::Percentile {
						max_retry_count: p.max_retry_count,
						percentile: p.percentile,
					}
				},
				crate::config::ObjectScyllaStoreSpeculativeExecution::Simple(s) => {
					object_store::scylla::SpeculativeExecution::Simple {
						max_retry_count: s.max_retry_count,
						retry_interval: std::time::Duration::from_millis(s.retry_interval),
					}
				},
			}),
			username: config.username.clone(),
		};
		let scylla = object_store::scylla::Store::new(&config).await?;
		Ok(Self::Scylla(scylla))
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn try_get_object_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<object_store::Object<'static>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object_sync(id),
			Self::Memory(memory) => Ok(memory.try_get_object(id)),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	#[expect(dead_code)]
	pub fn try_get_object_batch_sync(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<object_store::Object<'static>>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object_batch_sync(ids),
			Self::Memory(memory) => Ok(memory.try_get_object_batch(ids)),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object_data_sync(id),
			Self::Memory(memory) => memory.try_get_object_data(id),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn put_object_sync(&self, arg: PutObjectArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.put_object_sync(arg)?;
			},
			Self::Memory(memory) => {
				memory.put_object(arg);
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}
		Ok(())
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	#[expect(dead_code)]
	pub fn put_object_batch_sync(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.put_object_batch_sync(args)?;
			},
			Self::Memory(memory) => {
				memory.put_object_batch(args);
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}
		Ok(())
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	#[expect(dead_code)]
	pub fn delete_object_sync(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.delete_object_sync(arg)?;
			},
			Self::Memory(memory) => {
				memory.delete_object(arg);
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}
		Ok(())
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	#[expect(dead_code)]
	pub fn delete_object_batch_sync(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.delete_object_batch_sync(args)?;
			},
			Self::Memory(memory) => {
				memory.delete_object_batch(args);
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}
		Ok(())
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	#[expect(dead_code)]
	pub fn flush_sync(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.flush_sync()?;
			},
			Self::Memory(memory) => {
				memory.flush();
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}
		Ok(())
	}
}

impl object_store::Store for Store {
	async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<object_store::Object<'static>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object(id).await,
			Self::Memory(memory) => object_store::Store::try_get_object(memory, id).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_object(id).await,
		}
	}

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<object_store::Object<'static>>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object_batch(ids).await,
			Self::Memory(memory) => Ok(memory.try_get_object_batch(ids)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_object_batch(ids).await,
		}
	}

	async fn put_object(&self, arg: PutObjectArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_object(arg).await,
			Self::Memory(memory) => {
				memory.put_object(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_object(arg).await,
		}
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_object_batch(args).await,
			Self::Memory(memory) => {
				memory.put_object_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_object_batch(args).await,
		}
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_object(arg).await,
			Self::Memory(memory) => {
				memory.delete_object(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_object(arg).await,
		}
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_object_batch(args).await,
			Self::Memory(memory) => {
				memory.delete_object_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_object_batch(args).await,
		}
	}

	async fn flush(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.flush().await,
			Self::Memory(memory) => {
				memory.flush();
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.flush().await,
		}
	}
}
