#[cfg(feature = "lmdb")]
use std::path::Path;
use {tangram_client::prelude::*, tangram_store as store};

pub use store::{
	CachePointer, DeleteObjectArg, DeleteProcessLogArg, PutObjectArg, PutProcessLogArg,
	ReadProcessLogArg,
};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "lmdb")]
	Lmdb(store::lmdb::Store),

	Memory(store::memory::Store),

	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Store),
}

#[derive(Debug, derive_more::Display, derive_more::From, derive_more::Error)]
pub enum Error {
	#[cfg(feature = "lmdb")]
	Lmdb(store::lmdb::Error),

	Memory(store::memory::Error),

	#[cfg(feature = "scylla")]
	Scylla(store::scylla::Error),

	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbStore) -> Result<Self, Error> {
		let path = directory.join(&config.path);
		let config = store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
		};
		let lmdb = store::lmdb::Store::new(&config).map_err(|source| {
			Error::Other(Box::new(tg::error!(
				!source,
				path = %path.display(),
				"failed to create the lmdb store"
			)))
		})?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaStore) -> Result<Self, Error> {
		let config = store::scylla::Config {
			addr: config.addr.clone(),
			connections: config.connections,
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

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn try_get_object_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<store::Object<'static>>> {
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
	) -> tg::Result<Vec<Option<store::Object<'static>>>> {
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

impl store::Store for Store {
	type Error = Error;

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<store::Object<'static>>, Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object(id).await.map_err(Error::Lmdb),
			Self::Memory(memory) => store::Store::try_get_object(memory, id)
				.await
				.map_err(Error::Memory),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_object(id).await.map_err(Error::Scylla),
		}
	}

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<store::Object<'static>>>, Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_object_batch(ids).await.map_err(Error::Lmdb),
			Self::Memory(memory) => Ok(memory.try_get_object_batch(ids)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.try_get_object_batch(ids)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn put_object(&self, arg: PutObjectArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_object(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.put_object(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_object(arg).await.map_err(Error::Scylla),
		}
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_object_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.put_object_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_object_batch(args).await.map_err(Error::Scylla),
		}
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_object(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.delete_object(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_object(arg).await.map_err(Error::Scylla),
		}
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_object_batch(args).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.delete_object_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.delete_object_batch(args)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> Result<Vec<store::ProcessLogEntry<'static>>, Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_read_process_log(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => memory
				.try_read_process_log(arg)
				.await
				.map_err(Error::Memory),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.try_read_process_log(arg)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb
				.try_get_process_log_length(id, stream)
				.await
				.map_err(Error::Lmdb),
			Self::Memory(memory) => Ok(memory.try_get_process_log_length(id, stream)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla
				.try_get_process_log_length(id, stream)
				.await
				.map_err(Error::Scylla),
		}
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_process_log(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.put_process_log(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_process_log(arg).await.map_err(Error::Scylla),
		}
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_process_log(arg).await.map_err(Error::Lmdb),
			Self::Memory(memory) => {
				memory.delete_process_log(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_process_log(arg).await.map_err(Error::Scylla),
		}
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		match self {
			#[cfg(feature = "lmdb")]
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
