#[cfg(feature = "lmdb")]
use std::path::Path;
use {std::collections::BTreeSet, tangram_client::prelude::*, tangram_log_store as log_store};

pub use log_store::{DeleteProcessLogArg, PutProcessLogArg, ReadProcessLogArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "lmdb")]
	Lmdb(log_store::lmdb::Store),

	Memory(log_store::memory::Store),

	#[cfg(feature = "scylla")]
	Scylla(log_store::scylla::Store),
}

impl Store {
	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::LogLmdbStore) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = log_store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
		};
		let lmdb = log_store::lmdb::Store::new(&config).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to create the lmdb store"),
		)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(log_store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::LogScyllaStore) -> tg::Result<Self> {
		let config = log_store::scylla::Config {
			addr: config.addr.clone(),
			connections: config.connections,
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			speculative_execution: config.speculative_execution.as_ref().map(|se| match se {
				crate::config::LogScyllaStoreSpeculativeExecution::Percentile(p) => {
					log_store::scylla::SpeculativeExecution::Percentile {
						max_retry_count: p.max_retry_count,
						percentile: p.percentile,
					}
				},
				crate::config::LogScyllaStoreSpeculativeExecution::Simple(s) => {
					log_store::scylla::SpeculativeExecution::Simple {
						max_retry_count: s.max_retry_count,
						retry_interval: std::time::Duration::from_millis(s.retry_interval),
					}
				},
			}),
			username: config.username.clone(),
		};
		let scylla = log_store::scylla::Store::new(&config).await?;
		Ok(Self::Scylla(scylla))
	}
}

impl log_store::Store for Store {
	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> tg::Result<Vec<log_store::ProcessLogEntry<'static>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_read_process_log(arg).await,
			Self::Memory(memory) => memory.try_read_process_log(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_read_process_log(arg).await,
		}
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_process_log_length(id, streams).await,
			Self::Memory(memory) => Ok(memory.try_get_process_log_length(id, streams)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_process_log_length(id, streams).await,
		}
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_process_log(arg).await,
			Self::Memory(memory) => {
				memory.put_process_log(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_process_log(arg).await,
		}
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_process_log(arg).await,
			Self::Memory(memory) => {
				memory.delete_process_log(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_process_log(arg).await,
		}
	}
}
