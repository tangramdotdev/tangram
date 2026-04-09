#[cfg(feature = "lmdb")]
use std::path::Path;
use {std::collections::BTreeSet, tangram_client::prelude::*, tangram_log_store as log_store};

pub use log_store::{DeleteProcessLogArg, PutProcessLogArg, ReadProcessLogArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "foundationdb")]
	Fdb(log_store::fdb::Store),

	#[cfg(feature = "lmdb")]
	Lmdb(log_store::lmdb::Store),

	Memory(log_store::memory::Store),
}

impl Store {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(config: &crate::config::LogFdbStore) -> tg::Result<Self> {
		let options = log_store::fdb::Options {
			cluster: config.cluster.clone(),
			prefix: config.prefix.clone(),
		};
		let fdb = log_store::fdb::Store::new(&options)
			.map_err(|source| tg::error!(!source, "failed to create the foundationdb store"))?;
		Ok(Self::Fdb(fdb))
	}

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
}

impl log_store::Store for Store {
	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> tg::Result<Vec<log_store::ProcessLogEntry<'static>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_read_process_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_read_process_log(arg).await,
			Self::Memory(memory) => memory.try_read_process_log(arg).await,
		}
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_process_log_length(id, streams).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_process_log_length(id, streams).await,
			Self::Memory(memory) => Ok(memory.try_get_process_log_length(id, streams)),
		}
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.put_process_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_process_log(arg).await,
			Self::Memory(memory) => {
				memory.put_process_log(arg);
				Ok(())
			},
		}
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.delete_process_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_process_log(arg).await,
			Self::Memory(memory) => {
				memory.delete_process_log(arg);
				Ok(())
			},
		}
	}
}
