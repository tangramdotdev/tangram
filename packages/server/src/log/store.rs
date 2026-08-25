#[cfg(feature = "lmdb")]
use std::path::Path;
use {std::collections::BTreeSet, tangram_client::prelude::*, tangram_log_store as log_store};

pub use log_store::{DeleteArg, PutArg, ReadArg};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Store {
	#[cfg(feature = "foundationdb")]
	Fdb(log_store::fdb::Store),

	#[cfg(feature = "lmdb")]
	Lmdb(log_store::lmdb::Store),

	Memory(log_store::memory::Store),

	#[cfg(feature = "scylla")]
	Scylla(log_store::scylla::Store),
}

impl Store {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(config: &crate::config::FdbLogStore) -> tg::Result<Self> {
		let options = log_store::fdb::Options {
			cluster: config.cluster.clone(),
			prefix: config.prefix.clone(),
		};
		let fdb = log_store::fdb::Store::new(&options)
			.map_err(|error| tg::error!(!error, "failed to create the foundationdb store"))?;
		Ok(Self::Fdb(fdb))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbLogStore) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = log_store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
		};
		let lmdb = log_store::lmdb::Store::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the lmdb store"),
		)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(log_store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaLogStore) -> tg::Result<Self> {
		let config = log_store::scylla::Config {
			addr: config.addr.clone(),
			connections: config.connections,
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			username: config.username.clone(),
		};
		let scylla = log_store::scylla::Store::new(&config)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the scylla store"))?;

		Ok(Self::Scylla(scylla))
	}
}

impl log_store::Store for Store {
	async fn try_read(&self, arg: ReadArg) -> tg::Result<Vec<log_store::Entry<'static>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_read(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_read(arg).await,
			Self::Memory(memory) => memory.try_read(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_read(arg).await,
		}
	}

	async fn try_get_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.try_get_length(id, streams).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_length(id, streams).await,
			Self::Memory(memory) => Ok(memory.try_get_length(id, streams)),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_length(id, streams).await,
		}
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.put(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put(arg).await,
			Self::Memory(memory) => {
				memory.put(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put(arg).await,
		}
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(fdb) => fdb.delete(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete(arg).await,
			Self::Memory(memory) => {
				memory.delete(arg);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete(arg).await,
		}
	}
}
