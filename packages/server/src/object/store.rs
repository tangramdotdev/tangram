#[cfg(feature = "lmdb")]
use std::path::Path;
use {bytes::Bytes, tangram_client::prelude::*, tangram_object_store as object_store};

pub use object_store::{CachePointer, DeleteArg, PutArg, TryGetArg, TryGetBatchArg, TryGetOutput};

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
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbObjectStore) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = object_store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
		};
		let lmdb = object_store::lmdb::Store::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the lmdb store"),
		)?;
		Ok(Self::Lmdb(lmdb))
	}

	pub fn new_memory() -> Self {
		Self::Memory(object_store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaObjectStore) -> tg::Result<Self> {
		let config = object_store::scylla::Config {
			addr: config.addr.clone(),
			connections: config.connections,
			keyspace: config.keyspace.clone(),
			password: config.password.clone(),
			speculative_execution: config.speculative_execution.as_ref().map(|se| match se {
				crate::config::ScyllaObjectStoreSpeculativeExecution::Percentile(p) => {
					object_store::scylla::SpeculativeExecution::Percentile {
						max_retry_count: p.max_retry_count,
						percentile: p.percentile,
					}
				},
				crate::config::ScyllaObjectStoreSpeculativeExecution::Simple(s) => {
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
	pub fn try_get_sync(&self, arg: &TryGetArg) -> tg::Result<TryGetOutput> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_sync(arg),
			Self::Memory(memory) => Ok(memory.try_get_sync(arg)),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	pub fn try_get_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_data_sync(id),
			Self::Memory(memory) => memory.try_get_data(id),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn put_sync(&self, arg: PutArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.put_sync(arg)?;
			},
			Self::Memory(memory) => {
				memory.put(arg);
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
	pub fn put_batch_sync(&self, args: Vec<PutArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.put_batch_sync(args)?;
			},
			Self::Memory(memory) => {
				memory.put_batch(args);
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
	pub fn delete_sync(&self, arg: DeleteArg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.delete_sync(arg)?;
			},
			Self::Memory(memory) => {
				memory.delete(arg);
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
	pub fn delete_batch_sync(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => {
				lmdb.delete_batch_sync(args)?;
			},
			Self::Memory(memory) => {
				memory.delete_batch(args);
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

	pub fn supports_outbox(&self) -> bool {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(_) => false,
			Self::Memory(_) => false,
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => true,
		}
	}

	pub async fn enqueue_outbox_batch(
		&self,
		entries: Vec<(i64, tg::object::Id, i64, Vec<u8>)>,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(_) => Err(tg::error!("unimplemented")),
			Self::Memory(_) => Err(tg::error!("unimplemented")),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.enqueue_outbox_batch(entries).await,
		}
	}

	pub async fn dequeue_outbox(
		&self,
		partition: i64,
		limit: i32,
	) -> tg::Result<Vec<(tg::object::Id, Bytes)>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(_) => Err(tg::error!("unimplemented")),
			Self::Memory(_) => Err(tg::error!("unimplemented")),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => {
				let entries = scylla.dequeue_outbox(partition, limit).await?;
				let entries = entries
					.into_iter()
					.map(|entry| (entry.id, entry.payload))
					.collect();
				Ok(entries)
			},
		}
	}

	pub async fn delete_outbox(
		&self,
		partition: i64,
		ids: &[tg::object::Id],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(_) => Err(tg::error!("unimplemented")),
			Self::Memory(_) => Err(tg::error!("unimplemented")),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_outbox(partition, ids).await,
		}
	}

	pub async fn outbox_has_pending(
		&self,
		partitions: Vec<i64>,
		before: i64,
	) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(_) => Err(tg::error!("unimplemented")),
			Self::Memory(_) => Err(tg::error!("unimplemented")),
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.outbox_has_pending(partitions, before).await,
		}
	}
}

impl object_store::Store for Store {
	async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get(arg).await,
			Self::Memory(memory) => object_store::Store::try_get(memory, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get(arg).await,
		}
	}

	async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.try_get_batch(arg).await,
			Self::Memory(memory) => object_store::Store::try_get_batch(memory, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.try_get_batch(arg).await,
		}
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		match self {
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

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.put_batch(args).await,
			Self::Memory(memory) => {
				memory.put_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.put_batch(args).await,
		}
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		match self {
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

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(lmdb) => lmdb.delete_batch(args).await,
			Self::Memory(memory) => {
				memory.delete_batch(args);
				Ok(())
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(scylla) => scylla.delete_batch(args).await,
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
