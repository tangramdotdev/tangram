#[cfg(feature = "lmdb")]
use std::path::Path;
use {tangram_client::prelude::*, tangram_store as store};

pub use store::{log, object};

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

impl Store {
	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbStore) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = store::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
			posix_sem_prefix: config.resolved_posix_sem_prefix(),
			read_batch_size: config.read_batch_size,
			read_concurrency: config.read_concurrency,
			write_batch_size: config.write_batch_size,
		};
		let lmdb = store::lmdb::Store::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the lmdb store"),
		)?;

		Ok(Self::Lmdb(lmdb))
	}

	#[must_use]
	pub fn new_memory() -> Self {
		Self::Memory(store::memory::Store::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaStore) -> tg::Result<Self> {
		let capacity = config.capacity.as_ref().map(|capacity| match capacity {
			crate::config::ScyllaStoreCapacity::Prometheus(capacity) => {
				store::scylla::CapacityConfig {
					available_query: capacity.available_query.clone(),
					total_query: capacity.total_query.clone(),
					ttl: capacity.ttl,
					url: capacity.url.to_string(),
				}
			},
		});
		let speculative_execution =
			config
				.speculative_execution
				.as_ref()
				.map(|value| match value {
					crate::config::ScyllaStoreSpeculativeExecution::Percentile(value) => {
						store::scylla::SpeculativeExecution::Percentile {
							max_retry_count: value.max_retry_count,
							percentile: value.percentile,
						}
					},
					crate::config::ScyllaStoreSpeculativeExecution::Simple(value) => {
						store::scylla::SpeculativeExecution::Simple {
							max_retry_count: value.max_retry_count,
							retry_interval: std::time::Duration::from_millis(value.retry_interval),
						}
					},
				});
		let config = store::scylla::Config {
			addr: config.addr.clone(),
			capacity,
			connections: config.connections,
			keepalive: config.keepalive,
			keyspace: config.keyspace.clone(),
			partition_offset: config.partition_offset,
			password: config.password.clone(),
			speculative_execution,
			username: config.username.clone(),
		};
		let scylla = store::scylla::Store::new(&config)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the scylla store"))?;

		Ok(Self::Scylla(scylla))
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object_sync(arg)?,
			Self::Memory(store) => store.put_object(arg)?,
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => return Err(tg::error!("unimplemented")),
		}

		Ok(())
	}

	pub fn try_get_object_data_sync(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_get_object_data_sync(id),
			Self::Memory(store) => store.try_get_object_data(id),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn try_get_object_sync(&self, arg: &object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_get_object_sync(arg),
			Self::Memory(store) => Ok(store.try_get_object_sync(arg)),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}
}

impl store::Store for Store {
	async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store::Store::contains_object(store, arg).await,
			Self::Memory(store) => store::Store::contains_object(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store::Store::contains_object(store, arg).await,
		}
	}

	async fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object_cache_entry(arg).await,
			Self::Memory(store) => store::Store::delete_object_cache_entry(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object_cache_entry(arg).await,
		}
	}

	async fn delete_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object_archive_outbox_entries(arg).await,
			Self::Memory(store) => {
				store::Store::delete_object_archive_outbox_entries(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object_archive_outbox_entries(arg).await,
		}
	}

	async fn delete_log(&self, arg: log::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_log(arg).await,
			Self::Memory(store) => store::Store::delete_log(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_log(arg).await,
		}
	}

	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object(arg).await,
			Self::Memory(store) => store::Store::delete_object(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object(arg).await,
		}
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object_batch(args).await,
			Self::Memory(store) => store::Store::delete_object_batch(store, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object_batch(args).await,
		}
	}

	async fn delete_object_index_outbox_batch(
		&self,
		arg: object::index::outbox::batch::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object_index_outbox_batch(arg).await,
			Self::Memory(store) => store::Store::delete_object_index_outbox_batch(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object_index_outbox_batch(arg).await,
		}
	}

	async fn delete_object_index_outbox_fragments(
		&self,
		arg: object::index::outbox::fragment::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.delete_object_index_outbox_fragments(arg).await,
			Self::Memory(store) => {
				store::Store::delete_object_index_outbox_fragments(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.delete_object_index_outbox_fragments(arg).await,
		}
	}

	async fn dequeue_object_index_outbox_fragments(
		&self,
		arg: object::index::outbox::fragment::dequeue::Arg,
	) -> tg::Result<Vec<object::index::outbox::fragment::Fragment>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.dequeue_object_index_outbox_fragments(arg).await,
			Self::Memory(store) => {
				store::Store::dequeue_object_index_outbox_fragments(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.dequeue_object_index_outbox_fragments(arg).await,
		}
	}

	async fn dequeue_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::dequeue::Arg,
	) -> tg::Result<Vec<object::archive::outbox::Entry>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.dequeue_object_archive_outbox_entries(arg).await,
			Self::Memory(store) => {
				store::Store::dequeue_object_archive_outbox_entries(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.dequeue_object_archive_outbox_entries(arg).await,
		}
	}

	async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.get_object_cache_entries(arg).await,
			Self::Memory(store) => store::Store::get_object_cache_entries(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.get_object_cache_entries(arg).await,
		}
	}

	async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object_cache_entry(arg).await,
			Self::Memory(store) => store::Store::put_object_cache_entry(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_object_cache_entry(arg).await,
		}
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object_cache_entry_with_object(arg).await,
			Self::Memory(store) => {
				store::Store::put_object_cache_entry_with_object(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_object_cache_entry_with_object(arg).await,
		}
	}

	async fn put_object_archive_outbox_entries(
		&self,
		arg: object::archive::outbox::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object_archive_outbox_entries(arg).await,
			Self::Memory(store) => {
				store::Store::put_object_archive_outbox_entries(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_object_archive_outbox_entries(arg).await,
		}
	}

	async fn enqueue_object_index_outbox_batch(
		&self,
		arg: object::index::outbox::batch::enqueue::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.enqueue_object_index_outbox_batch(arg).await,
			Self::Memory(store) => {
				store::Store::enqueue_object_index_outbox_batch(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.enqueue_object_index_outbox_batch(arg).await,
		}
	}

	async fn flush(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.flush().await,
			Self::Memory(store) => store::Store::flush(store).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.flush().await,
		}
	}

	async fn put_log(&self, arg: log::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_log(arg).await,
			Self::Memory(store) => store::Store::put_log(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_log(arg).await,
		}
	}

	async fn put_log_batch(&self, args: Vec<log::put::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_log_batch(args).await,
			Self::Memory(store) => store::Store::put_log_batch(store, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_log_batch(args).await,
		}
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object(arg).await,
			Self::Memory(store) => store::Store::put_object(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_object(arg).await,
		}
	}

	async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.put_object_batch(args).await,
			Self::Memory(store) => store::Store::put_object_batch(store, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.put_object_batch(args).await,
		}
	}

	async fn try_get_log_length(&self, arg: log::length::Arg) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_get_log_length(arg).await,
			Self::Memory(store) => store::Store::try_get_log_length(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.try_get_log_length(arg).await,
		}
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_get_object(arg).await,
			Self::Memory(store) => store::Store::try_get_object(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.try_get_object(arg).await,
		}
	}

	async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_get_object_batch(arg).await,
			Self::Memory(store) => store::Store::try_get_object_batch(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.try_get_object_batch(arg).await,
		}
	}

	async fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: object::index::outbox::batch::get::Arg,
	) -> tg::Result<Option<object::index::outbox::batch::Id>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => {
				store
					.try_get_object_index_outbox_batch_at_or_before(arg)
					.await
			},
			Self::Memory(store) => {
				store::Store::try_get_object_index_outbox_batch_at_or_before(store, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => {
				store
					.try_get_object_index_outbox_batch_at_or_before(arg)
					.await
			},
		}
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<store::capacity::Capacity>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store::Store::try_get_capacity(store).await,
			Self::Memory(store) => store::Store::try_get_capacity(store).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store::Store::try_get_capacity(store).await,
		}
	}

	async fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(store) => store.try_read_log(arg).await,
			Self::Memory(store) => store::Store::try_read_log(store, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(store) => store.try_read_log(arg).await,
		}
	}
}
