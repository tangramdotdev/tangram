#[cfg(feature = "lmdb")]
use std::path::Path;
use {tangram_cache as cache, tangram_client::prelude::*};

pub use cache::{archive, index, log, object};

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Cache {
	#[cfg(feature = "lmdb")]
	Lmdb(cache::lmdb::Cache),

	Memory(cache::memory::Cache),

	#[cfg(feature = "scylla")]
	Scylla(cache::scylla::Cache),
}

impl Cache {
	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(directory: &Path, config: &crate::config::LmdbCache) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = cache::lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
			posix_sem_prefix: config.resolved_posix_sem_prefix(),
			read_batch_size: config.read_batch_size,
			read_concurrency: config.read_concurrency,
			write_batch_size: config.write_batch_size,
		};
		let lmdb = cache::lmdb::Cache::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the lmdb cache"),
		)?;

		Ok(Self::Lmdb(lmdb))
	}

	#[must_use]
	pub fn new_memory() -> Self {
		Self::Memory(cache::memory::Cache::new())
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaCache) -> tg::Result<Self> {
		let capacity = config.capacity.as_ref().map(|capacity| match capacity {
			crate::config::ScyllaCacheCapacity::Prometheus(capacity) => {
				cache::scylla::CapacityConfig {
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
					crate::config::ScyllaCacheSpeculativeExecution::Percentile(value) => {
						cache::scylla::SpeculativeExecution::Percentile {
							max_retry_count: value.max_retry_count,
							percentile: value.percentile,
						}
					},
					crate::config::ScyllaCacheSpeculativeExecution::Simple(value) => {
						cache::scylla::SpeculativeExecution::Simple {
							max_retry_count: value.max_retry_count,
							retry_interval: std::time::Duration::from_millis(value.retry_interval),
						}
					},
				});
		let config = cache::scylla::Config {
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
		let scylla = cache::scylla::Cache::new(&config)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the scylla cache"))?;

		Ok(Self::Scylla(scylla))
	}

	#[cfg_attr(
		not(any(feature = "lmdb", feature = "scylla")),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_sync(arg)?,
			Self::Memory(cache) => cache.put_object(arg)?,
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
			Self::Lmdb(cache) => cache.try_get_object_data_sync(id),
			Self::Memory(cache) => cache.try_get_object_data(id),
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
			Self::Lmdb(cache) => cache.try_get_object_sync(arg),
			Self::Memory(cache) => Ok(cache.try_get_object_sync(arg)),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}
}

impl cache::Cache for Cache {
	async fn delete_archive_queue_entry(&self, arg: archive::queue::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_archive_queue_entry(arg).await,
			Self::Memory(cache) => cache::Cache::delete_archive_queue_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_archive_queue_entry(arg).await,
		}
	}

	async fn delete_index_queue_fragment(&self, arg: index::queue::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_index_queue_fragment(arg).await,
			Self::Memory(cache) => cache::Cache::delete_index_queue_fragment(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_index_queue_fragment(arg).await,
		}
	}

	async fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_archive_queue_entry(arg).await,
			Self::Memory(cache) => cache::Cache::put_archive_queue_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_archive_queue_entry(arg).await,
		}
	}

	async fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_index_queue_fragment(arg).await,
			Self::Memory(cache) => cache::Cache::put_index_queue_fragment(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_index_queue_fragment(arg).await,
		}
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Entry>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_archive_queue_entry(arg).await,
			Self::Memory(cache) => cache::Cache::try_get_archive_queue_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_archive_queue_entry(arg).await,
		}
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_index_queue_fragment(arg).await,
			Self::Memory(cache) => cache::Cache::try_get_index_queue_fragment(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_index_queue_fragment(arg).await,
		}
	}

	async fn contains_object(&self, arg: object::contains::Arg) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache::Cache::contains_object(cache, arg).await,
			Self::Memory(cache) => cache::Cache::contains_object(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache::Cache::contains_object(cache, arg).await,
		}
	}

	async fn delete_object_cache_entry(&self, arg: object::cache::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object_cache_entry(arg).await,
			Self::Memory(cache) => cache::Cache::delete_object_cache_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object_cache_entry(arg).await,
		}
	}

	async fn delete_log(&self, arg: log::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_log(arg).await,
			Self::Memory(cache) => cache::Cache::delete_log(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_log(arg).await,
		}
	}

	async fn delete_object(&self, arg: object::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object(arg).await,
			Self::Memory(cache) => cache::Cache::delete_object(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object(arg).await,
		}
	}

	async fn delete_object_batch(&self, args: Vec<object::delete::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object_batch(args).await,
			Self::Memory(cache) => cache::Cache::delete_object_batch(cache, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object_batch(args).await,
		}
	}

	async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_object_cache_entries(arg).await,
			Self::Memory(cache) => cache::Cache::get_object_cache_entries(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_object_cache_entries(arg).await,
		}
	}

	async fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Entry>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_archive_queue_entries(arg).await,
			Self::Memory(cache) => cache::Cache::get_archive_queue_entries(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_archive_queue_entries(arg).await,
		}
	}

	async fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Fragment>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_index_queue_fragments(arg).await,
			Self::Memory(cache) => cache::Cache::get_index_queue_fragments(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_index_queue_fragments(arg).await,
		}
	}

	async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_cache_entry(arg).await,
			Self::Memory(cache) => cache::Cache::put_object_cache_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_cache_entry(arg).await,
		}
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_cache_entry_with_object(arg).await,
			Self::Memory(cache) => {
				cache::Cache::put_object_cache_entry_with_object(cache, arg).await
			},
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_cache_entry_with_object(arg).await,
		}
	}

	async fn flush(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.flush().await,
			Self::Memory(cache) => cache::Cache::flush(cache).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.flush().await,
		}
	}

	async fn put_log(&self, arg: log::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_log(arg).await,
			Self::Memory(cache) => cache::Cache::put_log(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_log(arg).await,
		}
	}

	async fn put_log_batch(&self, args: Vec<log::put::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_log_batch(args).await,
			Self::Memory(cache) => cache::Cache::put_log_batch(cache, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_log_batch(args).await,
		}
	}

	async fn put_log_end(&self, arg: log::end::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache::Cache::put_log_end(cache, arg).await,
			Self::Memory(cache) => cache::Cache::put_log_end(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache::Cache::put_log_end(cache, arg).await,
		}
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache::Cache::try_get_log_end(cache, process).await,
			Self::Memory(cache) => cache::Cache::try_get_log_end(cache, process).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache::Cache::try_get_log_end(cache, process).await,
		}
	}

	async fn put_object(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object(arg).await,
			Self::Memory(cache) => cache::Cache::put_object(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object(arg).await,
		}
	}

	async fn put_object_batch(&self, args: Vec<object::put::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_batch(args).await,
			Self::Memory(cache) => cache::Cache::put_object_batch(cache, args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_batch(args).await,
		}
	}

	async fn try_get_log_length(&self, arg: log::length::Arg) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_log_length(arg).await,
			Self::Memory(cache) => cache::Cache::try_get_log_length(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_log_length(arg).await,
		}
	}

	async fn try_get_object(&self, arg: object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object(arg).await,
			Self::Memory(cache) => cache::Cache::try_get_object(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_object(arg).await,
		}
	}

	async fn try_get_object_batch(
		&self,
		arg: object::get::batch::Arg,
	) -> tg::Result<Vec<object::get::Output>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object_batch(arg).await,
			Self::Memory(cache) => cache::Cache::try_get_object_batch(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_object_batch(arg).await,
		}
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<cache::capacity::Capacity>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache::Cache::try_get_capacity(cache).await,
			Self::Memory(cache) => cache::Cache::try_get_capacity(cache).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache::Cache::try_get_capacity(cache).await,
		}
	}

	async fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		match self {
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_read_log(arg).await,
			Self::Memory(cache) => cache::Cache::try_read_log(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_read_log(arg).await,
		}
	}
}
