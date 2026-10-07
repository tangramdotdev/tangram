use {tangram_cache as cache, tangram_client::prelude::*};

pub mod archive;
pub mod index;
pub mod log;
pub mod object;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Cache {
	#[cfg(feature = "fjall")]
	Fjall(tangram_cache_fjall::Cache),

	#[cfg(feature = "lmdb")]
	Lmdb(tangram_cache_lmdb::Cache),

	Memory(tangram_cache_memory::Cache),

	#[cfg(feature = "rocksdb")]
	Rocksdb(tangram_cache_rocksdb::Cache),

	#[cfg(feature = "scylla")]
	Scylla(tangram_cache_scylla::Cache),
}

impl Cache {
	#[cfg(feature = "fjall")]
	pub fn new_fjall(
		directory: &std::path::Path,
		config: &crate::config::FjallCache,
	) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = tangram_cache_fjall::Config {
			path: path.clone(),
			read_batch_size: config.read_batch_size,
			read_concurrency: config.read_concurrency,
			write_batch_size: config.write_batch_size,
		};
		let fjall = tangram_cache_fjall::Cache::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the fjall cache"),
		)?;

		Ok(Self::Fjall(fjall))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(
		directory: &std::path::Path,
		config: &crate::config::LmdbCache,
	) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = tangram_cache_lmdb::Config {
			map_size: config.map_size,
			path: path.clone(),
			posix_sem_prefix: config.resolved_posix_sem_prefix(),
			read_batch_size: config.read_batch_size,
			read_concurrency: config.read_concurrency,
			write_batch_size: config.write_batch_size,
		};
		let lmdb = tangram_cache_lmdb::Cache::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the lmdb cache"),
		)?;

		Ok(Self::Lmdb(lmdb))
	}

	#[must_use]
	pub fn new_memory() -> Self {
		Self::Memory(tangram_cache_memory::Cache::new())
	}

	#[cfg(feature = "rocksdb")]
	pub fn new_rocksdb(
		directory: &std::path::Path,
		config: &crate::config::RocksdbCache,
	) -> tg::Result<Self> {
		let path = directory.join(&config.path);
		let config = tangram_cache_rocksdb::Config {
			path: path.clone(),
			read_batch_size: config.read_batch_size,
			read_concurrency: config.read_concurrency,
			write_batch_size: config.write_batch_size,
		};
		let rocksdb = tangram_cache_rocksdb::Cache::new(&config).map_err(
			|error| tg::error!(!error, path = %path.display(), "failed to create the rocksdb cache"),
		)?;

		Ok(Self::Rocksdb(rocksdb))
	}

	#[cfg(feature = "scylla")]
	pub async fn new_scylla(config: &crate::config::ScyllaCache) -> tg::Result<Self> {
		let capacity = config.capacity.as_ref().map(|capacity| match capacity {
			crate::config::ScyllaCacheCapacity::Prometheus(capacity) => {
				tangram_cache_scylla::CapacityConfig {
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
						tangram_cache_scylla::SpeculativeExecution::Percentile {
							max_retry_count: value.max_retry_count,
							percentile: value.percentile,
						}
					},
					crate::config::ScyllaCacheSpeculativeExecution::Simple(value) => {
						tangram_cache_scylla::SpeculativeExecution::Simple {
							max_retry_count: value.max_retry_count,
							retry_interval: std::time::Duration::from_millis(value.retry_interval),
						}
					},
				});
		let config = tangram_cache_scylla::Config {
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
		let scylla = tangram_cache_scylla::Cache::new(&config)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the scylla cache"))?;

		Ok(Self::Scylla(scylla))
	}

	#[cfg_attr(
		not(any(
			feature = "fjall",
			feature = "lmdb",
			feature = "rocksdb",
			feature = "scylla"
		)),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn put_object_sync(&self, arg: object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_object_sync(arg)?,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_sync(arg)?,
			Self::Memory(cache) => cache.put_object(arg)?,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_object_sync(arg)?,
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
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_object_data_sync(id),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object_data_sync(id),
			Self::Memory(cache) => cache.try_get_object_data(id),
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_object_data_sync(id),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}

	#[cfg_attr(
		not(any(
			feature = "fjall",
			feature = "lmdb",
			feature = "rocksdb",
			feature = "scylla"
		)),
		expect(clippy::unnecessary_wraps)
	)]
	pub fn try_get_object_sync(&self, arg: &object::get::Arg) -> tg::Result<object::get::Output> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_object_sync(arg),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object_sync(arg),
			Self::Memory(cache) => Ok(cache.try_get_object_sync(arg)),
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_object_sync(arg),
			#[cfg(feature = "scylla")]
			Self::Scylla(_) => Err(tg::error!("unimplemented")),
		}
	}
}

impl cache::Cache for Cache {
	async fn flush(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.flush().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.flush().await,
			Self::Memory(cache) => cache::Cache::flush(cache).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.flush().await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.flush().await,
		}
	}

	async fn try_get_capacity(&self) -> tg::Result<Option<cache::capacity::Capacity>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache::Cache::try_get_capacity(cache).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache::Cache::try_get_capacity(cache).await,
			Self::Memory(cache) => cache::Cache::try_get_capacity(cache).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache::Cache::try_get_capacity(cache).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache::Cache::try_get_capacity(cache).await,
		}
	}
}
