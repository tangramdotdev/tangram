use {
	super::super::Indexer,
	futures::future,
	std::{sync::atomic::Ordering, time::Instant},
	tangram_client::prelude::*,
	tangram_store::Store as _,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
	Delete,
	Put,
}

impl Indexer {
	pub(in crate::indexer) async fn object_cache_task(
		&self,
		partitions: &crate::config::IndexerPartitions,
		cache: Option<&crate::config::ObjectCache>,
	) -> tg::Result<()> {
		let Some(cache) = cache else {
			return future::pending().await;
		};
		let mut mode = Mode::Put;
		let mut last_capacity = None;
		let mut partition = partitions.start;
		loop {
			match self.server.store.try_get_capacity().await {
				Ok(Some(capacity)) => {
					last_capacity = Some(Instant::now());
					mode = mode.next(&cache.capacity, capacity);
					self.server
						.object_cache_puts_enabled
						.store(mode == Mode::Put, Ordering::Release);
					if mode == Mode::Delete {
						let result = self.object_cache_partition_batch(cache, partition).await;
						if let Err(error) = result {
							tracing::error!(
								error = %error.trace(),
								%partition,
								"failed to delete object cache entries"
							);
						}
						partition += 1;
						if partition == partitions.end {
							partition = partitions.start;
						}
					}
				},
				Ok(None) => {
					return Err(tg::error!("the store does not report capacity"));
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to get the store capacity");
					if last_capacity.is_none_or(|last_capacity| {
						last_capacity.elapsed() >= cache.metrics_stale_after
					}) {
						self.server
							.object_cache_puts_enabled
							.store(false, Ordering::Release);
					}
				},
			}
			tokio::time::sleep(cache.poll_interval).await;
		}
	}

	async fn object_cache_partition_batch(
		&self,
		cache: &crate::config::ObjectCache,
		partition: u64,
	) -> tg::Result<()> {
		let arg = crate::store::object::cache::get::Arg {
			batch_size: cache.batch_size,
			partition,
		};
		let entries = self
			.server
			.store
			.get_object_cache_entries(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object cache entries"))?;
		future::try_join_all(entries.into_iter().map(|entry| async move {
			let id = entry.id.clone();
			let arg = crate::store::object::cache::delete::Arg { entry };
			self.server
				.store
				.delete_object_cache_entry(arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to delete an object cache entry"))
		}))
		.await?;

		Ok(())
	}
}

impl Mode {
	fn next(
		self,
		config: &crate::config::CapacityThreshold,
		capacity: tangram_store::capacity::Capacity,
	) -> Self {
		match self {
			Self::Delete if !config.should_stop(capacity.available, capacity.total) => Self::Delete,
			Self::Put if config.should_start(capacity.available, capacity.total) => Self::Delete,
			Self::Delete | Self::Put => Self::Put,
		}
	}
}
