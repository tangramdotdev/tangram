use {
	super::super::Indexer, futures::future, tangram_client::prelude::*, tangram_store::Store as _,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
	Delete,
	Idle,
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
		let mut mode = Mode::Idle;
		let mut empty_partition_count = 0;
		let mut partition = partitions.start;
		loop {
			let should_sleep = match self.server.store.try_get_capacity().await {
				Ok(Some(capacity)) => {
					mode = mode.next(&cache.capacity, capacity);
					if mode == Mode::Idle {
						empty_partition_count = 0;

						true
					} else {
						let current_partition = partition;
						let result = self
							.object_cache_partition_batch(cache, current_partition)
							.await;
						partition += 1;
						if partition == partitions.end {
							partition = partitions.start;
						}
						match result {
							Ok(0) => {
								empty_partition_count += 1;
								if empty_partition_count == partitions.end - partitions.start {
									empty_partition_count = 0;

									true
								} else {
									false
								}
							},
							Ok(_) => {
								empty_partition_count = 0;

								false
							},
							Err(error) => {
								tracing::error!(
									error = %error.trace(),
									partition = current_partition,
									"failed to delete object cache entries"
								);
								empty_partition_count = 0;

								true
							},
						}
					}
				},
				Ok(None) => {
					return Err(tg::error!("the store does not report capacity"));
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to get the store capacity");
					empty_partition_count = 0;

					true
				},
			};
			if should_sleep {
				tokio::time::sleep(cache.poll_interval).await;
			}
		}
	}

	async fn object_cache_partition_batch(
		&self,
		cache: &crate::config::ObjectCache,
		partition: u64,
	) -> tg::Result<usize> {
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
		let count = entries.len();
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

		Ok(count)
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
			Self::Idle if config.should_start(capacity.available, capacity.total) => Self::Delete,
			Self::Delete | Self::Idle => Self::Idle,
		}
	}
}
