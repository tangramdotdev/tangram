use {
	super::super::Indexer,
	futures::future,
	std::{sync::atomic::Ordering, time::Instant},
	tangram_client::prelude::*,
	tangram_store::Store as _,
};

impl Indexer {
	pub(in crate::indexer) async fn object_cache_task(
		&self,
		config: &crate::config::Indexer,
		cache: Option<&crate::config::ObjectCache>,
	) -> tg::Result<()> {
		let Some(cache) = cache else {
			return future::pending().await;
		};
		let mut deleting = false;
		let mut last_capacity = None;
		let mut partition = config.partition_start;
		loop {
			match self.server.store.try_get_capacity().await {
				Ok(Some(capacity)) => {
					last_capacity = Some(Instant::now());
					let available = capacity.available_ratio();
					if deleting {
						deleting = available < cache.target_available;
					} else {
						deleting = available < cache.minimum_available;
					}
					self.server
						.object_cache_puts_enabled
						.store(!deleting, Ordering::Release);
					if deleting {
						let result = self.object_cache_partition_batch(cache, partition).await;
						if let Err(error) = result {
							tracing::error!(
								error = %error.trace(),
								%partition,
								"failed to delete object cache entries"
							);
						}
						partition += 1;
						if partition == config.partition_end {
							partition = config.partition_start;
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
