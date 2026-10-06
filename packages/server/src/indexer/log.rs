use {
	super::{Indexer, partition},
	futures::future,
	tangram_cache::Cache as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
};

impl Indexer {
	pub(super) async fn log_cache_task(
		&self,
		config: &crate::config::IndexerLogCache,
		stopper: &Stopper,
	) -> tg::Result<()> {
		if !config.enabled || config.partitions.start == config.partitions.end {
			stopper.wait().await;
			return Ok(());
		}
		let futures = partition::ranges(
			config.partitions.start,
			config.partitions.end,
			config.concurrency,
		)
		.map(|range| self.log_cache_partition_task(config, range, stopper));
		future::try_join_all(futures).await?;
		Ok(())
	}

	async fn log_cache_partition_task(
		&self,
		config: &crate::config::IndexerLogCache,
		range: std::ops::Range<u64>,
		stopper: &Stopper,
	) -> tg::Result<()> {
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			let mut deleted = false;
			let mut failed = false;
			for partition in range.clone() {
				if stopper.stopped() {
					return Ok(());
				}
				match self.log_cache_partition_batch(config, partition).await {
					Ok(count) => deleted |= count > 0,
					Err(error) => {
						tracing::error!(error = %error.trace(), partition, "failed to delete log cache entries");
						failed = true;
						break;
					},
				}
			}
			if !deleted || failed {
				tokio::select! {
				() = stopper.wait() => return Ok(()),
				() = tokio::time::sleep(config.poll_interval) => {},
				}
			}
		}
	}

	async fn log_cache_partition_batch(
		&self,
		config: &crate::config::IndexerLogCache,
		partition: u64,
	) -> tg::Result<usize> {
		let now = self.server.clock.unix_timestamp()?;
		let arg = tangram_cache::log::cache::get::Arg {
			batch_size: config.batch_size,
			now,
			partition,
		};
		let entries = self.server.cache.get_log_cache_entries(arg).await?;
		let count = entries.len();
		for entry in entries {
			let process = entry.process.clone();
			let arg = tangram_cache::log::cache::delete::Arg { entry };
			self.server.cache.delete_log_cache_entry(arg).await?;
			crate::checkpoint!(self.server, "indexer.log_cache.delete", %process).await;
		}
		Ok(count)
	}
}
