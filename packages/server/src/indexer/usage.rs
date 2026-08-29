use {
	super::{Indexer, partition},
	futures::future,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Indexer {
	pub(super) async fn usage_aggregation_task(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let futures = partition::ranges(partition_start, partition_end, config.concurrency)
			.map(|range| self.usage_aggregation_task_inner(config, range.start, range.end));
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn usage_aggregation_task_inner(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		loop {
			crate::checkpoint!(self.server, "indexer.usage.aggregation.batch").await;
			let now = self.server.clock.now()?;
			let arg = tangram_index::usage::aggregate::Arg {
				batch_size: config.batch_size,
				now,
				partition_end,
				partition_start,
			};
			match self.server.index.aggregate_usage(arg).await {
				Ok(output) if output.count == 0 => {
					tokio::time::sleep(config.poll_interval).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to aggregate usage");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub(super) async fn usage_expiration_task(
		&self,
		config: &crate::config::IndexerUsageExpiration,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let usage = self.server.config.usage;
		let partition_total = self.server.index.usage_partition_total();
		let partition_end = partition_end.min(partition_total);
		let partition_start = partition_start.min(partition_total);
		if partition_end <= partition_start {
			return future::pending().await;
		}
		loop {
			let now = self.server.clock.now()?;
			let arg = tangram_index::usage::expire::Arg {
				batch_size: config.batch_size,
				day_time_to_live: usage.day_time_to_live,
				delta_time_to_live: usage.delta_time_to_live,
				hour_time_to_live: usage.hour_time_to_live,
				month_time_to_live: usage.month_time_to_live,
				now,
				partition_end,
				partition_start,
				week_time_to_live: usage.week_time_to_live,
			};
			match self.server.index.expire_usage(arg).await {
				Ok(output) if output.done => {
					tokio::time::sleep(config.poll_interval).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to expire usage");
					tokio::time::sleep(config.poll_interval).await;
				},
			}
		}
	}
}
