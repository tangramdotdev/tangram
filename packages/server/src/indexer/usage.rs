use {
	super::{Indexer, RETRY_OPTIONS, partition},
	futures::future,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::prelude::*,
};

impl Indexer {
	pub(super) async fn usage_aggregation_task(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		if partition_start == partition_end {
			stopper.wait().await;
			return Ok(());
		}
		let futures =
			partition::ranges(partition_start, partition_end, config.concurrency).map(|range| {
				self.usage_aggregation_task_inner(config, range.start, range.end, stopper)
			});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn usage_aggregation_task_inner(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			crate::checkpoint!(self.server, "indexer.usage.aggregation.batch").await;
			let now = self.server.clock.now()?;
			let arg = tangram_index::usage::aggregate::Arg {
				batch_size: config.batch_size,
				now,
				partition_end,
				partition_start,
			};
			let output = tangram_futures::retry(&RETRY_OPTIONS, || async {
				match self.server.index.aggregate_usage(arg.clone()).await {
					Ok(output) => Ok(ControlFlow::Break(output)),
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to aggregate usage");

						Ok(ControlFlow::Continue(error))
					},
				}
			})
			.await?;
			if output.count == 0 {
				tokio::select! {
					() = stopper.wait() => return Ok(()),
					() = tokio::time::sleep(config.poll_interval) => {},
				}
			}
		}
	}

	pub(super) async fn usage_expiration_task(
		&self,
		config: &crate::config::IndexerUsageExpiration,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		let usage = self.server.config.usage;
		let partition_total = self.server.index.usage_partition_total();
		let partition_end = partition_end.min(partition_total);
		let partition_start = partition_start.min(partition_total);
		if partition_end <= partition_start {
			stopper.wait().await;
			return Ok(());
		}
		let retry = tangram_futures::retry::Options {
			backoff: config.poll_interval,
			jitter: std::time::Duration::ZERO,
			max_delay: config.poll_interval,
			max_retries: u64::MAX,
		};
		loop {
			if stopper.stopped() {
				return Ok(());
			}
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
			let output = tangram_futures::retry(&retry, || async {
				match self.server.index.expire_usage(arg.clone()).await {
					Ok(output) => Ok(ControlFlow::Break(output)),
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to expire usage");

						Ok(ControlFlow::Continue(error))
					},
				}
			})
			.await?;
			if output.done {
				tokio::select! {
					() = stopper.wait() => return Ok(()),
					() = tokio::time::sleep(config.poll_interval) => {},
				}
			}
		}
	}
}
