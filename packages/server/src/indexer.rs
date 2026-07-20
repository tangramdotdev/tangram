use {
	crate::Server, futures::future, num::ToPrimitive as _, std::time::Duration,
	tangram_client::prelude::*, tangram_index::prelude::*, tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let partition_start = config.partition_start;
		let partition_count = config.partition_count;
		let concurrency = config.concurrency.to_u64().unwrap();
		loop {
			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_count / concurrency;
				let extra = partition_count % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				self.index
					.update_batch(config.batch_size, task_start, task_count)
			});
			let result = future::try_join_all(futures)
				.await
				.map(|counts| counts.into_iter().sum::<usize>());
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => {
					self.messenger
						.publish("indexer_progress".to_owned(), ())
						.await
						.ok();
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to index");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}
}
