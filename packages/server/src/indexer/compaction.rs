use {
	super::Indexer,
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub(crate) fn spawn_publish_log_compaction_notification_task(&self) {
		let subject = log_compaction_subject();
		tokio::spawn({
			let server = self.clone();
			async move {
				if let Err(error) = server.messenger.publish(subject, ()).await {
					tracing::error!(%error, "failed to publish a log compaction notification");
				}
			}
		});
	}
}

impl Indexer {
	pub(super) async fn log_compaction_task(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let concurrency = config.concurrency.to_u64().unwrap();
		let partition_length = partition_end - partition_start;
		let futures = (0..config.concurrency).filter_map(|task_index| {
			let task_index = task_index.to_u64().unwrap();
			let partitions_per_task = partition_length / concurrency;
			let extra = partition_length % concurrency;
			let task_start =
				partition_start + task_index * partitions_per_task + task_index.min(extra);
			let task_count = partitions_per_task + u64::from(task_index < extra);
			let task_end = task_start + task_count;
			(task_count > 0)
				.then(|| self.log_compaction_partition_task(config, task_start, task_end))
		});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn log_compaction_partition_task(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		loop {
			let result = self
				.log_compaction_partition_task_inner(config, partition_start, partition_end)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to compact logs");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn log_compaction_partition_task_inner(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let subject = log_compaction_subject();
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to log compaction notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(config.wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			crate::checkpoint!(self.server, "indexer.log_compaction.batch").await;
			let entries = self
				.server
				.index
				.log_compaction_batch(config.batch_size, partition_start, partition_end)
				.await
				.map_err(|error| tg::error!(!error, "failed to read log compactions"))?;
			if entries.is_empty() {
				if wakeups.next().await.is_none() {
					return Err(tg::error!("the log compaction wakeup stream ended"));
				}
				continue;
			}
			self.compact_logs(&entries).boxed().await?;
		}
	}

	async fn compact_logs(&self, entries: &[tangram_index::log::Entry]) -> tg::Result<()> {
		for entry in entries {
			self.compact_log(entry).boxed().await?;
		}

		Ok(())
	}

	async fn compact_log(&self, entry: &tangram_index::log::Entry) -> tg::Result<()> {
		let process = &entry.process;
		let session = self.server.session(&self.server.context);
		session
			.compact_process_log(process)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to compact the process log"))?;
		self.server
			.index
			.complete_log_compaction(entry)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to complete the log compaction"),
			)?;

		Ok(())
	}
}

pub(crate) fn log_compaction_subject() -> String {
	"index.log_compaction".to_owned()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn subject_has_no_partition() {
		assert_eq!(log_compaction_subject(), "index.log_compaction");
	}
}
