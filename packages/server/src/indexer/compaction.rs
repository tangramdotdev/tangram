use {
	super::{Indexer, RETRY_OPTIONS, partition},
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, future, stream},
	std::ops::ControlFlow,
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
		let futures = partition::ranges(partition_start, partition_end, config.concurrency)
			.map(|range| self.log_compaction_partition_task(config, range.start, range.end));
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn log_compaction_partition_task(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			match self
				.log_compaction_partition_task_inner(config, partition_start, partition_end)
				.await
			{
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to compact logs");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
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
