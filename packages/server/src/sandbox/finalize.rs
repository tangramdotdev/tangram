use {
	crate::Server,
	futures::{StreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_index::{self as index, prelude::*},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub(crate) async fn enqueue_sandbox_finalization(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		self.index
			.enqueue_finalization(&index::finalization::Item::Sandbox(id.clone()))
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to enqueue the sandbox finalization"),
			)?;
		self.spawn_publish_sandbox_finalize_message_task();

		Ok(())
	}

	pub(crate) fn spawn_publish_sandbox_finalize_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("sandboxes.finalize.queue".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox finalize message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn sandbox_finalizer_task(
		&self,
		config: &crate::config::Finalizer,
		stopper: Stopper,
	) -> tg::Result<()> {
		if config.concurrency == 0 {
			return Err(tg::error!(
				"the finalizer concurrency must be greater than zero"
			));
		}
		if config.partition_end <= config.partition_start {
			return Err(tg::error!(
				"the finalizer partition end must be greater than the partition start"
			));
		}

		let partition_start = config.partition_start;
		let partition_end = config.partition_end;
		let partition_length = partition_end - partition_start;
		let concurrency = config.concurrency.to_u64().unwrap();
		let futures = (0..config.concurrency).filter_map(|task_index| {
			let task_index = task_index.to_u64().unwrap();
			let partitions_per_task = partition_length / concurrency;
			let extra = partition_length % concurrency;
			let task_start =
				partition_start + task_index * partitions_per_task + task_index.min(extra);
			let task_count = partitions_per_task + u64::from(task_index < extra);
			let task_end = task_start + task_count;
			(task_count > 0).then(|| {
				self.sandbox_finalizer_task_inner(config, task_start, task_end, stopper.clone())
			})
		});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn sandbox_finalizer_task_inner(
		&self,
		config: &crate::config::Finalizer,
		partition_start: u64,
		partition_end: u64,
		stopper: Stopper,
	) -> tg::Result<()> {
		let batch_size = config.message_batch_size.max(1);
		let wakeups = self
			.messenger
			.subscribe::<()>("sandboxes.finalize.queue".to_owned())
			.await
			.map_err(|error| tg::error!(!error, "failed to subscribe"))?
			.map(|_| ());
		let interval = config.message_batch_timeout.max(Duration::from_millis(1));
		let interval = IntervalStream::new(tokio::time::interval(interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(wakeups, interval).with_stopper(Some(stopper));
		let mut wakeups = pin!(wakeups);
		loop {
			loop {
				let entries = match self
					.index
					.finalization_batch(
						index::finalization::Kind::Sandbox,
						batch_size,
						partition_start,
						partition_end,
					)
					.await
				{
					Ok(entries) if entries.is_empty() => break,
					Ok(entries) => entries,
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to read sandbox finalizations");
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				};
				if let Err(error) = self.handle_sandbox_finalize_entries(&entries).await {
					tracing::error!(error = %error.trace(), "failed to handle sandbox finalizations");
					tokio::time::sleep(Duration::from_secs(1)).await;
					break;
				}
				self.messenger
					.publish("sandboxes.finalizer.progress".to_owned(), ())
					.await
					.ok();
			}
			if wakeups.next().await.is_none() {
				break;
			}
		}

		Ok(())
	}

	async fn handle_sandbox_finalize_entries(
		&self,
		entries: &[index::finalization::Entry],
	) -> tg::Result<()> {
		for entry in entries {
			self.handle_sandbox_finalize_entry(entry).await?;
		}

		Ok(())
	}

	async fn handle_sandbox_finalize_entry(
		&self,
		entry: &index::finalization::Entry,
	) -> tg::Result<()> {
		let index::finalization::Item::Sandbox(sandbox) = &entry.item else {
			return Err(tg::error!("unexpected finalization item"));
		};
		let session = self.session(&self.context);
		let indexed = session
			.get_sandbox_from_index(sandbox)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to get the finalized sandbox"))?;
		if !indexed.data.is_some_and(|data| data.status.is_destroyed()) {
			return Err(tg::error!(%sandbox, "expected the sandbox to be destroyed"));
		}
		self.index.complete_finalization(entry).await.map_err(
			|error| tg::error!(!error, %sandbox, "failed to complete the sandbox finalization"),
		)?;

		Ok(())
	}
}
