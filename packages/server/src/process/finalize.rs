use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_index::{self as index, prelude::*},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub(crate) async fn enqueue_process_finalization(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		self.index
			.enqueue_finalization(&index::finalization::Item::Process(id.clone()))
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to enqueue the process finalization"),
			)?;
		self.spawn_publish_process_finalize_message_task();

		Ok(())
	}

	pub(crate) fn spawn_publish_process_finalize_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("processes.finalize.queue".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the process finalize message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn finalizer_task(
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
			(task_count > 0)
				.then(|| self.finalizer_task_inner(config, task_start, task_end, stopper.clone()))
		});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn finalizer_task_inner(
		&self,
		config: &crate::config::Finalizer,
		partition_start: u64,
		partition_end: u64,
		stopper: Stopper,
	) -> tg::Result<()> {
		let batch_size = config.message_batch_size.max(1);
		let wakeups = self
			.messenger
			.subscribe::<()>("processes.finalize.queue".to_owned())
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
						index::finalization::Kind::Process,
						batch_size,
						partition_start,
						partition_end,
					)
					.await
				{
					Ok(entries) if entries.is_empty() => break,
					Ok(entries) => entries,
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to read process finalizations");
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				};
				if let Err(error) = self.finalizer_handle_entries(&entries).boxed().await {
					tracing::error!(error = %error.trace(), "failed to handle process finalizations");
					tokio::time::sleep(Duration::from_secs(1)).await;
					break;
				}
			}
			if wakeups.next().await.is_none() {
				break;
			}
		}

		Ok(())
	}

	async fn finalizer_handle_entries(
		&self,
		entries: &[index::finalization::Entry],
	) -> tg::Result<()> {
		for entry in entries {
			self.finalizer_handle_entry(entry).boxed().await?;
		}

		Ok(())
	}

	async fn finalizer_handle_entry(&self, entry: &index::finalization::Entry) -> tg::Result<()> {
		let index::finalization::Item::Process(process) = &entry.item else {
			return Err(tg::error!("unexpected finalization item"));
		};
		let session = self.session(&self.context);
		session
			.compact_process_log(process)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to compact the process log"))?;
		self.index_finished_process(process).await?;
		self.index.complete_finalization(entry).await.map_err(
			|error| tg::error!(!error, %process, "failed to complete the process finalization"),
		)?;

		Ok(())
	}

	async fn index_finished_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		let tg::process::get::Output { data, .. } = self
			.try_get_process_local(id, false)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let data = data.without_tokens();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error = data.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				children.into_iter().collect::<Vec<_>>()
			},
			tg::Either::Right(id) => {
				let id = id.item.clone().into();
				vec![id]
			},
		});
		let mut output = BTreeSet::new();
		if let Some(data) = &data.output {
			data.children(&mut output);
		}
		let output = data
			.output
			.as_ref()
			.map(|_| output.into_iter().collect::<Vec<_>>());
		let children = data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|child| child.process.item.clone())
			.collect();
		let put_process = index::process::put::Arg {
			children: Some(children),
			command: data.command.clone().into(),
			data: Some(data.clone()),
			error: Some(error),
			id: id.clone(),
			log: Some(data.log.clone().map(|log| log.item.into())),
			metadata: tg::process::Metadata::default(),
			output: Some(output),
			parent: None,
			sandbox: None,
			stored: index::process::Stored::default(),
			time_to_touch: self.config.process.time_to_touch,
			touched_at: now,
		};
		self.index
			.batch(index::batch::Arg {
				items: vec![index::batch::Item::PutProcess(put_process)],
			})
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the process in the index"))?;

		Ok(())
	}
}
