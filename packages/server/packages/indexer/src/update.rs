use {
	super::{Indexer, partition},
	futures::future,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::prelude::*,
};

impl Indexer {
	pub(super) async fn update_task(
		&self,
		kind: tangram_index::update::Kind,
		config: &tangram_server_core::config::IndexerUpdate,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		if partition_start == partition_end {
			stopper.wait().await;
			return Ok(());
		}
		let checkpoint = match kind {
			tangram_index::update::Kind::Permission => "indexer.update.permission.batch",
			tangram_index::update::Kind::StorageAndMetadata => {
				"indexer.update.storage_and_metadata.batch"
			},
			tangram_index::update::Kind::Usage => "indexer.update.usage.batch",
		};
		let finish_tasks = tangram_futures::task::Map::<tg::process::Id, ()>::default();
		let poll_interval = self.server.config.indexer.updates.poll_interval;
		let retry = self.server.config.indexer.retry.clone().into();
		loop {
			if stopper.stopped() {
				finish_tasks.wait().await;
				return Ok(());
			}
			tangram_server_core::checkpoint!(self.server, checkpoint).await;
			let output = tangram_futures::retry(&retry, || async {
				let futures = partition::ranges(partition_start, partition_end, config.concurrency)
					.map(|range| {
						self.server.index.update_batch(
							kind,
							config.batch_size,
							range.start,
							range.end,
						)
					});
				let result = future::try_join_all(futures).await.map(|outputs| {
					outputs.into_iter().fold(
						tangram_index::update::Output::default(),
						|mut output, next| {
							output.merge(next);
							output
						},
					)
				});
				match result {
					Ok(output) => Ok(ControlFlow::Break(output)),
					Err(error) => {
						tracing::error!(error = %error.trace(), "failed to index");

						Ok(ControlFlow::Continue(error))
					},
				}
			})
			.await?;
			if output.count > 0 {
				tracing::debug!(
					?kind,
					count = output.count,
					"processed an index update batch"
				);
			}
			if output.count == 0 {
				tokio::select! {
					() = stopper.wait() => {},
					() = tokio::time::sleep(poll_interval) => {},
				}
			} else {
				self.component.index_changed.notify_waiters();
				for process in output.processes_with_depth_exceeded {
					let indexer = self.clone();
					finish_tasks.get_or_spawn(process.clone(), |_| async move {
						if let Err(error) = indexer.finish_process_for_max_depth(&process).await {
							tracing::error!(error = %error.trace(), %process, "failed to finish the process that exceeded the maximum depth");
						}
					});
				}
			}
		}
	}

	async fn finish_process_for_max_depth(&self, id: &tg::process::Id) -> tg::Result<()> {
		let session = self.server.session(&self.server.context);
		let process = &session.server.components.process;

		// Subscribe before sending the request, so that the request is sent again to a new holder of the process.
		let mut wakeups = process
			.create_process_status_wakeup_stream(&session, id, None, None)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to subscribe to the process"))?;

		// Finish the process at its holder.
		let error = tg::error::Data {
			message: Some("maximum depth exceeded".into()),
			..Default::default()
		};
		let request = tg::process::control::ServerRequestArg::Finish(
			tg::process::control::FinishServerRequestArg {
				error: Some(error),
				exit: 1,
			},
		);
		let retry = self.server.config.control.request_retry.clone().into();
		let timeout = self.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let arg = tangram_server_core::process::HolderArg {
			checkpoint: None,
			deadline: session.server.control_read_deadline(),
			exhaustive: true,
			id: tangram_server_core::control::id(),
			mode: tangram_server_core::holder::Mode::Write,
			options,
			request,
			wakeups: &mut wakeups,
		};
		let output = process
			.request_process_holder(&session, id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to finish the process"))?
			.ok_or_else(|| tg::error!(%id, "the process status wakeup stream ended"))?;

		// A finished process has nothing left to finish, and a process that neither the index nor a holder has does not exist.
		let finished = output
			.indexed
			.and_then(|(process, _)| process.status)
			.is_some_and(|status| status.is_finished());
		match output.reply {
			Some(Ok(tg::process::control::ClientResponseOutput::Finish(_))) => Ok(()),
			Some(Ok(_)) => Err(tg::error!(%id, "expected a finish response")),
			Some(Err(_)) | None if finished => Ok(()),
			Some(Err(error)) => {
				Err(tg::error!(!error, %id, "the finish process control request failed"))
			},
			None => Err(tg::error!(%id, "failed to find the process")),
		}
	}
}
