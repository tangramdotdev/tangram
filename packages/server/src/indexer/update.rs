use {
	super::{Indexer, partition},
	futures::future,
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Indexer {
	pub(super) async fn update_task(
		&self,
		kind: tangram_index::update::Kind,
		config: &crate::config::IndexerUpdate,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let checkpoint = match kind {
			tangram_index::update::Kind::Grant => "indexer.update.grant.batch",
			tangram_index::update::Kind::Node => "indexer.update.node.batch",
			tangram_index::update::Kind::Storage => "indexer.update.storage.batch",
		};
		loop {
			crate::checkpoint!(self.server, checkpoint).await;
			let futures = partition::ranges(partition_start, partition_end, config.concurrency)
				.map(|range| {
					self.server
						.index
						.update_batch(kind, config.batch_size, range.start, range.end)
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
				Ok(output) if output.count == 0 => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(output) => {
					for process in output.processes_with_depth_exceeded {
						self.spawn_finish_process_for_max_depth_task(process);
					}
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to index");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	fn spawn_finish_process_for_max_depth_task(&self, process: tg::process::Id) {
		let indexer = self.clone();
		tokio::spawn(async move {
			if let Err(error) = indexer.finish_process_for_max_depth(&process).await {
				tracing::error!(
					error = %error.trace(),
					%process,
					"failed to finish the process that exceeded the maximum depth"
				);
			}
		});
	}

	async fn finish_process_for_max_depth(&self, id: &tg::process::Id) -> tg::Result<()> {
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
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: Duration::from_secs(10),
		};
		let session = self.server.session(&self.server.context);
		let wait_future = session
			.try_wait_process_future(id, tg::process::wait::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let finish_future = session.send_process_control_request(id, request, options);
		let mut wait_future = pin!(wait_future);
		let mut finish_future = pin!(finish_future);
		tokio::select! {
			output = &mut wait_future => {
				let output = output
					.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
				if output.is_none() {
					return Err(tg::error!(%id, "the process wait ended without output"));
				}
			},
			response = &mut finish_future => {
				let response = response
					.map_err(
						|error| tg::error!(!error, %id, "failed to send the finish process control request"),
					)?
					.map_err(
						|error| tg::error!(!error, %id, "the finish process control request failed"),
					)?;
				response
					.try_unwrap_finish()
					.map_err(|_| tg::error!(%id, "expected a finish response"))?;
			},
		}

		Ok(())
	}
}
