use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, Response, ResponseOutput, Runner, Scheduler,
	},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
};

impl Scheduler {
	pub(super) async fn handle_add_runner_request(&self, id: String, request: AddRunnerRequestArg) {
		if let Some(mut runner) = self.runners.get_mut(&request.runner) {
			runner.capacity = request.capacity;
			runner.heartbeat_at = tokio::time::Instant::now();
			runner.host.clone_from(&request.host);
			runner.reservations.retain(|_, reservation| {
				!matches!(reservation.state, super::ReservationState::Succeeded)
			});
		} else {
			let runner = Runner {
				capacity: request.capacity,
				heartbeat_at: tokio::time::Instant::now(),
				host: request.host.clone(),
				requests: 0,
				reservations: std::collections::HashMap::new(),
			};
			self.runners.insert(request.runner.clone(), runner);
		}
		tokio::spawn({
			let scheduler = self.clone();
			async move {
				scheduler.add_runner_request_task(id, request).await;
			}
		});
	}

	async fn add_runner_request_task(&self, id: String, request: AddRunnerRequestArg) {
		let result = self.handle_add_runner(request).await;
		let (error, output) = match result {
			Ok(output) => {
				let error = None;
				let output = Some(ResponseOutput::AddRunner(output));
				(error, output)
			},
			Err(error) => {
				let error = Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				});
				let output = None;
				(error, output)
			},
		};
		let response = Response { error, id, output };
		self.send_response(response).await;
	}

	async fn handle_add_runner(
		&self,
		request: AddRunnerRequestArg,
	) -> tg::Result<AddRunnerResponseOutput> {
		// Upsert the runner into the database.
		let scheduler_id = self.id.clone();
		let runner_id = request.runner.clone();
		self.server
			.database
			.run(|transaction| {
				let scheduler_id = scheduler_id.clone();
				let runner_id = runner_id.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into runners (id, scheduler, status)
							values ({p}1, {p}2, {p}3)
							on conflict (id) do update set scheduler = {p}2, status = {p}3;
						"
					);
					let params =
						db::params![runner_id.to_string(), scheduler_id.to_string(), "started"];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
					Ok::<_, tg::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to upsert the runner"))?;

		self.server
			.index_tasks
			.spawn(|_| {
				let server = self.server.clone();
				let runner = request.runner.clone();
				let scheduler = self.id.clone();
				async move {
					let arg = tangram_index::batch::Arg {
						put_runners: vec![tangram_index::runner::put::Arg {
							id: runner,
							scheduler: Some(scheduler),
						}],
						..Default::default()
					};
					if let Err(error) = server.index.batch(arg).await {
						tracing::error!(error = %error.trace(), "failed to put the runner to the index");
					}
				}
			})
			.detach();

		let output = AddRunnerResponseOutput {
			runner: request.runner,
		};
		Ok(output)
	}

	pub(super) async fn handle_remove_runner_request(
		&self,
		id: String,
		request: RemoveRunnerRequestArg,
	) {
		tokio::spawn({
			let scheduler = self.clone();
			async move {
				scheduler.remove_runner_request_task(id, request).await;
			}
		});
	}

	pub(super) fn handle_expired_runner(&self, runner: tg::runner::Id) {
		tokio::spawn({
			let scheduler = self.clone();
			async move {
				let request = RemoveRunnerRequestArg {
					runner: runner.clone(),
				};
				if let Err(error) = scheduler.handle_remove_runner(request).await {
					tracing::error!(error = %error.trace(), %runner, "failed to remove the expired runner");
				}
			}
		});
	}

	async fn remove_runner_request_task(&self, id: String, request: RemoveRunnerRequestArg) {
		let result = self.handle_remove_runner(request).await;
		let (error, output) = match result {
			Ok(output) => {
				let error = None;
				let output = Some(ResponseOutput::RemoveRunner(output));
				(error, output)
			},
			Err(error) => {
				let error = Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				});
				let output = None;
				(error, output)
			},
		};
		let response = Response { error, id, output };
		self.send_response(response).await;
	}

	async fn handle_remove_runner(
		&self,
		request: RemoveRunnerRequestArg,
	) -> tg::Result<RemoveRunnerResponseOutput> {
		self.server
			.cleanup_lost_runner(&request.runner, &self.id)
			.await
			.map_err(
				|error| tg::error!(!error, runner = %request.runner, "failed to clean up the lost runner"),
			)?;

		// Mark the runner as stopped and clear its scheduler in the database.
		let runner_id = request.runner.clone();
		let scheduler_id = self.id.clone();
		self.server
			.database
			.run(|transaction| {
				let runner_id = runner_id.clone();
				let scheduler_id = scheduler_id.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							update runners
							set status = {p}1, scheduler = null
							where id = {p}2 and scheduler = {p}3;
						"
					);
					let params =
						db::params!["stopped", runner_id.to_string(), scheduler_id.to_string(),];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
					Ok::<_, tg::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to update the runner"))?;

		let output = RemoveRunnerResponseOutput {
			runner: request.runner,
		};
		Ok(output)
	}
}
