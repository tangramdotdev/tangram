use {
	super::{
		EndRunnerSandboxRequestArg, EndRunnerSandboxResponseOutput, Operation, ResponseOutput,
		Scheduler, SchedulerState,
		report::{Authorization, Authorized, EndedBoot, Held, Report},
	},
	futures::FutureExt as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

impl SchedulerState {
	/// Handle a runner's report that a boot that it held ended, which the runner sends after it finished every process of the boot. Authorize the report, and act on nothing in a report that is not authorized. Treat a report of a boot whose offer to the runner is unanswered as the runner's yes. Forget the boot, which frees its capacity, or remember a boot on the shortcut path that this scheduler did not know as ended, so that a late report of the sandbox does not track it again. Then, in an operation, record the end.
	pub(super) fn handle_end_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: EndRunnerSandboxRequestArg,
	) {
		let EndRunnerSandboxRequestArg { end, runner } = request;
		let boot = end.boot;
		let sandbox = end.sandbox.clone();

		// Authorize the report, which names the boot's processes.
		let processes = end.processes.iter().collect();
		let report = Report {
			boot,
			processes,
			proof: end.proof.as_ref(),
			runner: &runner,
			sandbox: &sandbox,
		};
		let Authorized { held, processes } = match self.authorize_runner_report(&report) {
			Authorization::Authorized(authorized) => authorized,
			Authorization::NotReady => {
				let output = EndRunnerSandboxResponseOutput {
					retry: Some(tg::control::Retry::NotReady),
				};
				let output = ResponseOutput::EndRunnerSandbox(output);
				let response = super::request::Response::new(id, Ok(output));
				scheduler.send_response(self, response);
				return;
			},
			Authorization::Refused(error) => {
				let response = super::request::Response::new(id, Err(error));
				scheduler.send_response(self, response);
				return;
			},
		};

		// Record the runner's yes to a boot whose offer is unanswered, because the runner reports only a boot that it held, and forget the boot.
		match held {
			Held::Accepted | Held::Offered(_) => {
				if let Held::Offered(placement) = held {
					let runner = placement.runner().clone();
					self.remove_reservation(&sandbox, &placement, true);
					self.accept_sandbox(scheduler, &sandbox, runner);
				}
				self.remove_sandbox(scheduler, &sandbox);
				self.stop_serving_sandbox_holder(&sandbox);
				self.queue.wake();
			},
			Held::Derived => {
				let ended = EndedBoot {
					processes: BTreeSet::new(),
					runner: runner.clone(),
				};
				self.sandboxes.ended.insert(sandbox.clone(), boot, ended);
			},
			Held::Ended => {},
		}

		// Record the end in an operation.
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.end_runner_sandbox(&runner, end, processes).await;
				Operation::EndRunnerSandbox { id, result }
			}
			.boxed(),
		);
	}
}

impl Scheduler {
	/// Record the end of the boot of an authorized report, unless the runner recorded the end through the boot's control stream, with the runner's data and with only the boot's processes that this scheduler offered into it, at the runner's boot and sequence, and remove the boot from this scheduler's queue in one write, and publish the sandbox's status.
	async fn end_runner_sandbox(
		&self,
		runner: &tg::runner::Id,
		end: tg::runner::control::EndSandboxClientRequestArg,
		authorized: BTreeSet<tg::process::Id>,
	) -> tg::Result<EndRunnerSandboxResponseOutput> {
		let tg::runner::control::EndSandboxClientRequestArg {
			boot,
			created_at,
			data,
			processes,
			proof: _,
			recorded,
			sandbox,
			sequence,
		} = end;
		let session = self.server.session(&self.server.context);

		// Record only the boot's processes that this scheduler offered into the boot.
		let processes = processes
			.into_iter()
			.filter(|process| authorized.contains(process))
			.collect();

		// Let tests observe the scheduler handling an authorized report.
		tangram_server_core::checkpoint!(self.server, "scheduler.sandbox.end", %sandbox, boot)
			.await;

		// Leave an end that the runner recorded at the sandbox's home through the boot's control stream.
		if recorded {
			return Ok(EndRunnerSandboxResponseOutput { retry: None });
		}

		// Record the end.
		let account = self
			.server
			.components
			.usage
			.usage_account(&session, &data.data.owner)
			.await?;
		let now = self.server.clock.unix_timestamp();
		let mut data = data;
		data.location = None;
		data.tokens = tg::authorization::Tokens::default();
		let put = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(data),
			id: sandbox.clone(),
			location: None,
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(sandbox.clone()),
			process: None,
			processes: Some(processes),
			runner: Some(runner.clone()),
			scheduler: None,
			sequence,
			stopped_boot: None,
			stopped_usage: None,
			touched_at: now,
		};
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot,
			sandbox: sandbox.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(put)),
				tangram_index::Item::DeleteSchedulerSandbox(Box::new(delete)),
			],
		};
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to record the end of the sandbox"),
			)?;

		// Publish the sandbox's status.
		self.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&self.server, &sandbox);

		Ok(EndRunnerSandboxResponseOutput { retry: None })
	}
}
