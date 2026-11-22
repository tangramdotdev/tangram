use {
	super::{
		EndRunnerSandboxRequestArg, EndRunnerSandboxResponseOutput, Operation, Scheduler,
		SchedulerState,
		report::{Authorization, Authorized, EndedBoot, Held, Report},
	},
	futures::FutureExt as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl SchedulerState {
	/// Handle a runner's report that a boot that it held ended, which the runner sends after it finished every process of the boot. Authorize the report, and act on nothing in a report that is not authorized. Treat a report of a boot whose offer to the runner is unanswered as the runner's yes. Forget the boot, which frees its capacity, or remember a boot on the shortcut path that this scheduler did not know as ended, so that a late report of the sandbox does not track it again. Then, in an operation, record the end, and ask the runner to send the report again if the write fails, which the memory of the ended boot authorizes.
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
			boot: Some(boot),
			processes,
			proof: end.proof.as_ref(),
			runner: &runner,
			sandbox: &sandbox,
		};
		let Authorized { held, processes } = match self.authorize_runner_report(&report) {
			Authorization::Authorized(authorized) => authorized,
			Authorization::NotReady => {
				let result = Ok(EndRunnerSandboxResponseOutput {
					retry: Some(tg::control::Retry::NotReady),
				});
				self.acknowledge_ended_runner_sandbox(scheduler, id, sandbox, boot, result);
				return;
			},
			Authorization::Refused(error) => {
				self.acknowledge_ended_runner_sandbox(scheduler, id, sandbox, boot, Err(error));
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
				self.remove_ended_sandbox(scheduler, &sandbox);
				self.stop_serving_sandbox_holder(&sandbox);
				self.queue.wake();
			},
			Held::Derived => {
				let ended = EndedBoot {
					declared: false,
					processes: BTreeSet::new(),
					runner: runner.clone(),
				};
				self.sandboxes.ended.insert(sandbox.clone(), boot, ended);
			},
			Held::Ended | Held::Removing => {},
		}

		// Record the end in an operation.
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.end_runner_sandbox(&runner, end, processes)
					.await
					.or_else(|error| {
						tracing::warn!(error = %error.trace(), %sandbox, "failed to record the end of the sandbox");
						Ok(EndRunnerSandboxResponseOutput {
							retry: Some(tg::control::Retry::NotReady),
						})
					});
				Operation::EndRunnerSandbox { id, result }
			}
			.boxed(),
		);
	}
}

impl SchedulerState {
	/// Answer a report of the end of a boot that this scheduler cannot authorize. Acknowledge it without acting on it if the index records the sandbox as destroyed or at a later boot, such as a sandbox that was destroyed while this scheduler was down, so that the runner finishes its end, and give the answer of the authorization otherwise.
	fn acknowledge_ended_runner_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		sandbox: tg::sandbox::Id,
		boot: u64,
		result: tg::Result<EndRunnerSandboxResponseOutput>,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let indexed = scheduler.server.index.try_get_sandbox(&sandbox).await;
				let ended = indexed
					.ok()
					.flatten()
					.and_then(|indexed| indexed.data)
					.is_some_and(|data| data.data.status.is_destroyed() || data.data.boot > boot);
				let result = if ended {
					Ok(EndRunnerSandboxResponseOutput { retry: None })
				} else {
					result
				};
				Operation::EndRunnerSandbox { id, result }
			}
			.boxed(),
		);
	}
}

impl Scheduler {
	/// Remove the boot of an authorized report from this scheduler's queue, and record the end of the boot unless the runner recorded it through the boot's control stream, with the runner's data and with only the boot's processes that this scheduler offered into it, at the runner's boot and sequence, in one write, and publish the sandbox's status.
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

		// Remove the boot from this scheduler's queue, and record the end unless the runner recorded it at the sandbox's home through the boot's control stream, in one write.
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot,
			sandbox: sandbox.clone(),
		};
		let mut items = vec![tangram_index::Item::DeleteSchedulerSandbox(Box::new(
			delete,
		))];
		if !recorded {
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
			items.insert(0, tangram_index::Item::PutSandbox(Box::new(put)));
		}
		let arg = tangram_index::write::Arg { items };
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to record the end of the sandbox"),
			)?;

		// Publish the sandbox's status.
		if !recorded {
			self.server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(&self.server, &sandbox);
		}

		Ok(EndRunnerSandboxResponseOutput { retry: None })
	}
}
