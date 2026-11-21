use {
	super::{Operation, RemoveRunnerRequestArg, Scheduler, SchedulerState},
	crate::Component,
	futures::FutureExt as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_server_core::Server,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

/// How long to wait before trying a read again that the scheduler must not give up.
pub(super) const READ_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// The requests that a restarted scheduler holds until its runners report what they have placed.
pub(super) struct Recovery {
	claimed: HashMap<tg::sandbox::Id, Claim, tg::id::BuildHasher>,
	releasing: bool,
	requests: HashMap<tg::sandbox::Id, EnqueueSandboxRequestArg, tg::id::BuildHasher>,
	runners: HashMap<tg::runner::Id, u64, tg::id::BuildHasher>,
}

/// A held request whose boot a runner reported, which belongs to the runner and not to the runner's connection, because a runner that reconnects still holds the boot.
#[derive(Clone)]
struct Claim {
	request: EnqueueSandboxRequestArg,
	runner: tg::runner::Id,
}

pub(super) type Statuses = HashMap<tg::sandbox::Id, tg::sandbox::Data, tg::id::BuildHasher>;

impl Scheduler {
	/// Reload the accepted requests from the index. A single server does not reload them, because its index writes are not durable when it acknowledges them and its clients do not survive its restart, and it ends what its previous run left when it starts instead.
	pub(super) async fn recover(&self, state: &mut SchedulerState) -> tg::Result<()> {
		if !self.server.config.advanced.multi_server {
			return Ok(());
		}

		// Await indexing, because an acknowledged write may not be applied yet.
		self.index().await?;

		// Reload the requests.
		let sandboxes = self
			.server
			.index
			.get_scheduler_sandboxes(&self.id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?;
		let mut requests = HashMap::default();
		for sandbox in sandboxes {
			let mut request = serde_json::from_slice::<EnqueueSandboxRequestArg>(&sandbox.request)
				.map_err(
					|error| tg::error!(!error, sandbox = %sandbox.id, "failed to deserialize the request"),
				)?;
			let token = self
				.server
				.components
				.authentication
				.create_sandbox_authentication_token(&self.server, sandbox.id.clone())?;
			request.token = Some(token);
			requests.insert(sandbox.id, request);
		}

		// Get the registered runners, which must report their sandboxes or expire.
		let ids = self
			.server
			.index
			.get_scheduler_runners(&self.id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's runners"))?;
		let mut runners = HashMap::default();
		for id in ids {
			let runner =
				self.server.index.try_get_runner(&id).await.map_err(
					|error| tg::error!(!error, runner = %id, "failed to get the runner"),
				)?;
			if let Some(runner) = runner {
				runners.insert(id, runner.registration);
			}
		}
		tracing::info!(
			requests = requests.len(),
			runners = runners.len(),
			"recovering the scheduler"
		);

		// Serve the holders of the held sandboxes, so that a request to one is told that this scheduler does not hold it until the runners report.
		for (id, request) in &requests {
			let process = request
				.process
				.as_ref()
				.and_then(|process| process.id.clone());
			state.serve_sandbox_holder(self, id, process);
		}

		// Hold the requests until the runners report.
		state.recovery = Some(Recovery {
			claimed: HashMap::default(),
			releasing: false,
			requests,
			runners,
		});
		if state
			.recovery
			.as_ref()
			.is_some_and(|recovery| recovery.runners.is_empty())
		{
			state.release_recovery(self);
		} else {
			let sleep = self.server.clock.sleep(self.config.runner_time_to_live);
			state.operations.push(
				async move {
					sleep.await;
					Operation::RecoveryExpired
				}
				.boxed(),
			);
		}

		Ok(())
	}
}

impl SchedulerState {
	/// Record the sandboxes that a runner reports, and release the held requests once every runner has reported or expired. A runner that restarted lost the boots that it claimed before, so they end as failed.
	pub(super) fn handle_recovery_report(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		restarted: bool,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) {
		let Some(recovery) = &mut self.recovery else {
			return;
		};

		// End the boots that a runner that restarted claimed before.
		if restarted {
			let lost = recovery
				.claimed
				.iter()
				.filter(|(_, claim)| claim.runner == *runner)
				.map(|(id, _)| id.clone())
				.collect::<Vec<_>>();
			let lost = lost
				.into_iter()
				.map(|id| recovery.claimed.remove(&id).unwrap().request)
				.collect::<Vec<_>>();
			for request in lost {
				self.fail_claimed_sandbox(scheduler, request);
			}
		}

		// Claim the held requests whose boots the runner reports.
		let Some(recovery) = &mut self.recovery else {
			return;
		};
		for sandbox in sandboxes {
			if recovery
				.requests
				.get(&sandbox.id)
				.is_some_and(|request| request.boot == sandbox.boot)
			{
				let request = recovery.requests.remove(&sandbox.id).unwrap();
				let claim = Claim {
					request,
					runner: runner.clone(),
				};
				recovery.claimed.insert(sandbox.id.clone(), claim);
			}
		}
		recovery.runners.remove(runner);
		if recovery.runners.is_empty() {
			self.release_recovery(scheduler);
		}
	}

	/// Whether a sandbox's request is held for recovery.
	pub(super) fn is_recovering(&self, sandbox: &tg::sandbox::Id) -> bool {
		self.recovery.as_ref().is_some_and(|recovery| {
			recovery.claimed.contains_key(sandbox) || recovery.requests.contains_key(sandbox)
		})
	}

	/// Get the sandboxes whose requests are held for recovery.
	pub(super) fn get_recovering_sandboxes(&self) -> Vec<tg::sandbox::Id> {
		self.recovery.as_ref().map_or_else(Vec::new, |recovery| {
			recovery
				.claimed
				.keys()
				.chain(recovery.requests.keys())
				.cloned()
				.collect()
		})
	}

	/// Get a request that is held for recovery.
	pub(super) fn try_get_recovering_request_mut(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<&mut EnqueueSandboxRequestArg> {
		let recovery = self.recovery.as_mut()?;
		if let Some(claim) = recovery.claimed.get_mut(sandbox) {
			return Some(&mut claim.request);
		}
		recovery.requests.get_mut(sandbox)
	}

	/// Expire the runners that did not report in time, and release the held requests.
	pub(super) fn handle_recovery_expired(&mut self, scheduler: &Scheduler) {
		let Some(recovery) = &mut self.recovery else {
			return;
		};
		let runners = std::mem::take(&mut recovery.runners);
		for (runner, registration_index) in runners {
			tracing::warn!(%runner, "the runner did not report after the scheduler restarted");

			// Leave the restarts that are queued for the runner to this scheduler, which holds them.
			let queued = recovery
				.requests
				.iter()
				.filter(|(_, request)| request.boot > 0 && request.runner.as_ref() == Some(&runner))
				.map(|(id, _)| id.clone())
				.collect::<Vec<_>>();
			let request = RemoveRunnerRequestArg {
				registration_index,
				runner,
			};
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let result = scheduler.remove_runner(request, queued).boxed().await;
					Operation::RemoveRunner { id: None, result }
				}
				.boxed(),
			);
		}
		self.release_recovery(scheduler);
	}

	fn release_recovery(&mut self, scheduler: &Scheduler) {
		let Some(recovery) = &mut self.recovery else {
			return;
		};
		if recovery.releasing {
			return;
		}
		recovery.releasing = true;
		let ids = recovery
			.claimed
			.keys()
			.chain(recovery.requests.keys())
			.cloned()
			.collect::<Vec<_>>();
		let server = scheduler.server.clone();
		self.operations.push(
			async move {
				// Read the sandboxes' statuses, because a destroy may have arrived before the restart. Keep trying, because the held requests are lost if the read is given up.
				let sandboxes = loop {
					match server.index.try_get_sandboxes(&ids).await {
						Ok(sandboxes) => break sandboxes,
						Err(error) => {
							tracing::error!(error = %error.trace(), "failed to read the recovered sandboxes");
							tokio::time::sleep(READ_RETRY_INTERVAL).await;
						},
					}
				};
				let statuses = std::iter::zip(ids, sandboxes)
					.filter_map(|(id, sandbox)| Some((id, sandbox?.data?.data)))
					.collect();
				Operation::Recovered { statuses }
			}
			.boxed(),
		);
	}

	/// Tear down the requests that are being destroyed, and queue the rest of the requests that no runner claimed.
	pub(super) fn handle_recovered(&mut self, scheduler: &Scheduler, mut statuses: Statuses) {
		let Some(recovery) = self.recovery.take() else {
			return;
		};
		for (id, claim) in recovery.claimed {
			let data = statuses.remove(&id);
			let destroying = data
				.as_ref()
				.is_some_and(|data| data.status.is_destroying());
			let error = data.and_then(|data| data.error);
			let registration_index = self
				.runners
				.entries
				.get(&claim.runner)
				.map(|runner| runner.registration_index);
			match registration_index {
				// Destroy a boot that is being destroyed on its runner.
				Some(registration_index) if destroying => {
					let destroy = super::sandbox::DestroySandbox {
						error: error.and_then(tg::Either::left).unwrap_or_default(),
						registration_index,
						request: Some(claim.request),
						runner: claim.runner,
						sandbox: id,
					};
					self.send_destroy_sandbox(scheduler, destroy);
				},

				// Treat the runner's report of the boot as its yes, which writes the handoff that this scheduler may not have written before it restarted.
				Some(registration_index) => {
					let runner = super::placement::RunnerRef {
						id: claim.runner,
						registration_index,
					};
					self.accept_claimed_sandbox(scheduler, &id, claim.request, runner);
				},

				// Record the destruction of a boot that is being destroyed and whose runner was declared dead since its report.
				None if destroying => {
					self.finish_destroy_sandbox(scheduler, claim.request, error);
				},

				// End a boot as failed whose runner was declared dead since its report, because the boot is lost with the runner.
				None => {
					self.fail_claimed_sandbox(scheduler, claim.request);
				},
			}
		}
		for (id, request) in recovery.requests {
			let data = statuses.remove(&id);
			if data
				.as_ref()
				.is_some_and(|data| data.status.is_destroying())
			{
				let error = data.and_then(|data| data.error);
				self.finish_destroy_sandbox(scheduler, request, error);
			} else {
				self.handle_enqueue_sandbox_request(scheduler, None, request, true);
			}
		}
	}

	/// Accept a boot that a runner claimed, replacing an entry of an older boot.
	fn accept_claimed_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		request: EnqueueSandboxRequestArg,
		runner: super::placement::RunnerRef,
	) {
		if self.sandboxes.entries.contains_key(id) {
			self.remove_sandbox(scheduler, id);
		}
		let boot = request.boot;
		let parent = request.parent.clone();
		let state = super::sandbox::State::Accepted {
			runner: runner.clone(),
		};
		let sandbox = super::sandbox::Sandbox::new(boot, parent, Some(request), state);
		self.sandboxes.entries.insert(id.clone(), sandbox);
		self.accept_sandbox(scheduler, id, runner);
	}

	/// End a boot as failed that a runner claimed and lost.
	fn fail_claimed_sandbox(&mut self, scheduler: &Scheduler, request: EnqueueSandboxRequestArg) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let error =
					tg::error!(sandbox = %request.sandbox, "the sandbox's runner is unavailable");
				let result = scheduler
					.fail_sandbox(&request, error.to_data_or_id())
					.await;
				Operation::Finished {
					context: "failed to record the failed sandbox",
					result,
				}
			}
			.boxed(),
		);
	}
}

impl Component {
	/// End the boots in this single server's queue, and the sandboxes of the runners other than this server's that registered with it, whose registrations can no longer expire after the restart. Stop each, or destroy it if its ttl is zero. This server's runner ends its own sandboxes when it registers.
	pub(super) async fn end_previous_run(&self, server: &Server) -> tg::Result<()> {
		// Get the scheduler's ID. A single server applies its index writes in its own process, so none of its previous run's writes are pending.
		let id = Self::scheduler_id(&server.config.scheduler)?;
		let error = tg::error::Data {
			code: Some(tg::error::Code::Internal),
			message: Some("server restarted".to_owned()),
			..Default::default()
		};

		// End the running boots in the queue, which also removes them from the queue.
		let ids = server
			.index
			.get_scheduler_sandboxes(&id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<Vec<_>>();
		let sandboxes = server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?;
		for (id, sandbox) in std::iter::zip(ids, sandboxes) {
			let Some(data) = sandbox
				.and_then(|sandbox| sandbox.data)
				.map(|data| data.data)
			else {
				continue;
			};
			if data.status.is_stopped_or_destroyed() {
				continue;
			}
			let result = if data
				.time_to_live
				.is_some_and(|time_to_live| time_to_live.is_zero())
			{
				self.destroy_runner_sandbox(server, &id, Some(error.clone()))
					.boxed()
					.await
			} else {
				self.stop_runner_sandbox(server, &id, error.clone())
					.boxed()
					.await
			};
			if let Err(error) = result {
				tracing::error!(sandbox = %id, error = %error.trace(), "failed to end the queued sandbox");
			}
		}

		// End the sandboxes of the other runners with their registrations.
		let local = server.components.runner.local_runner_id(server);
		let runners = server
			.index
			.get_scheduler_runners(&id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's runners"))?;
		for runner in runners {
			if local.as_ref() == Some(&runner) {
				continue;
			}
			let Some(indexed) = server
				.index
				.try_get_runner(&runner)
				.await
				.map_err(|error| tg::error!(!error, %runner, "failed to get the runner"))?
			else {
				continue;
			};
			let registration = tangram_index::runner::Registration {
				index: indexed.registration,
				runner: runner.clone(),
				scheduler: id.clone(),
			};
			let arg = super::runner::EndRunnerSandboxesArg {
				accepted: None,
				checkpoint: "scheduler.runner.recovered",
				directories: None,
				error: error.clone(),
				held: Vec::new(),
				queued: Vec::new(),
			};
			let result = self
				.end_runner_sandboxes(server, &registration, arg)
				.boxed()
				.await;
			if let Err(error) = result {
				tracing::error!(%runner, error = %error.trace(), "failed to end the runner's sandboxes");
			}
		}

		Ok(())
	}
}
