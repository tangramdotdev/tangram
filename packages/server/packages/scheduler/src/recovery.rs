use {
	super::{Operation, RemoveRunnerRequestArg, Scheduler, SchedulerState},
	futures::FutureExt as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
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

#[derive(Clone)]
struct Claim {
	registration_index: u64,
	request: EnqueueSandboxRequestArg,
	runner: tg::runner::Id,
}

pub(super) type Statuses = HashMap<tg::sandbox::Id, tg::sandbox::Data, tg::id::BuildHasher>;

impl Scheduler {
	/// Reload the accepted requests from the index. A single server does not recover, because its index writes are not durable when it acknowledges them, and it stops its unfinished sandboxes when it starts.
	pub(super) async fn recover(&self, state: &mut SchedulerState) -> tg::Result<()> {
		if !self.server.config.advanced.multi_server {
			return Ok(());
		}

		// Await indexing, because an acknowledged write may not be applied yet.
		let session = self.server.session(&self.server.context);
		self.server
			.components
			.indexer
			.index(&session, tg::index::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

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
			let sleep = self.server.clock.sleep(self.config.runner_ttl);
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
	/// Record the sandboxes that a runner reports, and release the held requests once every runner has reported or expired.
	pub(super) fn handle_recovery_report(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		registration_index: u64,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) {
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
					registration_index,
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
			let request = RemoveRunnerRequestArg {
				registration_index,
				runner,
			};
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let result = scheduler.remove_runner(request).boxed().await;
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
			if data
				.as_ref()
				.is_some_and(|data| data.status.is_destroying())
			{
				let error = data
					.and_then(|data| data.error)
					.and_then(tg::Either::left)
					.unwrap_or_default();
				let destroy = super::sandbox::DestroySandbox {
					error,
					registration_index: claim.registration_index,
					request: Some(claim.request),
					runner: claim.runner,
					sandbox: id,
				};
				self.send_destroy_sandbox(scheduler, destroy);
			} else {
				if claim.request.stop {
					self.stop_sandbox_when_started(scheduler, id.clone());
				}
				let placed = super::sandbox::Placed {
					generation: 0,
					registration_index: claim.registration_index,
					request: claim.request,
					runner: claim.runner,
				};
				self.insert_placed_sandbox(id, placed);
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
				if request.stop {
					self.stop_sandbox_when_started(scheduler, id);
				}
				self.handle_enqueue_sandbox_request(scheduler, None, request, true);
			}
		}
	}
}
