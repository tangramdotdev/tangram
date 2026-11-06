use {
	super::{Operation, RemoveRunnerRequestArg, Scheduler, SchedulerState},
	futures::FutureExt as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

/// The requests that a restarted scheduler holds until its runners report what they have placed.
pub(super) struct Recovery {
	claimed: HashMap<tg::sandbox::Id, Claim, tg::id::BuildHasher>,
	requests: HashMap<tg::sandbox::Id, EnqueueSandboxRequestArg, tg::id::BuildHasher>,
	runners: HashMap<tg::runner::Id, u64, tg::id::BuildHasher>,
}

#[derive(Clone)]
struct Claim {
	registration_index: u64,
	request: EnqueueSandboxRequestArg,
	runner: tg::runner::Id,
}

pub(super) struct Entry {
	claim: Option<Claim>,
	data: Option<tg::sandbox::Data>,
	request: EnqueueSandboxRequestArg,
}

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

		// Hold the requests until the runners report.
		state.recovery = Some(Recovery {
			claimed: HashMap::default(),
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

	/// Get the request of a sandbox that is held for recovery.
	pub(super) fn recovering_request(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> Option<EnqueueSandboxRequestArg> {
		let recovery = self.recovery.as_ref()?;
		let claimed = recovery
			.claimed
			.get(sandbox)
			.map(|claim| claim.request.clone());
		claimed.or_else(|| recovery.requests.get(sandbox).cloned())
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
		let Some(recovery) = self.recovery.take() else {
			return;
		};
		let claims = recovery
			.claimed
			.into_values()
			.map(|claim| (claim.request.clone(), Some(claim)));
		let requests = recovery
			.requests
			.into_values()
			.map(|request| (request, None));
		let entries = claims.chain(requests).collect::<Vec<_>>();
		let server = scheduler.server.clone();
		self.operations.push(
			async move {
				// Read the sandboxes' statuses, because a destroy may have arrived before the restart.
				let ids = entries
					.iter()
					.map(|(request, _)| request.sandbox.clone())
					.collect::<Vec<_>>();
				let result = server.index.try_get_sandboxes(&ids).await.map(|sandboxes| {
					std::iter::zip(entries, sandboxes)
						.map(|((request, claim), sandbox)| Entry {
							claim,
							data: sandbox
								.and_then(|sandbox| sandbox.data)
								.map(|data| data.data),
							request,
						})
						.collect()
				});
				Operation::Recovered { result }
			}
			.boxed(),
		);
	}

	/// Tear down the entries that are being destroyed, and queue the rest of the requests that no runner claimed.
	pub(super) fn handle_recovered(
		&mut self,
		scheduler: &Scheduler,
		result: tg::Result<Vec<Entry>>,
	) {
		let entries = match result {
			Ok(entries) => entries,
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to read the recovered sandboxes");
				return;
			},
		};
		for entry in entries {
			let Entry {
				claim,
				data,
				request,
			} = entry;
			let destroying = data
				.as_ref()
				.is_some_and(|data| data.status.is_destroying());
			match (destroying, claim) {
				(true, Some(claim)) => {
					let error = data
						.and_then(|data| data.error)
						.and_then(tg::Either::left);
					let error = error.unwrap_or_default();
					self.send_destroy_sandbox(
						scheduler,
						claim.runner,
						claim.registration_index,
						request.sandbox,
						error,
					);
				},
				(true, None) => {
					let error = data.and_then(|data| data.error);
					self.finish_destroy_sandbox(scheduler, request, error);
				},
				(false, Some(claim)) => {
					let placed = super::sandbox::Placed {
						registration_index: claim.registration_index,
						request,
						runner: claim.runner,
					};
					let sandbox = placed.request.sandbox.clone();
					self.insert_placed_sandbox(sandbox, placed);
				},
				(false, None) => {
					self.handle_enqueue_sandbox_request(scheduler, None, request, true);
				},
			}
		}
	}
}
