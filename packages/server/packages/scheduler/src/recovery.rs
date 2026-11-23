use {
	super::{Operation, Scheduler, SchedulerState},
	crate::Component,
	futures::FutureExt as _,
	std::collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	tangram_client::prelude::*,
	tangram_server_core::Server,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

#[cfg(test)]
pub(super) mod tests;

/// How long to wait before trying a read again that the scheduler must not give up.
pub(super) const READ_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

pub(super) type Statuses =
	HashMap<tg::sandbox::Id, tangram_index::sandbox::Sandbox, tg::id::BuildHasher>;

/// The requests that a restarted scheduler holds until its runners report what they have placed.
#[derive(Default)]
pub(super) struct Recovery {
	/// The running boots that the index records on each runner, which this scheduler rebuilt from the index and which wait for the runner's report.
	boots: HashMap<tg::runner::Id, Vec<RecoveredBoot>, tg::id::BuildHasher>,

	claimed: HashMap<tg::sandbox::Id, Claim, tg::id::BuildHasher>,

	/// The processes of the boots of the held requests, which this scheduler rebuilt from the index.
	processes: HashMap<tg::sandbox::Id, BTreeSet<tg::process::Id>, tg::id::BuildHasher>,

	releasing: bool,

	requests: HashMap<tg::sandbox::Id, EnqueueSandboxRequestArg, tg::id::BuildHasher>,

	runners: HashSet<tg::runner::Id, tg::id::BuildHasher>,

	spawns: Vec<super::spawn::Spawn>,
}

/// A boot that the index records on a runner of a restarted scheduler, which the scheduler rebuilt from the index.
pub(super) struct RecoveredBoot {
	pub(super) boot: u64,

	/// The boot's processes, which are its initial process and the processes that the scheduler offered into it.
	pub(super) processes: BTreeSet<tg::process::Id>,

	pub(super) sandbox: tg::sandbox::Id,
}

/// A held request whose boot a runner reported, which belongs to the runner and not to the runner's connection, because a runner that reconnects still holds the boot.
#[derive(Clone)]
struct Claim {
	request: EnqueueSandboxRequestArg,
	runner: tg::runner::Id,
}

/// The decision of a restarted scheduler on a saved boot that no runner claimed, from the index read after the scheduler awaited indexing.
pub(super) enum SavedBoot {
	/// The sandbox is being destroyed.
	Destroying {
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	},

	/// The index shows the boot ended or the sandbox destroyed or at a later boot, so the boot is never offered again.
	Ended,

	/// No runner wrote the boot, so this scheduler still holds it.
	Unwritten,

	/// A runner wrote the running boot, so the runner holds or held it.
	Written { runner: tg::runner::Id },
}

/// Decide a boot that this scheduler saved or offered and that no runner holds as far as this scheduler knows, from the index's record of its sandbox, which is the one decision of a restarted scheduler's saved boots and of a restarted runner's unanswered offers and claims.
pub(super) fn decide_saved_boot(
	boot: u64,
	indexed: Option<&tangram_index::sandbox::Sandbox>,
) -> SavedBoot {
	let Some((indexed, data)) =
		indexed.and_then(|indexed| Some((indexed, &indexed.data.as_ref()?.data)))
	else {
		return SavedBoot::Unwritten;
	};
	if data.status.is_destroying() {
		let error = data.error.clone();
		return SavedBoot::Destroying { error };
	}
	if data.boot > boot || (data.boot == boot && data.status.is_stopped_or_destroyed()) {
		return SavedBoot::Ended;
	}
	if data.boot == boot
		&& indexed.sequence >= tangram_index::sandbox::put::FIRST_HOLDER_WRITE
		&& let Some(runner) = &indexed.runner
	{
		let runner = runner.clone();
		return SavedBoot::Written { runner };
	}
	SavedBoot::Unwritten
}

impl Recovery {
	/// Whether this scheduler waits for a runner to report after it restarted.
	pub(super) fn has_runner(&self, runner: &tg::runner::Id) -> bool {
		self.runners.contains(runner)
	}
}

impl SavedBoot {
	/// Whether the index shows that a runner wrote the boot and did not end it, so that the runner holds or held it and only an end of the runner's boots ends it.
	pub(super) fn is_written(&self) -> bool {
		matches!(self, Self::Destroying { .. } | Self::Written { .. })
	}
}

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

		// Reload the spawns into existing sandboxes.
		let spawns = self.get_saved_spawns().await?;

		// Get the runners that belong to this scheduler, which must report their sandboxes or expire.
		let ids = self
			.server
			.index
			.get_scheduler_runners(&self.id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's runners"))?;
		let mut runners = HashSet::default();
		for id in ids {
			let runner =
				self.server.index.try_get_runner(&id).await.map_err(
					|error| tg::error!(!error, runner = %id, "failed to get the runner"),
				)?;
			if runner.is_some() {
				runners.insert(id);
			}
		}

		// Rebuild the boots that the index records on the runners, with their processes, so that this scheduler authorizes the runners' reports of them as before the restart. Remember the ended boots at once, whose ends their runners may still report, as declared if this scheduler ended them with an end numbered zero, so that it tells their runners when they connect again, and hold the running boots until their runners report.
		let mut boots = HashMap::<_, Vec<_>, tg::id::BuildHasher>::default();
		for runner in &runners {
			for (recovered, running, declared) in
				self.get_recovered_boots(runner, &requests).await?
			{
				if running {
					boots.entry(runner.clone()).or_default().push(recovered);
				} else {
					let ended = super::report::EndedBoot {
						declared,
						processes: recovered.processes,
						runner: runner.clone(),
					};
					state
						.sandboxes
						.ended
						.insert(recovered.sandbox, recovered.boot, ended);
				}
			}
		}

		// Rebuild the processes of the boots of the held requests.
		let mut processes = HashMap::default();
		for (id, request) in &requests {
			let recovered = self.get_boot_processes(id, request.boot).await?;
			processes.insert(id.clone(), recovered);
		}

		tracing::info!(
			requests = requests.len(),
			runners = runners.len(),
			spawns = spawns.len(),
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
			boots,
			claimed: HashMap::default(),
			processes,
			releasing: false,
			requests,
			runners,
			spawns,
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

	/// Get the boots that the index records on a runner, except the boots of the held requests, which the runner's report claims, each with its processes, whether it is running, and whether this scheduler ended it with an end numbered zero. Finish again the processes of a boot that this scheduler ended, whose statuses and logs it may not have finished before it restarted.
	async fn get_recovered_boots(
		&self,
		runner: &tg::runner::Id,
		requests: &HashMap<tg::sandbox::Id, EnqueueSandboxRequestArg, tg::id::BuildHasher>,
	) -> tg::Result<Vec<(RecoveredBoot, bool, bool)>> {
		let ids = self
			.server
			.index
			.get_runner_sandboxes(runner)
			.await
			.map_err(|error| tg::error!(!error, %runner, "failed to get the runner's sandboxes"))?;
		let indexed =
			self.server.index.try_get_sandboxes(&ids).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner's sandboxes"),
			)?;
		let mut boots = Vec::new();
		for (id, indexed) in std::iter::zip(ids, indexed) {
			let Some(indexed) = indexed else {
				continue;
			};
			if indexed.runner.as_ref() != Some(runner) {
				continue;
			}
			let Some(data) = &indexed.data else {
				continue;
			};
			let boot = data.data.boot;
			if requests
				.get(&id)
				.is_some_and(|request| request.boot == boot)
			{
				continue;
			}
			let declared = indexed.ends.get(&boot).is_some_and(|end| end.sequence == 0);
			if declared {
				self.component
					.finish_ended_boot_processes(&self.server, &id, boot)
					.await;
			}
			let running = matches!(
				data.data.status,
				tg::sandbox::Status::Started
					| tg::sandbox::Status::Starting
					| tg::sandbox::Status::Stopping
			);
			let processes = self.get_boot_processes(&id, boot).await?;
			let recovered = RecoveredBoot {
				boot,
				processes,
				sandbox: id,
			};
			boots.push((recovered, running, declared));
		}

		Ok(boots)
	}

	/// Get the processes that the index records in a boot of a sandbox.
	async fn get_boot_processes(
		&self,
		sandbox: &tg::sandbox::Id,
		boot: u64,
	) -> tg::Result<BTreeSet<tg::process::Id>> {
		let length = self
			.server
			.index
			.try_get_sandbox_processes_count(sandbox)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to get the sandbox's processes"))?
			.unwrap_or_default();
		let ids = self
			.server
			.index
			.try_get_sandbox_processes(sandbox, std::io::SeekFrom::Start(0), length)
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to get the sandbox's processes"))?
			.unwrap_or_default();
		let indexed = self
			.server
			.index
			.try_get_processes_with_data(&ids)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to get the sandbox's processes"),
			)?;
		let processes = std::iter::zip(ids, indexed)
			.filter(|(_, indexed)| {
				indexed
					.as_ref()
					.is_some_and(|(indexed, _)| indexed.boot.is_none_or(|indexed| indexed == boot))
			})
			.map(|(id, _)| id)
			.collect();

		Ok(processes)
	}
}

impl SchedulerState {
	/// Record the boots that a runner reports, once its connection is applied, and release the held requests once every runner has reported or expired. A runner that restarted lost the boots that it claimed before, which its first connection decided from the index: forget a boot that the runner wrote, which the connection ended, and return every other boot to the held requests, which the release decides again. Return the running boots that this scheduler rebuilt from the index on the runner, on the runner's first report.
	pub(super) fn handle_recovery_report(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		restarted: bool,
		sandboxes: &[tg::runner::control::ArgSandbox],
		decided: &BTreeMap<tg::sandbox::Id, SavedBoot>,
	) -> Vec<RecoveredBoot> {
		let Some(recovery) = &mut self.recovery else {
			return Vec::new();
		};
		let boots = recovery.boots.remove(runner).unwrap_or_default();

		// Decide the boots that a runner that restarted claimed before.
		if restarted {
			let lost = recovery
				.claimed
				.iter()
				.filter(|(_, claim)| claim.runner == *runner)
				.map(|(id, _)| id.clone())
				.collect::<Vec<_>>();
			for id in lost {
				let claim = recovery.claimed.remove(&id).unwrap();
				let written = decided.get(&id).is_some_and(SavedBoot::is_written);
				if !written {
					recovery.requests.insert(id, claim.request);
				}
			}
		}

		// Claim the held requests whose boots the runner reports.
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
		self.release_recovery(scheduler);

		boots
	}

	/// Stop waiting for the report of a runner that is declared dead, whose boots the end of the runner's boots ends, and release the held requests if no other runner is waited for.
	pub(super) fn forget_recovery_runner(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
	) {
		let Some(recovery) = &mut self.recovery else {
			return;
		};
		recovery.boots.remove(runner);
		recovery.runners.remove(runner);
		self.release_recovery(scheduler);
	}

	/// Whether a sandbox's request is held for recovery.
	pub(super) fn is_recovering(&self, sandbox: &tg::sandbox::Id) -> bool {
		self.recovery.as_ref().is_some_and(|recovery| {
			recovery.claimed.contains_key(sandbox) || recovery.requests.contains_key(sandbox)
		})
	}

	/// Get the running boot of a sandbox that this restarted scheduler rebuilt from the index on a runner that has not reported yet.
	pub(super) fn try_get_recovered_boot(&self, sandbox: &tg::sandbox::Id) -> Option<u64> {
		let recovery = self.recovery.as_ref()?;
		recovery
			.boots
			.values()
			.flatten()
			.find(|recovered| recovered.sandbox == *sandbox)
			.map(|recovered| recovered.boot)
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

	/// Expire the runners that did not report in time, and release the held requests. Keep waiting for a runner that connected and whose connection is not applied yet, which reports once its connection is applied, or expires as a connected runner.
	pub(super) fn handle_recovery_expired(&mut self, scheduler: &Scheduler) {
		let Some(recovery) = &mut self.recovery else {
			return;
		};
		let (connected, runners) = std::mem::take(&mut recovery.runners)
			.into_iter()
			.partition::<HashSet<_, tg::id::BuildHasher>, _>(|runner| {
				self.runners.entries.contains_key(runner)
			});
		recovery.runners = connected;
		for runner in runners {
			recovery.boots.remove(&runner);
			tracing::warn!(%runner, "the runner did not report after the scheduler restarted");

			// Leave the restarts that are queued for the runner to this scheduler, which holds them.
			let queued = recovery
				.requests
				.iter()
				.filter(|(_, request)| request.boot > 0 && request.runner.as_ref() == Some(&runner))
				.map(|(id, _)| id.clone())
				.collect::<Vec<_>>();
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let result = scheduler
						.end_dead_runner(&runner, queued, Vec::new(), false)
						.boxed()
						.await
						.map(|()| super::RemoveRunnerResponseOutput { runner });
					Operation::RemoveRunner { id: None, result }
				}
				.boxed(),
			);
		}
		self.release_recovery(scheduler);
	}

	/// Get the boots that a runner claimed in its reports to this restarted scheduler, as unanswered offers for the index check of the runner's first connection after it restarted.
	pub(super) fn get_claims(
		&self,
		runner: &tg::runner::Id,
	) -> Vec<super::runner::UnansweredOffer> {
		let Some(recovery) = &self.recovery else {
			return Vec::new();
		};
		recovery
			.claimed
			.iter()
			.filter(|(_, claim)| claim.runner == *runner)
			.map(|(id, claim)| super::runner::UnansweredOffer {
				boot: claim.request.boot,
				offer: None,
				sandbox: id.clone(),
			})
			.collect()
	}

	/// Release the held requests once no runner is waited for.
	pub(super) fn release_recovery(&mut self, scheduler: &Scheduler) {
		if self.is_recovery_waiting() {
			return;
		}
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
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				// Await indexing, so that the read has every write that a server accepted before the release. Keep trying, because the held requests are decided from the read.
				while let Err(error) = scheduler.index().await {
					tracing::error!(error = %error.trace(), "failed to await indexing before the release");
					tokio::time::sleep(READ_RETRY_INTERVAL).await;
				}

				// Read the sandboxes' statuses, because a destroy may have arrived before the restart. Keep trying, because the held requests are lost if the read is given up.
				let sandboxes = loop {
					match scheduler.server.index.try_get_sandboxes(&ids).await {
						Ok(sandboxes) => break sandboxes,
						Err(error) => {
							tracing::error!(error = %error.trace(), "failed to read the recovered sandboxes");
							tokio::time::sleep(READ_RETRY_INTERVAL).await;
						},
					}
				};
				let statuses = std::iter::zip(ids, sandboxes)
					.filter_map(|(id, sandbox)| Some((id, sandbox?)))
					.collect();
				Operation::Recovered { statuses }
			}
			.boxed(),
		);
	}

	/// Whether the release waits, because a runner has not reported, or because a runner that claimed a boot restarted and its first connection is not applied yet.
	fn is_recovery_waiting(&self) -> bool {
		let Some(recovery) = &self.recovery else {
			return false;
		};
		!recovery.runners.is_empty()
			|| recovery.claimed.values().any(|claim| {
				self.runners
					.entries
					.get(&claim.runner)
					.is_some_and(|runner| runner.restarting)
			})
	}

	/// Tear down the requests that are being destroyed, and queue the rest of the requests that no runner claimed.
	pub(super) fn handle_recovered(&mut self, scheduler: &Scheduler, mut statuses: Statuses) {
		// Wait for the first connection of a runner that claimed a boot and restarted during the release, whose application releases the requests again.
		if self.is_recovery_waiting() {
			if let Some(recovery) = &mut self.recovery {
				recovery.releasing = false;
			}
			return;
		}
		let Some(recovery) = self.recovery.take() else {
			return;
		};

		// Decide the boots that the runners claimed.
		for (id, claim) in recovery.claimed {
			let data = statuses
				.remove(&id)
				.and_then(|indexed| indexed.data)
				.map(|data| data.data);
			let destroying = data
				.as_ref()
				.is_some_and(|data| data.status.is_destroying());
			let error = data.and_then(|data| data.error);
			let connection = self
				.runners
				.entries
				.get(&claim.runner)
				.map(|runner| runner.connection);
			match connection {
				// Destroy a sandbox that is destroying on its runner.
				Some(connection) if destroying => {
					let destroy = super::sandbox::DestroySandbox {
						connection,
						error: error.and_then(tg::Either::left).unwrap_or_default(),
						request: Some(claim.request),
						runner: claim.runner,
						sandbox: id,
					};
					self.send_destroy_sandbox(scheduler, destroy);
				},

				// Treat the runner's report of the boot as its yes, which writes the handoff that this scheduler may not have written before it restarted.
				Some(connection) => {
					let runner = super::placement::RunnerRef {
						connection,
						id: claim.runner,
					};
					let processes = recovery.processes.get(&id).cloned().unwrap_or_default();
					self.accept_claimed_sandbox(scheduler, &id, claim.request, processes, runner);
				},

				// Record the destruction of a sandbox that is being destroyed and whose runner was declared dead since its report.
				None if destroying => {
					self.finish_destroy_sandbox(scheduler, claim.request, error);
				},

				// End a boot as failed whose runner was declared dead since its report, because the boot is lost with the runner.
				None => {
					self.fail_claimed_sandbox(scheduler, claim.request, claim.runner);
				},
			}
		}

		// Decide the saved boots that no runner claimed.
		for (id, request) in recovery.requests {
			let indexed = statuses.remove(&id);
			match decide_saved_boot(request.boot, indexed.as_ref()) {
				// Record the destruction of a sandbox that is being destroyed.
				SavedBoot::Destroying { error } => {
					self.finish_destroy_sandbox(scheduler, request, error);
				},

				// Remove a boot that ended from this scheduler's saved queue, because no scheduler holds an ended boot.
				SavedBoot::Ended => {
					self.delete_saved_boot(scheduler, request);
				},

				// Queue a boot that no runner wrote, which this scheduler still holds.
				SavedBoot::Unwritten => {
					self.handle_enqueue_sandbox_request(scheduler, None, request, true);
				},

				// End a boot that a runner wrote and did not claim, because the runner restarted or was declared dead.
				SavedBoot::Written { runner } => {
					self.fail_claimed_sandbox(scheduler, request, runner);
				},
			}
		}

		// Offer the saved spawns again, once the boots that they are for are tracked.
		self.recover_spawns(scheduler, recovery.spawns);

		// Offer the sandboxes, which waited for the runners to report.
		self.queue.wake();
	}

	/// Accept a boot that a runner claimed, replacing an entry of an older boot, with the boot's processes that this scheduler rebuilt from the index.
	fn accept_claimed_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		request: EnqueueSandboxRequestArg,
		processes: BTreeSet<tg::process::Id>,
		runner: super::placement::RunnerRef,
	) {
		self.remove_ended_sandbox(scheduler, id);
		let boot = request.boot;
		let parent = request.parent.clone();
		let state = super::sandbox::State::Accepted {
			runner: runner.clone(),
		};
		let mut sandbox = super::sandbox::Sandbox::new(boot, parent, Some(request), state);
		sandbox.processes.extend(processes);
		self.sandboxes.entries.insert(id.clone(), sandbox);
		self.accept_sandbox(scheduler, id, runner);
	}

	/// Remove a saved boot that ended from this scheduler's saved queue.
	pub(super) fn delete_saved_boot(
		&mut self,
		scheduler: &Scheduler,
		request: EnqueueSandboxRequestArg,
	) {
		tracing::info!(sandbox = %request.sandbox, boot = request.boot, "removing the saved boot that ended");
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let delete = tangram_index::scheduler::sandbox::delete::Arg {
					boot: request.boot,
					sandbox: request.sandbox.clone(),
				};
				let items = vec![tangram_index::Item::DeleteSchedulerSandbox(Box::new(
					delete,
				))];
				let arg = tangram_index::write::Arg { items };
				let result = scheduler
					.server
					.components
					.indexer
					.write(&scheduler.server, arg)
					.await
					.map_err(
						|error| tg::error!(!error, sandbox = %request.sandbox, "failed to remove the saved boot"),
					);
				Operation::Finished {
					context: "failed to remove the saved boot that ended",
					result,
				}
			}
			.boxed(),
		);
	}

	/// End a boot that a runner claimed and lost, that a runner wrote and did not claim, because the runner restarted or was declared dead, or whose offer the runner answered with the boot's end, through the end of a dead runner's boot, which leaves the sandbox stopped on the runner, or destroyed if its ttl is zero, and which leaves a boot whose end the index records as it is. Remember the boot as ended once the end applies, so that this scheduler tells the runner when it connects again.
	pub(super) fn fail_claimed_sandbox(
		&mut self,
		scheduler: &Scheduler,
		request: EnqueueSandboxRequestArg,
		runner: tg::runner::Id,
	) {
		let destroy = request
			.arg
			.time_to_live
			.is_some_and(|time_to_live| time_to_live.is_zero());
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let error =
					tg::error!(sandbox = %request.sandbox, "the sandbox's runner is unavailable")
						.to_data_or_id()
						.left()
						.unwrap_or_default();
				let arg = super::runner::EndDeadRunnerBootArg {
					boot: request.boot,
					destroy,
					error,
					persist: None,
					runner: &runner,
					sandbox: &request.sandbox,
					task: Some(&scheduler.holder_sender),
				};
				scheduler
					.component
					.end_dead_runner_boot(&scheduler.server, arg)
					.await;
				Operation::Finished {
					context: "failed to record the failed sandbox",
					result: Ok(()),
				}
			}
			.boxed(),
		);
	}
}

impl Component {
	/// End the boots in this single server's queue, and the sandboxes of the runners other than this server's that connected to it, whose connections can no longer expire after the restart. Stop each, or destroy it if its ttl is zero. This server's runner ends its own sandboxes when it connects. Return the messages that tell the scheduling task of each boot of the other runners that the end ended, so that the task remembers them and tells each runner when it connects again.
	pub(super) async fn end_previous_run(
		&self,
		server: &Server,
	) -> tg::Result<Vec<super::HolderMessage>> {
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
			let zero = data
				.time_to_live
				.is_some_and(|time_to_live| time_to_live.is_zero());
			let result = if zero {
				self.destroy_runner_sandbox(server, &id, Some(error.clone()))
					.boxed()
					.await
			} else {
				self.end_runner_sandbox(server, &id, None, None, Some(error.clone()))
					.boxed()
					.await
					.map(|_| ())
			};
			if let Err(error) = result {
				tracing::error!(sandbox = %id, error = %error.trace(), "failed to end the queued sandbox");
			}
		}

		// Finish the processes in the queue, which were on their way into sandboxes that ended with the run.
		let process_error = tg::Error::try_from(error.clone())
			.map_err(|error| tg::error!(!error, "failed to create the error"))?;
		if let Err(error) = super::spawn::end_spawns(server, &id, &process_error).await {
			tracing::error!(error = %error.trace(), "failed to finish the queued processes");
		}

		// End the boots of the other runners, and collect the messages of the boots that the end ended.
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
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
			let arg = super::runner::EndRunnerSandboxesArg {
				accepted: Vec::new(),
				checkpoint: "scheduler.runner.recovered",
				deleted: false,
				ended: Some(sender.clone()),
				error: error.clone(),
				held: Vec::new(),
				queued: Vec::new(),
				runner: runner.clone(),
				scheduler: None,
				written: Vec::new(),
			};
			let result = self.end_runner_sandboxes(server, &id, arg).boxed().await;
			if let Err(error) = result {
				tracing::error!(%runner, error = %error.trace(), "failed to end the runner's sandboxes");
			}
		}
		drop(sender);
		let mut messages = Vec::new();
		while let Some(message) = receiver.recv().await {
			messages.push(message);
		}

		Ok(messages)
	}
}
