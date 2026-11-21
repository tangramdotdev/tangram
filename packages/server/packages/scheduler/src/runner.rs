use {
	super::{
		AddRunnerRequestArg, AddRunnerResponseOutput, HeartbeatNotification, Operation,
		RemoveRunnerRequestArg, RemoveRunnerResponseOutput, Scheduler, SchedulerState,
		sandbox::State,
	},
	crate::Component,
	futures::FutureExt as _,
	std::{
		collections::{BTreeSet, HashMap, HashSet},
		sync::Arc,
		time::Duration,
	},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Server,
};

#[cfg(test)]
mod tests;

/// The runner connections that this scheduler numbered last, which a takeover for a connection checks under the lock before each of its writes, so that a takeover for a connection that a newer connection of the runner replaced ends nothing more.
#[derive(Clone, Default)]
pub(super) struct Connections {
	current: Arc<std::sync::Mutex<HashMap<tg::runner::Id, u64, tg::id::BuildHasher>>>,
	lock: Arc<tokio::sync::Mutex<()>>,
}

/// The sandboxes of a runner that a takeover ends.
pub(super) struct EndRunnerSandboxesArg {
	/// The checkpoint that the routine hits once it has read the runner's sandboxes.
	pub checkpoint: &'static str,

	/// The sandboxes that the runner has directories for, or `None` if they are unknown.
	pub directories: Option<BTreeSet<tg::sandbox::Id>>,

	/// The error that the ended sandboxes and their processes record.
	pub error: tg::error::Data,

	/// The runner connection that the takeover is for, which ends nothing once a newer connection replaces it, or `None` for a takeover that no connection bounds.
	pub fence: Option<Fence>,

	/// The boots that the runner holds.
	pub held: Vec<tg::runner::control::ArgSandbox>,

	/// The sandboxes whose current boot the scheduler holds.
	pub queued: Vec<tg::sandbox::Id>,

	pub runner: tg::runner::Id,

	/// The scheduling task, which the takeover asks after it reads the index for the sandboxes whose current boot the scheduler holds, or `None` before the task runs.
	pub scheduler: Option<tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,

	/// The sandboxes whose offers to the runner are unanswered and whose boots the runner wrote to the index, which the takeover ends although the scheduler's saved queue has them.
	pub written: Vec<tg::sandbox::Id>,
}

/// The output of a runner's connection.
pub(super) struct AddRunnerOutput {
	/// The reported boots that the index records as ended or replaced by a later boot, which this scheduler forgets.
	pub ended: Vec<tg::runner::control::ArgSandbox>,

	/// The reported boots and processes that the index does not have on the runner's first report to this restarted scheduler, which this scheduler cancels.
	pub lost: Lost,

	pub output: AddRunnerResponseOutput,

	pub owner: Option<tg::Id>,

	/// The sandboxes whose offers to the runner are unanswered and whose boots the runner wrote to the index.
	pub written: BTreeSet<tg::sandbox::Id>,
}

/// The boots and processes that a runner reports to a restarted scheduler and that the index does not have, which the scheduler lost before its first write of them.
#[derive(Default)]
pub(super) struct Lost {
	pub processes: Vec<tg::process::Id>,
	pub sandboxes: Vec<tg::runner::control::ArgSandbox>,
}

/// The connection that a takeover is for.
pub(super) struct Fence {
	pub connection: u64,
	pub connections: Connections,
}

/// An unanswered offer to a runner that restarted, which the runner's first connection decides from the index.
pub(super) struct UnansweredOffer {
	pub boot: u64,

	/// The offer, or `None` for a boot that the runner reported to this restarted scheduler and then lost by restarting, which this scheduler holds for recovery.
	pub offer: Option<String>,

	pub sandbox: tg::sandbox::Id,

	pub sequence: u64,
}

/// The reported sandboxes with directories that a takeover ended or leaves to the runner.
pub(super) struct EndRunnerSandboxesOutput {
	/// The reported sandboxes that the takeover destroyed.
	destroyed: Vec<tg::sandbox::Id>,

	/// The reported sandboxes whose directories the runner keeps, because the sandboxes are stopped on the runner or their boots wait in the scheduler's queue.
	kept: Vec<tg::sandbox::Id>,

	/// The reported sandboxes that the takeover stopped, which `kept` also lists.
	stopped: Vec<tg::sandbox::Id>,
}

pub(super) struct Runners {
	pub entries: HashMap<tg::runner::Id, Runner, tg::id::BuildHasher>,

	/// The number of the next runner connection, which this scheduler keeps only in memory and starts at a random number, so that a connection to an earlier run of the scheduler is not mistaken for a current one.
	pub next_connection: u64,
}

pub(super) struct Runner {
	/// The runner's sandboxes that lend capacity to their children.
	pub borrowable: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,

	/// The capacity that the runner's last heartbeat reported.
	pub capacity: tg::runner::control::Capacity,

	/// The capacity of the sandboxes that the runner accepted since its last heartbeat.
	pub committed: tg::runner::Capacity,

	pub connection: u64,

	/// Whether the scheduler failed to record the runner's connection, in which case the runner takes no sandboxes, and expires with its sandboxes unless it connects again.
	pub connection_failed: bool,

	pub heartbeat_at: tangram_server_core::clock::Instant,

	pub heartbeat_index: u64,

	pub host: String,

	pub owner: Option<tg::Id>,

	/// The capacity that each sandbox that is offered to the runner reserves, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
	pub reservations: HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher>,

	/// The sum of the reservations.
	pub reserved: tg::runner::Capacity,
}

impl Runner {
	/// Whether the runner's connection succeeded, and the runner has sent a heartbeat and takes sandboxes.
	pub fn ready(&self) -> bool {
		!self.connection_failed && self.heartbeat_index > 0 && self.capacity.total.sandboxes != 0
	}

	/// The capacity that the runner has now, which is what its last heartbeat reported without what the scheduler has reserved and committed since.
	pub fn available(&self) -> tg::runner::Capacity {
		let factor = self.capacity.overcommit;
		self.capacity
			.available
			.subtract(self.reserved, factor)
			.subtract(self.committed, factor)
	}

	/// Whether the runner has the capacity for a request now.
	pub fn fits(&self, capacity: tg::runner::Capacity) -> bool {
		self.available()
			.contains(capacity, self.capacity.overcommit)
	}
}

impl Runners {
	pub fn new() -> Self {
		Self {
			entries: HashMap::default(),
			next_connection: tangram_server_core::service::start() >> 1,
		}
	}

	pub fn expired(
		&self,
		now: tangram_server_core::clock::Instant,
		time_to_live: Duration,
	) -> Vec<(tg::runner::Id, u64)> {
		self.entries
			.iter()
			.filter(|(_, runner)| now - runner.heartbeat_at > time_to_live)
			.map(|(id, runner)| (id.clone(), runner.connection))
			.collect()
	}
}

impl SchedulerState {
	pub(super) fn handle_add_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: AddRunnerRequestArg,
	) {
		// Number the runner's connection, and record it as the runner's current connection, so that a takeover for an earlier connection ends nothing more.
		let connection = self.runners.next_connection;
		self.runners.next_connection = connection.wrapping_add(1);
		scheduler.connections.set(&request.runner, connection);
		tracing::info!(runner = %request.runner, connection, "connecting the runner");

		// Record the runner's report for the recovery of a restarted scheduler, and get the boots that the scheduler rebuilt from the index on the runner. On the runner's first report to the restarted scheduler, the boots and processes that the runner reports and that the index does not have were lost before the scheduler's first write of them.
		let recovering = self
			.recovery
			.as_ref()
			.is_some_and(|recovery| recovery.has_runner(&request.runner));
		let recovered = self.handle_recovery_report(
			scheduler,
			&request.runner,
			request.first,
			&request.sandboxes,
		);

		self.forget_accepted_sandboxes(scheduler, &request.runner, &request.sandboxes);
		if request.first {
			self.sandboxes.ended.forget_runner(&request.runner);
		}
		let offers = self.remove_runner(scheduler, &request.runner, true);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			connection,
			connection_failed: false,
			heartbeat_at: scheduler.server.clock.instant(),
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
		};
		self.runners.entries.insert(request.runner.clone(), runner);

		// Track the sandboxes that the runner reports, except the ones that a restarted scheduler holds for recovery, which the runner's report claims and which the end of the recovery accepts.
		for sandbox in &request.sandboxes {
			if self.is_recovering(&sandbox.id) {
				continue;
			}
			let runner = super::placement::RunnerRef {
				connection,
				id: request.runner.clone(),
			};
			let processes = recovered
				.iter()
				.find(|recovered| recovered.sandbox == sandbox.id && recovered.boot == sandbox.boot)
				.map_or_default(|recovered| recovered.processes.clone());
			self.track_accepted_sandbox(
				scheduler,
				&sandbox.id,
				sandbox.boot,
				None,
				processes,
				runner,
			);
		}

		// Remember the rebuilt boots that a runner that did not restart no longer reports, which ended while this scheduler was down, so that this scheduler authorizes the runner's reports of their ends.
		if !request.first {
			for recovered in recovered {
				let reported = request.sandboxes.iter().any(|sandbox| {
					sandbox.id == recovered.sandbox && sandbox.boot == recovered.boot
				});
				if reported {
					continue;
				}
				let ended = super::report::EndedBoot {
					processes: recovered.processes,
					runner: request.runner.clone(),
				};
				self.sandboxes
					.ended
					.insert(recovered.sandbox, recovered.boot, ended);
			}
		}

		// Keep the unanswered offers to a runner that restarted reserved on the runner and pointed at its earlier connection, so that their answers are ignored, until its first connection decides them from the index, together with the boots that the runner reported to this restarted scheduler and then lost by restarting. Send the unanswered offers again to a runner that reconnects, which is the same program and answers them from its memory of its replies.
		let unanswered = if request.first {
			let mut unanswered = self.get_unanswered_offers(&request.runner, offers);
			unanswered.extend(self.get_restarted_claims(&request.runner));
			unanswered
		} else {
			self.resend_runner_offers(scheduler, &request.runner, offers);
			Vec::new()
		};
		self.queue.wake();

		// Get the sandboxes whose current boot this scheduler holds, whose directories the runner keeps, and which a takeover on the runner's first connection leaves to this scheduler.
		let queued = self.get_held_sandboxes();

		let scheduler = scheduler.clone();
		let runner = request.runner.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.add_runner(request, connection, queued, &unanswered, recovering)
					.await;
				Operation::AddRunner {
					connection,
					id,
					result,
					runner,
					unanswered,
				}
			}
			.boxed(),
		);
	}

	/// Track a sandbox that a runner started on the shortcut path, so that this scheduler knows the sandbox and sends the spawns into it to the runner. Authorize the report with the proof that the runner derived the sandbox's ID, and ignore a report of a boot that already ended, so that a report that arrives after the boot's end does not track the boot again.
	pub(super) fn handle_add_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: tangram_server_scheduler_core::AddRunnerSandboxRequestArg,
	) {
		let current = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.connection == request.connection);
		let report = super::report::Report {
			boot: request.boot,
			processes: Vec::new(),
			proof: Some(&request.proof),
			runner: &request.runner,
			sandbox: &request.sandbox,
		};
		let authorization = self.authorize_runner_report(&report);
		let result = match authorization {
			_ if !current => Err(
				tg::error!(sandbox = %request.sandbox, runner = %request.runner, "the runner's connection is not current"),
			),
			super::report::Authorization::Authorized(authorized) => {
				if matches!(authorized.held, super::report::Held::Derived) {
					let runner = super::placement::RunnerRef {
						connection: request.connection,
						id: request.runner,
					};
					self.track_accepted_sandbox(
						scheduler,
						&request.sandbox,
						request.boot,
						None,
						BTreeSet::new(),
						runner,
					);
				}
				let output = tangram_server_scheduler_core::AddRunnerSandboxResponseOutput {};
				Ok(super::ResponseOutput::AddRunnerSandbox(output))
			},
			super::report::Authorization::NotReady => Err(
				tg::error!(sandbox = %request.sandbox, runner = %request.runner, "the runner did not derive the sandbox's ID"),
			),
			super::report::Authorization::Refused(error) => Err(error),
		};
		let response = super::request::Response::new(id, result);
		scheduler.send_response(self, response);
	}

	pub(super) fn handle_destroy_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: super::DestroyRunnerSandboxRequestArg,
	) {
		// Fail if the runner is not connected, because only the runner can destroy the sandbox.
		let Some(connection) = self
			.runners
			.entries
			.get(&request.runner)
			.filter(|runner| !runner.connection_failed)
			.map(|runner| runner.connection)
		else {
			let error = tg::error!(sandbox = %request.sandbox, runner = %request.runner, "the sandbox's runner is unavailable");
			let response = super::request::Response::new(id, Err(error));
			scheduler.send_response(self, response);
			return;
		};

		// Ask the runner to destroy the sandbox.
		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();
		let retry = scheduler.server.config.control.request_retry.clone().into();
		let timeout = scheduler.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		self.operations.push(
			async move {
				let arg = tg::runner::control::ServerRequestArg::DestroySandbox(
					tg::runner::control::DestroySandboxServerRequestArg {
						error: request.error.clone(),
						sandbox: request.sandbox.clone(),
					},
				);
				let result = session
					.server
					.components
					.runner
					.send_runner_control_request(
						&session,
						&request.runner,
						&scheduler_id,
						connection,
						tangram_server_core::control::id(),
						arg,
						options,
					)
					.await
					.and_then(std::convert::identity)
					.and_then(|output| {
						output
							.try_unwrap_destroy_sandbox()
							.map_err(|_| tg::error!("expected a destroy sandbox response"))
							.map(|output| output.destroyed)
					});
				super::Operation::DestroyRunnerSandbox { id, result }
			}
			.boxed(),
		);
	}

	pub(super) fn handle_remove_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: Option<String>,
		request: RemoveRunnerRequestArg,
	) {
		let current = self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| runner.connection == request.connection);
		if !current {
			if let Some(id) = id {
				let output = RemoveRunnerResponseOutput {
					runner: request.runner,
				};
				let response = super::request::Response::new(
					id,
					Ok(super::ResponseOutput::RemoveRunner(output)),
				);
				scheduler.send_response(self, response);
			}
			return;
		}
		tracing::info!(runner = %request.runner, connection = request.connection, "removing the runner");
		self.forget_accepted_sandboxes(scheduler, &request.runner, &[]);
		self.sandboxes.ended.forget_runner(&request.runner);
		self.remove_runner(scheduler, &request.runner, false);

		// Discard the starts pinned to the runner, so that a boot that the runner's sandboxes are marked stopped for never starts later. This scheduler holds those boots, so their discard alone records their end, and the runner's cleanup leaves them alone.
		let mut queued = Vec::new();
		for discarded in self.discard_pinned_sandboxes(scheduler, &request.runner) {
			queued.push(discarded.sandbox.clone());
			scheduler.publish_sandbox_discarded(self, discarded);
		}
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.remove_runner(request, queued).boxed().await;
				Operation::RemoveRunner { id, result }
			}
			.boxed(),
		);
	}

	/// Declare a deleted runner dead, and take over its boots as on the expiry of its ttl. Destroy its stopped sandboxes too, because the runner never returns to destroy them.
	pub(super) fn handle_delete_runner_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: tangram_server_scheduler_core::DeleteRunnerRequestArg,
	) {
		tracing::info!(runner = %request.runner, "deleting the runner");
		self.forget_accepted_sandboxes(scheduler, &request.runner, &[]);
		self.remove_runner(scheduler, &request.runner, false);
		let mut queued = Vec::new();
		for discarded in self.discard_pinned_sandboxes(scheduler, &request.runner) {
			queued.push(discarded.sandbox.clone());
			scheduler.publish_sandbox_discarded(self, discarded);
		}
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.delete_runner(&request.runner, queued)
					.boxed()
					.await;
				Operation::DeleteRunner { id, result }
			}
			.boxed(),
		);
	}

	pub(super) fn handle_heartbeat(
		&mut self,
		notification: &HeartbeatNotification,
		now: tangram_server_core::clock::Instant,
	) -> bool {
		let Some(runner) = self.runners.entries.get_mut(&notification.runner) else {
			return false;
		};
		if runner.connection_failed
			|| runner.connection != notification.connection
			|| runner.heartbeat_index >= notification.heartbeat_index
		{
			return false;
		}
		// Forget potential capacity after a parent is no longer available for borrowing.
		let expired: Vec<_> = runner
			.borrowable
			.iter()
			.filter(|parent| {
				!notification.sandboxes.contains(*parent)
					&& self
						.parents
						.0
						.get(*parent)
						.and_then(|parent| parent.borrowable.as_ref())
						.is_some_and(|borrowable| {
							notification.heartbeat_index > borrowable.heartbeat_index
						})
			})
			.cloned()
			.collect();
		for parent_id in expired {
			runner.borrowable.remove(&parent_id);
			if let Some(parent) = self.parents.0.get_mut(&parent_id) {
				parent.borrowable = None;
				if parent.is_empty() {
					self.parents.0.remove(&parent_id);
				}
			}
		}
		runner.capacity = notification.capacity;
		runner.committed = tg::runner::Capacity::default();
		runner.heartbeat_at = now;
		runner.heartbeat_index = notification.heartbeat_index;
		self.queue.wake();

		true
	}

	/// Get the sandboxes whose current boot this scheduler holds, which are the ones that it queued, offered, or ended and holds until the index has recorded the end, the ones whose next boot a start is reserving, and the ones that it holds for recovery.
	pub(super) fn get_held_sandboxes(&self) -> Vec<tg::sandbox::Id> {
		let mut sandboxes = self
			.sandboxes
			.entries
			.iter()
			.filter(|(_, sandbox)| !matches!(sandbox.state, State::Accepted { .. }))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		sandboxes.extend(self.sandboxes.reserving.keys().cloned());
		sandboxes.extend(self.get_recovering_sandboxes());

		sandboxes
	}

	/// Forget the sandboxes that a runner accepted, except the ones that it reports when it connects again. The runner held a boot from its acceptance, so a boot that it no longer reports is lost with the runner, and the runner's cleanup ends it.
	pub(super) fn forget_accepted_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		reported: &[tg::runner::control::ArgSandbox],
	) {
		let accepted = self
			.sandboxes
			.entries
			.iter()
			.filter(
				|(_, sandbox)| matches!(&sandbox.state, State::Accepted { runner } if runner.id == *id),
			)
			.map(|(sandbox, entry)| (sandbox.clone(), entry.boot))
			.filter(|(sandbox, boot)| {
				!reported
					.iter()
					.any(|reported| reported.id == *sandbox && reported.boot == *boot)
			})
			.collect::<Vec<_>>();
		for (sandbox, _) in accepted {
			self.remove_sandbox(scheduler, &sandbox);
			self.stop_serving_sandbox_holder(&sandbox);
		}
	}

	/// Forget the reported boots that the index records as ended or replaced by a later boot, which the runner gives up without reporting their ends, while this scheduler still tracks them as accepted by the runner at those boots.
	pub(super) fn forget_ended_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		ended: &[tg::runner::control::ArgSandbox],
	) {
		for sandbox in ended {
			let accepted = self
				.sandboxes
				.entries
				.get(&sandbox.id)
				.is_some_and(|entry| {
					entry.boot == sandbox.boot
						&& matches!(&entry.state, State::Accepted { runner } if runner.id == *id)
				});
			if accepted {
				self.remove_sandbox(scheduler, &sandbox.id);
				self.stop_serving_sandbox_holder(&sandbox.id);
			}
		}
	}

	/// Cancel the boots and processes that a runner reported to this restarted scheduler and that the index does not have, which this scheduler lost before its first write of them. Forget each lost boot and ask the runner to destroy it, which ends the boot and finishes its processes as failed, and send each lost process a kill signal, which its runner finishes it for.
	pub(super) fn cancel_lost_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		connection: u64,
		lost: Lost,
	) {
		let Lost {
			processes,
			sandboxes,
		} = lost;

		// Forget and destroy the lost boots.
		for sandbox in sandboxes {
			tracing::warn!(sandbox = %sandbox.id, boot = sandbox.boot, %runner, "cancelling the boot that the index does not have");
			let accepted = self
				.sandboxes
				.entries
				.get(&sandbox.id)
				.is_some_and(|entry| {
					entry.boot == sandbox.boot
						&& matches!(&entry.state, State::Accepted { runner: accepted } if accepted.id == *runner)
				});
			if accepted {
				self.remove_sandbox(scheduler, &sandbox.id);
				self.stop_serving_sandbox_holder(&sandbox.id);
			}
			let error = tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the scheduler lost the sandbox's record".to_owned()),
				..Default::default()
			};
			let destroy = super::sandbox::DestroySandbox {
				connection,
				error,
				request: None,
				runner: runner.clone(),
				sandbox: sandbox.id,
			};
			self.send_destroy_sandbox(scheduler, destroy);
		}

		// Kill the lost processes.
		for process in processes {
			tracing::warn!(%process, %runner, "cancelling the process that the index does not have");
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let session = scheduler.server.session(&scheduler.server.context);
					let id = tg::Either::Right(process);
					let arg = tg::process::signal::post::Arg {
						location: None,
						signal: tg::process::Signal::SIGKILL,
						tokens: tg::authorization::Tokens::default(),
					};
					let result = scheduler
						.server
						.components
						.process
						.try_signal_process(&session, &id, arg)
						.await
						.map(|_| ());
					Operation::Finished {
						context: "failed to cancel the lost process",
						result,
					}
				}
				.boxed(),
			);
		}
	}

	/// Record that the scheduler failed to record a runner's connection, and keep the runner, so that it takes no sandboxes and its expiry ends the sandboxes that it accepted unless it connects again.
	pub(super) fn fail_runner_connection(&mut self, id: &tg::runner::Id, connection: u64) {
		if let Some(runner) = self.runners.entries.get_mut(id)
			&& runner.connection == connection
		{
			runner.connection_failed = true;
		}
	}

	/// Remove a runner, and return the capacity that each of its unanswered offers reserves if the runner connects again, whose caller decides what becomes of the offers. End the unanswered offers of a runner that is declared dead as failed, because the runner may have accepted them, so that a boot is never offered to a second runner while the first may hold it, and end the ones that a dequeue waits for as destroyed. The sandboxes that the runner accepted stay until the runner connects again or expires.
	pub(super) fn remove_runner(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		reconnected: bool,
	) -> HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher> {
		let Some(runner) = self.runners.entries.remove(id) else {
			return HashMap::default();
		};

		// Forget the capacity that the runner's sandboxes lend.
		for parent_id in runner.borrowable {
			let Some(parent) = self.parents.0.get_mut(&parent_id) else {
				continue;
			};
			let remove = parent.borrowable.as_ref().is_some_and(|borrowable| {
				borrowable.runner == *id && borrowable.connection == Some(runner.connection)
			});
			if remove {
				parent.borrowable = None;
				if parent.is_empty() {
					self.parents.0.remove(&parent_id);
				}
			}
		}

		// Return the unanswered offers of a runner that connects again.
		if reconnected {
			return runner.reservations;
		}

		// End the boots that the runner reported to this restarted scheduler and then lost by restarting.
		self.fail_restarted_claims(scheduler, id);

		// End the unanswered offers of a runner that is declared dead.
		for sandbox in runner.reservations.into_keys() {
			let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			if dequeue_requests.is_empty() {
				let error =
					tg::error!(%sandbox, runner = %id, "the sandbox's runner is unavailable");
				let discarded = self.discard_sandbox(scheduler, &sandbox, &error);
				scheduler.publish_sandbox_discarded(self, discarded);
				continue;
			}
			self.end_sandbox_and_log(scheduler, &sandbox, tg::sandbox::Status::Destroyed, None);
			self.send_dequeue_responses(scheduler, dequeue_requests, true);
		}
		self.queue.wake();

		HashMap::default()
	}

	/// Return a sandbox whose offer the runner lost to the queue, and end it as destroyed if a dequeue waits for it.
	fn requeue_unanswered_sandbox(&mut self, scheduler: &Scheduler, sandbox: &tg::sandbox::Id) {
		self.requeue_sandbox(sandbox);
		let entry = self.sandboxes.entries.get_mut(sandbox).unwrap();
		let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
		if dequeue_requests.is_empty() {
			return;
		}
		self.end_sandbox_and_log(scheduler, sandbox, tg::sandbox::Status::Destroyed, None);
		self.send_dequeue_responses(scheduler, dequeue_requests, true);
	}

	/// Keep the unanswered offers to a runner that restarted reserved on the runner, and get them for the runner's first connection to decide.
	fn get_unanswered_offers(
		&mut self,
		runner: &tg::runner::Id,
		offers: HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher>,
	) -> Vec<UnansweredOffer> {
		let mut unanswered = Vec::new();
		for (sandbox, reserved) in offers {
			let Some(entry) = self.sandboxes.entries.get(&sandbox) else {
				continue;
			};
			let State::Offered { offer, .. } = &entry.state else {
				continue;
			};
			unanswered.push(UnansweredOffer {
				boot: entry.boot,
				offer: Some(offer.clone()),
				sandbox: sandbox.clone(),
				sequence: entry.sequence,
			});
			let entry = self.runners.entries.get_mut(runner).unwrap();
			super::offer::add(&mut entry.reserved, reserved);
			entry.reservations.insert(sandbox, reserved);
		}

		unanswered
	}

	/// Decide the unanswered offers to a runner that restarted, and the boots that it reported to this restarted scheduler and then lost by restarting, once its first connection has read the index. Every boot that a runner may have accepted before it restarted is decided here. Forget a boot that the runner wrote, which the connection took over, because the runner had accepted it. Return a boot that the runner did not write to the queue, because this scheduler still holds it, so that it is offered again.
	pub(super) fn handle_unanswered_offers(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		unanswered: Vec<UnansweredOffer>,
		written: &BTreeSet<tg::sandbox::Id>,
	) {
		for offer in unanswered {
			// Decide a boot that the runner reported to this restarted scheduler and then lost by restarting.
			let Some(id) = &offer.offer else {
				let written = written.contains(&offer.sandbox);
				self.decide_restarted_claim(scheduler, runner, &offer.sandbox, written);
				continue;
			};

			// Skip an offer that is no longer the sandbox's current offer.
			let current = self.sandboxes.entries.get(&offer.sandbox).is_some_and(
				|entry| matches!(&entry.state, State::Offered { offer: current, .. } if current == id),
			);
			if !current {
				continue;
			}

			// Release the offer's reservation.
			if let Some(entry) = self.runners.entries.get_mut(runner)
				&& let Some(reserved) = entry.reservations.remove(&offer.sandbox)
			{
				super::offer::subtract(&mut entry.reserved, reserved);
			}

			// Return a boot that the runner did not write to the queue.
			if !written.contains(&offer.sandbox) {
				self.requeue_unanswered_sandbox(scheduler, &offer.sandbox);
				continue;
			}

			// Forget a boot that the runner wrote, and reply to the waiting dequeues that the sandbox was not dequeued.
			let entry = self.sandboxes.entries.get_mut(&offer.sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			self.remove_queued_sandbox(&offer.sandbox);
			self.remove_sandbox(scheduler, &offer.sandbox);
			self.stop_serving_sandbox_holder(&offer.sandbox);
			self.send_dequeue_responses(scheduler, dequeue_requests, false);
		}
		self.queue.wake();
	}
}

impl Connections {
	/// Record a runner's current connection.
	pub(super) fn set(&self, runner: &tg::runner::Id, connection: u64) {
		self.current
			.lock()
			.unwrap()
			.insert(runner.clone(), connection);
	}

	/// Whether a connection is the runner's current connection, which it is if no newer connection of the runner replaced it.
	fn is_current(&self, runner: &tg::runner::Id, connection: u64) -> bool {
		self.current
			.lock()
			.unwrap()
			.get(runner)
			.is_none_or(|current| *current == connection)
	}
}

impl Scheduler {
	async fn add_runner(
		&self,
		request: AddRunnerRequestArg,
		connection: u64,
		queued: Vec<tg::sandbox::Id>,
		unanswered: &[UnansweredOffer],
		recovering: bool,
	) -> tg::Result<AddRunnerOutput> {
		// Wait for a takeover write for an earlier connection of the runner that is in flight, so that no takeover for an earlier connection applies after this connection.
		drop(self.connections.lock.lock().await);

		let session = self.server.session(&self.server.context);
		let runner = self
			.server
			.components
			.runner
			.try_get_runner_data(&session, &request.runner)
			.await?;
		let owner = match runner {
			Some(runner) => runner.owner.and_then(|owner| owner.to_id()),
			None if self
				.server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
				&& self.server.config.runner.id.is_none()
				&& self.server.components.runner.state().id().as_ref() == Some(&request.runner) =>
			{
				None
			},
			None => {
				return Err(tg::error!(runner = %request.runner, "failed to find the runner"));
			},
		};

		// Record that the runner belongs to this scheduler, which the index rejects if the runner belongs to another.
		tangram_server_core::checkpoint!(self.server, "scheduler.runner.connect", runner = %request.runner, connection).await;
		self.server
			.components
			.runner
			.set_runner_scheduler(&session, &request.runner, &self.id)
			.await?;

		// Await indexing, so that no acknowledged write is missed, or read the index as it is if indexing cannot be awaited. Fail the connection if indexing cannot be awaited on the runner's first connection while an offer to the runner is unanswered, or on the runner's first report to this restarted scheduler while the runner reports boots, because only the index shows whether the runner had accepted the offer and which reported boots and processes were lost.
		if let Err(error) = self.index().await {
			if (request.first && !unanswered.is_empty())
				|| (recovering && !request.sandboxes.is_empty())
			{
				return Err(
					tg::error!(!error, runner = %request.runner, "failed to await indexing"),
				);
			}
			tracing::warn!(error = %error.trace(), "failed to await indexing");
		}

		// Get the reported boots that the index records as ended or replaced by a later boot, such as by a takeover while the runner's heartbeat was expired, which the runner gives up.
		let ended = self.get_ended_sandboxes(&request.sandboxes).await?;

		// Get the reported boots and processes that the index does not have on the runner's first report to this restarted scheduler, which this scheduler lost before its first write of them, and which it cancels.
		let lost = if recovering {
			self.get_lost_sandboxes(&request.runner, &request.sandboxes)
				.await?
		} else {
			Lost::default()
		};

		// Take over the boots that the index records on a runner on its first connection after it starts, and that neither the runner reports nor this scheduler's queue has, and decide which of its directories the runner keeps. On a later connection, only decide which of its directories the runner keeps, and keep the directories of the destroying sandboxes, which this scheduler has the runner remove.
		let mut written = BTreeSet::new();
		let directories = request.directories.iter().cloned().collect::<BTreeSet<_>>();
		let decided = if request.first {
			// Get the unanswered offers whose boots the runner wrote to the index, which shows that the runner had accepted them, so that the takeover ends them.
			written = self.get_written_offers(unanswered).await?;
			let mut queued = queued;
			queued.retain(|sandbox| !written.contains(sandbox));

			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("runner restarted".to_owned()),
				..Default::default()
			};
			let arg = EndRunnerSandboxesArg {
				checkpoint: "scheduler.runner.restarted",
				directories: Some(directories),
				error,
				fence: Some(Fence {
					connection,
					connections: self.connections.clone(),
				}),
				held: request.sandboxes.clone(),
				queued,
				runner: request.runner.clone(),
				scheduler: Some(self.holder_sender.clone()),
				written: written.iter().cloned().collect(),
			};
			self.component
				.end_runner_sandboxes(&self.server, &self.id, arg)
				.boxed()
				.await?
		} else {
			let saved = self
				.component
				.get_scheduler_queue(&self.server, &self.id, Some(&self.holder_sender))
				.await?;
			let kept = self
				.component
				.get_kept_directories(
					&self.server,
					&request.runner,
					directories,
					&saved,
					&queued,
					true,
				)
				.await?;
			EndRunnerSandboxesOutput {
				destroyed: Vec::new(),
				kept,
				stopped: Vec::new(),
			}
		};

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.added", runner = %request.runner).await;
		let output = AddRunnerResponseOutput {
			connection,
			destroyed: decided.destroyed,
			ended: ended.iter().map(|sandbox| sandbox.id.clone()).collect(),
			kept: decided.kept,
			runner: request.runner,
			scheduler: self.id.clone(),
			stopped: decided.stopped,
		};
		let output = AddRunnerOutput {
			ended,
			lost,
			output,
			owner,
			written,
		};

		Ok(output)
	}

	/// Get the reported boots and processes that the index does not have. A boot that the runner started on the shortcut path, whose ID the runner proves that it derived, is not lost, because the runner's own server writes its first record.
	async fn get_lost_sandboxes(
		&self,
		runner: &tg::runner::Id,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) -> tg::Result<Lost> {
		// Read the reported sandboxes.
		let ids = sandboxes
			.iter()
			.map(|sandbox| sandbox.id.clone())
			.collect::<Vec<_>>();
		let indexed = self
			.server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reported sandboxes"))?;

		// Get the lost boots, and the reported processes of the other boots that the index records at the reported boot.
		let mut lost = Lost::default();
		let mut processes = Vec::new();
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
			let data = indexed
				.and_then(|indexed| indexed.data)
				.map(|data| data.data);
			let Some(data) = data else {
				let derived = sandbox.boot == 0
					&& sandbox
						.proof
						.as_ref()
						.is_some_and(|proof| proof.verify(&sandbox.id, runner).is_ok());
				if !derived {
					lost.sandboxes.push(sandbox.clone());
				}
				continue;
			};
			if data.boot == sandbox.boot && !data.status.is_stopped_or_destroyed() {
				processes.extend(sandbox.processes.iter().cloned());
			}
		}

		// Get the lost processes.
		let indexed = self
			.server
			.index
			.try_get_processes_with_data(&processes)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reported processes"))?;
		lost.processes = std::iter::zip(processes, indexed)
			.filter(|(_, indexed)| indexed.as_ref().is_none_or(|(_, data)| data.is_none()))
			.map(|(process, _)| process)
			.collect();

		Ok(lost)
	}

	/// Get the reported boots that the index records as stopped or destroyed, or at another boot.
	async fn get_ended_sandboxes(
		&self,
		sandboxes: &[tg::runner::control::ArgSandbox],
	) -> tg::Result<Vec<tg::runner::control::ArgSandbox>> {
		let ids = sandboxes
			.iter()
			.map(|sandbox| sandbox.id.clone())
			.collect::<Vec<_>>();
		let indexed = self
			.server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the reported sandboxes"))?;
		let ended = std::iter::zip(sandboxes, indexed)
			.filter(|(sandbox, indexed)| {
				indexed
					.as_ref()
					.and_then(|indexed| indexed.data.as_ref())
					.is_some_and(|data| {
						data.data.status.is_stopped_or_destroyed() || data.data.boot != sandbox.boot
					})
			})
			.map(|(sandbox, _)| sandbox.clone())
			.collect();

		Ok(ended)
	}

	/// Get the unanswered offers whose boots the runner wrote to the index. The runner's writes of a boot follow the scheduler's handoff write, whose sequence follows the offer's.
	async fn get_written_offers(
		&self,
		unanswered: &[UnansweredOffer],
	) -> tg::Result<BTreeSet<tg::sandbox::Id>> {
		let ids = unanswered
			.iter()
			.map(|offer| offer.sandbox.clone())
			.collect::<Vec<_>>();
		let indexed = self
			.server
			.index
			.try_get_sandboxes(&ids)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the offered sandboxes"))?;
		let written = std::iter::zip(unanswered, indexed)
			.filter(|(offer, indexed)| {
				indexed.as_ref().is_some_and(|indexed| {
					indexed.sequence > offer.sequence + 1
						&& indexed
							.data
							.as_ref()
							.is_some_and(|data| data.data.boot == offer.boot)
				})
			})
			.map(|(offer, _)| offer.sandbox.clone())
			.collect();

		Ok(written)
	}

	/// Take over the boots of a deleted runner, and destroy its stopped sandboxes.
	async fn delete_runner(
		&self,
		runner: &tg::runner::Id,
		queued: Vec<tg::sandbox::Id>,
	) -> tg::Result<()> {
		// Await indexing, so that the runner's sandboxes are read with every write that was acknowledged before the deletion.
		if let Err(error) = self.index().await {
			tracing::warn!(error = %error.trace(), "failed to await indexing");
		}

		// Take over the runner's boots.
		let error = tg::error::Data {
			code: Some(tg::error::Code::Internal),
			message: Some("the runner was deleted".to_owned()),
			..Default::default()
		};
		let arg = EndRunnerSandboxesArg {
			checkpoint: "scheduler.runner.deleted",
			directories: None,
			error: error.clone(),
			fence: None,
			held: Vec::new(),
			queued: queued.clone(),
			runner: runner.clone(),
			scheduler: Some(self.holder_sender.clone()),
			written: Vec::new(),
		};
		self.component
			.end_runner_sandboxes(&self.server, &self.id, arg)
			.boxed()
			.await?;

		// Destroy the runner's stopped and destroying sandboxes, whose directories are gone with the runner.
		let sandboxes = self.server.index.get_runner_sandboxes(runner).await?;
		for sandbox in sandboxes {
			if queued.contains(&sandbox) {
				continue;
			}
			self.component
				.destroy_runner_sandbox(&self.server, &sandbox, Some(error.clone()), None, false)
				.boxed()
				.await?;
		}

		Ok(())
	}

	/// Finish the sandboxes that a removed runner held. The queued sandboxes are the ones whose current boot this scheduler holds, which the runner never held.
	pub(super) async fn remove_runner(
		&self,
		request: RemoveRunnerRequestArg,
		queued: Vec<tg::sandbox::Id>,
	) -> tg::Result<RemoveRunnerResponseOutput> {
		// Await indexing, so that the runner's sandboxes are read with every write that was acknowledged before the runner was removed, and read the index as it is if indexing cannot be awaited.
		if let Err(error) = self.index().await {
			tracing::warn!(error = %error.trace(), "failed to await indexing");
		}

		// Take over the runner's boots for the removed connection.
		let fence = Fence {
			connection: request.connection,
			connections: self.connections.clone(),
		};
		self.component
			.handle_expired_runner(
				&self.server,
				&self.id,
				&request.runner,
				&queued,
				Some(fence),
				&self.holder_sender,
			)
			.boxed()
			.await?;

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.removed", runner = %request.runner).await;

		let output = RemoveRunnerResponseOutput {
			runner: request.runner,
		};

		Ok(output)
	}
}

impl Component {
	pub(crate) async fn handle_expired_runner(
		&self,
		server: &Server,
		scheduler: &tg::scheduler::Id,
		runner: &tg::runner::Id,
		queued: &[tg::sandbox::Id],
		fence: Option<Fence>,
		task: &tokio::sync::mpsc::UnboundedSender<super::HolderMessage>,
	) -> tg::Result<()> {
		let error = tg::error::Data {
			code: Some(tg::error::Code::Internal),
			message: Some("heartbeat expired".to_owned()),
			..Default::default()
		};
		let arg = EndRunnerSandboxesArg {
			checkpoint: "scheduler.runner.expired",
			directories: None,
			error,
			fence,
			held: Vec::new(),
			queued: queued.to_vec(),
			runner: runner.clone(),
			scheduler: Some(task.clone()),
			written: Vec::new(),
		};
		self.end_runner_sandboxes(server, scheduler, arg).await?;

		Ok(())
	}

	/// Take over the boots that the index records on a runner and that neither the runner reports nor the scheduler holds or has in its saved queue, each with one conditional write that ends the boot only while the index has it at the boot, not ended, on the runner, and decide which of the sandboxes with directories that the runner reports keep their directories.
	pub(super) async fn end_runner_sandboxes(
		&self,
		server: &Server,
		scheduler: &tg::scheduler::Id,
		arg: EndRunnerSandboxesArg,
	) -> tg::Result<EndRunnerSandboxesOutput> {
		let EndRunnerSandboxesArg {
			checkpoint,
			directories,
			error,
			fence,
			held,
			queued,
			runner,
			scheduler: task,
			written,
		} = arg;
		let runner = &runner;

		// Read the sandboxes that the index records on the runner.
		let sandboxes =
			server.index.get_runner_sandboxes(runner).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		tracing::warn!(%runner, sandboxes = sandboxes.len(), reason = error.message.as_deref(), "ending the runner sandboxes");
		tangram_server_core::checkpoint!(server, checkpoint, %runner).await;
		let indexed =
			server.index.try_get_sandboxes(&sandboxes).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;

		// Get the scheduler's queue after reading the index, which is its saved queue, the sandboxes whose current boot it holds in memory, and the ones whose next boot a start is reserving, so that a boot that the index read shows in the queue is left to the scheduler, because only a boot's holder records its end.
		let saved = self
			.get_scheduler_queue(server, scheduler, task.as_ref())
			.await?;
		let mut destroyed = Vec::new();
		let mut stopped = Vec::new();
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
			// Skip a sandbox in the scheduler's queue, unless the runner wrote the boot of its unanswered offer.
			let in_queue = queued.contains(&sandbox) || saved.contains(&sandbox);
			if in_queue && !written.contains(&sandbox) {
				continue;
			}

			// Skip a sandbox whose recorded boot the runner holds.
			let data = indexed
				.and_then(|indexed| indexed.data)
				.map(|data| data.data);
			let boot = data.as_ref().map(|data| data.boot);
			if held
				.iter()
				.any(|held| held.id == sandbox && Some(held.boot) == boot)
			{
				continue;
			}

			// Destroy a sandbox with a zero ttl, because its processes are lost, a sandbox without a directory, because it cannot start again, and a sandbox that is being destroyed on a runner that reports its directories, which removes the directory, and stop the others.
			let destroying = directories.is_some()
				&& data
					.as_ref()
					.is_some_and(|data| data.status.is_destroying());
			let time_to_live = data.and_then(|data| data.time_to_live);
			let zero = time_to_live.is_some_and(|time_to_live| time_to_live.is_zero());
			let missing = directories
				.as_ref()
				.is_some_and(|directories| !directories.contains(&sandbox));
			let status = if destroying || zero || missing {
				tg::sandbox::Status::Destroyed
			} else {
				tg::sandbox::Status::Stopped
			};

			// Hold the connections' lock for the write, and end nothing more once a newer connection of the runner replaced the one that the takeover is for, because the newer connection reported the boots that the runner holds.
			let guard = if let Some(fence) = &fence {
				let guard = fence.connections.lock.lock().await;
				if !fence.connections.is_current(runner, fence.connection) {
					tracing::info!(%runner, connection = fence.connection, "skipping the takeover for a replaced runner connection");
					break;
				}
				Some(guard)
			} else {
				None
			};
			self.finish_runner_sandbox(
				server,
				&sandbox,
				boot,
				Some(error.clone()),
				status,
				Some(runner),
				true,
			)
			.boxed()
			.await?;
			drop(guard);
			if directories
				.as_ref()
				.is_some_and(|directories| directories.contains(&sandbox))
			{
				if status.is_destroyed() {
					destroyed.push(sandbox);
				} else {
					stopped.push(sandbox);
				}
			}
		}

		// Decide which of the reported directories the runner keeps, which are none of the ones whose sandboxes the takeover destroyed.
		let mut kept = if let Some(directories) = directories {
			self.get_kept_directories(server, runner, directories, &saved, &queued, false)
				.await?
		} else {
			Vec::new()
		};
		kept.retain(|sandbox| !destroyed.contains(sandbox));
		stopped.retain(|sandbox| kept.contains(sandbox));
		let output = EndRunnerSandboxesOutput {
			destroyed,
			kept,
			stopped,
		};

		Ok(output)
	}

	/// Get the reported directories that the runner keeps, which are the ones of the sandboxes that the index records as stopped on the runner, whose boot the scheduler holds, or that the scheduler's saved queue has, and, if `destroying` is set, the ones of the sandboxes that the index records as destroying on the runner, whose directories this scheduler has the runner remove. The runner removes every other directory.
	async fn get_kept_directories(
		&self,
		server: &Server,
		runner: &tg::runner::Id,
		directories: BTreeSet<tg::sandbox::Id>,
		saved: &HashSet<tg::sandbox::Id, tg::id::BuildHasher>,
		queued: &[tg::sandbox::Id],
		destroying: bool,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let directories = directories.into_iter().collect::<Vec<_>>();
		let indexed =
			server.index.try_get_sandboxes(&directories).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		let mut kept = Vec::new();
		for (sandbox, indexed) in std::iter::zip(directories, indexed) {
			let Some(indexed) = indexed else {
				continue;
			};
			if indexed.runner.as_ref() != Some(runner) {
				continue;
			}
			let data = indexed.data.as_ref().map(|data| &data.data);
			let keep = data.is_some_and(|data| {
				data.status.is_stopped() || (destroying && data.status.is_destroying())
			});
			if keep || saved.contains(&sandbox) || queued.contains(&sandbox) {
				kept.push(sandbox);
			}
		}

		Ok(kept)
	}

	/// Get the sandboxes in a scheduler's queue, which are the ones in its saved queue and, if its scheduling task runs, the ones whose current boot it holds in memory, including the ones whose next boot a start is reserving.
	async fn get_scheduler_queue(
		&self,
		server: &Server,
		scheduler: &tg::scheduler::Id,
		task: Option<&tokio::sync::mpsc::UnboundedSender<super::HolderMessage>>,
	) -> tg::Result<HashSet<tg::sandbox::Id, tg::id::BuildHasher>> {
		// Read the saved queue.
		let mut queue = server
			.index
			.get_scheduler_sandboxes(scheduler)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<HashSet<_, tg::id::BuildHasher>>();

		// Ask the scheduling task for the sandboxes that it holds in memory.
		if let Some(task) = task {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			task.send(super::HolderMessage::Held { sender })
				.map_err(|_| tg::error!("the scheduling task stopped"))?;
			let held = receiver
				.await
				.map_err(|_| tg::error!("the scheduling task stopped"))?;
			queue.extend(held);
		}

		Ok(queue)
	}

	pub(crate) async fn destroy_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		takeover: Option<&tg::runner::Id>,
		processes: bool,
	) -> tg::Result<()> {
		self.finish_runner_sandbox(
			server,
			id,
			None,
			error,
			tg::sandbox::Status::Destroyed,
			takeover,
			processes,
		)
		.boxed()
		.await
	}

	pub(crate) async fn stop_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
		processes: bool,
	) -> tg::Result<()> {
		self.finish_runner_sandbox(
			server,
			id,
			None,
			Some(error),
			tg::sandbox::Status::Stopped,
			None,
			processes,
		)
		.boxed()
		.await
	}

	#[expect(
		clippy::too_many_arguments,
		reason = "the end of a runner's sandbox has this many inputs"
	)]
	async fn finish_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		read_boot: Option<u64>,
		error: Option<tg::error::Data>,
		status: tg::sandbox::Status,
		takeover: Option<&tg::runner::Id>,
		processes: bool,
	) -> tg::Result<()> {
		// Leave a sandbox that is already stopped or destroyed alone, or that moved past the boot that the caller read.
		let data = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.and_then(|sandbox| sandbox.data);
		let boot = data.as_ref().map(|data| data.data.boot);
		if let (Some(read_boot), Some(boot)) = (read_boot, boot)
			&& boot > read_boot
		{
			return Ok(());
		}
		let stopped_or_destroyed = data.as_ref().is_some_and(|data| match status {
			tg::sandbox::Status::Destroyed => data.data.status.is_destroyed(),
			tg::sandbox::Status::Destroying
			| tg::sandbox::Status::Started
			| tg::sandbox::Status::Starting
			| tg::sandbox::Status::Stopped
			| tg::sandbox::Status::Stopping => data.data.status.is_stopped_or_destroyed(),
		});
		if stopped_or_destroyed {
			return Ok(());
		}
		tracing::warn!(sandbox = %id, reason = error.as_ref().and_then(|error| error.message.as_deref()), %status, "finishing the runner sandbox");

		// End a running boot of a runner that is taken over with one write that the index applies only while the boot is not ended and on the runner, and leave a boot that its holder ended or a later boot alone. The takeover leaves the sandbox stopped, unless the caller ends it as destroyed, such as a sandbox with a zero ttl, which has no stopped state.
		let running = data.as_ref().is_some_and(|data| {
			matches!(
				data.data.status,
				tg::sandbox::Status::Started
					| tg::sandbox::Status::Starting
					| tg::sandbox::Status::Stopping
			)
		});
		let takeover =
			takeover
				.filter(|_| running)
				.zip(read_boot.or(boot))
				.map(|(runner, boot)| tangram_index::sandbox::put::Takeover {
					boot,
					runner: runner.clone(),
				});
		let ended = self
			.put_runner_sandbox_status(server, id, error.clone(), status, takeover)
			.await?;

		// Finish the processes of the ended boot with the error, or with a cancellation if the sandbox ends without one.
		if ended
			&& processes
			&& let Some(boot) = boot
		{
			let error = error.unwrap_or_else(|| tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the sandbox was destroyed".to_owned()),
				..Default::default()
			});
			self.finish_runner_sandbox_processes(server, id, boot, error)
				.boxed()
				.await?;
		}

		// Publish the sandbox's status once the ended boot's processes are finished, so that a reader that observes the end also observes its processes' ends.
		if ended {
			server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(server, id);
		}

		Ok(())
	}

	/// Write the status of a runner's sandbox, as a takeover of the runner's boot if a runner is given, and as a destroy that the index applies only while the sandbox is stopped at its boot if the sandbox is stopped, because a stopped sandbox has no holder and the index decides its destroy. Return whether the index applied the write.
	async fn put_runner_sandbox_status(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		status: tg::sandbox::Status,
		takeover: Option<tangram_index::sandbox::put::Takeover>,
	) -> tg::Result<bool> {
		// Read the sandbox and its processes.
		let now = server.clock.unix_timestamp();
		let length = server
			.index
			.try_get_sandbox_processes_count(id)
			.await?
			.unwrap_or_default();
		let processes = server
			.index
			.try_get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
			.await?
			.unwrap_or_default();
		let mut indexed = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox in the index"))?;

		// Set the sandbox's error and status, and clear its usage, which the index already records for each boot, so that the sum of every boot is not recorded again as this boot's usage.
		let data = indexed
			.data
			.as_mut()
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?;
		let stopped_boot =
			(status.is_destroyed() && data.data.status.is_stopped()).then_some(data.data.boot);

		// Record no error if the sandbox ends without one, except that a destroying or stopped sandbox keeps the error that it recorded.
		if error.is_some() || !(data.data.status.is_destroying() || data.data.status.is_stopped()) {
			data.data.error = error.map(tg::Either::Left);
		}
		data.data.status = status;
		data.data.usage = None;

		// Record a stop as of now, so that the stopped sandbox is charged and expires from now rather than from an earlier stop.
		if status.is_stopped() {
			data.data.expires_at = data.data.time_to_live.map(|time_to_live| {
				now.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.data.stopped_at = Some(now);
		}
		let account = if server.config.usage.enabled {
			indexed.account
		} else {
			None
		};

		// Remove the ended boot from its scheduler's queue in the same write as its status, because no scheduler holds an ended boot.
		let delete = tangram_index::scheduler::sandbox::delete::Arg {
			boot: data.data.boot,
			sandbox: id.clone(),
		};
		let put = tangram_index::sandbox::put::Arg {
			account,
			created_at: indexed.created_at,
			data: indexed.data,
			id: id.clone(),
			location: None,
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			process: None,
			processes: Some(processes),
			runner: indexed.runner,
			scheduler: None,
			sequence: 0,
			stopped_boot,
			stopped_usage: None,
			takeover,
			touched_at: now,
		};
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(put)),
				tangram_index::Item::DeleteSchedulerSandbox(Box::new(delete)),
			],
		};
		let applied = server
			.index
			.write(arg)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the finished sandbox in the index"),
			)?
			.is_ok();
		Ok(applied)
	}

	/// Finish the unfinished processes of exactly a boot that ended, and leave the processes of other boots.
	async fn finish_runner_sandbox_processes(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		boot: u64,
		error: tg::error::Data,
	) -> tg::Result<()> {
		// Read the sandbox's processes.
		let length = server
			.index
			.try_get_sandbox_processes_count(id)
			.await?
			.unwrap_or_default();
		let processes = server
			.index
			.get_sandbox_processes(id, std::io::SeekFrom::Start(0), length)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox processes"))?;
		let indexed = server
			.index
			.try_get_processes_with_data(&processes)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process records"))?;
		let process_error = error;

		// Finish each unfinished process of the boot.
		for (process, indexed) in processes.iter().zip(indexed) {
			let Some((indexed, data)) = indexed else {
				continue;
			};
			let process = process.clone();
			if indexed.boot.is_some_and(|value| value != boot) {
				continue;
			}
			let Some(mut data) = data else {
				continue;
			};
			if data.status.is_queued() {
				let error = tg::Either::Left(process_error.clone());
				let result = server
					.components
					.process
					.finish_queued_process(server, &process, data, error)
					.boxed()
					.await;
				if let Err(error) = result {
					tracing::debug!(error = %error.trace(), %process, "failed to finish the queued process");
				}
			} else if data.status.is_started() {
				let recover_log =
					data.log.is_none() && (data.stdout.is_log() || data.stderr.is_log());
				if recover_log {
					match self
						.finish_expired_process_log(server, &process)
						.boxed()
						.await
					{
						Ok(log) => data.log = Some(log),
						Err(error) => {
							tracing::error!(error = %error.trace(), %process, "failed to recover the process log");
						},
					}
				}
				data.cacheable = false;
				data.set_error(Some(tg::Either::Left(process_error.clone())));
				data.exit = Some(1);
				data.finished_at = Some(server.clock.unix_timestamp());
				data.status = tg::process::Status::Finished;
				let mut context = server.context.clone();
				context.principal = tg::Principal::Process(process.clone());
				let session = server.session(&context);
				let entry = tg::process::put::Arg {
					data: data.clone(),
					location: None,
					public: false,
				};
				let options = tangram_server_process_core::put::Options {
					command_sync: None,
					defer_index: false,
					location: None,
					outcome_sync: None,
					store_data: true,
				};

				// Finish the started process.
				let result = session
					.server
					.components
					.process
					.put_process_local(&session, &process, entry, options)
					.boxed()
					.await;
				if let Err(error) = result {
					tracing::debug!(error = %error.trace(), %process, "failed to finish the started process");
					continue;
				}

				// Finalize the cache once the process write succeeds.
				if recover_log {
					self.end_expired_process_log(server, &process).await?;
				}
				let session = server.session(&server.context);
				if let Err(error) = session
					.server
					.components
					.process
					.expire_process_log_cache(&session, &process, &data)
					.await
				{
					tracing::error!(error = %error.trace(), %process, "failed to expire the process log cache");
				}
				server
					.components
					.process
					.notify_process_log(server, &process);
				session
					.server
					.components
					.process
					.spawn_process_finish_tasks(&session, &process);
			}
		}

		Ok(())
	}

	async fn finish_expired_process_log(
		&self,
		server: &Server,
		process: &tg::process::Id,
	) -> tg::Result<tg::Referent<tg::blob::Id>> {
		let session = server.session(&server.context);
		let mut writer = tangram_server_log_common::Writer::new(&session);
		let mut position = 0;
		let streams = BTreeSet::from([
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		]);
		loop {
			let arg = log::read::Arg {
				length: 64 * 1024,
				position,
				process: process.clone(),
				streams: streams.clone(),
			};
			let entries = server.cache.try_read_log(arg).await?;
			if entries.is_empty() {
				break;
			}
			for entry in entries {
				let bytes = bytes::Bytes::copy_from_slice(&entry.bytes);
				position = entry.position + u64::try_from(bytes.len()).unwrap();
				let chunk = tg::process::stdio::Chunk {
					bytes,
					combined_position: entry.position,
					stream: entry.stream,
					stream_position: entry.stream_position,
					timestamp: Some(entry.timestamp),
				};
				writer.write(&chunk).await?;
			}
		}
		let log = writer.end().await?;
		Ok(log)
	}

	async fn end_expired_process_log(
		&self,
		server: &Server,
		process: &tg::process::Id,
	) -> tg::Result<()> {
		if server.cache.try_get_log_end(process).await?.is_some() {
			return Ok(());
		}

		// Recover the stored positions because the expired writer cannot send its end request.
		let length = async |streams| {
			let arg = log::length::Arg {
				process: process.clone(),
				streams,
			};
			let position = server
				.cache
				.try_get_log_length(arg)
				.await?
				.unwrap_or_default();
			Ok::<_, tg::Error>(position)
		};
		let (position, stderr_position, stdout_position) = tokio::try_join!(
			length(BTreeSet::from([
				tg::process::stdio::Stream::Stderr,
				tg::process::stdio::Stream::Stdout,
			])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stderr])),
			length(BTreeSet::from([tg::process::stdio::Stream::Stdout])),
		)
		.map_err(|error| tg::error!(!error, %process, "failed to get the log positions"))?;
		let end = tg::process::log::End {
			position,
			stderr_position,
			stdout_position,
		};
		let arg = log::end::Arg {
			end,
			process: process.clone(),
		};

		// Persist the marker before publishing completion.
		server
			.cache
			.put_log_end(arg)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to store the log end"))?;

		Ok(())
	}
}
