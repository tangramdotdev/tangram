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

/// The runner connections that this scheduler numbered last, which a takeover for a connection checks under the lock before each of its writes, so that a takeover for a connection that a newer registration of the runner replaced ends nothing more.
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
}

/// The connection that a takeover is for.
pub(super) struct Fence {
	pub connection: u64,
	pub connections: Connections,
}

/// The reported sandboxes with directories that a registration ended or leaves to the runner.
pub(super) struct EndRunnerSandboxesOutput {
	/// The reported sandboxes that the registration destroyed.
	destroyed: Vec<tg::sandbox::Id>,

	/// The reported sandboxes whose directories the runner keeps, because the sandboxes are stopped on the runner or their boots wait in the scheduler's queue.
	kept: Vec<tg::sandbox::Id>,

	/// The reported sandboxes that the registration stopped, which `kept` also lists.
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

	pub heartbeat_at: tangram_server_core::clock::Instant,

	pub heartbeat_index: u64,

	pub host: String,

	pub owner: Option<tg::Id>,

	/// Whether the scheduler failed to record the runner's registration, in which case the runner takes no sandboxes, and expires with its sandboxes unless it registers again.
	pub registration_failed: bool,

	pub connection: u64,

	/// The capacity that each sandbox that is offered to the runner reserves, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
	pub reservations: HashMap<tg::sandbox::Id, tg::runner::Capacity, tg::id::BuildHasher>,

	/// The sum of the reservations.
	pub reserved: tg::runner::Capacity,
}

impl Runner {
	/// Whether the runner's registration succeeded, and the runner has sent a heartbeat and takes sandboxes.
	pub fn ready(&self) -> bool {
		!self.registration_failed && self.heartbeat_index > 0 && self.capacity.total.sandboxes != 0
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
		tracing::info!(runner = %request.runner, connection, "registering the runner");
		self.handle_recovery_report(scheduler, &request.runner, connection, &request.sandboxes);

		self.forget_accepted_sandboxes(scheduler, &request.runner, &request.sandboxes);
		self.remove_runner(scheduler, &request.runner, true);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			heartbeat_at: scheduler.server.clock.instant(),
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
			registration_failed: false,
			connection,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
		};
		self.runners.entries.insert(request.runner.clone(), runner);

		// Track the sandboxes that the runner reports, which a restarted scheduler rebuilds its tracking from.
		for sandbox in &request.sandboxes {
			let runner = super::placement::RunnerRef {
				connection,
				id: request.runner.clone(),
			};
			self.track_accepted_sandbox(scheduler, &sandbox.id, sandbox.boot, None, runner);
		}
		self.queue.wake();

		// Get the sandboxes whose current boot this scheduler holds if the runner registers for the first time after it starts, which the runner's registration leaves to this scheduler.
		let queued = request
			.directories
			.is_some()
			.then(|| self.get_held_sandboxes());

		let scheduler = scheduler.clone();
		let runner = request.runner.clone();
		self.operations.push(
			async move {
				let result = scheduler.add_runner(request, connection, queued).await;
				Operation::AddRunner {
					connection,
					id,
					result,
					runner,
				}
			}
			.boxed(),
		);
	}

	/// Track a sandbox that a runner started on the shortcut path, so that this scheduler knows the sandbox and sends the spawns into it to the runner.
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
		let result = if current {
			let runner = super::placement::RunnerRef {
				id: request.runner,
				connection: request.connection,
			};
			self.track_accepted_sandbox(scheduler, &request.sandbox, request.boot, None, runner);
			let output = tangram_server_scheduler_core::AddRunnerSandboxResponseOutput {};
			Ok(super::ResponseOutput::AddRunnerSandbox(output))
		} else {
			Err(
				tg::error!(sandbox = %request.sandbox, runner = %request.runner, "the runner's connection is not current"),
			)
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
			.filter(|runner| !runner.registration_failed)
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
		if runner.registration_failed
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

	/// Get the sandboxes whose current boot this scheduler holds, which are the ones that it queued, offered, or ended and holds until the index has recorded the end, and the ones that it holds for recovery.
	fn get_held_sandboxes(&self) -> Vec<tg::sandbox::Id> {
		let mut sandboxes = self
			.sandboxes
			.entries
			.iter()
			.filter(|(_, sandbox)| !matches!(sandbox.state, State::Accepted { .. }))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		sandboxes.extend(self.get_recovering_sandboxes());

		sandboxes
	}

	/// Forget the sandboxes that a runner accepted, except the ones that it reports when it registers again. The runner held a boot from its acceptance, so a boot that it no longer reports is lost with the runner, and the runner's cleanup ends it.
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

	/// Record that the scheduler failed to record a runner's registration, and keep the runner, so that it takes no sandboxes and its expiry ends the sandboxes that it accepted unless it registers again.
	pub(super) fn fail_runner_registration(&mut self, id: &tg::runner::Id, connection: u64) {
		if let Some(runner) = self.runners.entries.get_mut(id)
			&& runner.connection == connection
		{
			runner.registration_failed = true;
		}
	}

	/// Remove a runner. If the runner registers again, return the sandboxes whose offers to it are unanswered to the queue, because the runner reports every boot that it holds, and the sandboxes that it reports are tracked as accepted. Otherwise end them as failed, because the runner is declared dead and may have accepted them. The sandboxes that it accepted stay until the runner registers again or expires.
	pub(super) fn remove_runner(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		registered: bool,
	) {
		let Some(runner) = self.runners.entries.remove(id) else {
			return;
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

		// Return the sandboxes whose offers to a runner that registers again are unanswered to the queue, and end the ones of a runner that is declared dead as failed, because the runner may have accepted them, so that a boot is never offered to a second runner while the first may hold it. End the ones that a dequeue waits for as destroyed.
		let sandboxes = runner.reservations.into_keys().collect::<Vec<_>>();
		for sandbox in sandboxes {
			if registered {
				self.requeue_sandbox(&sandbox);
			}
			let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			if dequeue_requests.is_empty() {
				if !registered {
					let error =
						tg::error!(%sandbox, runner = %id, "the sandbox's runner is unavailable");
					let discarded = self.discard_sandbox(scheduler, &sandbox, &error);
					scheduler.publish_sandbox_discarded(self, discarded);
				}
				continue;
			}
			self.end_sandbox_and_log(scheduler, &sandbox, tg::sandbox::Status::Destroyed, None);
			self.send_dequeue_responses(scheduler, dequeue_requests, true);
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

	/// Whether a connection is the runner's current connection, which it is if no newer registration of the runner replaced it.
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
		queued: Option<Vec<tg::sandbox::Id>>,
	) -> tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)> {
		// Wait for a takeover write for an earlier connection of the runner that is in flight, so that no takeover for an earlier connection applies after this registration.
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
		tangram_server_core::checkpoint!(self.server, "scheduler.runner.register", runner = %request.runner, connection).await;
		self.server
			.components
			.runner
			.set_runner_scheduler(&session, &request.runner, &self.id)
			.await?;

		// Take over the boots that the index records on a runner that registers for the first time after it starts, and that neither the runner reports nor this scheduler's queue has, and decide which of its directories the runner keeps, after pending indexing so that no acknowledged write is missed, or with the index as it is if indexing cannot be awaited.
		let ended = if let (Some(directories), Some(queued)) = (&request.directories, queued) {
			if let Err(error) = self.index().await {
				tracing::warn!(error = %error.trace(), "failed to await indexing");
			}
			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("runner restarted".to_owned()),
				..Default::default()
			};
			let arg = EndRunnerSandboxesArg {
				checkpoint: "scheduler.runner.restarted",
				directories: Some(directories.iter().cloned().collect()),
				error,
				fence: Some(Fence {
					connection,
					connections: self.connections.clone(),
				}),
				held: request.sandboxes.clone(),
				queued,
				runner: request.runner.clone(),
			};
			let output = self
				.component
				.end_runner_sandboxes(&self.server, &self.id, arg)
				.boxed()
				.await?;
			Some(output)
		} else {
			None
		};

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.added", runner = %request.runner).await;
		let (destroyed, kept, stopped) = ended.map_or_else(Default::default, |output| {
			(output.destroyed, output.kept, output.stopped)
		});
		let output = AddRunnerResponseOutput {
			connection,
			destroyed,
			kept,
			runner: request.runner,
			scheduler: self.id.clone(),
			stopped,
		};

		Ok((output, owner))
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
		} = arg;
		let runner = &runner;

		// Get the sandboxes that the runner held, which excludes the ones whose current boot the scheduler holds and the ones in the scheduler's saved queue, because only a boot's holder records its end.
		let saved = server
			.index
			.get_scheduler_sandboxes(scheduler)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the scheduler's sandboxes"))?
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<HashSet<_, tg::id::BuildHasher>>();
		let sandboxes = server
			.index
			.get_runner_sandboxes(runner)
			.await
			.map_err(|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"))?
			.into_iter()
			.filter(|sandbox| !queued.contains(sandbox) && !saved.contains(sandbox))
			.collect::<Vec<_>>();
		tracing::warn!(%runner, sandboxes = sandboxes.len(), reason = error.message.as_deref(), "ending the runner sandboxes");
		tangram_server_core::checkpoint!(server, checkpoint, %runner).await;
		let indexed =
			server.index.try_get_sandboxes(&sandboxes).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
		let mut destroyed = Vec::new();
		let mut stopped = Vec::new();
		for (sandbox, indexed) in std::iter::zip(sandboxes, indexed) {
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

			// Hold the connections' lock for the write, and end nothing more once a newer connection of the runner replaced the one that the takeover is for, because the newer connection's registration reported the boots that the runner holds.
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

		// Keep the directory of each reported sandbox that the index now records as stopped on the runner, whose boot the scheduler holds, or that the scheduler's saved queue has, so that the runner removes every other directory.
		let mut kept = Vec::new();
		if let Some(directories) = directories {
			let directories = directories.into_iter().collect::<Vec<_>>();
			let indexed = server.index.try_get_sandboxes(&directories).await.map_err(
				|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"),
			)?;
			for (sandbox, indexed) in std::iter::zip(directories, indexed) {
				let Some(indexed) = indexed else {
					continue;
				};
				if indexed.runner.as_ref() != Some(runner) {
					continue;
				}
				let data = indexed.data.as_ref().map(|data| &data.data);
				let stopped = data.is_some_and(|data| data.status.is_stopped());
				if stopped || saved.contains(&sandbox) || queued.contains(&sandbox) {
					kept.push(sandbox);
				}
			}
		}
		stopped.retain(|sandbox| kept.contains(sandbox));
		let output = EndRunnerSandboxesOutput {
			destroyed,
			kept,
			stopped,
		};

		Ok(output)
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
		let takeover = takeover.filter(|_| running);
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
		takeover: Option<&tg::runner::Id>,
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
		let takeover = takeover.map(|runner| tangram_index::sandbox::put::Takeover {
			boot: data.data.boot,
			runner: runner.clone(),
		});

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
