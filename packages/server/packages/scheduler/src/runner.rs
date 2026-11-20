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
		time::Duration,
	},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::Server,
};

#[cfg(test)]
mod tests;

/// The sandboxes of a runner that a registration ends.
pub(super) struct EndRunnerSandboxesArg {
	/// The checkpoint that the routine hits once it has read the runner's sandboxes.
	pub checkpoint: &'static str,

	/// The sandboxes that the runner has directories for, or `None` if they are unknown.
	pub directories: Option<BTreeSet<tg::sandbox::Id>>,

	/// The error that the ended sandboxes and their processes record.
	pub error: tg::error::Data,

	/// The boots that the runner holds.
	pub held: Vec<tg::runner::control::ArgSandbox>,

	/// The sandboxes whose current boot the scheduler holds.
	pub queued: Vec<tg::sandbox::Id>,
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

	pub registration_index: u64,

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
			.map(|(id, runner)| (id.clone(), runner.registration_index))
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
		if self
			.runners
			.entries
			.get(&request.runner)
			.is_some_and(|runner| request.registration_index < runner.registration_index)
		{
			let error = tg::error!(
				code = tg::error::Code::Cancellation,
				"the runner registration was superseded"
			);
			let response = super::request::Response::new(id, Err(error));
			scheduler.send_response(self, response);
			return;
		}
		let registration_index = request.registration_index;
		tracing::info!(runner = %request.runner, registration_index, "registering the runner");
		self.handle_recovery_report(
			scheduler,
			&request.runner,
			registration_index,
			&request.sandboxes,
		);
		self.forget_accepted_sandboxes(scheduler, &request.runner, &request.sandboxes);
		self.remove_runner(scheduler, &request.runner);
		let runner = Runner {
			borrowable: HashSet::default(),
			capacity: request.capacity,
			committed: tg::runner::Capacity::default(),
			heartbeat_at: scheduler.server.clock.instant(),
			heartbeat_index: 0,
			host: request.host.clone(),
			owner: None,
			registration_failed: false,
			registration_index: request.registration_index,
			reservations: HashMap::default(),
			reserved: tg::runner::Capacity::default(),
		};
		self.runners.entries.insert(request.runner.clone(), runner);

		// Track the sandboxes that the runner reports, which a restarted scheduler rebuilds its tracking from.
		for sandbox in &request.sandboxes {
			let runner = super::placement::RunnerRef {
				registration_index,
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
				let result = scheduler.add_runner(request, queued).await;
				Operation::AddRunner {
					registration_index,
					id,
					result,
					runner,
				}
			}
			.boxed(),
		);
	}

	pub(super) fn handle_destroy_runner_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: super::DestroyRunnerSandboxRequestArg,
	) {
		// Fail if the runner is not connected, because only the runner can destroy the sandbox.
		let Some(registration_index) = self
			.runners
			.entries
			.get(&request.runner)
			.filter(|runner| !runner.registration_failed)
			.map(|runner| runner.registration_index)
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
						registration_index,
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
			.is_some_and(|runner| runner.registration_index == request.registration_index);
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
		tracing::info!(runner = %request.runner, registration_index = request.registration_index, "removing the runner");
		self.forget_accepted_sandboxes(scheduler, &request.runner, &[]);
		self.remove_runner(scheduler, &request.runner);

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

	pub(super) fn handle_heartbeat(
		&mut self,
		notification: &HeartbeatNotification,
		now: tangram_server_core::clock::Instant,
	) -> bool {
		let Some(runner) = self.runners.entries.get_mut(&notification.runner) else {
			return false;
		};
		if runner.registration_failed
			|| runner.registration_index != notification.registration_index
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

	/// Forget the sandboxes that a runner accepted, except the ones that it reports when it registers again, and queue the ones again whose control connections were not established, if their keys remain.
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
		let mut lost = Vec::new();
		for (sandbox, _) in accepted {
			let entry = self.remove_sandbox(scheduler, &sandbox).unwrap();
			if let Some(request) = entry.request {
				lost.push(request);
			} else {
				self.stop_serving_sandbox_holder(&sandbox);
			}
		}
		if !lost.is_empty() {
			self.read_lost_sandboxes(scheduler, lost);
		}
	}

	/// Record that the scheduler failed to record a runner's registration, and keep the runner, so that it takes no sandboxes and its expiry ends the sandboxes that it accepted unless it registers again.
	pub(super) fn fail_runner_registration(
		&mut self,
		id: &tg::runner::Id,
		registration_index: u64,
	) {
		if let Some(runner) = self.runners.entries.get_mut(id)
			&& runner.registration_index == registration_index
		{
			runner.registration_failed = true;
		}
	}

	/// Remove a runner, and return the sandboxes that are offered to it to the queue. The sandboxes that it accepted stay until the runner registers again or expires.
	pub(super) fn remove_runner(&mut self, scheduler: &Scheduler, id: &tg::runner::Id) {
		let Some(runner) = self.runners.entries.remove(id) else {
			return;
		};

		// Forget the capacity that the runner's sandboxes lend.
		for parent_id in runner.borrowable {
			let Some(parent) = self.parents.0.get_mut(&parent_id) else {
				continue;
			};
			let remove = parent.borrowable.as_ref().is_some_and(|borrowable| {
				borrowable.runner == *id
					&& borrowable.registration_index == Some(runner.registration_index)
			});
			if remove {
				parent.borrowable = None;
				if parent.is_empty() {
					self.parents.0.remove(&parent_id);
				}
			}
		}

		// Return the sandboxes that are offered to the runner to the queue, and end the ones that a dequeue waits for.
		let sandboxes = runner.reservations.into_keys().collect::<Vec<_>>();
		for sandbox in sandboxes {
			self.requeue_sandbox(&sandbox);
			let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			if dequeue_requests.is_empty() {
				continue;
			}
			self.end_sandbox_and_log(scheduler, &sandbox, tg::sandbox::Status::Destroyed, None);
			self.send_dequeue_responses(scheduler, dequeue_requests, true);
		}
		self.queue.wake();
	}
}

impl Scheduler {
	async fn add_runner(
		&self,
		request: AddRunnerRequestArg,
		queued: Option<Vec<tg::sandbox::Id>>,
	) -> tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)> {
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

		tangram_server_core::checkpoint!(self.server, "scheduler.runner.register", runner = %request.runner, registration_index = request.registration_index).await;
		self.server
			.components
			.runner
			.set_runner_scheduler(
				&session,
				&request.runner,
				&self.id,
				request.registration_index,
			)
			.await?;

		// End the sandboxes that the index records on a runner that registers for the first time after it starts, and that neither the runner nor this scheduler holds, and decide which of its directories the runner keeps, after pending indexing so that no acknowledged write is missed, or with the index as it is if indexing cannot be awaited.
		let ended = if let (Some(directories), Some(queued)) = (&request.directories, queued) {
			if let Err(error) = self.index().await {
				tracing::warn!(error = %error.trace(), "failed to await indexing");
			}
			let registration = tangram_index::runner::Registration {
				index: request.registration_index,
				runner: request.runner.clone(),
				scheduler: self.id.clone(),
			};
			let error = tg::error::Data {
				code: Some(tg::error::Code::Internal),
				message: Some("runner restarted".to_owned()),
				..Default::default()
			};
			let arg = EndRunnerSandboxesArg {
				checkpoint: "scheduler.runner.restarted",
				directories: Some(directories.iter().cloned().collect()),
				error,
				held: request.sandboxes.clone(),
				queued,
			};
			let output = self
				.component
				.end_runner_sandboxes(&self.server, &registration, arg)
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
			destroyed,
			kept,
			runner: request.runner,
			scheduler: self.id.clone(),
			stopped,
		};

		Ok((output, owner))
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

		// Stop the runner's sandboxes only while this registration still owns the runner.
		let registration = tangram_index::runner::Registration {
			index: request.registration_index,
			runner: request.runner.clone(),
			scheduler: self.id.clone(),
		};
		let result = self
			.component
			.handle_expired_runner(&self.server, &registration, &queued)
			.boxed()
			.await;
		if let Err(error) = result
			&& !is_cancellation(&error)
		{
			return Err(error);
		}

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
		registration: &tangram_index::runner::Registration,
		queued: &[tg::sandbox::Id],
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
			held: Vec::new(),
			queued: queued.to_vec(),
		};
		self.end_runner_sandboxes(server, registration, arg).await?;

		Ok(())
	}

	/// End the sandboxes that the index records on a runner and that neither the runner nor the scheduler holds, with writes that only the registration may make, and decide which of the sandboxes with directories that the runner reports keep their directories.
	pub(super) async fn end_runner_sandboxes(
		&self,
		server: &Server,
		registration: &tangram_index::runner::Registration,
		arg: EndRunnerSandboxesArg,
	) -> tg::Result<EndRunnerSandboxesOutput> {
		let EndRunnerSandboxesArg {
			checkpoint,
			directories,
			error,
			held,
			queued,
		} = arg;

		// Record the registration before finishing the sandboxes, so that the index accepts the writes for a registration that the scheduler failed to record. The index rejects the registration if a newer one owns the runner.
		let session = server.session(&server.context);
		server
			.components
			.runner
			.set_runner_scheduler(
				&session,
				&registration.runner,
				&registration.scheduler,
				registration.index,
			)
			.await?;

		// Get the sandboxes that the runner held, which excludes the ones whose current boot the scheduler holds, because only a boot's holder records its end.
		let runner = &registration.runner;
		let sandboxes = server
			.index
			.get_runner_sandboxes(runner)
			.await
			.map_err(|error| tg::error!(!error, %runner, "failed to get the runner sandboxes"))?
			.into_iter()
			.filter(|sandbox| !queued.contains(sandbox))
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
			self.finish_runner_sandbox(
				server,
				&sandbox,
				Some(error.clone()),
				status,
				Some(registration),
			)
			.boxed()
			.await?;
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

		// Keep the directory of each reported sandbox that the index now records as stopped on the runner, or whose boot the scheduler holds, so that the runner removes every other directory.
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
				let stopped = indexed
					.data
					.as_ref()
					.is_some_and(|data| data.data.status.is_stopped());
				if indexed.runner.as_ref() == Some(runner) && (stopped || queued.contains(&sandbox))
				{
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
	) -> tg::Result<()> {
		self.finish_runner_sandbox(server, id, error, tg::sandbox::Status::Destroyed, None)
			.boxed()
			.await
	}

	pub(crate) async fn stop_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		self.finish_runner_sandbox(server, id, Some(error), tg::sandbox::Status::Stopped, None)
			.boxed()
			.await
	}

	async fn finish_runner_sandbox(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
		status: tg::sandbox::Status,
		registration: Option<&tangram_index::runner::Registration>,
	) -> tg::Result<()> {
		// Leave a sandbox that is already stopped or destroyed alone, and get the boot that ends.
		let data = server
			.index
			.try_get_sandbox(id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox from the index"))?
			.and_then(|sandbox| sandbox.data);
		let boot = data.as_ref().map(|data| data.data.boot);
		let stopped_or_destroyed = data.is_some_and(|data| match status {
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

		// Finish the started processes with the error, or with a cancellation if the sandbox ends without one, and the queued processes, which can never start once the sandbox ends, because a start of a stopped sandbox does not carry them and a spawn into it is refused.
		let process_error = error.clone().unwrap_or_else(|| tg::error::Data {
			code: Some(tg::error::Code::Cancellation),
			message: Some("the sandbox was destroyed".to_owned()),
			..Default::default()
		});
		let now = server.clock.unix_timestamp();
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
		for (process, indexed) in processes.iter().zip(indexed) {
			let Some((indexed, data)) = indexed else {
				continue;
			};
			let process = process.clone();
			let mut data = data.ok_or_else(|| tg::error!(%process, "missing the process data"))?;
			if data.status.is_queued() {
				// Finish the queued process only while the sandbox is at the boot that ends, and leave a process that is not finished, because the sandbox moved to a later boot or another finisher finished it first, to the later boot or that finisher. A superseded registration ends the routine.
				let error = tg::Either::Left(process_error.clone());
				let sandbox_boot = boot.map(|boot| tangram_index::process::put::SandboxBoot {
					boot,
					sandbox: id.clone(),
				});
				let result = server
					.components
					.process
					.finish_queued_process(
						server,
						&process,
						data,
						error,
						sandbox_boot,
						registration.cloned(),
					)
					.boxed()
					.await;
				if let Err(error) = result {
					if is_cancellation(&error) {
						return Err(error);
					}
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
				data.children.get_or_insert_default();
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
					registration: registration.cloned(),
					sandbox_boot: boot.map(|boot| tangram_index::process::put::SandboxBoot {
						boot,
						sandbox: id.clone(),
					}),
					store_data: true,
				};

				// Leave a process that is not finished, because the sandbox moved to a later boot or another finisher finished it first, to the later boot or that finisher. A superseded registration ends the routine.
				let result = session
					.server
					.components
					.process
					.put_process_local(&session, &process, entry, options)
					.boxed()
					.await;
				if let Err(error) = result {
					if is_cancellation(&error) {
						return Err(
							tg::error!(!error, %process, "failed to store the finished process"),
						);
					}
					tracing::debug!(error = %error.trace(), %process, "failed to finish the started process");
					continue;
				}

				// Finalize the cache only after the fenced process write succeeds.
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
			} else {
				// Remove the tokens before updating the index.
				let data = data.without_location_and_tokens_except_sync();

				let arg = tangram_index::write::Arg {
					items: std::iter::once(tangram_index::Item::PutProcess(Box::new(
						tangram_index::process::put::Arg {
							account: None,
							cached: false,
							captures: tangram_index::permission::capture::Captures::default(),
							children: None,
							command: Some(
								data.command
									.objects()
									.into_iter()
									.map(|object| object.node)
									.collect(),
							),
							command_id: data.command.command_id()?.into(),
							data: Some(data.clone()),
							error: None,
							id: process.clone(),
							location: None,
							log: None,
							metadata: indexed.metadata,
							options: tg::referent::Options::default(),
							output: None,
							parent: None,
							permissions: Vec::new(),
							principal: tg::Principal::Process(process.clone()),
							registration: registration.cloned(),
							sandbox: None,
							sandbox_boot: None,
							storage: indexed.storage,
							time_to_touch: server.config.process.time_to_touch,
							touched_at: now,
						},
					)))
					.collect(),
				};
				server
					.index
					.write(arg)
					.await
					.and_then(std::convert::identity)
					.map_err(
						|source| tg::error!(!source, %process, "failed to update the process in the index"),
					)?;
			}
		}

		// Finalize the process history that survived the runner.
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

		// Record no error if the sandbox ends without one, except that a destroying sandbox keeps the error that its destroy recorded.
		if error.is_some() || !data.data.status.is_destroying() {
			data.data.error = error.map(tg::Either::Left);
		}
		data.data.status = status;
		data.data.usage = None;

		// Record a stop as of now, so that the stopped sandbox is charged and expires from now rather than from an earlier stop.
		if status.is_stopped() {
			let stopped_at = server.clock.unix_timestamp();
			data.data.expires_at = data.data.time_to_live.map(|time_to_live| {
				stopped_at.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.data.stopped_at = Some(stopped_at);
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
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(tangram_index::sandbox::put::Arg {
					account,
					created_at: indexed.created_at,
					data: indexed.data,
					id: id.clone(),
					location: None,
					permissions: Vec::new(),
					principal: tg::Principal::Sandbox(id.clone()),
					process: None,
					processes: Some(processes),
					registration: registration.cloned(),
					runner: indexed.runner,
					stopped_boot: None,
					stopped_usage: None,
					touched_at: now,
				})),
				tangram_index::Item::DeleteSchedulerSandbox(Box::new(delete)),
			],
		};
		server
			.index
			.write(arg)
			.await
			.and_then(std::convert::identity)
			.map_err(
				|source| tg::error!(!source, %id, "failed to update the finished sandbox in the index"),
			)?;
		server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(server, id);

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

/// Get whether an error or one of its sources is a cancellation, such as the rejection of a superseded runner registration.
fn is_cancellation(error: &tg::Error) -> bool {
	let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
	while let Some(error) = source {
		if error
			.downcast_ref::<tg::Error>()
			.is_some_and(|error| matches!(error.code(), Some(tg::error::Code::Cancellation)))
		{
			return true;
		}
		source = error.source();
	}

	false
}
