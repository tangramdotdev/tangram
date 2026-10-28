use {
	super::{
		AcquireProcessLeaseRequestArg, AcquireProcessLeaseResponseOutput,
		BorrowableCapacityNotification, CancelSandboxStopRequestArg,
		CancelSandboxStopResponseOutput, Config, DequeueSandboxRequestArg,
		DequeueSandboxResponseOutput, EndSandboxRequestArg, EndSandboxResponseOutput,
		EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput, HeartbeatNotification, Operation,
		OwnerAncestors, PendingEnqueue, ReleaseProcessLeaseRequestArg,
		ReleaseProcessLeaseResponseOutput, ResponseOutput, Scheduler, SchedulerState,
		runner::{Reservation, ReservationState, Runner},
	},
	futures::{FutureExt as _, StreamExt as _, future},
	std::{
		collections::{HashMap, HashSet},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

pub(super) struct Parents(HashMap<tg::sandbox::Id, Parent, tg::id::BuildHasher>);

pub(super) struct Queue {
	entries: HashMap<tg::sandbox::Id, Links, tg::id::BuildHasher>,
	head: Option<tg::sandbox::Id>,
	remaining: usize,
	scan: Option<tg::sandbox::Id>,
	tail: Option<tg::sandbox::Id>,
}

pub(super) struct Sandboxes {
	attempts: usize,
	entries: HashMap<tg::sandbox::Id, Sandbox, tg::id::BuildHasher>,

	/// The generation of the most recently placed sandbox.
	generation: u64,

	loading: usize,

	/// The sandboxes that a runner holds whose control connections may not be established yet.
	placed: HashMap<tg::sandbox::Id, Placed, tg::id::BuildHasher>,

	/// The handles of the tasks that stop sandboxes once they start.
	stops: HashMap<tg::sandbox::Id, future::AbortHandle, tg::id::BuildHasher>,
}

/// A destroy of a sandbox on a runner. The request is present if the sandbox has a record to finish.
pub(super) struct DestroySandbox {
	pub error: tg::error::Data,
	pub registration_index: u64,
	pub request: Option<EnqueueSandboxRequestArg>,
	pub runner: tg::runner::Id,
	pub sandbox: tg::sandbox::Id,
}

pub(super) struct Placed {
	pub generation: u64,
	pub registration_index: u64,
	pub request: EnqueueSandboxRequestArg,
	pub runner: tg::runner::Id,
}

/// An end of a sandbox that waits for an in-flight placement to finish.
pub(super) struct HeldEnd {
	error: Option<tg::error::Data>,
	ids: Vec<String>,
	placed: Option<RunnerRef>,
	request: Option<EnqueueSandboxRequestArg>,
}

struct Parent {
	borrowable: Option<Borrowable>,
	queue: Queue,
}

struct Borrowable {
	available: bool,
	capacity: tg::runner::Capacity,
	registration_index: Option<u64>,
	heartbeat_index: u64,
	runner: tg::runner::Id,
}

struct Links {
	next: Option<tg::sandbox::Id>,
	previous: Option<tg::sandbox::Id>,
}

struct Sandbox {
	abandoned: bool,
	attempts: usize,
	blocked: HashMap<tg::runner::Id, Block, tg::id::BuildHasher>,
	capacity: tg::runner::Capacity,
	dequeue_requests: Vec<String>,
	held_end: Option<HeldEnd>,
	owner_ancestors: OwnerAncestors,
	persisted: bool,
	request: EnqueueSandboxRequestArg,
	retrying: bool,
	state: SandboxState,
	unplaceable_since: Option<tangram_server_core::clock::Instant>,
}

enum SandboxState {
	Creating { placement: Placement },
	Pending,
	Uncertain { placement: Placement },
}

struct Block {
	registration_index: u64,
	heartbeat_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Placement {
	Borrowed {
		parent: tg::sandbox::Id,
		runner: RunnerRef,
	},
	Regular {
		runner: RunnerRef,
	},
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RunnerRef {
	registration_index: u64,
	id: tg::runner::Id,
}

pub(super) struct Completion {
	placement: Placement,
	result: tg::Result<tg::Result<bool>>,
	sandbox: tg::sandbox::Id,
}

pub(super) struct AbandonedSandbox {
	runner: RunnerRef,
	sandbox: tg::sandbox::Id,
}

pub(super) struct CreateSandboxCompletionOutput {
	pub abandoned: Option<AbandonedSandbox>,
	pub dequeue_completions: Vec<DequeueCompletion>,
	pub discarded: Option<DiscardedSandbox>,
	pub held: Option<HeldEnd>,
}

pub(super) struct DequeueCompletion {
	pub output: DequeueSandboxResponseOutput,
	pub request: String,
}

pub(super) struct DiscardedSandbox {
	pub error: tg::Either<tg::error::Data, tg::error::Id>,
	pub request: EnqueueSandboxRequestArg,
	pub sandbox: tg::sandbox::Id,
}

impl Parents {
	pub fn new() -> Self {
		Self(HashMap::default())
	}
}

impl Queue {
	pub fn new() -> Self {
		Self {
			entries: HashMap::default(),
			head: None,
			remaining: 0,
			scan: None,
			tail: None,
		}
	}

	pub fn head(&self) -> Option<&tg::sandbox::Id> {
		self.head.as_ref()
	}

	pub fn insert(&mut self, id: tg::sandbox::Id) {
		if self.entries.contains_key(&id) {
			return;
		}
		let links = Links {
			next: None,
			previous: self.tail.clone(),
		};
		if let Some(tail) = &self.tail {
			self.entries.get_mut(tail).unwrap().next = Some(id.clone());
		} else {
			self.head = Some(id.clone());
		}
		self.tail = Some(id.clone());
		self.entries.insert(id.clone(), links);
		if self.scan.is_none() {
			self.scan = Some(id);
		}
		self.remaining += 1;
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn next(&mut self) -> Option<tg::sandbox::Id> {
		if self.remaining == 0 {
			return None;
		}
		let Some(id) = self.scan.clone() else {
			self.remaining = 0;
			return None;
		};
		self.scan = self.entries.get(&id).and_then(|links| links.next.clone());
		self.remaining -= 1;

		Some(id)
	}

	pub fn remove(&mut self, id: &tg::sandbox::Id) -> bool {
		self.remove_inner(id)
	}

	pub fn wake(&mut self) {
		self.remaining = self.entries.len();
		self.scan.clone_from(&self.head);
	}

	fn remove_inner(&mut self, id: &tg::sandbox::Id) -> bool {
		let Some(links) = self.entries.remove(id) else {
			return false;
		};
		if let Some(previous) = &links.previous {
			self.entries.get_mut(previous).unwrap().next = links.next.clone();
		} else {
			self.head.clone_from(&links.next);
		}
		if let Some(next) = &links.next {
			self.entries.get_mut(next).unwrap().previous = links.previous.clone();
		} else {
			self.tail.clone_from(&links.previous);
		}
		if self.scan.as_ref() == Some(id) {
			self.scan = links.next;
			self.remaining = self.remaining.saturating_sub(1);
		}

		true
	}
}

impl Sandboxes {
	pub fn new() -> Self {
		Self {
			attempts: 0,
			entries: HashMap::default(),
			generation: 0,
			loading: 0,
			placed: HashMap::default(),
			stops: HashMap::default(),
		}
	}
}

impl Borrowable {
	#[must_use]
	fn new(
		notification: BorrowableCapacityNotification,
		registration_index: Option<u64>,
		previous: Option<&Self>,
	) -> Self {
		// A pending borrower must not revoke an existing unconsumed reservation.
		let available = notification.available
			|| previous.is_some_and(|previous| {
				previous.available
					&& previous.capacity == notification.capacity
					&& previous.registration_index == registration_index
					&& previous.runner == notification.runner
			});
		let heartbeat_index = previous
			.filter(|previous| {
				previous.runner == notification.runner
					&& previous.registration_index == registration_index
			})
			.map_or(notification.heartbeat_index, |previous| {
				previous.heartbeat_index.max(notification.heartbeat_index)
			});
		Self {
			available,
			capacity: notification.capacity,
			registration_index,
			heartbeat_index,
			runner: notification.runner,
		}
	}
}

impl Sandboxes {
	/// Forget the task that stopped a sandbox.
	pub(super) fn finish_stop(&mut self, sandbox: &tg::sandbox::Id) {
		self.stops.remove(sandbox);
	}
}

impl SchedulerState {
	pub(super) fn new() -> Self {
		Self {
			operations: futures::stream::FuturesUnordered::new(),
			owner_ancestor_requests: HashMap::default(),
			owner_ancestors: HashMap::default(),
			parents: Parents::new(),
			pending_enqueues: HashMap::default(),
			persisting: HashMap::default(),
			queue: Queue::new(),
			recovery: None,
			requests: super::Requests {
				active: tangram_server_core::control::requests::Requests::default(),
				inbox: std::collections::HashSet::new(),
				outbox: HashMap::new(),
				replies: 0,
			},
			runners: super::runner::Runners::new(),
			sandboxes: Sandboxes::new(),
		}
	}

	pub(super) fn can_schedule(&self, scheduler: &Scheduler) -> bool {
		self.sandboxes.attempts < scheduler.config.max_create_sandbox_requests
			&& self.queue.remaining > 0
	}

	pub(super) fn handle_enqueue_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: Option<String>,
		request: EnqueueSandboxRequestArg,
		recovered: bool,
	) {
		let ids = id.into_iter().collect::<Vec<_>>();
		if self.sandboxes.entries.contains_key(&request.sandbox) {
			let sandbox = request.sandbox;
			self.finish_enqueue(scheduler, ids, &sandbox, Ok(()));
			return;
		}
		if let Some(pending) = self.pending_enqueues.get_mut(&request.sandbox) {
			pending.ids.extend(ids);
			return;
		}
		if !recovered
			&& self.queue.len() + self.sandboxes.loading
				>= scheduler.config.create_sandbox_queue_capacity
		{
			let error = tg::error!(sandbox = %request.sandbox, "the scheduler queue is full");
			let sandbox = request.sandbox;
			self.finish_enqueue(scheduler, ids, &sandbox, Err(error));
			return;
		}
		let owner = request.owner.to_id();
		let Some(owner) = owner else {
			let ancestors = Arc::new(HashSet::default());
			let sandbox = request.sandbox.clone();
			let result = self.enqueue_sandbox(scheduler, request, ancestors, recovered);
			self.finish_enqueue(scheduler, ids, &sandbox, result);
			return;
		};
		if let Some(ancestors) = self.owner_ancestors.get(&owner).cloned() {
			let sandbox = request.sandbox.clone();
			let result = self.enqueue_sandbox(scheduler, request, ancestors, recovered);
			self.finish_enqueue(scheduler, ids, &sandbox, result);
			return;
		}
		self.sandboxes.loading += 1;
		let sandbox = request.sandbox.clone();
		let pending = PendingEnqueue {
			ids,
			recovered,
			request,
		};
		self.pending_enqueues.insert(sandbox.clone(), pending);
		let requests = self
			.owner_ancestor_requests
			.entry(owner.clone())
			.or_default();
		requests.push(sandbox);
		if requests.len() > 1 {
			return;
		}
		let server = scheduler.server.clone();
		let owner_for_operation = owner.clone();
		self.operations.push(
			async move {
				let result = server
					.index
					.try_get_ancestors(&owner_for_operation)
					.await
					.and_then(|ancestors| {
						ancestors.ok_or_else(
							|| tg::error!(owner = %owner_for_operation, "failed to find the owner"),
						)
					});
				Operation::LoadOwnerAncestors { owner, result }
			}
			.boxed(),
		);
	}

	pub(super) fn handle_load_owner_ancestors_completion(
		&mut self,
		scheduler: &Scheduler,
		owner: tg::Id,
		result: tg::Result<Vec<tg::Id>>,
	) {
		let sandboxes = self
			.owner_ancestor_requests
			.remove(&owner)
			.unwrap_or_default();
		let result: tg::Result<OwnerAncestors> =
			result.map(|ancestors| Arc::new(ancestors.into_iter().collect()));
		if let Ok(ancestors) = &result {
			self.owner_ancestors.insert(owner, ancestors.clone());
		}
		for sandbox in sandboxes {
			let Some(pending) = self.pending_enqueues.remove(&sandbox) else {
				continue;
			};
			self.sandboxes.loading = self.sandboxes.loading.saturating_sub(1);
			let enqueued = match &result {
				Ok(ancestors) => self.enqueue_sandbox(
					scheduler,
					pending.request,
					ancestors.clone(),
					pending.recovered,
				),
				Err(error) => Err(error.clone()),
			};
			self.finish_enqueue(scheduler, pending.ids, &sandbox, enqueued);
		}
	}

	/// Respond to enqueue requests. A request that the scheduling task accepted responds once its index write is enqueued.
	fn finish_enqueue(
		&mut self,
		scheduler: &Scheduler,
		ids: Vec<String>,
		sandbox: &tg::sandbox::Id,
		result: tg::Result<()>,
	) {
		if result.is_ok()
			&& let Some(entry) = self.sandboxes.entries.get(sandbox)
			&& !entry.persisted
		{
			let request = entry.request.clone();
			let first = !self.persisting.contains_key(sandbox);
			self.persisting
				.entry(sandbox.clone())
				.or_default()
				.extend(ids);
			if first {
				let scheduler = scheduler.clone();
				let sandbox = sandbox.clone();
				self.operations.push(
					async move {
						let result = scheduler.persist_sandbox(&request).await;
						Operation::PersistSandbox { result, sandbox }
					}
					.boxed(),
				);
			}
			return;
		}
		self.send_enqueue_responses(scheduler, ids, result);
	}

	fn send_enqueue_responses(
		&mut self,
		scheduler: &Scheduler,
		ids: Vec<String>,
		result: tg::Result<()>,
	) {
		let result =
			result.map(|()| ResponseOutput::EnqueueSandbox(EnqueueSandboxResponseOutput {}));
		for id in ids {
			let response = scheduler.response(id, result.clone());
			scheduler.send_response(self, response);
		}
	}

	pub(super) fn handle_persist_sandbox_completion(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		result: tg::Result<()>,
	) {
		let ids = self.persisting.remove(id).unwrap_or_default();
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to record the accepted sandbox");
		}
		if let Some(sandbox) = self.sandboxes.entries.get_mut(id) {
			if result.is_ok() {
				sandbox.persisted = true;
			} else if matches!(sandbox.state, SandboxState::Pending) {
				self.remove_queued_sandbox(id);
				self.sandboxes.entries.remove(id);
			} else {
				sandbox.abandoned = true;
			}
		}
		self.send_enqueue_responses(scheduler, ids, result);
	}

	fn enqueue_sandbox(
		&mut self,
		scheduler: &Scheduler,
		mut request: EnqueueSandboxRequestArg,
		owner_ancestors: OwnerAncestors,
		recovered: bool,
	) -> tg::Result<()> {
		if self.sandboxes.entries.contains_key(&request.sandbox) {
			return Ok(());
		}
		// Resolve the sandbox's resources, so that the runner creates the sandbox with the bounds that the scheduler admits.
		tangram_server_sandbox_common::resolve_sandbox_resources(
			&scheduler.config.default_capacity,
			&mut request.arg,
		);

		// Place a start of a stopped sandbox without its storage, because its runner keeps the storage reserved for the sandbox's directory and acquires it again when the sandbox starts.
		let mut capacity = tangram_server_sandbox_common::sandbox_capacity(&request.arg);
		if request.boot > 0 && request.runner.is_some() {
			capacity.storage = tg::sandbox::Resource::default();
		}

		// Validate the sandbox's resources.
		if capacity.cpu.max.is_some_and(|max| max.millis() == 0) {
			return Err(tg::error!("the sandbox CPU must be greater than zero"));
		}
		if capacity.memory.max == Some(0) {
			return Err(tg::error!("the sandbox memory must be greater than zero"));
		}
		let id = request.sandbox.clone();
		let parent = request.parent.clone();
		let sandbox = Sandbox {
			abandoned: false,
			attempts: 0,
			blocked: HashMap::default(),
			capacity,
			dequeue_requests: Vec::new(),
			held_end: None,
			owner_ancestors,
			persisted: recovered,
			request,
			retrying: false,
			state: SandboxState::Pending,
			unplaceable_since: None,
		};

		// Reject a request that no runner could ever fit if the configuration does not wait for one.
		if !recovered
			&& scheduler.config.unplaceable_timeout == Some(std::time::Duration::ZERO)
			&& !self.placeable(&sandbox)
		{
			return Err(tg::error!(sandbox = %id, "no runners available"));
		}
		self.sandboxes.entries.insert(id.clone(), sandbox);
		self.queue.insert(id.clone());
		if let Some(parent) = parent {
			self.parents
				.0
				.entry(parent)
				.or_insert_with(|| Parent {
					borrowable: None,
					queue: Queue::new(),
				})
				.queue
				.insert(id);
		}

		Ok(())
	}

	/// Ask a runner to destroy a sandbox that it holds.
	pub(super) fn send_destroy_sandbox(&mut self, scheduler: &Scheduler, destroy: DestroySandbox) {
		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();

		// Make one attempt, because the scheduler decides whether to try again from the runner's state.
		let retry = tangram_futures::retry::Options {
			max_retries: 0,
			..tangram_futures::retry::Options::default()
		};
		let timeout = scheduler.server.config.control.request_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		self.operations.push(
			async move {
				let arg = tg::runner::control::ServerRequestArg::DestroySandbox(
					tg::runner::control::DestroySandboxServerRequestArg {
						error: Some(destroy.error.clone()),
						sandbox: destroy.sandbox.clone(),
					},
				);
				let result = session
					.server
					.components
					.runner
					.send_runner_control_request(
						&session,
						&destroy.runner,
						&scheduler_id,
						destroy.registration_index,
						arg,
						options,
					)
					.await
					.and_then(std::convert::identity)
					.and_then(|output| {
						output
							.try_unwrap_destroy_sandbox()
							.map_err(|_| tg::error!("expected a destroy sandbox response"))
					})
					.map(|output| output.destroyed);

				// Wait before the scheduler tries again, which it does until the runner answers or is gone.
				if let Err(error) = &result {
					tracing::warn!(error = %error.trace(), sandbox = %destroy.sandbox, "failed to destroy the sandbox on its runner");
					tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
				}
				Operation::DestroySandbox { destroy, result }
			}
			.boxed(),
		);
	}

	/// Finish a destroy that the runner did not carry out. A runner that does not have the sandbox, or that is gone, will never report it, so the scheduler records it as destroyed.
	pub(super) fn handle_destroy_sandbox(
		&mut self,
		scheduler: &Scheduler,
		mut destroy: DestroySandbox,
		result: tg::Result<bool>,
	) {
		let current = self
			.runners
			.entries
			.get(&destroy.runner)
			.map(|runner| runner.registration_index);
		match (result, current) {
			(Ok(true), _) => (),
			(Err(_), Some(registration_index)) => {
				destroy.registration_index = registration_index;
				self.send_destroy_sandbox(scheduler, destroy);
			},
			(Ok(false), _) | (Err(_), None) => {
				if let Some(request) = destroy.request {
					let error = Some(tg::Either::Left(destroy.error));
					self.finish_destroy_sandbox(scheduler, request, error);
				}
			},
		}
	}

	/// Record a sandbox that never reached a runner as destroyed, which removes its key.
	pub(super) fn finish_destroy_sandbox(
		&mut self,
		scheduler: &Scheduler,
		request: EnqueueSandboxRequestArg,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.put_sandbox_status(&request, tg::sandbox::Status::Destroyed, error, None)
					.await;
				Operation::Publish {
					context: "failed to record the destroyed sandbox",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Stop or destroy a sandbox that this scheduler owns.
	pub(super) fn handle_end_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		arg: EndSandboxRequestArg,
	) {
		let EndSandboxRequestArg {
			error,
			sandbox,
			stop,
		} = arg;

		// Record the stop of a sandbox that has nothing to stop until it starts.
		if stop && let Some(request) = self.try_get_unstarted_request_mut(&sandbox) {
			let armed = !request.stop;
			request.stop = true;
			let request = request.clone();
			self.hold_sandbox_stop(scheduler, id, request, armed);
			return;
		}

		// Remove a request that is not placed, and hold the end of a placement that is in flight.
		if let Some(entry) = self.sandboxes.entries.get_mut(&sandbox) {
			match &entry.state {
				SandboxState::Creating { .. } => {
					let held = entry.held_end.get_or_insert(HeldEnd {
						error,
						ids: Vec::new(),
						placed: None,
						request: None,
					});
					held.ids.push(id);
				},
				SandboxState::Pending => {
					self.remove_queued_sandbox(&sandbox);
					let request = self.sandboxes.entries.remove(&sandbox).unwrap().request;
					self.queue.wake();
					let status = if stop && request.boot > 0 {
						tg::sandbox::Status::Stopped
					} else {
						tg::sandbox::Status::Destroyed
					};
					self.put_end_status(scheduler, vec![id], request, status, error);
				},
				SandboxState::Uncertain { placement } => {
					let placement = placement.clone();
					self.remove_reservation(&sandbox, &placement, true);
					let request = self.sandboxes.entries.remove(&sandbox).unwrap().request;
					let runner = placement.runner().clone();
					let destroy = DestroySandbox {
						error: error.clone().unwrap_or_default(),
						registration_index: runner.registration_index,
						request: Some(request.clone()),
						runner: runner.id,
						sandbox,
					};
					self.send_destroy_sandbox(scheduler, destroy);
					let status = tg::sandbox::Status::Destroying;
					self.put_end_status(scheduler, vec![id], request, status, error);
				},
			}
			return;
		}

		// Tear down a sandbox that a runner holds, whose control connection may not be established.
		if !stop && let Some(placed) = self.sandboxes.placed.remove(&sandbox) {
			let destroy = DestroySandbox {
				error: error.clone().unwrap_or_default(),
				registration_index: placed.registration_index,
				request: Some(placed.request.clone()),
				runner: placed.runner,
				sandbox,
			};
			self.send_destroy_sandbox(scheduler, destroy);
			let status = tg::sandbox::Status::Destroying;
			self.put_end_status(scheduler, vec![id], placed.request, status, error);
			return;
		}

		// End a sandbox whose request is held for recovery.
		if !stop && let Some((request, runner)) = self.take_recovering_request(&sandbox) {
			let status = if let Some((runner, registration_index)) = runner {
				let destroy = DestroySandbox {
					error: error.clone().unwrap_or_default(),
					registration_index,
					request: Some(request.clone()),
					runner,
					sandbox,
				};
				self.send_destroy_sandbox(scheduler, destroy);
				tg::sandbox::Status::Destroying
			} else {
				tg::sandbox::Status::Destroyed
			};
			self.put_end_status(scheduler, vec![id], request, status, error);
			return;
		}

		self.send_end_sandbox_response(scheduler, id, false);
	}

	/// Get the request of a sandbox that has not started, except for a queued restart, which a stop cancels because the sandbox is already stopped.
	fn try_get_unstarted_request_mut(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<&mut EnqueueSandboxRequestArg> {
		let queued_restart = self.sandboxes.entries.get(sandbox).is_some_and(|entry| {
			matches!(entry.state, SandboxState::Pending) && entry.request.boot > 0
		});
		if queued_restart {
			return None;
		}
		self.try_get_request_mut(sandbox)
	}

	/// Get the request of a sandbox that this scheduler holds.
	fn try_get_request_mut(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<&mut EnqueueSandboxRequestArg> {
		if self.sandboxes.entries.contains_key(sandbox) {
			return self
				.sandboxes
				.entries
				.get_mut(sandbox)
				.map(|entry| &mut entry.request);
		}
		if self.sandboxes.placed.contains_key(sandbox) {
			return self
				.sandboxes
				.placed
				.get_mut(sandbox)
				.map(|placed| &mut placed.request);
		}
		self.try_get_recovering_request_mut(sandbox)
	}

	/// Accept a stop once it is durable, so that a restarted scheduler carries it out.
	fn hold_sandbox_stop(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: EnqueueSandboxRequestArg,
		armed: bool,
	) {
		if armed {
			self.stop_sandbox_when_started(scheduler, request.sandbox.clone());
		}
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.persist_sandbox(&request)
					.await
					.map(|()| ResponseOutput::EndSandbox(EndSandboxResponseOutput { ended: true }));
				Operation::Respond { id, result }
			}
			.boxed(),
		);
	}

	/// Stop a sandbox once it starts, because it has no control connection until then.
	pub(super) fn stop_sandbox_when_started(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
	) {
		let scheduler = scheduler.clone();
		let id = sandbox.clone();
		let (future, handle) =
			future::abortable(async move { scheduler.wait_and_stop_sandbox(&id).await });
		if let Some(previous) = self.sandboxes.stops.insert(sandbox.clone(), handle) {
			previous.abort();
		}
		self.operations.push(
			async move {
				let result = future.await.unwrap_or(Ok(()));
				Operation::FinishSandboxStop { result, sandbox }
			}
			.boxed(),
		);
	}

	/// Cancel a held stop, because a start of the sandbox came after it.
	pub(super) fn handle_cancel_sandbox_stop_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		arg: &CancelSandboxStopRequestArg,
	) {
		if let Some(handle) = self.sandboxes.stops.remove(&arg.sandbox) {
			handle.abort();
		}

		// Respond once the cancellation is durable, so that a restarted scheduler does not carry out the stop.
		let request = self.try_get_request_mut(&arg.sandbox).map(|request| {
			let stop = std::mem::take(&mut request.stop);
			(stop, request.clone())
		});
		let held = request.is_some();
		let Some((true, request)) = request else {
			let output = CancelSandboxStopResponseOutput { held };
			let output = ResponseOutput::CancelSandboxStop(output);
			let response = scheduler.response(id, Ok(output));
			scheduler.send_response(self, response);
			return;
		};
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.persist_sandbox(&request).await.map(|()| {
					ResponseOutput::CancelSandboxStop(CancelSandboxStopResponseOutput { held })
				});
				Operation::Respond { id, result }
			}
			.boxed(),
		);
	}

	/// Add a lease to a queued process. The assignment carries the leases, so a process whose request was sent to a runner takes leases over process control once it connects.
	pub(super) fn handle_acquire_process_lease_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		arg: &AcquireProcessLeaseRequestArg,
	) {
		let lease = self
			.sandboxes
			.entries
			.get_mut(&arg.sandbox)
			.filter(|entry| matches!(entry.state, SandboxState::Pending))
			.and_then(|entry| {
				let process = entry.request.process.as_mut()?;
				let lease = tg::process::Lease::new();
				process.leases.push(lease);
				Some((lease, entry.request.clone()))
			});
		let Some((lease, request)) = lease else {
			let output = AcquireProcessLeaseResponseOutput { lease: None };
			let output = ResponseOutput::AcquireProcessLease(output);
			let response = scheduler.response(id, Ok(output));
			scheduler.send_response(self, response);
			return;
		};

		// Respond once the lease is durable.
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.persist_sandbox(&request).await.map(|()| {
					let lease = Some(lease);
					let output = AcquireProcessLeaseResponseOutput { lease };
					ResponseOutput::AcquireProcessLease(output)
				});
				Operation::Respond { id, result }
			}
			.boxed(),
		);
	}

	/// Release a lease of a queued process, and end the process and its sandbox if no leases remain.
	pub(super) fn handle_release_process_lease_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		arg: ReleaseProcessLeaseRequestArg,
	) {
		let ReleaseProcessLeaseRequestArg { lease, sandbox } = arg;
		let output = |released| {
			let output = ReleaseProcessLeaseResponseOutput { released };
			ResponseOutput::ReleaseProcessLease(output)
		};

		// Remove the lease.
		let process = self
			.sandboxes
			.entries
			.get_mut(&sandbox)
			.filter(|entry| matches!(entry.state, SandboxState::Pending))
			.and_then(|entry| entry.request.process.as_mut());
		let Some(process) = process else {
			let response = scheduler.response(id, Ok(output(None)));
			scheduler.send_response(self, response);
			return;
		};
		let position = process.leases.iter().position(|value| *value == lease);
		let released = if let Some(position) = position {
			process.leases.remove(position);
			true
		} else if process.lease == lease {
			if let Some(next) = process.leases.pop() {
				process.lease = next;
				true
			} else {
				// End the process and its sandbox, because this was the last lease.
				self.remove_queued_sandbox(&sandbox);
				let request = self.sandboxes.entries.remove(&sandbox).unwrap().request;
				self.queue.wake();
				let error = tg::error::Data {
					code: Some(tg::error::Code::Cancellation),
					message: Some("the process was canceled".to_owned()),
					..Default::default()
				};
				let scheduler = scheduler.clone();
				self.operations.push(
					async move {
						let status = tg::sandbox::Status::Destroyed;
						let error = Some(tg::Either::Left(error));
						let result = scheduler
							.put_sandbox_status(&request, status, error, None)
							.await
							.map(|()| output(Some(true)));
						Operation::Respond { id, result }
					}
					.boxed(),
				);
				return;
			}
		} else {
			false
		};
		if !released {
			let response = scheduler.response(id, Ok(output(Some(false))));
			scheduler.send_response(self, response);
			return;
		}

		// Respond once the remaining leases are durable.
		let request = self.sandboxes.entries[&sandbox].request.clone();
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.persist_sandbox(&request)
					.await
					.map(|()| output(Some(true)));
				Operation::Respond { id, result }
			}
			.boxed(),
		);
	}

	fn send_end_sandbox_response(&mut self, scheduler: &Scheduler, id: String, ended: bool) {
		let output = ResponseOutput::EndSandbox(EndSandboxResponseOutput { ended });
		let response = scheduler.response(id, Ok(output));
		scheduler.send_response(self, response);
	}

	fn put_end_status(
		&mut self,
		scheduler: &Scheduler,
		ids: Vec<String>,
		request: EnqueueSandboxRequestArg,
		status: tg::sandbox::Status,
		error: Option<tg::error::Data>,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let error = error.map(tg::Either::Left);
				let result = scheduler
					.put_sandbox_status(&request, status, error, None)
					.await;
				Operation::EndSandbox { ids, result }
			}
			.boxed(),
		);
	}

	/// Finish an end that waited for a placement, tearing down the sandbox if the placement reached a runner.
	pub(super) fn finish_held_end(&mut self, scheduler: &Scheduler, held: HeldEnd) {
		let HeldEnd {
			error,
			ids,
			placed,
			request,
		} = held;
		let Some(request) = request else {
			return;
		};
		let status = if let Some(runner) = placed {
			let destroy = DestroySandbox {
				error: error.clone().unwrap_or_default(),
				registration_index: runner.registration_index,
				request: Some(request.clone()),
				runner: runner.id,
				sandbox: request.sandbox.clone(),
			};
			self.send_destroy_sandbox(scheduler, destroy);
			tg::sandbox::Status::Destroying
		} else {
			tg::sandbox::Status::Destroyed
		};
		self.put_end_status(scheduler, ids, request, status, error);
	}

	/// Record a sandbox that a restarted scheduler found on a runner.
	pub(super) fn insert_placed_sandbox(&mut self, sandbox: tg::sandbox::Id, mut placed: Placed) {
		self.sandboxes.generation += 1;
		placed.generation = self.sandboxes.generation;
		self.sandboxes.placed.insert(sandbox, placed);
	}

	/// Find the sandboxes that were placed on a lost runner and that have not connected.
	pub(super) fn handle_lost_runner(&mut self, scheduler: &Scheduler, runner: &tg::runner::Id) {
		let ids = self
			.sandboxes
			.placed
			.iter()
			.filter(|(_, placed)| placed.runner == *runner)
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		if ids.is_empty() {
			return;
		}
		let lost = ids
			.iter()
			.filter_map(|id| self.sandboxes.placed.remove(id))
			.map(|placed| placed.request)
			.collect();
		self.read_scheduler_sandboxes(scheduler, lost);
	}

	/// Compare the sandboxes placed on a runner that registered again with the sandboxes that it reports. Keep a reported sandbox under the new registration, and treat the others as lost.
	pub(super) fn reconcile_placed_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		runner: &tg::runner::Id,
		registration_index: u64,
		reported: &[tg::runner::control::ArgSandbox],
	) {
		let ids = self
			.sandboxes
			.placed
			.iter()
			.filter(|(_, placed)| placed.runner == *runner)
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		let mut lost = Vec::new();
		for id in ids {
			let placed = self.sandboxes.placed.get_mut(&id).unwrap();
			let boot = placed.request.boot;
			if reported
				.iter()
				.any(|sandbox| sandbox.id == id && sandbox.boot == boot)
			{
				placed.registration_index = registration_index;
			} else {
				let placed = self.sandboxes.placed.remove(&id).unwrap();
				lost.push(placed.request);
			}
		}
		if !lost.is_empty() {
			self.read_scheduler_sandboxes(scheduler, lost);
		}
	}

	/// Forget the placed sandboxes that have connected, whose keys are gone.
	pub(super) fn prune_placed_sandboxes(&mut self, scheduler: &Scheduler) {
		if !self.sandboxes.placed.is_empty() {
			self.read_scheduler_sandboxes(scheduler, Vec::new());
		}
	}

	fn read_scheduler_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		lost: Vec<EnqueueSandboxRequestArg>,
	) {
		let generation = self.sandboxes.generation;
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				// Keep trying, because the lost sandboxes are gone if the read is given up.
				let keys = loop {
					match scheduler.read_scheduler_sandbox_keys().await {
						Ok(keys) => break keys,
						Err(error) => {
							tracing::error!(error = %error.trace(), "failed to read the scheduler's sandboxes");
							tokio::time::sleep(super::recovery::READ_RETRY_INTERVAL).await;
						},
					}
				};
				Operation::PlacedSandboxes {
					generation,
					keys,
					lost,
				}
			}
			.boxed(),
		);
	}

	/// Requeue the sandboxes lost with a runner that still have keys, and forget the placed sandboxes that do not.
	pub(super) fn handle_placed_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		generation: u64,
		keys: Vec<tg::sandbox::Id>,
		lost: Vec<EnqueueSandboxRequestArg>,
	) {
		let keys = keys
			.into_iter()
			.collect::<HashSet<_, tg::id::BuildHasher>>();

		// Keep the sandboxes placed after the read began, whose keys the read may not include.
		self.sandboxes
			.placed
			.retain(|id, placed| placed.generation > generation || keys.contains(id));
		for request in lost {
			if !keys.contains(&request.sandbox) {
				continue;
			}

			// A boot that must run on a runner that is gone cannot move, so it fails.
			if let Some(runner) = request
				.runner
				.clone()
				.filter(|runner| !self.runners.entries.contains_key(runner))
			{
				let sandbox = request.sandbox.clone();
				let error = tg::error!(%sandbox, %runner, "the sandbox's runner is unavailable");
				let discarded = DiscardedSandbox {
					error: error.to_data_or_id(),
					request,
					sandbox,
				};
				scheduler.publish_sandbox_discarded(self, discarded);
				continue;
			}
			self.handle_enqueue_sandbox_request(scheduler, None, request, true);
		}
	}

	pub(super) fn destroy_abandoned_sandbox(
		&mut self,
		scheduler: &Scheduler,
		abandoned: AbandonedSandbox,
	) {
		let AbandonedSandbox { runner, sandbox } = abandoned;
		let error = tg::error::Data {
			code: Some(tg::error::Code::Internal),
			message: Some("failed to record the accepted sandbox".to_owned()),
			..Default::default()
		};
		let destroy = DestroySandbox {
			error,
			registration_index: runner.registration_index,
			request: None,
			runner: runner.id,
			sandbox,
		};
		self.send_destroy_sandbox(scheduler, destroy);
	}

	pub(super) fn handle_borrowable_capacity(
		&mut self,
		scheduler: &Scheduler,
		notification: BorrowableCapacityNotification,
	) {
		let registration_index = self
			.runners
			.entries
			.get(&notification.runner)
			.map(|runner| runner.registration_index);
		let runner = notification.runner.clone();
		let parent_id = notification.parent.clone();
		let parent = self
			.parents
			.0
			.entry(parent_id.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				queue: Queue::new(),
			});
		let borrowable =
			Borrowable::new(notification, registration_index, parent.borrowable.as_ref());
		let previous = parent.borrowable.take();
		if borrowable.capacity.sandboxes != 0 {
			parent.borrowable = Some(borrowable);
		}
		if let Some(previous) = previous
			&& let Some(runner) = self.runners.entries.get_mut(&previous.runner)
		{
			runner.borrowable.remove(&parent_id);
		}
		if parent.borrowable.is_some()
			&& let Some(runner) = self.runners.entries.get_mut(&runner)
		{
			runner.borrowable.insert(parent_id.clone());
		}
		let sandbox = parent.queue.head().cloned();
		if parent.queue.len() == 0 && parent.borrowable.is_none() {
			self.parents.0.remove(&parent_id);
		}
		if let Some(sandbox) = sandbox
			&& self.sandboxes.attempts < scheduler.config.max_create_sandbox_requests
		{
			self.try_schedule(scheduler, &sandbox);
		}
		self.queue.wake();
	}

	pub(super) fn handle_heartbeat(
		&mut self,
		notification: &HeartbeatNotification,
		now: tangram_server_core::clock::Instant,
	) -> bool {
		let Some(runner) = self.runners.entries.get_mut(&notification.runner) else {
			return false;
		};
		if runner.registration_index != notification.registration_index
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
				if parent.queue.len() == 0 {
					self.parents.0.remove(&parent_id);
				}
			}
		}
		runner.capacity = notification.capacity;
		runner.committed = tg::runner::Capacity::default();
		runner.heartbeat_at = now;
		runner.heartbeat_index = notification.heartbeat_index;
		runner.ready = runner.capacity.total.sandboxes != 0;
		self.queue.wake();

		true
	}

	pub(super) fn dequeue_sandbox(
		&mut self,
		scheduler: &Scheduler,
		request_id: String,
		request: DequeueSandboxRequestArg,
	) -> Option<DequeueSandboxResponseOutput> {
		let id = request.sandbox;
		let Some(sandbox) = self.sandboxes.entries.get_mut(&id) else {
			return Some(DequeueSandboxResponseOutput { dequeued: false });
		};
		let output = match &sandbox.state {
			SandboxState::Creating { .. } => {
				sandbox.dequeue_requests.push(request_id);
				None
			},
			SandboxState::Pending => {
				self.remove_queued_sandbox(&id);
				let request = self.sandboxes.entries.remove(&id).unwrap().request;
				self.finish_destroy_sandbox(scheduler, request, None);
				Some(DequeueSandboxResponseOutput { dequeued: true })
			},
			SandboxState::Uncertain { placement } => {
				let placement = placement.clone();
				self.remove_reservation(&id, &placement, true);
				self.sandboxes.entries.remove(&id);
				Some(DequeueSandboxResponseOutput { dequeued: false })
			},
		};
		self.queue.wake();

		output
	}

	pub(super) fn handle_create_sandbox_completion(
		&mut self,
		config: &Config,
		completion: Completion,
	) -> CreateSandboxCompletionOutput {
		self.sandboxes.attempts = self.sandboxes.attempts.saturating_sub(1);
		let Completion {
			placement,
			result,
			sandbox: id,
		} = completion;
		let Some(sandbox) = self.sandboxes.entries.get(&id) else {
			return CreateSandboxCompletionOutput {
				abandoned: None,
				dequeue_completions: Vec::new(),
				discarded: None,
				held: None,
			};
		};
		let SandboxState::Creating { placement: current } = &sandbox.state else {
			return CreateSandboxCompletionOutput {
				abandoned: None,
				dequeue_completions: Vec::new(),
				discarded: None,
				held: None,
			};
		};
		if current != &placement {
			return CreateSandboxCompletionOutput {
				abandoned: None,
				dequeue_completions: Vec::new(),
				discarded: None,
				held: None,
			};
		}
		let dequeue_requests = sandbox.dequeue_requests.clone();
		let request = sandbox.request.clone();
		let runner_ref = placement.runner();
		let runner_current = self
			.runners
			.entries
			.get(&runner_ref.id)
			.is_some_and(|runner| runner.registration_index == runner_ref.registration_index);
		let failure = if let Ok(Err(error)) = &result {
			Some(error.clone())
		} else {
			None
		};
		let failed = failure.is_some();
		if let Ok(Err(error)) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner failed to create the sandbox");
		}

		let mut held = self
			.sandboxes
			.entries
			.get_mut(&id)
			.unwrap()
			.held_end
			.take()
			.map(|mut held| {
				held.request = Some(request.clone());
				held
			});
		let ending = held.is_some();
		let mut abandoned = None;
		let mut discarded = None;
		let output = match result {
			Ok(Ok(true)) if runner_current => {
				self.remove_reservation(&id, &placement, true);
				if self
					.sandboxes
					.entries
					.remove(&id)
					.is_some_and(|sandbox| sandbox.abandoned)
				{
					abandoned = Some(AbandonedSandbox {
						runner: runner_ref.clone(),
						sandbox: id.clone(),
					});
				} else if let Some(held) = &mut held {
					held.placed = Some(runner_ref.clone());
				} else {
					let placed = Placed {
						generation: 0,
						registration_index: runner_ref.registration_index,
						request,
						runner: runner_ref.id.clone(),
					};
					self.insert_placed_sandbox(id.clone(), placed);
				}
				DequeueSandboxResponseOutput { dequeued: false }
			},
			Ok(Ok(false) | Err(_)) => {
				self.remove_reservation(&id, &placement, false);
				let sandbox = self.sandboxes.entries.get_mut(&id).unwrap();
				if failed {
					sandbox.attempts += 1;
				}
				if !dequeue_requests.is_empty() || ending {
					self.sandboxes.entries.remove(&id);
				} else if failed && sandbox.attempts >= config.max_create_sandbox_attempts {
					let source = failure.as_ref().unwrap().clone();
					let attempts = sandbox.attempts;
					let error = tg::error!(
						!source,
						sandbox = %id,
						%attempts,
						"failed to create the sandbox after too many attempts"
					);
					tracing::error!(error = %error.trace(), "giving up on the sandbox");
					let request = self.sandboxes.entries.remove(&id).unwrap().request;
					discarded = Some(DiscardedSandbox {
						error: error.to_data_or_id(),
						request,
						sandbox: id.clone(),
					});
				} else {
					if matches!(placement, Placement::Regular { .. })
						&& let Some(runner) = self.runners.entries.get(&runner_ref.id)
					{
						let entry = Block {
							registration_index: runner.registration_index,
							heartbeat_index: runner.heartbeat_index,
						};
						self.sandboxes
							.entries
							.get_mut(&id)
							.unwrap()
							.blocked
							.insert(runner_ref.id.clone(), entry);
					}
					self.requeue_sandbox(&id);
				}
				DequeueSandboxResponseOutput { dequeued: true }
			},
			Err(error) if runner_current => {
				tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "failed to create the sandbox on the runner");
				if let Some(held) = &mut held {
					self.remove_reservation(&id, &placement, true);
					self.sandboxes.entries.remove(&id);
					held.placed = Some(runner_ref.clone());
				} else if dequeue_requests.is_empty() {
					if let Some(runner) = self.runners.entries.get_mut(&runner_ref.id)
						&& let Some(reservation) = runner.reservations.get_mut(&id)
					{
						runner.requests = runner.requests.saturating_sub(1);
						reservation.state = ReservationState::Uncertain;
					}
					self.sandboxes.entries.get_mut(&id).unwrap().state =
						SandboxState::Uncertain { placement };
				} else {
					self.remove_reservation(&id, &placement, true);
					self.sandboxes.entries.remove(&id);
				}
				DequeueSandboxResponseOutput { dequeued: false }
			},
			Ok(Ok(true)) | Err(_) => {
				if dequeue_requests.is_empty() && !ending {
					self.requeue_sandbox(&id);
				} else {
					self.sandboxes.entries.remove(&id);
				}
				DequeueSandboxResponseOutput { dequeued: false }
			},
		};
		let dequeue_completions = dequeue_requests
			.into_iter()
			.map(|request| DequeueCompletion {
				output: output.clone(),
				request,
			})
			.collect();

		CreateSandboxCompletionOutput {
			abandoned,
			dequeue_completions,
			discarded,
			held,
		}
	}

	pub(super) fn remove_runner(&mut self, id: &tg::runner::Id) -> Vec<DequeueCompletion> {
		let Some(runner) = self.runners.entries.remove(id) else {
			return Vec::new();
		};
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
				if parent.queue.len() == 0 {
					self.parents.0.remove(&parent_id);
				}
			}
		}
		let sandboxes = runner.reservations.into_keys().collect::<Vec<_>>();
		let mut completions = Vec::new();
		for sandbox in sandboxes {
			let dequeue_requests = self.sandboxes.entries[&sandbox].dequeue_requests.clone();
			if dequeue_requests.is_empty() {
				self.requeue_sandbox(&sandbox);
			} else {
				self.sandboxes.entries.remove(&sandbox);
				let output = DequeueSandboxResponseOutput { dequeued: false };
				completions.extend(
					dequeue_requests
						.into_iter()
						.map(|request| DequeueCompletion {
							output: output.clone(),
							request,
						}),
				);
			}
		}
		self.queue.wake();

		completions
	}

	pub(super) fn discard_pinned_sandboxes(
		&mut self,
		runner: &tg::runner::Id,
	) -> Vec<DiscardedSandbox> {
		let ids = self
			.sandboxes
			.entries
			.iter()
			.filter(|(_, sandbox)| {
				matches!(sandbox.state, SandboxState::Pending)
					&& sandbox.request.runner.as_ref() == Some(runner)
			})
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		let mut discarded = Vec::new();
		for id in ids {
			self.remove_queued_sandbox(&id);
			let request = self.sandboxes.entries.remove(&id).unwrap().request;
			let error = tg::error!(sandbox = %id, %runner, "the sandbox's runner is unavailable");
			let sandbox = DiscardedSandbox {
				error: error.to_data_or_id(),
				request,
				sandbox: id,
			};
			discarded.push(sandbox);
		}
		discarded
	}

	pub(super) fn schedule_one(&mut self, scheduler: &Scheduler) {
		let Some(sandbox) = self.queue.next() else {
			return;
		};
		self.try_schedule(scheduler, &sandbox);
	}

	pub(super) fn handle_retry_sandbox(&mut self, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get_mut(id) else {
			return;
		};
		if !matches!(sandbox.state, SandboxState::Pending) || !sandbox.retrying {
			return;
		}
		sandbox.retrying = false;
		self.queue.wake();
	}

	fn find_placement(&self, scheduler: &Scheduler, sandbox: &Sandbox) -> Option<Placement> {
		if let Some(placement) = self.borrowed_placement(
			sandbox,
			scheduler.config.max_create_sandbox_requests_per_runner,
		) {
			return Some(placement);
		}

		let mut best: Option<(RunnerRef, (u128, u128))> = None;
		for (id, runner) in &self.runners.entries {
			let blocked = sandbox.blocked.get(id).is_some_and(|block| {
				block.registration_index == runner.registration_index
					&& block.heartbeat_index == runner.heartbeat_index
			});
			let owner_matches = runner
				.owner
				.as_ref()
				.is_none_or(|owner| sandbox.owner_ancestors.contains(owner));
			if !owner_matches
				|| !runner.ready
				|| blocked || !matches_host(runner, &sandbox.request)
				|| !matches_runner(id, &sandbox.request)
				|| runner.requests >= scheduler.config.max_create_sandbox_requests_per_runner
			{
				continue;
			}
			let available = available(runner);
			if !available.contains(sandbox.capacity, runner.capacity.overcommit)
				|| sandbox
					.capacity
					.cpu
					.max
					.is_some_and(|max| max.threads() > cpu_limit(runner))
			{
				continue;
			}
			let score = score(runner, available, sandbox.capacity);
			if best.as_ref().is_none_or(|(best_runner, best_score)| {
				score < *best_score || (score == *best_score && id < &best_runner.id)
			}) {
				let runner = RunnerRef {
					registration_index: runner.registration_index,
					id: id.clone(),
				};
				best = Some((runner, score));
			}
		}

		best.map(|(runner, _)| Placement::Regular { runner })
	}

	fn borrowed_placement(&self, sandbox: &Sandbox, request_limit: usize) -> Option<Placement> {
		let (borrowable, runner) = self.borrowable(sandbox)?;
		if !matches_runner(&borrowable.runner, &sandbox.request) {
			return None;
		}
		if !borrowable.available || runner.requests >= request_limit {
			return None;
		}
		let runner = RunnerRef {
			registration_index: runner.registration_index,
			id: borrowable.runner.clone(),
		};
		Some(Placement::Borrowed {
			parent: sandbox.request.parent.clone().unwrap(),
			runner,
		})
	}

	/// Find the capacity that a sandbox's parent lends on a runner, where the runner also has the capacity that the sandbox must acquire in addition to what it borrows.
	fn borrowable(&self, sandbox: &Sandbox) -> Option<(&Borrowable, &Runner)> {
		let parent = sandbox.request.parent.as_ref()?;
		let borrowable = self.parents.0.get(parent)?.borrowable.as_ref()?;
		let runner = self.runners.entries.get(&borrowable.runner)?;
		let (_, acquired) = borrowable.capacity.try_borrow(sandbox.capacity)?;
		if !available(runner).contains(acquired, runner.capacity.overcommit)
			|| borrowable
				.registration_index
				.is_some_and(|index| index != runner.registration_index)
			|| !runner.ready
			|| !matches_host(runner, &sandbox.request)
		{
			return None;
		}
		Some((borrowable, runner))
	}

	fn placeable(&self, sandbox: &Sandbox) -> bool {
		self.borrowable(sandbox).is_some()
			|| self
				.runners
				.entries
				.iter()
				.any(|(id, runner)| runner.ready && placeable(id, runner, sandbox))
	}

	fn requeue_sandbox(&mut self, id: &tg::sandbox::Id) {
		self.sandboxes.entries.get_mut(id).unwrap().state = SandboxState::Pending;
		self.queue_sandbox(id);
	}

	fn queue_sandbox(&mut self, id: &tg::sandbox::Id) {
		let parent = self.sandboxes.entries[id].request.parent.clone();
		self.queue.insert(id.clone());
		if let Some(parent) = parent {
			self.parents
				.0
				.entry(parent)
				.or_insert_with(|| Parent {
					borrowable: None,
					queue: Queue::new(),
				})
				.queue
				.insert(id.clone());
		}
	}

	fn remove_queued_sandbox(&mut self, id: &tg::sandbox::Id) {
		let parent_id = self.sandboxes.entries[id].request.parent.clone();
		self.queue.remove(id);
		if let Some(parent_id) = parent_id
			&& let Some(parent) = self.parents.0.get_mut(&parent_id)
		{
			parent.queue.remove(id);
			if parent.queue.len() == 0 && parent.borrowable.is_none() {
				self.parents.0.remove(&parent_id);
			}
		}
	}

	fn remove_reservation(&mut self, id: &tg::sandbox::Id, placement: &Placement, commit: bool) {
		let runner_ref = placement.runner();
		let Some(runner) = self.runners.entries.get_mut(&runner_ref.id) else {
			return;
		};
		if runner.registration_index != runner_ref.registration_index {
			return;
		}
		runner.requests = runner.requests.saturating_sub(1);
		let Some(reservation) = runner.reservations.remove(id) else {
			return;
		};
		subtract(&mut runner.reserved, reservation.capacity);
		if commit {
			add(&mut runner.committed, reservation.capacity);
		}
	}

	fn try_schedule(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) -> bool {
		let Some(sandbox) = self.sandboxes.entries.get(id) else {
			return false;
		};
		if !matches!(sandbox.state, SandboxState::Pending) {
			return false;
		}
		let Some(placement) = self.find_placement(scheduler, sandbox) else {
			// Keep the sandbox queued if a runner could satisfy it once it has capacity.
			if self.placeable(sandbox) {
				self.sandboxes
					.entries
					.get_mut(id)
					.unwrap()
					.unplaceable_since = None;
				return false;
			}

			// Keep a sandbox that no runner can fit queued if the configuration waits for one.
			let Some(timeout) = scheduler.config.unplaceable_timeout else {
				return false;
			};

			// Discard the sandbox once no runner has been able to fit it for the timeout.
			let now = scheduler.server.clock.instant();
			let sandbox = self.sandboxes.entries.get_mut(id).unwrap();
			let since = *sandbox.unplaceable_since.get_or_insert(now);
			let elapsed = now - since;
			if elapsed >= timeout {
				let error = tg::error!(sandbox = %id, "no runners available");
				tracing::error!(error = %error.trace(), "giving up on the sandbox");
				self.remove_queued_sandbox(id);
				let request = self.sandboxes.entries.remove(id).unwrap().request;
				let discarded = DiscardedSandbox {
					error: error.to_data_or_id(),
					request,
					sandbox: id.clone(),
				};
				scheduler.publish_sandbox_discarded(self, discarded);
				return false;
			}

			// Check again when the timeout expires, in case no runner becomes available.
			if !sandbox.retrying {
				sandbox.retrying = true;
				let sandbox = id.clone();
				let sleep = scheduler
					.server
					.clock
					.sleep(timeout.saturating_sub(elapsed));
				self.operations.push(
					async move {
						sleep.await;
						Operation::RetrySandbox { sandbox }
					}
					.boxed(),
				);
			}

			return false;
		};
		let capacity = sandbox.capacity;
		let request = sandbox.request.clone();
		let runner_ref = placement.runner().clone();
		self.remove_queued_sandbox(id);

		// Reserve the sandbox's capacity on the runner, which for a borrowed sandbox is only the capacity that it acquires in addition to what it borrows.
		let reserved = match &placement {
			Placement::Borrowed { parent, .. } => {
				let borrowable = self
					.parents
					.0
					.get_mut(parent)
					.and_then(|parent| parent.borrowable.as_mut())
					.unwrap();
				borrowable.available = false;
				let (_, acquired) = borrowable.capacity.try_borrow(capacity).unwrap();
				acquired
			},
			Placement::Regular { .. } => capacity,
		};
		let runner = self.runners.entries.get_mut(&runner_ref.id).unwrap();
		runner.requests += 1;
		add(&mut runner.reserved, reserved);
		let entry = Reservation {
			capacity: reserved,
			state: ReservationState::Pending,
		};
		runner.reservations.insert(id.clone(), entry);
		self.sandboxes.entries.get_mut(id).unwrap().state = SandboxState::Creating {
			placement: placement.clone(),
		};
		self.sandboxes.attempts += 1;

		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();
		let timeout = scheduler.config.create_sandbox_timeout;
		let borrowed = matches!(placement, Placement::Borrowed { .. });
		let sandbox = id.clone();
		self.operations.push(
			async move {
				let runner = placement.runner();
				let result = create_sandbox(
					&session,
					&scheduler_id,
					runner,
					borrowed,
					capacity,
					&request,
					timeout,
				)
				.boxed()
				.await;
				Operation::CreateSandbox(Completion {
					placement,
					result,
					sandbox,
				})
			}
			.boxed(),
		);

		true
	}
}

impl Scheduler {
	/// Write the starting record and the key that associates the sandbox with this scheduler.
	/// Read the sandboxes that this scheduler has keys for, after pending indexing, so that every key written before the read is included.
	async fn read_scheduler_sandbox_keys(&self) -> tg::Result<Vec<tg::sandbox::Id>> {
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
		let sandboxes = self.server.index.get_scheduler_sandboxes(&self.id).await?;
		let keys = sandboxes
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<Vec<_>>();

		Ok(keys)
	}

	async fn wait_and_stop_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		let session = self.server.session(&self.server.context);
		let component = &self.server.components.sandbox;
		let mut wakeups = component
			.create_sandbox_status_wakeup_stream(
				&session,
				id,
				session.context.stopper.clone(),
				None,
			)
			.await?;
		loop {
			let status = component
				.try_get_sandbox_from_index(&session, id)
				.await?
				.and_then(|sandbox| sandbox.data)
				.map(|data| data.data.status);
			match status {
				Some(tg::sandbox::Status::Started) => break,
				Some(tg::sandbox::Status::Starting) => {
					if wakeups.next().await.is_none() {
						return Ok(());
					}
				},
				_ => return Ok(()),
			}
		}
		let arg = tg::sandbox::stop::Arg::default();
		component.try_stop_sandbox(&session, id, arg).await?;

		Ok(())
	}

	pub(super) async fn persist_sandbox(
		&self,
		request: &EnqueueSandboxRequestArg,
	) -> tg::Result<()> {
		let stored = EnqueueSandboxRequestArg {
			token: None,
			..request.clone()
		};
		let stored = serde_json::to_vec(&stored)
			.map_err(|error| tg::error!(!error, "failed to serialize the request"))?;
		let scheduled = tangram_index::sandbox::put::Scheduled {
			request: stored,
			scheduler: self.id.clone(),
		};
		self.put_sandbox_status(
			request,
			tg::sandbox::Status::Starting,
			None,
			Some(scheduled),
		)
		.await
	}

	/// Record a failure after acceptance in the sandbox's error field. A new sandbox is destroyed, and a sandbox that was starting again is stopped.
	pub(super) async fn fail_sandbox(
		&self,
		request: &EnqueueSandboxRequestArg,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let status = if request.boot == 0 {
			tg::sandbox::Status::Destroyed
		} else {
			tg::sandbox::Status::Stopped
		};
		self.put_sandbox_status(request, status, Some(error), None)
			.await
	}

	pub(super) async fn put_sandbox_status(
		&self,
		request: &EnqueueSandboxRequestArg,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		scheduled: Option<tangram_index::sandbox::put::Scheduled>,
	) -> tg::Result<()> {
		// Create the sandbox's data.
		let session = self.server.session(&self.server.context);
		let id = &request.sandbox;
		let now = self.server.clock.unix_timestamp();
		let mut data = tangram_server_scheduler_core::sandbox_data(request, status);
		let process_error = error.clone().unwrap_or_else(|| {
			tg::Either::Left(tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the sandbox was destroyed".to_owned()),
				..Default::default()
			})
		});
		data.error = error;
		if status.is_stopped() {
			data.expires_at = data.time_to_live.map(|time_to_live| {
				now.saturating_add(time_to_live.as_secs().try_into().unwrap_or(i64::MAX))
			});
			data.stopped_at = Some(now);
		}
		let account = self
			.server
			.components
			.usage
			.usage_account(&session, &request.owner)
			.await?;
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data,
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};

		// Enqueue the write.
		let processes = (request.boot == 0 && status.is_destroyed()).then(Vec::new);
		let arg = tangram_index::sandbox::put::Arg {
			account,
			created_at: request.created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes,
			registration: None,
			runner: request.runner.clone(),
			scheduled,
			stopped_usage: None,
			touched_at: now,
		};
		let mut items = vec![tangram_index::Item::PutSandbox(arg)];
		if status.is_starting()
			&& let Some(process) = &request.process
		{
			items.push(self.starting_process_item(process, now)?);
		}
		let arg = tangram_index::write::Arg { items };
		self.server
			.components
			.indexer
			.write(&self.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the sandbox in the index"))?;

		// Finish a process that never started, because no runner will report it.
		if !status.is_starting()
			&& let Some(process) = &request.process
		{
			self.finish_unstarted_process(process, process_error)
				.await?;
		}
		self.server
			.components
			.sandbox
			.spawn_publish_sandbox_status_task(&self.server, id);

		Ok(())
	}
}

impl Scheduler {
	/// Create the starting record of a process, which is visible until its runner connects.
	fn starting_process_item(
		&self,
		process: &tg::runner::control::Process,
		now: i64,
	) -> tg::Result<tangram_index::Item> {
		let id = process
			.id
			.clone()
			.ok_or_else(|| tg::error!("expected the scheduled process to have an id"))?;
		let mut data = process
			.data
			.clone()
			.without_location_and_tokens_except_sync();
		data.started_at = None;
		data.status = tg::process::Status::Starting;
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let arg = tangram_index::process::put::Arg {
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
			data: Some(data),
			error: None,
			id: id.clone(),
			location: Some(location),
			log: None,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: None,
			parent: None,
			permissions: Vec::new(),
			principal: tg::Principal::Process(id),
			registration: None,
			sandbox: None,
			storage: tg::process::storage::Set::NODE,
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};

		Ok(tangram_index::Item::PutProcess(arg))
	}

	/// Record the failure of a process whose sandbox ended before the process started.
	async fn finish_unstarted_process(
		&self,
		process: &tg::runner::control::Process,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Result<()> {
		let Some(id) = process.id.clone() else {
			return Ok(());
		};
		let mut data = process.data.clone();
		data.cacheable = false;
		data.children.get_or_insert_default();
		data.exit = Some(1);
		data.finished_at = Some(self.server.clock.unix_timestamp());
		data.set_error(Some(error.map_right(tg::Referent::with_node)));
		data.started_at = None;
		data.status = tg::process::Status::Finished;
		let mut context = self.server.context.clone();
		context.principal = tg::Principal::Process(id.clone());
		let session = self.server.session(&context);
		let entry = tg::process::put::Arg {
			data,
			location: None,
		};
		let options = tangram_server_process_core::put::Options {
			command_sync: None,
			defer_index: false,
			location: None,
			outcome_sync: None,
			registration: None,
			store_data: true,
		};
		self.server
			.components
			.process
			.put_process_local(&session, &id, entry, options)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, process = %id, "failed to store the process"))?;
		let session = self.server.session(&self.server.context);
		self.server
			.components
			.process
			.spawn_process_finish_tasks(&session, &id);

		Ok(())
	}
}

impl Placement {
	fn runner(&self) -> &RunnerRef {
		match self {
			Self::Borrowed { runner, .. } | Self::Regular { runner } => runner,
		}
	}
}

#[tracing::instrument(name = "sandbox.assign", level = "debug", skip_all, fields(runner = %runner.id, sandbox = %request.sandbox, scheduler = %scheduler), err(level = "debug"))]
async fn create_sandbox(
	session: &Session,
	scheduler: &tg::scheduler::Id,
	runner: &RunnerRef,
	borrowed: bool,
	capacity: tg::runner::Capacity,
	request: &EnqueueSandboxRequestArg,
	timeout: std::time::Duration,
) -> tg::Result<tg::Result<bool>> {
	tracing::info!(borrowed, runner = %runner.id, sandbox = %request.sandbox, scheduler = %scheduler, "assigned the sandbox to a runner");
	let arg = tg::runner::control::ServerRequestArg::CreateSandbox(
		tg::runner::control::CreateSandboxServerRequestArg {
			arg: request.arg.clone(),
			boot: request.boot,
			borrowed,
			capacity,
			creator: request.creator.clone(),
			owner: request.owner.clone(),
			parent: request.parent.clone(),
			process: request.process.clone(),
			sandbox: request.sandbox.clone(),
			token: request.token.clone(),
		},
	);
	let retry = session.server.config.control.request_retry.clone().into();
	let options = tangram_server_core::control::Options { retry, timeout };
	let result = session
		.server
		.components.runner
		.send_runner_control_request(
			session,
			&runner.id,
			scheduler,
			runner.registration_index,
			arg,
			options,
		)
		.boxed()
		.await
		.map_err(|error| {
			tg::error!(!error, sandbox = %request.sandbox, runner = %runner.id, "failed to send the create sandbox request to the runner")
		})?;
	let result = result
		.map_err(|error| {
			tg::error!(!error, sandbox = %request.sandbox, runner = %runner.id, "the runner create sandbox request failed")
		})
		.and_then(|output| {
			output
				.try_unwrap_create_sandbox()
				.map_err(|_| tg::error!("expected a create sandbox response"))
				.map(|output| output.created)
		});

	Ok(result)
}

/// Add a request to a sum of requests, which accounts for each request's maximum as its minimum when it has none.
fn add(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	let max = capacity
		.cpu
		.accounted_max()
		.millis()
		.saturating_add(value.cpu.accounted_max().millis());
	let min = capacity
		.cpu
		.min
		.millis()
		.saturating_add(value.cpu.min.millis());
	capacity.cpu = tg::sandbox::Cpu::new(
		tg::sandbox::Amount::from_millis(min),
		tg::sandbox::Amount::from_millis(max),
	);
	for (resource, value) in [
		(&mut capacity.memory, value.memory),
		(&mut capacity.storage, value.storage),
	] {
		let max = resource
			.accounted_max()
			.saturating_add(value.accounted_max());
		let min = resource.min.saturating_add(value.min);
		*resource = tg::sandbox::Resource::new(min, max);
	}
	capacity.sandboxes = capacity.sandboxes.saturating_add(value.sandboxes);
}

fn available(runner: &Runner) -> tg::runner::Capacity {
	let factor = runner.capacity.overcommit;
	runner
		.capacity
		.available
		.subtract(runner.reserved, factor)
		.subtract(runner.committed, factor)
}

fn cpu_limit(runner: &Runner) -> u64 {
	runner.capacity.cpu_limit
}

fn matches_host(runner: &Runner, request: &EnqueueSandboxRequestArg) -> bool {
	request
		.arg
		.host
		.as_ref()
		.is_none_or(|host| host == &runner.host)
}

fn matches_runner(id: &tg::runner::Id, request: &EnqueueSandboxRequestArg) -> bool {
	request.runner.as_ref().is_none_or(|runner| runner == id)
}

fn placeable(id: &tg::runner::Id, runner: &Runner, sandbox: &Sandbox) -> bool {
	let owner_matches = runner
		.owner
		.as_ref()
		.is_none_or(|owner| sandbox.owner_ancestors.contains(owner));
	owner_matches
		&& matches_host(runner, &sandbox.request)
		&& matches_runner(id, &sandbox.request)
		&& runner
			.capacity
			.total
			.contains(sandbox.capacity, runner.capacity.overcommit)
		&& sandbox
			.capacity
			.cpu
			.max
			.is_none_or(|max| max.threads() <= runner.capacity.cpu_limit)
}

fn score(
	runner: &Runner,
	available: tg::runner::Capacity,
	requested: tg::runner::Capacity,
) -> (u128, u128) {
	let remaining = available.subtract(requested, runner.capacity.overcommit);
	let total = runner.capacity.total;
	let cpu = u128::from(remaining.cpu.max.map_or(0, tg::sandbox::Amount::millis)) * 1_000_000
		/ u128::from(total.cpu.max.map_or(1, tg::sandbox::Amount::millis).max(1));
	let memory = u128::from(remaining.memory.max.unwrap_or_default()) * 1_000_000
		/ u128::from(total.memory.max.unwrap_or(1).max(1));
	(cpu.max(memory), cpu + memory)
}

/// Subtract a request from a sum of requests.
fn subtract(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	let max = capacity
		.cpu
		.accounted_max()
		.millis()
		.saturating_sub(value.cpu.accounted_max().millis());
	let min = capacity
		.cpu
		.min
		.millis()
		.saturating_sub(value.cpu.min.millis());
	capacity.cpu = tg::sandbox::Cpu::new(
		tg::sandbox::Amount::from_millis(min),
		tg::sandbox::Amount::from_millis(max),
	);
	for (resource, value) in [
		(&mut capacity.memory, value.memory),
		(&mut capacity.storage, value.storage),
	] {
		let max = resource
			.accounted_max()
			.saturating_sub(value.accounted_max());
		let min = resource.min.saturating_sub(value.min);
		*resource = tg::sandbox::Resource::new(min, max);
	}
	capacity.sandboxes = capacity.sandboxes.saturating_sub(value.sandboxes);
}
