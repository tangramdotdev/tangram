use {
	super::{
		BorrowableCapacityNotification, DequeueSandboxRequestArg, DequeueSandboxResponseOutput,
		EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput, HeartbeatNotification,
		HolderMessage, Operation, OwnerAncestors, PendingEnqueue, ResponseOutput,
		SandboxEndedNotification, Scheduler, SchedulerState,
		runner::{Reservation, Runner},
	},
	futures::{FutureExt as _, StreamExt as _, future},
	std::{
		collections::{BTreeMap, HashMap, HashSet},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_server_core::{
		Session,
		control::accept::{Accept, Path},
	},
};

#[cfg(test)]
mod tests;

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

	/// The tasks that serve the requests to the holders of the sandboxes that this scheduler holds or has offered to a runner, and of their queued processes, until their control connections are established.
	holders: HashMap<
		tg::sandbox::Id,
		Vec<tangram_futures::task::Task<tg::Result<()>>>,
		tg::id::BuildHasher,
	>,

	loading: usize,

	/// The sandboxes that a runner holds whose control connections may not be established yet.
	placed: HashMap<tg::sandbox::Id, Placed, tg::id::BuildHasher>,

	/// The boots that runners accepted, which are tracked until their runners report their ends, so that an enqueue of a tracked or older boot is a no-op.
	scheduled: HashMap<tg::sandbox::Id, Scheduled, tg::id::BuildHasher>,
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

/// A boot that a runner accepted.
struct Scheduled {
	boot: u64,

	/// The parent of the sandbox, if the scheduler knows it.
	parent: Option<tg::sandbox::Id>,

	registration_index: u64,

	runner: tg::runner::Id,
}

struct Parent {
	borrowable: Option<Borrowable>,

	/// The runner of each child that the scheduler offered to a runner or that a runner accepted, so that the scheduler prefers the runner with the most of a parent's children for its next child.
	children: BTreeMap<tg::sandbox::Id, tg::runner::Id>,

	queue: Queue,
}

struct Borrowable {
	available: bool,
	capacity: tg::runner::Capacity,
	registration_index: Option<u64>,
	heartbeat_index: u64,
	runner: tg::runner::Id,
}

/// The acceptance of an enqueued sandbox, and the enqueue requests that wait for it.
#[derive(Default)]
pub(super) struct Accepting {
	accept: Accept,
	ids: Vec<String>,
	persisted: bool,
}

struct Links {
	next: Option<tg::sandbox::Id>,
	previous: Option<tg::sandbox::Id>,
}

struct Sandbox {
	attempts: usize,
	blocked: HashMap<tg::runner::Id, Block, tg::id::BuildHasher>,
	capacity: tg::runner::Capacity,
	dequeue_requests: Vec<String>,
	owner_ancestors: OwnerAncestors,
	persisted: bool,
	request: EnqueueSandboxRequestArg,
	retrying: bool,
	state: SandboxState,
	unplaceable_since: Option<tangram_server_core::clock::Instant>,
}

enum SandboxState {
	/// The scheduler offered the boot to a runner, and sends the same offer again until the runner answers.
	Creating {
		offer: String,
		placement: Placement,
		retries: u64,
	},

	Pending,
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

/// A runner that can take a sandbox, with its score and its number of the sandbox's siblings.
struct Candidate<'a> {
	id: &'a tg::runner::Id,
	registration_index: u64,
	score: (u128, u128),
	siblings: usize,
}

struct CreateSandboxArg<'a> {
	borrowed: bool,
	capacity: tg::runner::Capacity,
	offer: String,
	request: &'a EnqueueSandboxRequestArg,
	runner: &'a RunnerRef,
	scheduler: &'a tg::scheduler::Id,
	timeout: std::time::Duration,
}

pub(super) struct Completion {
	placement: Placement,
	result: tg::Result<tg::Result<bool>>,
	sandbox: tg::sandbox::Id,
}

pub(super) struct CreateSandboxCompletionOutput {
	pub accepted: Option<tg::sandbox::Id>,
	pub dequeue_completions: Vec<DequeueCompletion>,
	pub discarded: Option<DiscardedSandbox>,
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
			holders: HashMap::default(),
			loading: 0,
			placed: HashMap::default(),
			scheduled: HashMap::default(),
		}
	}
}

impl Parent {
	/// Whether the entry has no queued children, no capacity to lend, and no children on runners, so that it can be dropped.
	fn is_empty(&self) -> bool {
		self.queue.len() == 0 && self.borrowable.is_none() && self.children.is_empty()
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

impl SchedulerState {
	pub(super) fn new() -> Self {
		Self {
			accepting: HashMap::default(),
			operations: futures::stream::FuturesUnordered::new(),
			owner_ancestor_requests: HashMap::default(),
			owner_ancestors: HashMap::default(),
			parents: Parents::new(),
			pending_enqueues: HashMap::default(),
			queue: Queue::new(),
			recovery: None,
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

		// Reply to an enqueue of a boot that a runner already accepted, or of an older boot, without enqueueing it again.
		if let Some(scheduled) = self.sandboxes.scheduled.get(&request.sandbox) {
			if request.boot <= scheduled.boot {
				self.send_enqueue_responses(scheduler, ids, Ok(()));
				return;
			}
			self.forget_scheduled_sandbox(&request.sandbox);
		}

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

	/// Respond to enqueue requests. A request that the scheduling task accepted responds once either its index write is enqueued or a runner accepts the sandbox, whichever is first.
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
			// Write the sandbox to the index queue concurrently with its handoff to a runner.
			let request = entry.request.clone();
			let first = !self.accepting.contains_key(sandbox);
			self.accepting
				.entry(sandbox.clone())
				.or_default()
				.ids
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

	/// Record the result of a path of a sandbox's acceptance, and respond to its enqueue requests the first time that the acceptance is decided.
	pub(super) fn finish_accept(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		path: Path,
		result: tg::Result<()>,
	) {
		let Some(accepting) = self.accepting.get_mut(sandbox) else {
			return;
		};
		if let Some(result) = accepting.accept.finish(path, result) {
			let ids = std::mem::take(&mut accepting.ids);
			self.send_enqueue_responses(scheduler, ids, result);
		}

		// Remove the acceptance once it is decided and its index write is finished, so that a later enqueue request does not wait for it.
		let accepting = self.accepting.get_mut(sandbox).unwrap();
		if path == Path::Queued {
			accepting.persisted = true;
		}
		if accepting.accept.decided() && accepting.persisted {
			self.accepting.remove(sandbox);
		}
	}

	/// Remove a sandbox's entry, and fail its acceptance's handoff, which responds to its enqueue requests if its index write also failed.
	fn remove_sandbox_entry(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
	) -> Option<Sandbox> {
		let sandbox = self.sandboxes.entries.remove(id)?;
		if let Some(parent) = &sandbox.request.parent {
			self.remove_child(parent, id);
		}
		self.stop_serving_sandbox_holder(id);
		let error =
			tg::error!(sandbox = %id, "the sandbox was removed before a runner accepted it");
		self.finish_accept(scheduler, id, Path::Handled, Err(error));

		Some(sandbox)
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
			let response = super::request::Response::new(id, result.clone());
			scheduler.send_response(self, response);
		}
	}

	pub(super) fn handle_persist_sandbox_completion(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		result: tg::Result<()>,
	) {
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to record the accepted sandbox");
		}
		if result.is_ok()
			&& let Some(sandbox) = self.sandboxes.entries.get_mut(id)
		{
			sandbox.persisted = true;
		}
		self.finish_accept(scheduler, id, Path::Queued, result);
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
			attempts: 0,
			blocked: HashMap::default(),
			capacity,
			dequeue_requests: Vec::new(),
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
		let process = sandbox
			.request
			.process
			.as_ref()
			.and_then(|process| process.id.clone());
		self.sandboxes.entries.insert(id.clone(), sandbox);
		self.serve_sandbox_holder(scheduler, &id, process);
		self.queue.insert(id.clone());
		if let Some(parent) = parent {
			self.parents
				.0
				.entry(parent)
				.or_insert_with(|| Parent {
					borrowable: None,
					children: BTreeMap::new(),
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

	/// Serve the requests to the holder of a sandbox that this scheduler holds, and to the holder of the sandbox's queued process, until the sandbox's control connection is established.
	pub(super) fn serve_sandbox_holder(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		process: Option<tg::process::Id>,
	) {
		// Serve a sandbox once, and record it before its subscriptions are established, so that a request is not served twice.
		if self.sandboxes.holders.contains_key(sandbox) {
			return;
		}
		self.sandboxes.holders.insert(sandbox.clone(), Vec::new());
		let scheduler = scheduler.clone();
		let sandbox = sandbox.clone();
		self.operations.push(
			async move {
				let result = scheduler
					.serve_sandbox_holder(&sandbox, process.as_ref())
					.await;
				Operation::ServeSandboxHolder { result, sandbox }
			}
			.boxed(),
		);
	}

	/// Keep the tasks that serve a sandbox's holder, unless this scheduler stopped serving it in the meantime.
	pub(super) fn handle_serve_sandbox_holder_completion(
		&mut self,
		sandbox: &tg::sandbox::Id,
		result: tg::Result<Vec<tangram_futures::task::Task<tg::Result<()>>>>,
	) {
		let tasks = match result {
			Ok(tasks) => tasks,
			Err(error) => {
				tracing::error!(error = %error.trace(), %sandbox, "failed to serve the sandbox's holder");
				self.sandboxes.holders.remove(sandbox);
				return;
			},
		};
		if let Some(holder) = self.sandboxes.holders.get_mut(sandbox) {
			*holder = tasks;
		}
	}

	/// Stop serving the requests to the holder of a sandbox that this scheduler no longer holds.
	fn stop_serving_sandbox_holder(&mut self, sandbox: &tg::sandbox::Id) {
		self.sandboxes.holders.remove(sandbox);
	}

	/// Stop serving the holders of the sandboxes that this scheduler neither holds, nor has placed on a runner, nor holds for recovery.
	pub(super) fn prune_sandbox_holders(&mut self) {
		let sandboxes = self
			.sandboxes
			.holders
			.keys()
			.filter(|sandbox| {
				!(self.sandboxes.entries.contains_key(*sandbox)
					|| self.sandboxes.placed.contains_key(*sandbox)
					|| self.is_recovering(sandbox))
			})
			.cloned()
			.collect::<Vec<_>>();
		for sandbox in sandboxes {
			self.sandboxes.holders.remove(&sandbox);
		}
	}

	/// Answer a request to the holder of a sandbox that this scheduler holds, and reply that this scheduler does not hold a sandbox that it has offered to a runner.
	pub(super) fn handle_sandbox_holder_message(
		&mut self,
		scheduler: &Scheduler,
		message: HolderMessage,
	) {
		let (request, sandbox) = match message {
			HolderMessage::Connected { sandbox } => {
				// Stop serving a sandbox whose runner serves it, unless this scheduler holds a later boot of it.
				if !self.sandboxes.entries.contains_key(&sandbox) {
					self.stop_serving_sandbox_holder(&sandbox);
				}
				return;
			},
			HolderMessage::ProcessRequest { request, sandbox } => {
				self.handle_process_holder_request(scheduler, &sandbox, request);
				return;
			},
			HolderMessage::Request { request, sandbox } => (request, sandbox),
		};
		let tg::sandbox::control::ServerRequest { arg, id } = request;

		// Ignore a request for a sandbox that this scheduler does not serve.
		let pending = self
			.sandboxes
			.entries
			.get(&sandbox)
			.map(|entry| matches!(entry.state, SandboxState::Pending));
		if pending.is_none() && !self.sandboxes.holders.contains_key(&sandbox) {
			return;
		}

		// Reply that this scheduler does not hold a sandbox that it has offered to a runner, whether or not the runner has accepted it, because the offer carries the boot's pending stop.
		if pending != Some(true) {
			self.publish_sandbox_holder_retry(
				scheduler,
				sandbox,
				id,
				tg::control::Retry::NotHolder,
			);
			return;
		}

		// Reply that this scheduler holds a queued sandbox but is not ready for a spawn, which only a runner can carry out once the sandbox starts.
		if matches!(arg, tg::sandbox::control::ServerRequestArg::SpawnProcess(_)) {
			self.publish_sandbox_holder_retry(scheduler, sandbox, id, tg::control::Retry::NotReady);
			return;
		}
		let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();

		match arg {
			// Destroy a queued sandbox by removing it.
			tg::sandbox::control::ServerRequestArg::Destroy(arg) => {
				self.remove_queued_sandbox(&sandbox);
				let request = self
					.remove_sandbox_entry(scheduler, &sandbox)
					.unwrap()
					.request;
				self.queue.wake();
				let output = tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: true },
				);
				let scheduler_ = scheduler.clone();
				let write = async move {
					let status = tg::sandbox::Status::Destroyed;
					let error = arg.error.map(tg::Either::Left);
					scheduler_
						.put_sandbox_status(&request, status, error, None)
						.await
				}
				.boxed();
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},

			// Answer a get with the queued boot.
			tg::sandbox::control::ServerRequestArg::Get(_) => {
				let data = Self::queued_sandbox_get_output(scheduler, &entry.request);
				let output = tg::sandbox::control::ClientResponseOutput::Get(
					tg::sandbox::control::GetClientResponseOutput { data },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			// Answer a get of the processes with the queued process.
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => {
				let processes = entry
					.request
					.process
					.as_ref()
					.and_then(|process| process.id.clone())
					.into_iter()
					.collect::<Vec<_>>();
				let length = u64::try_from(processes.len()).unwrap();
				let processes = processes
					.into_iter()
					.skip(usize::try_from(arg.position).unwrap())
					.take(usize::try_from(arg.length).unwrap())
					.collect();
				let output = tg::sandbox::control::ClientResponseOutput::GetProcesses(
					tg::sandbox::control::GetProcessesClientResponseOutput {
						length,
						processes,
						status: tg::sandbox::Status::Starting,
					},
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => unreachable!(),

			// Answer a start of a queued boot with the boot, which is starting, and cancel its pending stop, because the start came after the stop.
			tg::sandbox::control::ServerRequestArg::Start(_) => {
				let write = if std::mem::take(&mut entry.request.stop) {
					let request = entry.request.clone();
					let scheduler_ = scheduler.clone();
					Some(async move { scheduler_.persist_sandbox(&request).await }.boxed())
				} else {
					None
				};
				let data = Self::queued_sandbox_get_output(scheduler, &entry.request);
				let output = tg::sandbox::control::ClientResponseOutput::Start(
					tg::sandbox::control::StartClientResponseOutput { data },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, write, output);
			},

			// Stop a queued restart by removing it, because the sandbox is already stopped, and record the stop of a first boot, which has nothing to stop until it starts.
			tg::sandbox::control::ServerRequestArg::Stop(_) => {
				let output = tg::sandbox::control::ClientResponseOutput::Stop(
					tg::sandbox::control::StopClientResponseOutput { stopped: true },
				);
				let scheduler_ = scheduler.clone();
				let write = if entry.request.boot > 0 {
					self.remove_queued_sandbox(&sandbox);
					let request = self
						.remove_sandbox_entry(scheduler, &sandbox)
						.unwrap()
						.request;
					self.queue.wake();
					async move {
						scheduler_
							.put_sandbox_status(&request, tg::sandbox::Status::Stopped, None, None)
							.await
					}
					.boxed()
				} else {
					entry.request.stop = true;
					let request = entry.request.clone();
					async move { scheduler_.persist_sandbox(&request).await }.boxed()
				};
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},
		}
	}

	/// Create the output of a get of a queued boot.
	fn queued_sandbox_get_output(
		scheduler: &Scheduler,
		request: &EnqueueSandboxRequestArg,
	) -> tg::sandbox::get::Output {
		let data =
			tangram_server_scheduler_core::sandbox_data(request, tg::sandbox::Status::Starting);
		let location = tg::Location::Local(tg::location::Local {
			region: scheduler.server.config.region.clone(),
		});
		tg::sandbox::get::Output {
			data,
			location: Some(location),
			tokens: tg::authorization::Tokens::default(),
		}
	}

	/// Answer a request for a lease of a queued process, reply that this scheduler is not ready for any other request of a queued process, which only a runner can carry out once the process starts, and reply that this scheduler does not hold a process once it has offered the process's sandbox to a runner. The assignment carries the leases, so a process whose request was sent to a runner takes leases over process control once it connects.
	fn handle_process_holder_request(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		request: tg::process::control::ServerRequest,
	) {
		let tg::process::control::ServerRequest { arg, id } = request;

		// Ignore a request for a process whose sandbox this scheduler does not serve.
		let entry = self
			.sandboxes
			.entries
			.get_mut(sandbox)
			.filter(|entry| matches!(entry.state, SandboxState::Pending));
		if entry.is_none() && !self.sandboxes.holders.contains_key(sandbox) {
			return;
		}
		let Some(process) = entry.and_then(|entry| {
			let process = entry.request.process.as_ref()?.id.clone()?;
			Some(process)
		}) else {
			let Some(process) = self.try_get_served_process(sandbox) else {
				return;
			};
			self.publish_process_holder_retry(
				scheduler,
				process,
				id,
				tg::control::Retry::NotHolder,
			);
			return;
		};
		let entry = self.sandboxes.entries.get_mut(sandbox).unwrap();
		let queued = entry.request.process.as_mut().unwrap();

		match arg {
			// Add a lease to the queued process.
			tg::process::control::ServerRequestArg::AcquireLease(_) => {
				let lease = tg::process::Lease::new();
				queued.leases.push(lease);
				let data = queued.data.clone();
				let request = entry.request.clone();
				let output = tg::process::control::ClientResponseOutput::AcquireLease(
					tg::process::control::AcquireLeaseClientResponseOutput {
						data,
						lease: Some(lease),
					},
				);
				let scheduler_ = scheduler.clone();
				let write = async move { scheduler_.persist_sandbox(&request).await }.boxed();
				self.publish_process_holder_response(scheduler, process, id, Some(write), output);
			},

			// Release a lease of the queued process, and end the process and its sandbox if no leases remain.
			tg::process::control::ServerRequestArg::ReleaseLease(arg) => {
				let lease = arg.lease;
				let output = |released| {
					tg::process::control::ClientResponseOutput::ReleaseLease(
						tg::process::control::ReleaseLeaseClientResponseOutput { released },
					)
				};
				let position = queued.leases.iter().position(|value| *value == lease);
				let last = position.is_none() && queued.lease == lease && queued.leases.is_empty();
				if let Some(position) = position {
					queued.leases.remove(position);
				} else if queued.lease == lease
					&& let Some(next) = queued.leases.pop()
				{
					queued.lease = next;
				} else if !last {
					self.publish_process_holder_response(
						scheduler,
						process,
						id,
						None,
						output(false),
					);
					return;
				}
				let scheduler_ = scheduler.clone();
				let write = if last {
					self.remove_queued_sandbox(sandbox);
					let request = self
						.remove_sandbox_entry(scheduler, sandbox)
						.unwrap()
						.request;
					self.queue.wake();
					let error = tg::error::Data {
						code: Some(tg::error::Code::Cancellation),
						message: Some("the process was canceled".to_owned()),
						..Default::default()
					};
					async move {
						let status = tg::sandbox::Status::Destroyed;
						let error = Some(tg::Either::Left(error));
						scheduler_
							.put_sandbox_status(&request, status, error, None)
							.await
					}
					.boxed()
				} else {
					let request = entry.request.clone();
					async move { scheduler_.persist_sandbox(&request).await }.boxed()
				};
				self.publish_process_holder_response(
					scheduler,
					process,
					id,
					Some(write),
					output(true),
				);
			},

			tg::process::control::ServerRequestArg::Finish(_)
			| tg::process::control::ServerRequestArg::Get(_)
			| tg::process::control::ServerRequestArg::GetChildren(_)
			| tg::process::control::ServerRequestArg::Open(_)
			| tg::process::control::ServerRequestArg::Signal(_)
			| tg::process::control::ServerRequestArg::Tty(_) => {
				self.publish_process_holder_retry(
					scheduler,
					process,
					id,
					tg::control::Retry::NotReady,
				);
			},
		}
	}

	/// Publish this scheduler's response to a request to the holder of a sandbox. A response that follows a write is acknowledged first, and is sent once the write is durable.
	fn publish_sandbox_holder_response(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		write: Option<futures::future::BoxFuture<'static, tg::Result<()>>>,
		output: tg::sandbox::control::ClientResponseOutput,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let component = &scheduler.server.components.sandbox;
				let result = async {
					let mut error = None;
					let mut output = Some(output);
					if let Some(write) = write {
						let ack = tg::sandbox::control::ClientAck { id: id.clone() };
						component
							.publish_sandbox_holder_ack(&scheduler.server, &sandbox, ack)
							.await?;
						if let Err(source) = write.await {
							error = Some(tg::error::Data {
								message: Some(source.to_string()),
								..Default::default()
							});
							output = None;
						}
					}
					let response = tg::sandbox::control::ClientResponse {
						error,
						id,
						output,
						retry: None,
					};
					component
						.publish_sandbox_holder_response(&scheduler.server, &sandbox, response)
						.await
				}
				.await;
				Operation::Publish {
					context: "failed to answer the request to the sandbox's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Publish this scheduler's reply to a request to the holder of a sandbox that asks the request's sender to send the request again.
	fn publish_sandbox_holder_retry(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		retry: tg::control::Retry,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let response = tg::sandbox::control::ClientResponse::with_retry(id, retry);
				let result = scheduler
					.server
					.components
					.sandbox
					.publish_sandbox_holder_response(&scheduler.server, &sandbox, response)
					.await;
				Operation::Publish {
					context: "failed to answer the request to the sandbox's holder",
					result,
				}
			}
			.boxed(),
		);
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

	/// Get the queued process of a sandbox whose holder this scheduler serves.
	fn try_get_served_process(&mut self, sandbox: &tg::sandbox::Id) -> Option<tg::process::Id> {
		self.try_get_request_mut(sandbox)?
			.process
			.as_ref()?
			.id
			.clone()
	}

	/// Publish this scheduler's response to a request to the holder of a process. A response that follows a write is acknowledged first, and is sent once the write is durable.
	fn publish_process_holder_response(
		&mut self,
		scheduler: &Scheduler,
		process: tg::process::Id,
		id: String,
		write: Option<futures::future::BoxFuture<'static, tg::Result<()>>>,
		output: tg::process::control::ClientResponseOutput,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let component = &scheduler.server.components.process;
				let result = async {
					let mut error = None;
					let mut output = Some(output);
					if let Some(write) = write {
						let ack = tg::process::control::ClientAck { id: id.clone() };
						component
							.publish_process_holder_ack(&scheduler.server, &process, ack)
							.await?;
						if let Err(source) = write.await {
							error = Some(tg::error::Data {
								message: Some(source.to_string()),
								..Default::default()
							});
							output = None;
						}
					}
					let response = tg::process::control::ClientResponse {
						error,
						id,
						output,
						retry: None,
					};
					component
						.publish_process_holder_response(&scheduler.server, &process, response)
						.await
				}
				.await;
				Operation::Publish {
					context: "failed to answer the request to the process's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Publish this scheduler's reply to a request to the holder of a process that asks the request's sender to send the request again.
	fn publish_process_holder_retry(
		&mut self,
		scheduler: &Scheduler,
		process: tg::process::Id,
		id: String,
		retry: tg::control::Retry,
	) {
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let response = tg::process::control::ClientResponse::with_retry(id, retry);
				let result = scheduler
					.server
					.components
					.process
					.publish_process_holder_response(&scheduler.server, &process, response)
					.await;
				Operation::Publish {
					context: "failed to answer the request to the process's holder",
					result,
				}
			}
			.boxed(),
		);
	}

	/// Track a boot that a runner accepted or reported, so that an enqueue of it or of an older boot is a no-op.
	pub(super) fn track_scheduled_sandbox(
		&mut self,
		sandbox: tg::sandbox::Id,
		boot: u64,
		parent: Option<&tg::sandbox::Id>,
		runner: &tg::runner::Id,
		registration_index: u64,
	) {
		if self
			.sandboxes
			.scheduled
			.get(&sandbox)
			.is_some_and(|scheduled| scheduled.boot > boot)
		{
			return;
		}

		// Count the boot as a child of its parent on its runner, keeping the parent that the scheduler already knows if this report does not name it.
		let parent = parent.cloned().or_else(|| {
			self.sandboxes
				.scheduled
				.get(&sandbox)
				.and_then(|scheduled| scheduled.parent.clone())
		});
		if let Some(parent) = &parent {
			self.insert_child(parent, &sandbox, runner);
		}
		let scheduled = Scheduled {
			boot,
			parent,
			registration_index,
			runner: runner.clone(),
		};
		self.sandboxes.scheduled.insert(sandbox, scheduled);
	}

	/// Forget a boot that a runner accepted, and its count as a child of its parent.
	fn forget_scheduled_sandbox(&mut self, sandbox: &tg::sandbox::Id) {
		let Some(scheduled) = self.sandboxes.scheduled.remove(sandbox) else {
			return;
		};
		if let Some(parent) = &scheduled.parent {
			self.remove_child(parent, sandbox);
		}
	}

	/// Record the runner of a parent's child.
	fn insert_child(
		&mut self,
		parent: &tg::sandbox::Id,
		child: &tg::sandbox::Id,
		runner: &tg::runner::Id,
	) {
		self.parents
			.0
			.entry(parent.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				children: BTreeMap::new(),
				queue: Queue::new(),
			})
			.children
			.insert(child.clone(), runner.clone());
	}

	/// Forget the runner of a parent's child, and the parent's entry if it is empty.
	fn remove_child(&mut self, parent: &tg::sandbox::Id, child: &tg::sandbox::Id) {
		let Some(entry) = self.parents.0.get_mut(parent) else {
			return;
		};
		entry.children.remove(child);
		if entry.is_empty() {
			self.parents.0.remove(parent);
		}
	}

	/// Forget a boot that its runner reports ended.
	pub(super) fn handle_sandbox_ended(&mut self, notification: &SandboxEndedNotification) {
		let ended = self
			.sandboxes
			.scheduled
			.get(&notification.sandbox)
			.is_some_and(|scheduled| {
				scheduled.runner == notification.runner
					&& scheduled.registration_index == notification.registration_index
					&& scheduled.boot == notification.boot
			});
		if ended {
			self.forget_scheduled_sandbox(&notification.sandbox);
		}
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
				children: BTreeMap::new(),
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
		if parent.is_empty() {
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
				if parent.is_empty() {
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
				let request = self.remove_sandbox_entry(scheduler, &id).unwrap().request;
				self.finish_destroy_sandbox(scheduler, request, None);
				Some(DequeueSandboxResponseOutput { dequeued: true })
			},
		};
		self.queue.wake();

		output
	}

	pub(super) fn handle_create_sandbox_completion(
		&mut self,
		scheduler: &Scheduler,
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
				accepted: None,
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		};
		let SandboxState::Creating {
			placement: current, ..
		} = &sandbox.state
		else {
			return CreateSandboxCompletionOutput {
				accepted: None,
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		};
		if current != &placement {
			return CreateSandboxCompletionOutput {
				accepted: None,
				dequeue_completions: Vec::new(),
				discarded: None,
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

		// Send the offer again if the runner did not answer it, because only the runner's answer shows whether the runner holds the boot, and keep the reservation and the waiting dequeues until it answers.
		if let Err(error) = &result
			&& runner_current
		{
			tracing::warn!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner did not answer the offer of the sandbox");
			self.retry_sandbox_offer(scheduler, &id);
			return CreateSandboxCompletionOutput {
				accepted: None,
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		}
		if let Ok(Err(error)) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner failed to create the sandbox");
		}

		let mut accepted = None;
		let mut discarded = None;
		let output = match result {
			Ok(Ok(true)) if runner_current => {
				self.remove_reservation(&id, &placement, true);
				self.sandboxes.entries.remove(&id);
				self.track_scheduled_sandbox(
					id.clone(),
					request.boot,
					request.parent.as_ref(),
					&runner_ref.id,
					runner_ref.registration_index,
				);
				accepted = Some(id.clone());
				let placed = Placed {
					generation: 0,
					registration_index: runner_ref.registration_index,
					request,
					runner: runner_ref.id.clone(),
				};
				self.insert_placed_sandbox(id.clone(), placed);
				DequeueSandboxResponseOutput { dequeued: false }
			},
			Ok(Ok(false) | Err(_)) => {
				self.remove_reservation(&id, &placement, false);
				let sandbox = self.sandboxes.entries.get_mut(&id).unwrap();
				if failed {
					sandbox.attempts += 1;
				}
				if !dequeue_requests.is_empty() {
					self.remove_sandbox_entry(scheduler, &id);
				} else if failed && sandbox.attempts >= scheduler.config.max_create_sandbox_attempts
				{
					let source = failure.as_ref().unwrap().clone();
					let attempts = sandbox.attempts;
					let error = tg::error!(
						!source,
						sandbox = %id,
						%attempts,
						"failed to create the sandbox after too many attempts"
					);
					tracing::error!(error = %error.trace(), "giving up on the sandbox");
					let request = self.remove_sandbox_entry(scheduler, &id).unwrap().request;
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
			Ok(Ok(true)) | Err(_) => {
				if dequeue_requests.is_empty() {
					self.requeue_sandbox(&id);
				} else {
					self.remove_sandbox_entry(scheduler, &id);
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
			accepted,
			dequeue_completions,
			discarded,
		}
	}

	pub(super) fn remove_runner(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
	) -> Vec<DequeueCompletion> {
		let Some(runner) = self.runners.entries.remove(id) else {
			return Vec::new();
		};
		let forgotten = self
			.sandboxes
			.scheduled
			.iter()
			.filter(|(_, scheduled)| scheduled.runner == *id)
			.map(|(sandbox, _)| sandbox.clone())
			.collect::<Vec<_>>();
		for sandbox in forgotten {
			self.forget_scheduled_sandbox(&sandbox);
		}
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
		let sandboxes = runner.reservations.into_keys().collect::<Vec<_>>();
		let mut completions = Vec::new();
		for sandbox in sandboxes {
			let dequeue_requests = self.sandboxes.entries[&sandbox].dequeue_requests.clone();
			if dequeue_requests.is_empty() {
				self.requeue_sandbox(&sandbox);
			} else {
				self.remove_sandbox_entry(scheduler, &sandbox);
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
		scheduler: &Scheduler,
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
			let request = self.remove_sandbox_entry(scheduler, &id).unwrap().request;
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

		// Find the runners that can take the sandbox, with their scores and their numbers of the sandbox's siblings.
		let parent = sandbox
			.request
			.parent
			.as_ref()
			.and_then(|parent| self.parents.0.get(parent));
		let mut candidates = Vec::new();
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
			let siblings = parent.map_or(0, |parent| {
				parent
					.children
					.values()
					.filter(|runner| *runner == id)
					.count()
			});
			let candidate = Candidate {
				id,
				registration_index: runner.registration_index,
				score,
				siblings,
			};
			candidates.push(candidate);
		}

		// Prefer the runner that runs the sandbox's parent, which the scheduler knows from the parent's boot, and then the runner that runs the most of its siblings.
		let parent_runner = sandbox
			.request
			.parent
			.as_ref()
			.and_then(|parent| self.sandboxes.scheduled.get(parent))
			.filter(|scheduled| {
				self.runners
					.entries
					.get(&scheduled.runner)
					.is_some_and(|runner| runner.registration_index == scheduled.registration_index)
			})
			.map(|scheduled| &scheduled.runner);
		let candidate = choose_runner(&candidates, parent_runner)?;
		let runner = RunnerRef {
			registration_index: candidate.registration_index,
			id: candidate.id.clone(),
		};

		Some(Placement::Regular { runner })
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
		if let Some(parent) = self.sandboxes.entries[id].request.parent.clone() {
			self.remove_child(&parent, id);
		}
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
					children: BTreeMap::new(),
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
			if parent.is_empty() {
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
				let request = self.remove_sandbox_entry(scheduler, id).unwrap().request;
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
		let entry = Reservation { capacity: reserved };
		runner.reservations.insert(id.clone(), entry);
		if let Some(parent) = self.sandboxes.entries[id].request.parent.clone() {
			self.insert_child(&parent, id, &runner_ref.id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = SandboxState::Creating {
			offer: tangram_server_core::control::id(),
			placement,
			retries: 0,
		};

		// Offer the boot to the runner.
		self.offer_sandbox(scheduler, id);

		true
	}

	/// Send the offer of a boot to its runner, whose answer completes the offer.
	fn offer_sandbox(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let sandbox = &self.sandboxes.entries[id];
		let SandboxState::Creating {
			offer, placement, ..
		} = &sandbox.state
		else {
			return;
		};
		let capacity = sandbox.capacity;
		let offer = offer.clone();
		let placement = placement.clone();
		let request = sandbox.request.clone();
		self.sandboxes.attempts += 1;
		let session = scheduler.server.session(&scheduler.server.context);
		let scheduler_id = scheduler.id.clone();
		let timeout = scheduler.config.create_sandbox_timeout;
		let borrowed = matches!(placement, Placement::Borrowed { .. });
		let sandbox = id.clone();
		self.operations.push(
			async move {
				let runner = placement.runner();
				let arg = CreateSandboxArg {
					borrowed,
					capacity,
					offer,
					request: &request,
					runner,
					scheduler: &scheduler_id,
					timeout,
				};
				let result = create_sandbox(&session, arg).boxed().await;
				Operation::CreateSandbox(Completion {
					placement,
					result,
					sandbox,
				})
			}
			.boxed(),
		);
	}

	/// Send an offer that its runner did not answer again after a delay, until the retries are exhausted, after which the offer waits for the runner to be removed.
	fn retry_sandbox_offer(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get_mut(id) else {
			return;
		};
		let SandboxState::Creating { offer, retries, .. } = &mut sandbox.state else {
			return;
		};
		if *retries >= scheduler.config.create_sandbox_retry.max_retries {
			tracing::error!(sandbox = %id, "the runner did not answer the offer of the sandbox after too many attempts");
			return;
		}
		*retries += 1;
		let delay = tangram_futures::retry::delay_for_attempt(
			*retries,
			&scheduler.config.create_sandbox_retry,
		);
		let offer = offer.clone();
		let sandbox = id.clone();
		self.operations.push(
			async move {
				tokio::time::sleep(delay).await;
				Operation::RetrySandboxOffer { offer, sandbox }
			}
			.boxed(),
		);
	}

	/// Send an offer again, unless the runner answered it or the runner was removed in the meantime.
	pub(super) fn handle_retry_sandbox_offer(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		offer: &str,
	) {
		let current = self
			.sandboxes
			.entries
			.get(id)
			.is_some_and(|sandbox| match &sandbox.state {
				SandboxState::Creating {
					offer: current,
					placement,
					..
				} => {
					let runner = placement.runner();
					current == offer
						&& self.runners.entries.get(&runner.id).is_some_and(|entry| {
							entry.registration_index == runner.registration_index
						})
				},
				SandboxState::Pending => false,
			});
		if current {
			self.offer_sandbox(scheduler, id);
		}
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

	/// Subscribe to the requests to the holder of a sandbox and of its queued process, and to the sandbox's control connection, and send each to the scheduling task.
	async fn serve_sandbox_holder(
		&self,
		sandbox: &tg::sandbox::Id,
		process: Option<&tg::process::Id>,
	) -> tg::Result<Vec<tangram_futures::task::Task<tg::Result<()>>>> {
		let mut tasks = Vec::new();

		// Serve the requests to the sandbox's holder.
		let handle = {
			let sender = self.holder_sender.clone();
			let sandbox = sandbox.clone();
			move |message: tg::sandbox::control::ServerMessage| {
				if let tg::sandbox::control::ServerMessage::Request(request) = message {
					let sandbox = sandbox.clone();
					sender
						.send(HolderMessage::Request { request, sandbox })
						.ok();
				}
				future::ready(Ok(())).boxed()
			}
		};
		let requests = self
			.server
			.components
			.sandbox
			.serve_sandbox_holder(&self.server, sandbox, Box::new(handle))
			.await?;
		tasks.push(requests);

		// Serve the requests to the holder of the sandbox's queued process.
		if let Some(process) = process {
			let handle = {
				let sender = self.holder_sender.clone();
				let sandbox = sandbox.clone();
				move |message: tg::process::control::ServerMessage| {
					if let tg::process::control::ServerMessage::Request(request) = message {
						let sandbox = sandbox.clone();
						sender
							.send(HolderMessage::ProcessRequest { request, sandbox })
							.ok();
					}
					future::ready(Ok(())).boxed()
				}
			};
			let requests = self
				.server
				.components
				.process
				.serve_process_holder(&self.server, process, Box::new(handle))
				.await?;
			tasks.push(requests);
		}

		// Observe the sandbox's control connection.
		let subject = tangram_server_sandbox_common::control::connected_subject(sandbox);
		let mut connected = self
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to subscribe to the sandbox control connection"),
			)?;
		let connected = tangram_futures::task::Task::spawn({
			let sender = self.holder_sender.clone();
			let sandbox = sandbox.clone();
			move |_| async move {
				while connected.next().await.is_some() {
					let sandbox = sandbox.clone();
					sender.send(HolderMessage::Connected { sandbox }).ok();
				}
				Ok(())
			}
		});
		tasks.push(connected);

		Ok(tasks)
	}

	pub(super) async fn persist_sandbox(
		&self,
		request: &EnqueueSandboxRequestArg,
	) -> tg::Result<()> {
		tangram_server_core::checkpoint!(self.server, "scheduler.sandbox.persist", sandbox = %request.sandbox)
			.await;
		let stored = EnqueueSandboxRequestArg {
			token: None,
			..request.clone()
		};
		let stored = serde_json::to_vec(&stored)
			.map_err(|error| tg::error!(!error, "failed to serialize the request"))?;
		self.put_sandbox_status(request, tg::sandbox::Status::Starting, None, Some(stored))
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
		stored: Option<Vec<u8>>,
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
			process: None,
			processes,
			registration: None,
			runner: request.runner.clone(),
			stopped_boot: None,
			stopped_usage: None,
			touched_at: now,
		};

		// Carry a starting process in the sandbox's item, so that the two are written together.
		let mut arg = arg;
		if status.is_starting()
			&& let Some(process) = &request.process
		{
			let process = self.starting_process_arg(process, id, now)?;
			arg.process = Some(Box::new(process));
		}
		let mut items = vec![tangram_index::Item::PutSandbox(Box::new(arg))];

		// Record the accepted request in the scheduler's queue with a starting sandbox, keep it while the scheduler destroys the sandbox, and remove it once the boot ends unplaced, in the same write as the sandbox's status.
		let boot = request.boot;
		if let Some(request) = stored {
			let arg = tangram_index::scheduler::sandbox::put::Arg {
				boot,
				request,
				sandbox: id.clone(),
				scheduler: self.id.clone(),
			};
			items.push(tangram_index::Item::PutSchedulerSandbox(Box::new(arg)));
		} else if !status.is_starting() && status != tg::sandbox::Status::Destroying {
			let arg = tangram_index::scheduler::sandbox::delete::Arg {
				boot,
				sandbox: id.clone(),
			};
			items.push(tangram_index::Item::DeleteSchedulerSandbox(Box::new(arg)));
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
	fn starting_process_arg(
		&self,
		process: &tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
		now: i64,
	) -> tg::Result<tangram_index::process::put::Arg> {
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
			sandbox: Some(sandbox.clone()),
			storage: tg::process::storage::Set::NODE,
			time_to_touch: self.server.config.process.time_to_touch,
			touched_at: now,
		};

		Ok(arg)
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

#[tracing::instrument(name = "sandbox.assign", level = "debug", skip_all, fields(runner = %arg.runner.id, sandbox = %arg.request.sandbox, scheduler = %arg.scheduler), err(level = "debug"))]
async fn create_sandbox(
	session: &Session,
	arg: CreateSandboxArg<'_>,
) -> tg::Result<tg::Result<bool>> {
	let CreateSandboxArg {
		borrowed,
		capacity,
		offer,
		request,
		runner,
		scheduler,
		timeout,
	} = arg;
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
			stop: request.stop,
			token: request.token.clone(),
		},
	);

	// Send the offer once and wait for the answer until the timeout, because the scheduler sends the offer again with the same ID if the runner does not answer.
	let retry = tangram_futures::retry::Options {
		max_retries: 0,
		..session.server.config.control.request_retry.clone().into()
	};
	let options = tangram_server_core::control::Options { retry, timeout };
	let future = session
		.server
		.components
		.runner
		.send_runner_control_request(
			session,
			&runner.id,
			scheduler,
			runner.registration_index,
			offer,
			arg,
			options,
		)
		.boxed();
	let result = tokio::time::timeout(timeout, future)
		.await
		.map_err(|error| tg::error!(!error, "timed out waiting for the runner to answer"))
		.and_then(std::convert::identity);
	if result.is_err() {
		tangram_server_core::checkpoint!(session.server, "scheduler.sandbox.offer.unanswered", sandbox = %request.sandbox)
			.await;
	}
	let result = result.map_err(|error| {
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

/// Choose a runner for a sandbox among the runners that can take it: the runner of the sandbox's parent, then the runner with the most of its siblings, and then the runner with the best score, breaking ties by score and then by runner ID.
fn choose_runner<'a>(
	candidates: &'a [Candidate<'a>],
	parent_runner: Option<&tg::runner::Id>,
) -> Option<&'a Candidate<'a>> {
	if let Some(parent_runner) = parent_runner
		&& let Some(candidate) = candidates
			.iter()
			.find(|candidate| candidate.id == parent_runner)
	{
		return Some(candidate);
	}
	let best = |a: &&Candidate, b: &&Candidate| (a.score, a.id).cmp(&(b.score, b.id));
	if let Some(candidate) = candidates
		.iter()
		.filter(|candidate| candidate.siblings > 0)
		.min_by(|a, b| b.siblings.cmp(&a.siblings).then_with(|| best(a, b)))
	{
		return Some(candidate);
	}
	candidates.iter().min_by(best)
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
