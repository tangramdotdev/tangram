use {
	super::{
		BorrowableCapacityNotification, DequeueSandboxRequestArg, DequeueSandboxResponseOutput,
		EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput, HeartbeatNotification,
		HolderMessage, Operation, ResponseOutput, SandboxEndedNotification, Scheduler,
		SchedulerState,
		runner::{Reservation, Runner},
	},
	futures::{FutureExt as _, StreamExt as _, future},
	std::collections::{BTreeMap, HashMap, HashSet},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_messenger::Messenger as _,
	tangram_server_core::{
		Session,
		control::accept::{Accept, Path},
	},
};

#[cfg(test)]
mod tests;

/// The write of the terminal state of a sandbox that the scheduler ended, which the response that follows it and the removal of the sandbox's entry share.
pub(super) type Write = future::Shared<future::BoxFuture<'static, tg::Result<()>>>;

pub(super) struct Parents(HashMap<tg::sandbox::Id, Parent, tg::id::BuildHasher>);

/// The sandboxes that the scheduler holds, in the order that they were enqueued. The scheduler passes over the queue one sandbox at a time, and at most one pass is in progress.
pub(super) struct Queue {
	/// Whether the capacity changed during the pass that is in progress, so that another pass follows it.
	again: bool,

	/// The next sandbox of the pass that is in progress.
	cursor: Option<tg::sandbox::Id>,

	list: List,
}

/// A list of sandboxes whose links are in the sandboxes' entries, so that adding and removing a sandbox take constant time.
#[derive(Default)]
struct List {
	head: Option<tg::sandbox::Id>,
	len: usize,
	tail: Option<tg::sandbox::Id>,
}

type Entries = HashMap<tg::sandbox::Id, Sandbox, tg::id::BuildHasher>;

pub(super) struct Sandboxes {
	attempts: usize,

	/// The sandboxes that the scheduler tracks, from their enqueues until the index has recorded their ends or their runners report their ends.
	entries: Entries,

	/// The tasks that serve the requests to the holders of the sandboxes that this scheduler holds or has offered to a runner, and of their queued processes, until their control connections are established.
	holders: HashMap<
		tg::sandbox::Id,
		Vec<tangram_futures::task::Task<tg::Result<()>>>,
		tg::id::BuildHasher,
	>,
}

/// A destroy of a sandbox on a runner. The request is present if the sandbox has a record to finish.
pub(super) struct DestroySandbox {
	pub error: tg::error::Data,
	pub registration_index: u64,
	pub request: Option<EnqueueSandboxRequestArg>,
	pub runner: tg::runner::Id,
	pub sandbox: tg::sandbox::Id,
}

struct Parent {
	borrowable: Option<Borrowable>,

	/// The runner of each child that the scheduler offered to a runner or that a runner accepted, so that the scheduler prefers the runner with the most of a parent's children for its next child.
	children: BTreeMap<tg::sandbox::Id, tg::runner::Id>,

	/// The parent's children that the scheduler holds, in the order that they were enqueued.
	queue: List,
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
struct Accepting {
	accept: Accept,
	ids: Vec<String>,
	persisted: bool,
}

#[derive(Default)]
struct Links {
	next: Option<tg::sandbox::Id>,
	previous: Option<tg::sandbox::Id>,
}

struct Sandbox {
	/// The acceptance of the sandbox's enqueue requests, which is present until it is decided and the sandbox's index write is finished.
	accept: Option<Accepting>,

	attempts: usize,

	blocked: HashMap<tg::runner::Id, Block, tg::id::BuildHasher>,

	boot: u64,

	capacity: tg::runner::Capacity,

	dequeue_requests: Vec<String>,

	/// The sandbox's links in the queue.
	links: Links,

	/// The parent of the sandbox, if the scheduler knows it.
	parent: Option<tg::sandbox::Id>,

	/// The sandbox's links in its parent's queue.
	parent_links: Links,

	persisted: bool,

	/// The sandbox's request, which is absent once a runner holds the sandbox and its control connection is established, and for a sandbox that the scheduler knows only from its runner's report.
	request: Option<EnqueueSandboxRequestArg>,

	retrying: bool,

	state: State,

	unplaceable_since: Option<tangram_server_core::clock::Instant>,
}

enum State {
	/// A runner accepted the sandbox, and the scheduler tracks it until its runner reports that it ended or is removed, so that an enqueue of it or of an older boot is a no-op.
	Accepted { runner: RunnerRef },

	/// The scheduler offered the sandbox to a runner, and sends the same offer again until the runner answers. The sandbox keeps its place in the queue.
	Offered {
		offer: String,
		placement: Placement,
		retries: u64,
	},

	/// The sandbox waits in the queue for a runner.
	Queued,

	/// The scheduler ended the sandbox before a runner accepted it, and holds it until the index has recorded its terminal state.
	Terminal {
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

		/// Whether the index has recorded the terminal state.
		indexed: bool,

		status: tg::sandbox::Status,
	},
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
pub(super) struct RunnerRef {
	pub registration_index: u64,
	pub id: tg::runner::Id,
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

#[derive(Default)]
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

	/// The write of the failure, if the scheduler held the sandbox and already began it.
	pub write: Option<Write>,
}

impl Parents {
	pub fn new() -> Self {
		Self(HashMap::default())
	}
}

impl Queue {
	pub fn new() -> Self {
		Self {
			again: false,
			cursor: None,
			list: List::default(),
		}
	}

	pub fn len(&self) -> usize {
		self.list.len
	}

	/// Whether a pass is in progress.
	fn passing(&self) -> bool {
		self.cursor.is_some()
	}

	/// Add a sandbox to the end of the queue, which the pass that is in progress reaches, and which a new pass starts at otherwise.
	fn push(&mut self, entries: &mut Entries, id: &tg::sandbox::Id) {
		self.list.push(entries, Sandbox::links_mut, id);
		if self.cursor.is_none() {
			self.cursor = Some(id.clone());
		}
	}

	/// Take the next sandbox of the pass that is in progress.
	fn next(&mut self, entries: &Entries) -> Option<tg::sandbox::Id> {
		let id = self.cursor.take()?;
		self.cursor.clone_from(&entries[&id].links.next);
		self.finish_pass();

		Some(id)
	}

	fn remove(&mut self, entries: &mut Entries, id: &tg::sandbox::Id) {
		let next = self.list.remove(entries, Sandbox::links_mut, id);
		if self.cursor.as_ref() == Some(id) {
			self.cursor = next;
			self.finish_pass();
		}
	}

	/// Pass over the queue because the capacity changed. A change during a pass does not start the pass over, and is picked up by one pass that follows it.
	pub fn wake(&mut self) {
		if self.cursor.is_some() {
			self.again = true;
		} else {
			self.cursor.clone_from(&self.list.head);
		}
	}

	/// Start another pass if the pass that is in progress reached the end of the queue and the capacity changed during it.
	fn finish_pass(&mut self) {
		if self.cursor.is_none() && self.again {
			self.again = false;
			self.cursor.clone_from(&self.list.head);
		}
	}
}

impl List {
	fn push(
		&mut self,
		entries: &mut Entries,
		links: fn(&mut Sandbox) -> &mut Links,
		id: &tg::sandbox::Id,
	) {
		let previous = self.tail.replace(id.clone());
		if let Some(previous) = &previous {
			links(entries.get_mut(previous).unwrap()).next = Some(id.clone());
		} else {
			self.head = Some(id.clone());
		}
		*links(entries.get_mut(id).unwrap()) = Links {
			next: None,
			previous,
		};
		self.len += 1;
	}

	/// Remove a sandbox, and return the sandbox that followed it.
	fn remove(
		&mut self,
		entries: &mut Entries,
		links: fn(&mut Sandbox) -> &mut Links,
		id: &tg::sandbox::Id,
	) -> Option<tg::sandbox::Id> {
		let Links { next, previous } = std::mem::take(links(entries.get_mut(id).unwrap()));
		if let Some(previous) = &previous {
			links(entries.get_mut(previous).unwrap())
				.next
				.clone_from(&next);
		} else {
			self.head.clone_from(&next);
		}
		if let Some(next) = &next {
			links(entries.get_mut(next).unwrap()).previous = previous;
		} else {
			self.tail = previous;
		}
		self.len -= 1;

		next
	}
}

impl Sandboxes {
	pub fn new() -> Self {
		Self {
			attempts: 0,
			entries: HashMap::default(),
			holders: HashMap::default(),
		}
	}
}

impl Parent {
	/// Whether the entry has no queued children, no capacity to lend, and no children on runners, so that it can be dropped.
	fn is_empty(&self) -> bool {
		self.queue.len == 0 && self.borrowable.is_none() && self.children.is_empty()
	}
}

impl Sandbox {
	fn new(
		boot: u64,
		parent: Option<tg::sandbox::Id>,
		request: Option<EnqueueSandboxRequestArg>,
		state: State,
	) -> Self {
		Self {
			accept: None,
			attempts: 0,
			blocked: HashMap::default(),
			boot,
			capacity: tg::runner::Capacity::default(),
			dequeue_requests: Vec::new(),
			links: Links::default(),
			parent,
			parent_links: Links::default(),
			persisted: false,
			request,
			retrying: false,
			state,
			unplaceable_since: None,
		}
	}

	/// Get the request of a sandbox that no runner holds, which always has one.
	fn request(&self) -> &EnqueueSandboxRequestArg {
		self.request.as_ref().unwrap()
	}

	fn request_mut(&mut self) -> &mut EnqueueSandboxRequestArg {
		self.request.as_mut().unwrap()
	}

	fn links_mut(&mut self) -> &mut Links {
		&mut self.links
	}

	fn parent_links_mut(&mut self) -> &mut Links {
		&mut self.parent_links
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
			operations: futures::stream::FuturesUnordered::new(),
			parents: Parents::new(),
			queue: Queue::new(),
			recovery: None,
			runners: super::runner::Runners::new(),
			sandboxes: Sandboxes::new(),
		}
	}

	pub(super) fn can_schedule(&self, scheduler: &Scheduler) -> bool {
		self.sandboxes.attempts < scheduler.config.max_create_sandbox_requests
			&& self.queue.passing()
	}

	pub(super) fn handle_enqueue_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: Option<String>,
		request: EnqueueSandboxRequestArg,
		recovered: bool,
	) {
		let ids = id.into_iter().collect::<Vec<_>>();

		// Reply to an enqueue of a sandbox that the scheduler tracks without enqueueing it again. An enqueue of a later boot replaces a sandbox that a runner accepted or that the scheduler ended.
		if let Some(entry) = self.sandboxes.entries.get(&request.sandbox) {
			match entry.state {
				State::Offered { .. } | State::Queued => {
					let sandbox = request.sandbox;
					self.finish_enqueue(scheduler, ids, &sandbox, Ok(()));
					return;
				},
				State::Accepted { .. } | State::Terminal { .. } if request.boot <= entry.boot => {
					self.send_enqueue_responses(scheduler, ids, Ok(()));
					return;
				},
				State::Accepted { .. } | State::Terminal { .. } => {
					self.remove_sandbox(scheduler, &request.sandbox);
				},
			}
		}
		if !recovered && self.queue.len() >= scheduler.config.create_sandbox_queue_capacity {
			let error = tg::error!(sandbox = %request.sandbox, "the scheduler queue is full");
			let sandbox = request.sandbox;
			self.finish_enqueue(scheduler, ids, &sandbox, Err(error));
			return;
		}
		let sandbox = request.sandbox.clone();
		let result = self.enqueue_sandbox(scheduler, request, recovered);
		self.finish_enqueue(scheduler, ids, &sandbox, result);
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
			&& let Some(entry) = self.sandboxes.entries.get_mut(sandbox)
			&& matches!(entry.state, State::Offered { .. } | State::Queued)
			&& !entry.persisted
		{
			// Write the sandbox to the index queue concurrently with its handoff to a runner.
			let first = entry.accept.is_none();
			entry.accept.get_or_insert_default().ids.extend(ids);
			if first {
				let boot = entry.boot;
				let request = entry.request().clone();
				let scheduler = scheduler.clone();
				let sandbox = sandbox.clone();
				self.operations.push(
					async move {
						let result = scheduler.persist_sandbox(&request).await;
						Operation::PersistSandbox {
							boot,
							result,
							sandbox,
						}
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
		let Some(entry) = self.sandboxes.entries.get_mut(sandbox) else {
			return;
		};
		let Some(accept) = &mut entry.accept else {
			return;
		};
		let output = accept
			.accept
			.finish(path, result)
			.map(|result| (std::mem::take(&mut accept.ids), result));

		// Remove the acceptance once it is decided and its index write is finished, so that a later enqueue request does not wait for it.
		if path == Path::Queued {
			accept.persisted = true;
		}
		if accept.accept.decided() && accept.persisted {
			entry.accept = None;
		}
		if let Some((ids, result)) = output {
			self.send_enqueue_responses(scheduler, ids, result);
		}
	}

	/// Remove the entry of a sandbox that is not in the queue, and fail the enqueue requests that still wait for its acceptance.
	fn remove_sandbox(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) -> Option<Sandbox> {
		let mut sandbox = self.sandboxes.entries.remove(id)?;
		if let Some(parent) = &sandbox.parent {
			self.remove_child(parent, id);
		}
		if let Some(accept) = sandbox.accept.take()
			&& !accept.ids.is_empty()
		{
			let error =
				tg::error!(sandbox = %id, "the sandbox was removed before a runner accepted it");
			self.send_enqueue_responses(scheduler, accept.ids, Err(error));
		}

		Some(sandbox)
	}

	/// End a sandbox that no runner accepted, and write its terminal state. Keep the sandbox's entry until the index has recorded the terminal state, so that an enqueue of the sandbox that the lagging index sends again does not start it. Return the write, which a response that follows it awaits.
	fn end_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> Write {
		// End the sandbox, which leaves the queue.
		self.remove_queued_sandbox(id);
		let entry = self.sandboxes.entries.get_mut(id).unwrap();
		entry.state = State::Terminal {
			error: error.clone(),
			indexed: false,
			status,
		};
		let boot = entry.boot;
		let request = entry.request().clone();
		let source =
			tg::error!(sandbox = %id, "the sandbox was removed before a runner accepted it");
		self.finish_accept(scheduler, id, Path::Handled, Err(source));

		// Write the terminal state.
		let write = {
			let scheduler = scheduler.clone();
			async move {
				scheduler
					.put_sandbox_status(&request, status, error, None)
					.await
			}
			.boxed()
			.shared()
		};

		// Remove the sandbox's entry once the index has recorded the terminal state.
		let scheduler = scheduler.clone();
		let sandbox = id.clone();
		let written = write.clone();
		self.operations.push(
			async move {
				if written.await.is_ok() {
					let result = scheduler.index().await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), %sandbox, "failed to await the indexing of the ended sandbox");
					}
				}
				Operation::RemoveTerminalSandbox { boot, sandbox }
			}
			.boxed(),
		);

		write
	}

	/// End a sandbox, and record the failure of its write if no response reports it.
	fn end_sandbox_and_log(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) {
		let write = self.end_sandbox(scheduler, id, status, error);
		self.operations.push(
			async move {
				let result = write.await;
				Operation::Publish {
					context: "failed to record the ended sandbox",
					result,
				}
			}
			.boxed(),
		);
	}

	/// End a sandbox that this scheduler gives up on, which records the failure in the sandbox's error field. A new sandbox is destroyed, and a sandbox that was starting again is stopped.
	fn discard_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		error: &tg::Error,
	) -> DiscardedSandbox {
		let error = error.to_data_or_id();
		let request = self.sandboxes.entries[id].request().clone();
		let status = failed_sandbox_status(&request);
		let write = self.end_sandbox(scheduler, id, status, Some(error.clone()));
		DiscardedSandbox {
			error,
			request,
			sandbox: id.clone(),
			write: Some(write),
		}
	}

	/// Remove the entry of a sandbox that this scheduler ended once the index has recorded its terminal state, unless a later boot replaced it.
	pub(super) fn handle_remove_terminal_sandbox(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
		boot: u64,
	) {
		if let Some(entry) = self.sandboxes.entries.get_mut(sandbox)
			&& entry.boot == boot
			&& let State::Terminal { indexed, .. } = &mut entry.state
		{
			*indexed = true;
			self.try_remove_terminal_sandbox(scheduler, sandbox);
		}
	}

	/// Remove the entry of a sandbox that this scheduler ended, if the index has recorded its terminal state and no enqueue request waits for its index write.
	fn try_remove_terminal_sandbox(&mut self, scheduler: &Scheduler, sandbox: &tg::sandbox::Id) {
		let remove = self.sandboxes.entries.get(sandbox).is_some_and(|entry| {
			matches!(entry.state, State::Terminal { indexed: true, .. }) && entry.accept.is_none()
		});
		if remove {
			self.remove_sandbox(scheduler, sandbox);
			self.stop_serving_sandbox_holder(sandbox);
		}
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
		boot: u64,
		result: tg::Result<()>,
	) {
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to record the accepted sandbox");
		}
		let Some(sandbox) = self
			.sandboxes
			.entries
			.get_mut(id)
			.filter(|sandbox| sandbox.boot == boot)
		else {
			return;
		};
		if result.is_ok() {
			sandbox.persisted = true;
		}
		self.finish_accept(scheduler, id, Path::Queued, result);
		self.try_remove_terminal_sandbox(scheduler, id);
	}

	fn enqueue_sandbox(
		&mut self,
		scheduler: &Scheduler,
		mut request: EnqueueSandboxRequestArg,
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
		let mut sandbox = Sandbox::new(request.boot, parent.clone(), Some(request), State::Queued);
		sandbox.capacity = capacity;
		sandbox.persisted = recovered;

		// Reject a request that no runner could ever fit if the configuration does not wait for one.
		if !recovered
			&& scheduler.config.unplaceable_timeout == Some(std::time::Duration::ZERO)
			&& !self.placeable(&sandbox)
		{
			return Err(tg::error!(sandbox = %id, "no runners available"));
		}
		let process = sandbox
			.request()
			.process
			.as_ref()
			.and_then(|process| process.id.clone());
		self.sandboxes.entries.insert(id.clone(), sandbox);
		self.serve_sandbox_holder(scheduler, &id, process);

		// Add the sandbox to the end of the queue, and of its parent's queue.
		self.queue.push(&mut self.sandboxes.entries, &id);
		if let Some(parent) = parent {
			self.parents
				.0
				.entry(parent)
				.or_insert_with(|| Parent {
					borrowable: None,
					children: BTreeMap::new(),
					queue: List::default(),
				})
				.queue
				.push(&mut self.sandboxes.entries, Sandbox::parent_links_mut, &id);
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

	/// Stop serving the holders of the sandboxes that this scheduler neither has a request for nor holds for recovery.
	pub(super) fn prune_sandbox_holders(&mut self) {
		let sandboxes = self
			.sandboxes
			.holders
			.keys()
			.filter(|sandbox| {
				let held = self
					.sandboxes
					.entries
					.get(*sandbox)
					.is_some_and(|entry| entry.request.is_some());
				!(held || self.is_recovering(sandbox))
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
				// Drop the request of a sandbox whose runner serves it, and stop serving the sandbox's holder, unless this scheduler holds a later boot of it.
				let entry = self.sandboxes.entries.get_mut(&sandbox);
				let held = match entry {
					Some(entry) if matches!(entry.state, State::Accepted { .. }) => {
						entry.request = None;
						false
					},
					Some(_) => true,
					None => false,
				};
				if !held {
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
		if !self.sandboxes.holders.contains_key(&sandbox) {
			return;
		}
		let state = self
			.sandboxes
			.entries
			.get(&sandbox)
			.map(|entry| &entry.state);
		match state {
			// Reply that this scheduler does not hold a sandbox that it has offered to a runner, whether or not the runner has accepted it, because the offer carries the boot's pending stop.
			None | Some(State::Accepted { .. } | State::Offered { .. }) => {
				self.publish_sandbox_holder_retry(
					scheduler,
					sandbox,
					id,
					tg::control::Retry::NotHolder,
				);
				return;
			},

			Some(State::Queued) => (),

			// Answer a request for a sandbox that this scheduler ended with the sandbox's terminal state, which the index may not have recorded yet.
			Some(State::Terminal { .. }) => {
				self.handle_terminal_sandbox_holder_request(scheduler, sandbox, id, &arg);
				return;
			},
		}

		// Reply that this scheduler holds a queued sandbox but is not ready for a spawn, which only a runner can carry out once the sandbox starts.
		if matches!(arg, tg::sandbox::control::ServerRequestArg::SpawnProcess(_)) {
			self.publish_sandbox_holder_retry(scheduler, sandbox, id, tg::control::Retry::NotReady);
			return;
		}
		let request = self
			.sandboxes
			.entries
			.get_mut(&sandbox)
			.unwrap()
			.request_mut();

		match arg {
			// Destroy a queued sandbox by ending it.
			tg::sandbox::control::ServerRequestArg::Destroy(arg) => {
				let status = tg::sandbox::Status::Destroyed;
				let error = arg.error.map(tg::Either::Left);
				let write = self.end_sandbox(scheduler, &sandbox, status, error).boxed();
				self.queue.wake();
				let output = tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: true },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},

			// Answer a get with the queued boot.
			tg::sandbox::control::ServerRequestArg::Get(_) => {
				let data = Self::queued_sandbox_get_output(scheduler, request);
				let output = tg::sandbox::control::ClientResponseOutput::Get(
					tg::sandbox::control::GetClientResponseOutput { data },
				);
				self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
			},

			// Answer a get of the processes with the queued process.
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => {
				let processes = request
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
				let write = if std::mem::take(&mut request.stop) {
					let request = request.clone();
					let scheduler_ = scheduler.clone();
					Some(async move { scheduler_.persist_sandbox(&request).await }.boxed())
				} else {
					None
				};
				let data = Self::queued_sandbox_get_output(scheduler, request);
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
				let write = if request.boot > 0 {
					let status = tg::sandbox::Status::Stopped;
					let write = self.end_sandbox(scheduler, &sandbox, status, None).boxed();
					self.queue.wake();
					write
				} else {
					request.stop = true;
					let request = request.clone();
					async move { scheduler_.persist_sandbox(&request).await }.boxed()
				};
				self.publish_sandbox_holder_response(scheduler, sandbox, id, Some(write), output);
			},
		}
	}

	/// Answer a request to the holder of a sandbox that this scheduler ended. A get, a get of the processes, and a start are answered with the sandbox's terminal state, so that a start of a stopped sandbox reserves the next boot, and a stop of a stopped sandbox and a destroy of a destroyed sandbox have nothing left to do. Any other request waits for the index, which answers it once it has recorded the terminal state.
	fn handle_terminal_sandbox_holder_request(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		id: String,
		arg: &tg::sandbox::control::ServerRequestArg,
	) {
		let entry = &self.sandboxes.entries[&sandbox];
		let State::Terminal { error, status, .. } = &entry.state else {
			return;
		};
		let status = *status;
		let data = Self::sandbox_get_output(scheduler, entry.request(), status, error.clone());
		let output = match arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) if status.is_destroyed() => {
				tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: false },
				)
			},
			tg::sandbox::control::ServerRequestArg::Get(_) => {
				tg::sandbox::control::ClientResponseOutput::Get(
					tg::sandbox::control::GetClientResponseOutput { data },
				)
			},
			tg::sandbox::control::ServerRequestArg::GetProcesses(_) => {
				tg::sandbox::control::ClientResponseOutput::GetProcesses(
					tg::sandbox::control::GetProcessesClientResponseOutput {
						length: 0,
						processes: Vec::new(),
						status,
					},
				)
			},
			tg::sandbox::control::ServerRequestArg::Start(_) => {
				tg::sandbox::control::ClientResponseOutput::Start(
					tg::sandbox::control::StartClientResponseOutput { data },
				)
			},
			tg::sandbox::control::ServerRequestArg::Stop(_) if status.is_stopped() => {
				tg::sandbox::control::ClientResponseOutput::Stop(
					tg::sandbox::control::StopClientResponseOutput { stopped: false },
				)
			},
			tg::sandbox::control::ServerRequestArg::Destroy(_)
			| tg::sandbox::control::ServerRequestArg::SpawnProcess(_)
			| tg::sandbox::control::ServerRequestArg::Stop(_) => {
				self.publish_sandbox_holder_retry(
					scheduler,
					sandbox,
					id,
					tg::control::Retry::NotReady,
				);
				return;
			},
		};
		self.publish_sandbox_holder_response(scheduler, sandbox, id, None, output);
	}

	/// Create the output of a get of a queued sandbox.
	fn queued_sandbox_get_output(
		scheduler: &Scheduler,
		request: &EnqueueSandboxRequestArg,
	) -> tg::sandbox::get::Output {
		Self::sandbox_get_output(scheduler, request, tg::sandbox::Status::Starting, None)
	}

	/// Create the output of a get of a sandbox that this scheduler holds.
	fn sandbox_get_output(
		scheduler: &Scheduler,
		request: &EnqueueSandboxRequestArg,
		status: tg::sandbox::Status,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	) -> tg::sandbox::get::Output {
		let mut data = tangram_server_scheduler_core::sandbox_data(request, status);
		data.error = error;
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
		if !self.sandboxes.holders.contains_key(sandbox) {
			return;
		}

		// Reply that this scheduler is not ready for a request of the process of a sandbox that it ended, which the index answers once it has recorded the finished process.
		if let Some(entry) = self.sandboxes.entries.get(sandbox)
			&& matches!(entry.state, State::Terminal { .. })
		{
			let Some(process) = entry
				.request()
				.process
				.as_ref()
				.and_then(|process| process.id.clone())
			else {
				return;
			};
			self.publish_process_holder_retry(scheduler, process, id, tg::control::Retry::NotReady);
			return;
		}
		let entry = self
			.sandboxes
			.entries
			.get(sandbox)
			.filter(|entry| matches!(entry.state, State::Queued));
		let Some(process) = entry.and_then(|entry| {
			let process = entry.request().process.as_ref()?.id.clone()?;
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
		let request = self
			.sandboxes
			.entries
			.get_mut(sandbox)
			.unwrap()
			.request_mut();
		let queued = request.process.as_mut().unwrap();

		match arg {
			// Add a lease to the queued process.
			tg::process::control::ServerRequestArg::AcquireLease(_) => {
				let lease = tg::process::Lease::new();
				queued.leases.push(lease);
				let data = queued.data.clone();
				let request = request.clone();
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
					let error = tg::error::Data {
						code: Some(tg::error::Code::Cancellation),
						message: Some("the process was canceled".to_owned()),
						..Default::default()
					};
					let status = tg::sandbox::Status::Destroyed;
					let error = Some(tg::Either::Left(error));
					let write = self.end_sandbox(scheduler, sandbox, status, error).boxed();
					self.queue.wake();
					write
				} else {
					let request = request.clone();
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

	/// Get the request of a sandbox that this scheduler has a request for.
	fn try_get_request_mut(
		&mut self,
		sandbox: &tg::sandbox::Id,
	) -> Option<&mut EnqueueSandboxRequestArg> {
		let tracked = self
			.sandboxes
			.entries
			.get(sandbox)
			.is_some_and(|entry| entry.request.is_some());
		if tracked {
			return self
				.sandboxes
				.entries
				.get_mut(sandbox)
				.and_then(|entry| entry.request.as_mut());
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

	/// Track a sandbox that a runner holds, which a runner reports when it registers and which a restarted scheduler recovers, so that an enqueue of it or of an older boot is a no-op. The request is present if the sandbox's control connection may not be established.
	pub(super) fn track_accepted_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
		boot: u64,
		request: Option<EnqueueSandboxRequestArg>,
		runner: RunnerRef,
	) {
		let mut parent = request.as_ref().and_then(|request| request.parent.clone());
		let mut request = request;
		if let Some(entry) = self.sandboxes.entries.get(id) {
			if entry.boot > boot {
				return;
			}
			match &entry.state {
				// Record that the runner accepted a queued sandbox that it holds, whose offer's answer the scheduler did not receive.
				State::Queued if entry.boot == boot => {
					self.accept_sandbox(id, runner);
					self.finish_accept(scheduler, id, Path::Handled, Ok(()));
					return;
				},

				// Leave a sandbox that the scheduler offered to a runner to the offer's answer, and a queued later boot alone.
				State::Offered { .. } | State::Queued => return,

				// Destroy a sandbox on its runner that the scheduler ended.
				State::Terminal { error, .. } if entry.boot == boot => {
					let error = error.clone().and_then(tg::Either::left).unwrap_or_default();
					let destroy = DestroySandbox {
						error,
						registration_index: runner.registration_index,
						request: None,
						runner: runner.id,
						sandbox: id.clone(),
					};
					self.send_destroy_sandbox(scheduler, destroy);
					return;
				},

				// Replace an older boot, and keep what the scheduler knows about the boot if the report does not carry it.
				State::Accepted { .. } | State::Terminal { .. } => {
					let previous = self.remove_sandbox(scheduler, id).unwrap();
					parent = parent.or(previous.parent);
					if previous.boot == boot {
						request = request.or(previous.request);
					}
				},
			}
		}
		if let Some(parent) = &parent {
			self.insert_child(parent, id, &runner.id);
		}
		let sandbox = Sandbox::new(boot, parent, request, State::Accepted { runner });
		self.sandboxes.entries.insert(id.clone(), sandbox);
	}

	/// Record that a runner accepted a sandbox, which leaves the queue. The scheduler keeps the sandbox's request until its control connection is established.
	fn accept_sandbox(&mut self, id: &tg::sandbox::Id, runner: RunnerRef) {
		self.remove_queued_sandbox(id);
		let entry = self.sandboxes.entries.get_mut(id).unwrap();
		entry.blocked = HashMap::default();
		let parent = entry.parent.clone();
		if let Some(parent) = &parent {
			self.insert_child(parent, id, &runner.id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Accepted { runner };
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
				queue: List::default(),
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

	/// Forget a sandbox that its runner reports ended.
	pub(super) fn handle_sandbox_ended(
		&mut self,
		scheduler: &Scheduler,
		notification: &SandboxEndedNotification,
	) {
		let ended = self
			.sandboxes
			.entries
			.get(&notification.sandbox)
			.is_some_and(|entry| {
				entry.boot == notification.boot
					&& matches!(
						&entry.state,
						State::Accepted { runner }
							if runner.id == notification.runner
								&& runner.registration_index == notification.registration_index
					)
			});
		if ended {
			self.remove_sandbox(scheduler, &notification.sandbox);
			self.stop_serving_sandbox_holder(&notification.sandbox);
		}
	}

	/// Read the sandboxes that this scheduler has keys for, to queue the sandboxes again that were lost with a runner before their control connections were established.
	fn read_lost_sandboxes(&mut self, scheduler: &Scheduler, lost: Vec<EnqueueSandboxRequestArg>) {
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
				Operation::LostSandboxes { keys, lost }
			}
			.boxed(),
		);
	}

	/// Queue the sandboxes again that were lost with a runner and that still have keys, whose control connections were not established.
	pub(super) fn handle_lost_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		keys: Vec<tg::sandbox::Id>,
		lost: Vec<EnqueueSandboxRequestArg>,
	) {
		let keys = keys
			.into_iter()
			.collect::<HashSet<_, tg::id::BuildHasher>>();
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
					write: None,
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
				queue: List::default(),
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
		if parent.is_empty() {
			self.parents.0.remove(&parent_id);
		}
		let sandbox = self.first_queued_child(&parent_id);
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
			State::Accepted { .. } => Some(DequeueSandboxResponseOutput { dequeued: false }),

			// Wait for the runner's answer to the offer.
			State::Offered { .. } => {
				sandbox.dequeue_requests.push(request_id);
				None
			},

			State::Queued => {
				self.end_sandbox_and_log(scheduler, &id, tg::sandbox::Status::Destroyed, None);
				Some(DequeueSandboxResponseOutput { dequeued: true })
			},

			// Report that a sandbox that this scheduler ended is dequeued only if it is destroyed, so that the caller destroys a stopped sandbox.
			State::Terminal { status, .. } => Some(DequeueSandboxResponseOutput {
				dequeued: status.is_destroyed(),
			}),
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
		let mut output = CreateSandboxCompletionOutput::default();

		// Ignore the answer to an offer that is not the sandbox's current offer. Removing a runner returns the sandboxes that are offered to it to the queue, so the runner of the current offer is registered.
		let current = self.sandboxes.entries.get(&id).is_some_and(|sandbox| {
			matches!(&sandbox.state, State::Offered { placement: current, .. } if current == &placement)
		});
		if !current {
			return output;
		}
		let runner_ref = placement.runner().clone();

		// Send the offer again if the runner did not answer it, because only the runner's answer shows whether the runner holds the sandbox, and keep the reservation and the waiting dequeues until it answers.
		let result = match result {
			Ok(result) => result,
			Err(error) => {
				tracing::warn!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner did not answer the offer of the sandbox");
				self.retry_sandbox_offer(scheduler, &id);
				return output;
			},
		};
		let sandbox = self.sandboxes.entries.get_mut(&id).unwrap();
		let dequeue_requests = std::mem::take(&mut sandbox.dequeue_requests);
		let failure = match result {
			// The runner accepted the sandbox.
			Ok(true) => {
				self.remove_reservation(&id, &placement, true);
				self.accept_sandbox(&id, runner_ref);
				output.accepted = Some(id);
				let dequeued = false;
				output.dequeue_completions = dequeue_requests
					.into_iter()
					.map(|request| DequeueCompletion {
						output: DequeueSandboxResponseOutput { dequeued },
						request,
					})
					.collect();
				return output;
			},

			// The runner declined the sandbox.
			Ok(false) => None,

			// The runner failed to create the sandbox.
			Err(error) => {
				tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner failed to create the sandbox");
				sandbox.attempts += 1;
				Some(error)
			},
		};

		// Return the sandbox to the queue.
		let attempts = sandbox.attempts;
		self.remove_reservation(&id, &placement, false);
		self.requeue_sandbox(&id);
		if !dequeue_requests.is_empty() {
			// End a sandbox that a dequeue waits for.
			self.end_sandbox_and_log(scheduler, &id, tg::sandbox::Status::Destroyed, None);
		} else if let Some(source) = failure
			&& attempts >= scheduler.config.max_create_sandbox_attempts
		{
			let error = tg::error!(
				!source,
				sandbox = %id,
				%attempts,
				"failed to create the sandbox after too many attempts"
			);
			tracing::error!(error = %error.trace(), "giving up on the sandbox");
			output.discarded = Some(self.discard_sandbox(scheduler, &id, &error));
		} else {
			// Try another runner at once, and not this runner until its next heartbeat.
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
			self.try_schedule(scheduler, &id);
		}
		output.dequeue_completions = dequeue_requests
			.into_iter()
			.map(|request| DequeueCompletion {
				output: DequeueSandboxResponseOutput { dequeued: true },
				request,
			})
			.collect();

		output
	}

	/// Remove a runner. Return the sandboxes that are offered to it to the queue, and forget the sandboxes that it accepted, except the ones that it reports when it registers again.
	pub(super) fn remove_runner(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::runner::Id,
		reported: &[tg::runner::control::ArgSandbox],
	) -> Vec<DequeueCompletion> {
		let Some(runner) = self.runners.entries.remove(id) else {
			return Vec::new();
		};

		// Forget the sandboxes that the runner accepted, and queue the ones again whose control connections were not established, if their keys remain.
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
		let mut completions = Vec::new();
		for sandbox in sandboxes {
			self.requeue_sandbox(&sandbox);
			let entry = self.sandboxes.entries.get_mut(&sandbox).unwrap();
			let dequeue_requests = std::mem::take(&mut entry.dequeue_requests);
			if dequeue_requests.is_empty() {
				continue;
			}
			self.end_sandbox_and_log(scheduler, &sandbox, tg::sandbox::Status::Destroyed, None);
			let output = DequeueSandboxResponseOutput { dequeued: true };
			completions.extend(
				dequeue_requests
					.into_iter()
					.map(|request| DequeueCompletion {
						output: output.clone(),
						request,
					}),
			);
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
				matches!(sandbox.state, State::Queued)
					&& sandbox.request().runner.as_ref() == Some(runner)
			})
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		let mut discarded = Vec::new();
		for id in ids {
			let error = tg::error!(sandbox = %id, %runner, "the sandbox's runner is unavailable");
			let sandbox = self.discard_sandbox(scheduler, &id, &error);
			discarded.push(sandbox);
		}
		discarded
	}

	pub(super) fn schedule_one(&mut self, scheduler: &Scheduler) {
		let Some(sandbox) = self.queue.next(&self.sandboxes.entries) else {
			return;
		};
		self.try_schedule(scheduler, &sandbox);
	}

	/// Try to place a sandbox that no runner could fit again once its timeout expires, which discards it if no runner can fit it still.
	pub(super) fn handle_retry_sandbox(&mut self, scheduler: &Scheduler, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get_mut(id) else {
			return;
		};
		if !matches!(sandbox.state, State::Queued) || !sandbox.retrying {
			return;
		}
		sandbox.retrying = false;
		self.try_schedule(scheduler, id);
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
				.is_none_or(|owner| sandbox.request().owner_ancestors.contains(owner));
			if !owner_matches
				|| !runner.ready
				|| blocked
				|| !matches_host(runner, sandbox.request())
				|| !matches_runner(id, sandbox.request())
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
			.parent
			.as_ref()
			.and_then(|parent| self.sandboxes.entries.get(parent))
			.and_then(|parent| match &parent.state {
				State::Accepted { runner } => Some(runner),
				State::Offered { .. } | State::Queued | State::Terminal { .. } => None,
			})
			.filter(|runner| {
				self.runners
					.entries
					.get(&runner.id)
					.is_some_and(|entry| entry.registration_index == runner.registration_index)
			})
			.map(|runner| &runner.id);
		let candidate = choose_runner(&candidates, parent_runner)?;
		let runner = RunnerRef {
			registration_index: candidate.registration_index,
			id: candidate.id.clone(),
		};

		Some(Placement::Regular { runner })
	}

	fn borrowed_placement(&self, sandbox: &Sandbox, request_limit: usize) -> Option<Placement> {
		let (borrowable, runner) = self.borrowable(sandbox)?;
		if !matches_runner(&borrowable.runner, sandbox.request()) {
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
			parent: sandbox.parent.clone().unwrap(),
			runner,
		})
	}

	/// Find the capacity that a sandbox's parent lends on a runner, where the runner also has the capacity that the sandbox must acquire in addition to what it borrows.
	fn borrowable(&self, sandbox: &Sandbox) -> Option<(&Borrowable, &Runner)> {
		let parent = sandbox.parent.as_ref()?;
		let borrowable = self.parents.0.get(parent)?.borrowable.as_ref()?;
		let runner = self.runners.entries.get(&borrowable.runner)?;
		let (_, acquired) = borrowable.capacity.try_borrow(sandbox.capacity)?;
		if !available(runner).contains(acquired, runner.capacity.overcommit)
			|| borrowable
				.registration_index
				.is_some_and(|index| index != runner.registration_index)
			|| !runner.ready
			|| !matches_host(runner, sandbox.request())
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

	/// Return a sandbox whose offer was not accepted to the queued state. The sandbox kept its place in the queue while it was offered.
	fn requeue_sandbox(&mut self, id: &tg::sandbox::Id) {
		if let Some(parent) = self.sandboxes.entries[id].parent.clone() {
			self.remove_child(&parent, id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Queued;
	}

	/// Remove a sandbox from the queue, and from its parent's queue.
	fn remove_queued_sandbox(&mut self, id: &tg::sandbox::Id) {
		let parent_id = self.sandboxes.entries[id].parent.clone();
		self.queue.remove(&mut self.sandboxes.entries, id);
		if let Some(parent_id) = parent_id
			&& let Some(parent) = self.parents.0.get_mut(&parent_id)
		{
			parent
				.queue
				.remove(&mut self.sandboxes.entries, Sandbox::parent_links_mut, id);
			if parent.is_empty() {
				self.parents.0.remove(&parent_id);
			}
		}
	}

	/// Find the first of a parent's children that the scheduler has not offered to a runner.
	fn first_queued_child(&self, parent: &tg::sandbox::Id) -> Option<tg::sandbox::Id> {
		let mut child = self.parents.0.get(parent)?.queue.head.as_ref();
		while let Some(id) = child {
			let sandbox = &self.sandboxes.entries[id];
			if matches!(sandbox.state, State::Queued) {
				return Some(id.clone());
			}
			child = sandbox.parent_links.next.as_ref();
		}

		None
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
		if !matches!(sandbox.state, State::Queued) {
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
				let discarded = self.discard_sandbox(scheduler, id, &error);
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
		if let Some(parent) = self.sandboxes.entries[id].parent.clone() {
			self.insert_child(&parent, id, &runner_ref.id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Offered {
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
		let State::Offered {
			offer, placement, ..
		} = &sandbox.state
		else {
			return;
		};
		let capacity = sandbox.capacity;
		let offer = offer.clone();
		let placement = placement.clone();
		let request = sandbox.request().clone();
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
		let State::Offered { offer, retries, .. } = &mut sandbox.state else {
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
				State::Offered {
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
				State::Accepted { .. } | State::Queued | State::Terminal { .. } => false,
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
		self.index().await?;
		let sandboxes = self.server.index.get_scheduler_sandboxes(&self.id).await?;
		let keys = sandboxes
			.into_iter()
			.map(|sandbox| sandbox.id)
			.collect::<Vec<_>>();

		Ok(keys)
	}

	/// Await indexing, so that the index has applied every write that was acknowledged before.
	pub(super) async fn index(&self) -> tg::Result<()> {
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

		Ok(())
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
		let status = failed_sandbox_status(request);
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

/// Get the status that records a failure of a sandbox after its acceptance, which destroys a new sandbox and stops a sandbox that was starting again.
fn failed_sandbox_status(request: &EnqueueSandboxRequestArg) -> tg::sandbox::Status {
	if request.boot == 0 {
		tg::sandbox::Status::Destroyed
	} else {
		tg::sandbox::Status::Stopped
	}
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
		.is_none_or(|owner| sandbox.request().owner_ancestors.contains(owner));
	owner_matches
		&& matches_host(runner, sandbox.request())
		&& matches_runner(id, sandbox.request())
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
