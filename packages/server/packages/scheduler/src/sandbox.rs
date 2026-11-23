use {
	super::{
		AddSandboxParentRequestArg, AddSandboxParentResponseOutput, DequeueSandboxRequestArg,
		DequeueSandboxResponseOutput, EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput,
		Operation, ResponseOutput, Scheduler, SchedulerState,
		placement::{Block, Parents, Placement, RunnerRef},
		queue::{Links, Queue},
		record::failed_sandbox_status,
	},
	futures::{FutureExt as _, future},
	std::collections::{BTreeSet, HashMap},
	tangram_client::prelude::*,
	tangram_server_core::control::accept::{Accept, Path},
};

#[cfg(test)]
mod tests;

/// The write of the terminal state of a sandbox that the scheduler ended, which the response that follows it and the removal of the sandbox's entry share.
pub(super) type Write = future::Shared<future::BoxFuture<'static, tg::Result<()>>>;

pub(super) type Entries = HashMap<tg::sandbox::Id, Sandbox, tg::id::BuildHasher>;

pub(super) struct Sandboxes {
	/// The boots that runners held and that ended or that a later boot replaced, which authorize the runners' late reports of them.
	pub(super) ended: super::report::Ended,

	/// The sandboxes that the scheduler tracks, from their enqueues until the index has recorded their ends or their runners report their ends.
	pub(super) entries: Entries,

	/// The tasks that serve the requests to the holders of the sandboxes that this scheduler knows, and of their queued processes until a runner holds them, which are absent until the subscriptions are established.
	pub(super) holders: HashMap<tg::sandbox::Id, Option<HolderTasks>, tg::id::BuildHasher>,

	/// Whether the scheduler awaits indexing for sandboxes that it ended.
	pub(super) indexing: bool,

	/// The number of offers that the scheduler is sending.
	pub(super) offers: usize,

	/// The sandboxes that the scheduler ended, with their boots, whose terminal states are written and which the next await of indexing covers.
	pub(super) written: Vec<(tg::sandbox::Id, u64)>,
}

/// The tasks that serve the requests to the holders of a sandbox and of its queued process, and observe its control connection.
#[expect(
	dead_code,
	reason = "the tasks run until they are dropped, so the scheduler keeps them without reading them"
)]
pub(super) struct HolderTasks {
	pub(super) connected: tangram_futures::task::Task<tg::Result<()>>,

	/// The task that serves the requests to the holder of the sandbox's queued process, which stops once a runner holds the process.
	pub(super) process: Option<tangram_futures::task::Task<tg::Result<()>>>,

	pub(super) requests: tangram_futures::task::Task<tg::Result<()>>,
}

/// What remains to be sent after the state is updated for a sandbox that a runner holds.
pub(super) enum Tracked {
	/// The scheduler ended the sandbox as destroyed, so its runner destroys it.
	Destroy(DestroySandbox),

	/// The scheduler's entry stays as it is.
	Ignored,

	/// The scheduler tracks the sandbox on the runner, and the enqueue requests that waited for the acceptance of the entry it replaced fail.
	Inserted { ids: Vec<String> },
}

/// A destroy of a sandbox on a runner. The request is present if the sandbox has a record to finish.
pub(super) struct DestroySandbox {
	pub(super) error: tg::error::Data,
	pub(super) registration_index: u64,
	pub(super) request: Option<EnqueueSandboxRequestArg>,
	pub(super) runner: tg::runner::Id,
	pub(super) sandbox: tg::sandbox::Id,
}

/// The acceptance of an enqueued sandbox, and the enqueue requests that wait for it.
#[derive(Default)]
pub(super) struct Accepting {
	pub(super) accept: Accept,
	pub(super) ids: Vec<String>,
	pub(super) persisted: bool,
}

pub(super) struct Sandbox {
	/// The acceptance of the sandbox's enqueue requests, which is present until it is decided and the sandbox's index write is finished.
	pub(super) accept: Option<Accepting>,

	/// The runners that declined the sandbox, which the scheduler does not offer the sandbox to again until their next heartbeats.
	pub(super) blocked: HashMap<tg::runner::Id, Block, tg::id::BuildHasher>,

	pub(super) boot: u64,

	pub(super) capacity: tg::runner::Capacity,

	/// The dequeue requests that wait for the runner's answer to the sandbox's offer.
	pub(super) dequeue_requests: Vec<String>,

	/// The number of offers of the sandbox that failed.
	pub(super) failures: usize,

	/// The sandbox's links in the queue.
	pub(super) links: Links,

	/// The sandbox that created the sandbox, if the scheduler knows it.
	pub(super) parent: Option<tg::sandbox::Id>,

	/// The sandboxes that the sandbox can borrow capacity from while the scheduler holds it, which are the sandbox that created it and each sandbox whose process waits for its process after a cache hit, with the sandbox's links in each one's queue.
	pub(super) parents: Vec<ParentLinks>,

	pub(super) persisted: bool,

	/// The processes that the scheduler offered into the boot, which are the boot's initial process and the spawns into it, and which are the only processes that the runner's reports of the boot may finish.
	pub(super) processes: BTreeSet<tg::process::Id>,

	/// The sandbox's request, which is absent once a runner holds the sandbox and its control connection is established, and for a sandbox that the scheduler knows only from its runner's report.
	pub(super) request: Option<EnqueueSandboxRequestArg>,

	pub(super) retrying: bool,

	/// The number of this scheduler's writes of the boot, which is zero, and after which the runner's writes follow, beginning with the number that the offer carries.
	pub(super) sequence: u64,

	pub(super) state: State,

	pub(super) unplaceable_since: Option<tangram_server_core::clock::Instant>,
}

/// A queued sandbox's links in the queue of one of its parents.
pub(super) struct ParentLinks {
	pub(super) links: Links,
	pub(super) parent: tg::sandbox::Id,
}

pub(super) enum State {
	/// A runner accepted the sandbox, and the scheduler tracks it until its runner reports that it ended or is removed, so that an enqueue of it or of an older boot is a no-op.
	Accepted { runner: RunnerRef },

	/// The scheduler offered the sandbox to a runner, and sends the same offer again until the runner answers. The sandbox keeps its place in the queue.
	Offered {
		/// Whether the sandbox's control connection is established, which can precede the runner's answer.
		connected: bool,

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

pub(super) struct DiscardedSandbox {
	pub(super) error: tg::Either<tg::error::Data, tg::error::Id>,

	pub(super) request: EnqueueSandboxRequestArg,

	pub(super) sandbox: tg::sandbox::Id,

	/// The write of the failure, if the scheduler held the sandbox and already began it.
	pub(super) write: Option<Write>,
}

impl Sandboxes {
	pub(super) fn new() -> Self {
		Self {
			ended: super::report::Ended::default(),
			entries: HashMap::default(),
			holders: HashMap::default(),
			indexing: false,
			offers: 0,
			written: Vec::new(),
		}
	}
}

impl Sandbox {
	pub(super) fn new(
		boot: u64,
		parent: Option<tg::sandbox::Id>,
		request: Option<EnqueueSandboxRequestArg>,
		state: State,
	) -> Self {
		let processes = request
			.as_ref()
			.and_then(|request| request.process.as_ref())
			.and_then(|process| process.id.clone())
			.into_iter()
			.collect();
		Self {
			accept: None,
			blocked: HashMap::default(),
			boot,
			capacity: tg::runner::Capacity::default(),
			dequeue_requests: Vec::new(),
			failures: 0,
			links: Links::default(),
			parent,
			parents: Vec::new(),
			persisted: false,
			processes,
			request,
			retrying: false,
			sequence: 0,
			state,
			unplaceable_since: None,
		}
	}

	/// Get the request of a sandbox that no runner holds, which always has one.
	pub(super) fn request(&self) -> &EnqueueSandboxRequestArg {
		self.request.as_ref().unwrap()
	}

	pub(super) fn request_mut(&mut self) -> &mut EnqueueSandboxRequestArg {
		self.request.as_mut().unwrap()
	}

	pub(super) fn links_mut(&mut self) -> &mut Links {
		&mut self.links
	}

	/// Get the sandbox's links in the queue of one of its parents.
	pub(super) fn parent_links(&self, parent: &tg::sandbox::Id) -> &Links {
		let links = self
			.parents
			.iter()
			.find(|links| links.parent == *parent)
			.unwrap();
		&links.links
	}

	pub(super) fn parent_links_mut(&mut self, parent: &tg::sandbox::Id) -> &mut Links {
		let links = self
			.parents
			.iter_mut()
			.find(|links| links.parent == *parent)
			.unwrap();
		&mut links.links
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
		if !recovered && self.queue.len() >= scheduler.config.queue_capacity {
			let error = tg::error!(sandbox = %request.sandbox, "the scheduler queue is full");
			let sandbox = request.sandbox;
			self.finish_enqueue(scheduler, ids, &sandbox, Err(error));
			return;
		}

		// Start the next boot of a stopped sandbox with a write that the index applies only while the sandbox is stopped at the boot before it, and hold the boot only once the write applies, so that of several starts only one creates the boot.
		if !recovered && request.boot > 0 {
			let scheduler = scheduler.clone();
			self.operations.push(
				async move {
					let result = scheduler.reserve_sandbox_boot(&request).await;
					Operation::ReserveSandbox {
						ids,
						request,
						result,
					}
				}
				.boxed(),
			);
			return;
		}
		let sandbox = request.sandbox.clone();
		let result = self.enqueue_sandbox(scheduler, request, recovered);

		// Reply to a request that the scheduler declined, which it does not track.
		if let Ok(EnqueueSandboxResponseOutput::Unplaceable) = result {
			for id in ids {
				let output =
					ResponseOutput::EnqueueSandbox(EnqueueSandboxResponseOutput::Unplaceable);
				let response = super::request::Response::new(id, Ok(output));
				scheduler.send_response(self, response);
			}
			return;
		}

		self.finish_enqueue(scheduler, ids, &sandbox, result.map(|_| ()));
	}

	/// Hold the boot that the start's write reserved, whose record and queue entry the write recorded, or reply that the sandbox is no longer stopped at the boot before it. A write that failed did not apply, because the reservation reads the index after a failed write and reports a write that applied as reserved.
	pub(super) fn handle_reserve_sandbox(
		&mut self,
		scheduler: &Scheduler,
		ids: Vec<String>,
		request: EnqueueSandboxRequestArg,
		result: tg::Result<bool>,
	) {
		match result {
			Ok(true) if self.sandboxes.entries.contains_key(&request.sandbox) => {
				let sandbox = request.sandbox;
				self.finish_enqueue(scheduler, ids, &sandbox, Ok(()));
			},
			Ok(true) => {
				let sandbox = request.sandbox.clone();
				let result = self.enqueue_sandbox(scheduler, request, true);
				self.finish_enqueue(scheduler, ids, &sandbox, result.map(|_| ()));
			},
			Ok(false) => {
				for id in ids {
					let output =
						ResponseOutput::EnqueueSandbox(EnqueueSandboxResponseOutput::Superseded);
					let response = super::request::Response::new(id, Ok(output));
					scheduler.send_response(self, response);
				}
			},
			Err(error) => {
				self.send_enqueue_responses(scheduler, ids, Err(error));
			},
		}
	}

	/// Respond to enqueue requests. A request that the scheduling task accepted responds once either its index write is enqueued or a runner accepts the sandbox, whichever is first.
	pub(super) fn finish_enqueue(
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
						let result = scheduler.persist_sandbox(&request, true).await;
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
	pub(super) fn remove_sandbox(
		&mut self,
		scheduler: &Scheduler,
		id: &tg::sandbox::Id,
	) -> Option<Sandbox> {
		let (sandbox, ids) = self.remove_sandbox_inner(id)?;
		if !ids.is_empty() {
			let error =
				tg::error!(sandbox = %id, "the sandbox was removed before a runner accepted it");
			self.send_enqueue_responses(scheduler, ids, Err(error));
		}

		Some(sandbox)
	}

	/// Remove the entry of a sandbox that is not in the queue, and return it with the enqueue requests that still wait for its acceptance. Remember a boot that a runner held as ended, so that the runner's late reports of it are authorized.
	fn remove_sandbox_inner(&mut self, id: &tg::sandbox::Id) -> Option<(Sandbox, Vec<String>)> {
		let mut sandbox = self.sandboxes.entries.remove(id)?;
		if let State::Accepted { runner } = &sandbox.state {
			let ended = super::report::EndedBoot {
				processes: sandbox.processes.clone(),
				runner: runner.id.clone(),
			};
			self.sandboxes.ended.insert(id.clone(), sandbox.boot, ended);
		}
		if let Some(parent) = &sandbox.parent {
			self.remove_child(parent, id);
		}
		let ids = sandbox
			.accept
			.take()
			.map_or_else(Vec::new, |accept| accept.ids);

		Some((sandbox, ids))
	}

	/// End a sandbox that no runner accepted, and write its terminal state. Keep the sandbox's entry until the index has recorded the terminal state, so that an enqueue of the sandbox that the lagging index sends again does not start it. Return the write, which a response that follows it awaits.
	pub(super) fn end_sandbox(
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
				tangram_server_core::checkpoint!(scheduler.server, "scheduler.sandbox.end", sandbox = %request.sandbox, status = %status)
					.await;
				scheduler
					.put_sandbox_status(&request, status, error, None, false)
					.await
			}
			.boxed()
			.shared()
		};

		// Report the write, so that the sandbox's entry is removed once the index has recorded the terminal state.
		let sandbox = id.clone();
		let written = write.clone();
		self.operations.push(
			async move {
				let written = written.await.is_ok();
				Operation::WriteTerminalSandbox {
					boot,
					sandbox,
					written,
				}
			}
			.boxed(),
		);

		write
	}

	/// End a sandbox, and record the failure of its write if no response reports it.
	pub(super) fn end_sandbox_and_log(
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
				Operation::Finished {
					context: "failed to record the ended sandbox",
					result,
				}
			}
			.boxed(),
		);
	}

	/// End a sandbox that this scheduler gives up on, which records the failure in the sandbox's error field. A new sandbox is destroyed, and a sandbox that was starting again is stopped.
	pub(super) fn discard_sandbox(
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

	/// Await indexing for a sandbox that this scheduler ended and whose terminal state is written, and remove the entry of a sandbox whose write failed.
	pub(super) fn handle_write_terminal_sandbox(
		&mut self,
		scheduler: &Scheduler,
		sandbox: tg::sandbox::Id,
		boot: u64,
		written: bool,
	) {
		if !written {
			self.remove_terminal_sandbox(scheduler, &sandbox, boot);
			return;
		}
		self.sandboxes.written.push((sandbox, boot));
		self.index_terminal_sandboxes(scheduler);
	}

	/// Remove the entries of the ended sandboxes that an await of indexing covered, and await indexing for the sandboxes that were ended since.
	pub(super) fn handle_index_terminal_sandboxes(
		&mut self,
		scheduler: &Scheduler,
		sandboxes: Vec<(tg::sandbox::Id, u64)>,
	) {
		self.sandboxes.indexing = false;
		for (sandbox, boot) in sandboxes {
			self.remove_terminal_sandbox(scheduler, &sandbox, boot);
		}
		self.index_terminal_sandboxes(scheduler);
	}

	/// Await indexing once for all of the ended sandboxes whose terminal states are written, unless an await is running, which the next one follows.
	fn index_terminal_sandboxes(&mut self, scheduler: &Scheduler) {
		if self.sandboxes.indexing || self.sandboxes.written.is_empty() {
			return;
		}
		self.sandboxes.indexing = true;
		let sandboxes = std::mem::take(&mut self.sandboxes.written);
		let scheduler = scheduler.clone();
		self.operations.push(
			async move {
				let result = scheduler.index().await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to await the indexing of the ended sandboxes");
				}
				Operation::IndexTerminalSandboxes { sandboxes }
			}
			.boxed(),
		);
	}

	/// Remove the entry of a sandbox that this scheduler ended once the index has recorded its terminal state, unless a later boot replaced it.
	fn remove_terminal_sandbox(
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
	pub(super) fn try_remove_terminal_sandbox(
		&mut self,
		scheduler: &Scheduler,
		sandbox: &tg::sandbox::Id,
	) {
		let remove = self.sandboxes.entries.get(sandbox).is_some_and(|entry| {
			matches!(entry.state, State::Terminal { indexed: true, .. }) && entry.accept.is_none()
		});
		if remove {
			self.remove_sandbox(scheduler, sandbox);
			self.stop_serving_sandbox_holder(sandbox);
		}
	}

	pub(super) fn send_enqueue_responses(
		&mut self,
		scheduler: &Scheduler,
		ids: Vec<String>,
		result: tg::Result<()>,
	) {
		let result =
			result.map(|()| ResponseOutput::EnqueueSandbox(EnqueueSandboxResponseOutput::Enqueued));
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

	pub(super) fn enqueue_sandbox(
		&mut self,
		scheduler: &Scheduler,
		mut request: EnqueueSandboxRequestArg,
		recovered: bool,
	) -> tg::Result<EnqueueSandboxResponseOutput> {
		// Decline a sandbox whose host this scheduler does not accept, before tracking or persisting it.
		if !recovered
			&& let Some(host) = &request.arg.host
			&& !scheduler.config.hosts.contains(host)
		{
			return Ok(EnqueueSandboxResponseOutput::Unplaceable);
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
			self.add_parent(&id, &parent);
		}

		Ok(EnqueueSandboxResponseOutput::Enqueued)
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
					.put_sandbox_status(
						&request,
						tg::sandbox::Status::Destroyed,
						error,
						None,
						false,
					)
					.await;
				Operation::Finished {
					context: "failed to record the destroyed sandbox",
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
		match self.track_accepted_sandbox_inner(id, boot, request, runner) {
			Tracked::Destroy(destroy) => self.send_destroy_sandbox(scheduler, destroy),
			Tracked::Ignored => {},
			Tracked::Inserted { ids } => {
				let error = tg::error!(sandbox = %id, "the sandbox was removed before a runner accepted it");
				self.send_enqueue_responses(scheduler, ids, Err(error));

				// Serve the spawns into the sandbox, which this scheduler sends to its runner.
				self.serve_sandbox_holder(scheduler, id, None);
			},
		}
	}

	/// Update the state for a sandbox that a runner holds, and return what remains to be sent.
	pub(super) fn track_accepted_sandbox_inner(
		&mut self,
		id: &tg::sandbox::Id,
		boot: u64,
		request: Option<EnqueueSandboxRequestArg>,
		runner: RunnerRef,
	) -> Tracked {
		let mut parent = request.as_ref().and_then(|request| request.parent.clone());
		let mut request = request;
		let mut ids = Vec::new();
		let mut processes = BTreeSet::new();
		if let Some(entry) = self.sandboxes.entries.get(id) {
			if entry.boot > boot {
				return Tracked::Ignored;
			}
			match &entry.state {
				// Leave a sandbox that the scheduler offered to a runner to the offer's answer, and a queued boot to its offer, because only a runner's answer to an offer is its yes.
				State::Offered { .. } | State::Queued => return Tracked::Ignored,

				// Leave a boot that the scheduler ended as stopped to its runner, which stops the boots that the index records as stopped when it registers, so that the sandbox's directory stays for its next start.
				State::Terminal { status, .. } if entry.boot == boot && status.is_stopped() => {
					return Tracked::Ignored;
				},

				// Destroy a sandbox on its runner that the scheduler ended as destroyed.
				State::Terminal { error, .. } if entry.boot == boot => {
					let error = error.clone().and_then(tg::Either::left).unwrap_or_default();
					let destroy = DestroySandbox {
						error,
						registration_index: runner.registration_index,
						request: None,
						runner: runner.id,
						sandbox: id.clone(),
					};
					return Tracked::Destroy(destroy);
				},

				// Replace an older boot, and keep what the scheduler knows about the boot if the report does not carry it.
				State::Accepted { .. } | State::Terminal { .. } => {
					let (previous, waiting) = self.remove_sandbox_inner(id).unwrap();
					ids = waiting;
					parent = parent.or(previous.parent);
					if previous.boot == boot {
						request = request.or(previous.request);
						processes = previous.processes;
					}
				},
			}
		}
		if let Some(parent) = &parent {
			self.insert_child(parent, id, &runner.id);
		}
		let mut sandbox = Sandbox::new(boot, parent, request, State::Accepted { runner });
		sandbox.processes.extend(processes);
		self.sandboxes.entries.insert(id.clone(), sandbox);

		Tracked::Inserted { ids }
	}

	/// Add a parent to a sandbox that the scheduler holds, so that the sandbox can borrow the capacity of a sandbox whose process waits for its process after a cache hit, and offer a queued sandbox if the parent already lends its capacity. Reply with the sandbox's capacity, or with none if the scheduler does not hold the sandbox.
	pub(super) fn add_sandbox_parent(
		&mut self,
		scheduler: &Scheduler,
		request: &AddSandboxParentRequestArg,
	) -> AddSandboxParentResponseOutput {
		let output = self.add_sandbox_parent_inner(request);

		// Offer the sandbox if the parent already lends its capacity.
		let queued = self
			.sandboxes
			.entries
			.get(&request.sandbox)
			.is_some_and(|entry| matches!(entry.state, State::Queued));
		if output.capacity.is_some()
			&& queued
			&& self.sandboxes.offers < scheduler.config.max_offers
		{
			self.try_offer_sandbox(scheduler, &request.sandbox);
		}

		output
	}

	/// Add a parent to a sandbox that the scheduler holds as queued or offered, unless the sandbox has it already. Reply with the sandbox's capacity, or with none if the scheduler does not hold the sandbox.
	pub(super) fn add_sandbox_parent_inner(
		&mut self,
		request: &AddSandboxParentRequestArg,
	) -> AddSandboxParentResponseOutput {
		let AddSandboxParentRequestArg { parent, sandbox } = request;
		let entry = self.sandboxes.entries.get(sandbox).filter(|entry| {
			matches!(entry.state, State::Offered { .. } | State::Queued) && sandbox != parent
		});
		let Some(entry) = entry else {
			return AddSandboxParentResponseOutput { capacity: None };
		};
		let capacity = entry.capacity;
		if !entry.parents.iter().any(|links| links.parent == *parent) {
			self.add_parent(sandbox, parent);
		}

		AddSandboxParentResponseOutput {
			capacity: Some(capacity),
		}
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

	/// Reply to dequeue requests.
	pub(super) fn send_dequeue_responses(
		&mut self,
		scheduler: &Scheduler,
		requests: Vec<String>,
		dequeued: bool,
	) {
		for request in requests {
			let output = DequeueSandboxResponseOutput { dequeued };
			scheduler.send_dequeue_sandbox_response(self, request, output);
		}
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
}
