use {
	super::{
		BorrowableCapacityNotification, Config, DequeueSandboxRequestArg,
		DequeueSandboxResponseOutput, EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput,
		HeartbeatNotification, Operation, OwnerAncestors, PendingEnqueue, ResponseOutput,
		Scheduler, State,
		runner::{Reservation, ReservationSource, ReservationState, Runner},
	},
	futures::FutureExt as _,
	std::{
		collections::{HashMap, HashSet},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
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
	loading: usize,
}

struct Parent {
	borrowable: Option<Borrowable>,
	queue: Queue,
}

struct Borrowable {
	capacity: tg::runner::Capacity,
	connection_index: Option<u64>,
	runner: tg::runner::Id,
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
	request: EnqueueSandboxRequestArg,
	state: SandboxState,
}

enum SandboxState {
	Creating { placement: Placement },
	Pending,
	Placing,
	Uncertain { placement: Placement },
}

struct Block {
	connection_index: u64,
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
	connection_index: u64,
	id: tg::runner::Id,
}

pub(super) struct Completion {
	placement: Placement,
	result: tg::Result<tg::Result<bool>>,
	sandbox: tg::sandbox::Id,
}

pub(super) struct CreateSandboxCompletionOutput {
	pub dequeue_completions: Vec<DequeueCompletion>,
	pub discarded: Option<DiscardedSandbox>,
}

pub(super) struct DequeueCompletion {
	pub output: DequeueSandboxResponseOutput,
	pub request: String,
}

pub(super) struct DiscardedSandbox {
	pub error: tg::Either<tg::error::Data, tg::error::Id>,
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
			loading: 0,
		}
	}
}

impl State {
	pub(super) fn new() -> Self {
		Self {
			operations: futures::stream::FuturesUnordered::new(),
			owner_ancestor_requests: HashMap::default(),
			owner_ancestors: HashMap::default(),
			parents: Parents::new(),
			pending_enqueues: HashMap::default(),
			queue: Queue::new(),
			requests: super::Requests {
				inbox: std::collections::HashSet::new(),
				outbox: HashMap::new(),
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
		id: String,
		request: EnqueueSandboxRequestArg,
	) {
		if self.sandboxes.entries.contains_key(&request.sandbox) {
			let output = EnqueueSandboxResponseOutput { enqueued: true };
			let response = scheduler.response(id, Ok(ResponseOutput::EnqueueSandbox(output)));
			scheduler.send_response(self, response);
			return;
		}
		if let Some(pending) = self.pending_enqueues.get_mut(&request.sandbox) {
			pending.ids.push(id);
			return;
		}
		if self.queue.len() + self.sandboxes.loading
			>= scheduler.config.create_sandbox_queue_capacity
		{
			let output = EnqueueSandboxResponseOutput { enqueued: false };
			let response = scheduler.response(id, Ok(ResponseOutput::EnqueueSandbox(output)));
			scheduler.send_response(self, response);
			return;
		}
		let owner = request.arg.owner.as_ref().and_then(tg::Principal::to_id);
		let Some(owner) = owner else {
			let ancestors = Arc::new(HashSet::default());
			let result = self.enqueue_sandbox(scheduler, request, ancestors);
			let result = result.map(ResponseOutput::EnqueueSandbox);
			let response = scheduler.response(id, result);
			scheduler.send_response(self, response);
			return;
		};
		if let Some(ancestors) = self.owner_ancestors.get(&owner).cloned() {
			let result = self.enqueue_sandbox(scheduler, request, ancestors);
			let result = result.map(ResponseOutput::EnqueueSandbox);
			let response = scheduler.response(id, result);
			scheduler.send_response(self, response);
			return;
		}
		self.sandboxes.loading += 1;
		let sandbox = request.sandbox.clone();
		let pending = PendingEnqueue {
			ids: vec![id],
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
			let result = match &result {
				Ok(ancestors) => {
					self.enqueue_sandbox(scheduler, pending.request, ancestors.clone())
				},
				Err(error) => Err(error.clone()),
			};
			let result = result.map(ResponseOutput::EnqueueSandbox);
			for id in pending.ids {
				let response = scheduler.response(id, result.clone());
				scheduler.send_response(self, response);
			}
		}
	}

	fn enqueue_sandbox(
		&mut self,
		scheduler: &Scheduler,
		request: EnqueueSandboxRequestArg,
		owner_ancestors: OwnerAncestors,
	) -> tg::Result<EnqueueSandboxResponseOutput> {
		if self.sandboxes.entries.contains_key(&request.sandbox) {
			return Ok(EnqueueSandboxResponseOutput { enqueued: true });
		}
		let capacity = tg::runner::Capacity {
			cpus: request
				.arg
				.cpu
				.unwrap_or(scheduler.config.default_capacity.cpus),
			memory: request
				.arg
				.memory
				.unwrap_or(scheduler.config.default_capacity.memory),
		};
		if capacity.cpus == 0 {
			return Err(tg::error!("the sandbox CPU must be greater than zero"));
		}
		if capacity.memory == 0 {
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
			request,
			state: SandboxState::Pending,
		};
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

		Ok(EnqueueSandboxResponseOutput { enqueued: true })
	}

	pub(super) fn handle_borrowable_capacity(
		&mut self,
		scheduler: &Scheduler,
		notification: BorrowableCapacityNotification,
	) {
		let connection_index = self
			.runners
			.entries
			.get(&notification.runner)
			.map(|runner| runner.connection_index);
		let runner = notification.runner;
		let parent_id = notification.parent;
		let parent = self
			.parents
			.0
			.entry(parent_id.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				queue: Queue::new(),
			});
		let previous = parent.borrowable.replace(Borrowable {
			capacity: notification.capacity,
			connection_index,
			runner: runner.clone(),
		});
		if let Some(previous) = previous
			&& let Some(runner) = self.runners.entries.get_mut(&previous.runner)
		{
			runner.borrowable.remove(&parent_id);
		}
		if let Some(runner) = self.runners.entries.get_mut(&runner) {
			runner.borrowable.insert(parent_id.clone());
		}
		let sandbox = parent.queue.head().cloned();
		if let Some(sandbox) = sandbox
			&& self.sandboxes.attempts < scheduler.config.max_create_sandbox_requests
		{
			self.try_schedule(scheduler, &sandbox);
		}
		self.queue.wake();
	}

	pub(super) fn handle_heartbeat(
		&mut self,
		_scheduler: &Scheduler,
		notification: &HeartbeatNotification,
	) -> bool {
		let Some(runner) = self.runners.entries.get_mut(&notification.runner) else {
			return false;
		};
		if runner.connection_index != notification.connection_index
			|| runner.heartbeat_index >= notification.heartbeat_index
		{
			return false;
		}
		runner.capacity = notification.capacity;
		runner.committed = tg::runner::Capacity::default();
		runner.heartbeat_at = tokio::time::Instant::now();
		runner.heartbeat_index = notification.heartbeat_index;
		runner.ready = true;
		self.queue.wake();

		true
	}

	pub(super) fn dequeue_sandbox(
		&mut self,
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
				self.sandboxes.entries.remove(&id);
				Some(DequeueSandboxResponseOutput { dequeued: true })
			},
			SandboxState::Placing => {
				self.sandboxes.entries.remove(&id);
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
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		};
		let SandboxState::Creating { placement: current } = &sandbox.state else {
			return CreateSandboxCompletionOutput {
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		};
		if current != &placement {
			return CreateSandboxCompletionOutput {
				dequeue_completions: Vec::new(),
				discarded: None,
			};
		}
		let dequeue_requests = sandbox.dequeue_requests.clone();
		let runner_ref = placement.runner();
		let runner_current = self
			.runners
			.entries
			.get(&runner_ref.id)
			.is_some_and(|runner| runner.connection_index == runner_ref.connection_index);
		let failure = if let Ok(Err(error)) = &result {
			Some(error.clone())
		} else {
			None
		};
		let failed = failure.is_some();
		if let Ok(Err(error)) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "the runner failed to create the sandbox");
		}

		let mut discarded = None;
		let output = match result {
			Ok(Ok(true)) if runner_current => {
				self.remove_reservation(&id, &placement, true);
				self.sandboxes.entries.remove(&id);
				DequeueSandboxResponseOutput { dequeued: false }
			},
			Ok(Ok(false) | Err(_)) => {
				self.remove_reservation(&id, &placement, false);
				let sandbox = self.sandboxes.entries.get_mut(&id).unwrap();
				if failed {
					sandbox.attempts += 1;
				}
				if !dequeue_requests.is_empty() {
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
					discarded = Some(DiscardedSandbox {
						error: error.to_data_or_id(),
						sandbox: id.clone(),
					});
					self.sandboxes.entries.remove(&id);
				} else {
					if matches!(placement, Placement::Regular { .. })
						&& let Some(runner) = self.runners.entries.get(&runner_ref.id)
					{
						self.sandboxes.entries.get_mut(&id).unwrap().blocked.insert(
							runner_ref.id.clone(),
							Block {
								connection_index: runner.connection_index,
								heartbeat_index: runner.heartbeat_index,
							},
						);
					}
					self.requeue_sandbox(&id);
				}
				DequeueSandboxResponseOutput { dequeued: true }
			},
			Err(error) if runner_current => {
				tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_ref.id, "failed to create the sandbox on the runner");
				if dequeue_requests.is_empty() {
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
				if dequeue_requests.is_empty() {
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
			dequeue_completions,
			discarded,
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
					&& borrowable.connection_index == Some(runner.connection_index)
			});
			if remove {
				parent.borrowable.as_mut().unwrap().connection_index = None;
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

	pub(super) fn schedule_one(&mut self, scheduler: &Scheduler) {
		let Some(sandbox) = self.queue.next() else {
			return;
		};
		self.try_schedule(scheduler, &sandbox);
	}

	pub(super) fn handle_retry_sandbox(&mut self, id: &tg::sandbox::Id) {
		let Some(sandbox) = self.sandboxes.entries.get(id) else {
			return;
		};
		if !matches!(sandbox.state, SandboxState::Placing) {
			return;
		}
		self.requeue_sandbox(id);
	}

	fn find_placement(&self, scheduler: &Scheduler, sandbox: &Sandbox) -> Option<Placement> {
		if let Some(parent) = sandbox.request.parent.as_ref()
			&& let Some(borrowable) = self
				.parents
				.0
				.get(parent)
				.and_then(|parent| parent.borrowable.as_ref())
			&& contains(borrowable.capacity, sandbox.capacity)
			&& let Some(runner) = self.runners.entries.get(&borrowable.runner)
			&& borrowable
				.connection_index
				.is_none_or(|connection_index| connection_index == runner.connection_index)
			&& runner.ready
			&& matches_host(runner, &sandbox.request)
			&& runner.requests < scheduler.config.max_create_sandbox_requests_per_runner
		{
			let runner = RunnerRef {
				connection_index: runner.connection_index,
				id: borrowable.runner.clone(),
			};
			return Some(Placement::Borrowed {
				parent: parent.clone(),
				runner,
			});
		}

		let mut best: Option<(RunnerRef, (u128, u128))> = None;
		for (id, runner) in &self.runners.entries {
			let blocked = sandbox.blocked.get(id).is_some_and(|block| {
				block.connection_index == runner.connection_index
					&& block.heartbeat_index == runner.heartbeat_index
			});
			let owner_matches = runner
				.owner
				.as_ref()
				.is_none_or(|owner| sandbox.owner_ancestors.contains(owner));
			if !owner_matches
				|| !runner.ready
				|| blocked || !matches_host(runner, &sandbox.request)
				|| runner.requests >= scheduler.config.max_create_sandbox_requests_per_runner
			{
				continue;
			}
			let available = available(runner);
			if !contains(available, sandbox.capacity) {
				continue;
			}
			let score = score(runner, available, sandbox.capacity);
			if best.as_ref().is_none_or(|(best_runner, best_score)| {
				score < *best_score || (score == *best_score && id < &best_runner.id)
			}) {
				let runner = RunnerRef {
					connection_index: runner.connection_index,
					id: id.clone(),
				};
				best = Some((runner, score));
			}
		}

		best.map(|(runner, _)| Placement::Regular { runner })
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
		if runner.connection_index != runner_ref.connection_index {
			return;
		}
		runner.requests = runner.requests.saturating_sub(1);
		let Some(reservation) = runner.reservations.remove(id) else {
			return;
		};
		if matches!(reservation.source, ReservationSource::Regular) {
			subtract(&mut runner.reserved, reservation.capacity);
			if commit {
				add(&mut runner.committed, reservation.capacity);
			}
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
			if self
				.runners
				.entries
				.values()
				.any(|runner| placeable(runner, sandbox))
			{
				return false;
			}

			// No runner can satisfy the sandbox, so count the attempt and discard it once they are exhausted.
			self.remove_queued_sandbox(id);
			let sandbox = self.sandboxes.entries.get_mut(id).unwrap();
			sandbox.attempts += 1;
			let attempts = sandbox.attempts;
			if attempts >= scheduler.config.max_create_sandbox_attempts {
				let error = tg::error!(sandbox = %id, %attempts, "no runners available");
				tracing::error!(error = %error.trace(), "giving up on the sandbox");
				self.sandboxes.entries.remove(id);
				let discarded = DiscardedSandbox {
					error: error.to_data_or_id(),
					sandbox: id.clone(),
				};
				scheduler.publish_sandbox_discarded(self, discarded);
				return false;
			}

			// Retry after the grace period in case a runner becomes available.
			sandbox.state = SandboxState::Placing;
			let duration = scheduler.config.create_sandbox_timeout;
			let sandbox = id.clone();
			self.operations.push(
				async move {
					tokio::time::sleep(duration).await;
					Operation::RetrySandbox { sandbox }
				}
				.boxed(),
			);

			return false;
		};
		let capacity = sandbox.capacity;
		let request = sandbox.request.clone();
		let runner_ref = placement.runner().clone();
		self.remove_queued_sandbox(id);
		if let Placement::Borrowed { parent, .. } = &placement
			&& let Some(parent) = self.parents.0.get_mut(parent)
		{
			parent.borrowable = None;
		}
		if let Placement::Borrowed { parent, .. } = &placement {
			self.runners
				.entries
				.get_mut(&runner_ref.id)
				.unwrap()
				.borrowable
				.remove(parent);
			if self.parents.0[parent].queue.len() == 0 {
				self.parents.0.remove(parent);
			}
		}
		let runner = self.runners.entries.get_mut(&runner_ref.id).unwrap();
		runner.requests += 1;
		let source = match &placement {
			Placement::Borrowed { .. } => ReservationSource::Borrowed,
			Placement::Regular { .. } => {
				add(&mut runner.reserved, capacity);
				ReservationSource::Regular
			},
		};
		runner.reservations.insert(
			id.clone(),
			Reservation {
				capacity,
				source,
				state: ReservationState::Pending,
			},
		);
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

impl Placement {
	fn runner(&self) -> &RunnerRef {
		match self {
			Self::Borrowed { runner, .. } | Self::Regular { runner } => runner,
		}
	}
}

async fn create_sandbox(
	session: &crate::Session,
	scheduler: &tg::scheduler::Id,
	runner: &RunnerRef,
	borrowed: bool,
	capacity: tg::runner::Capacity,
	request: &EnqueueSandboxRequestArg,
	timeout: std::time::Duration,
) -> tg::Result<tg::Result<bool>> {
	let arg = tg::runner::control::ServerRequestArg::CreateSandbox(
		tg::runner::control::CreateSandboxServerRequestArg {
			arg: request.arg.clone(),
			borrowed,
			capacity,
			creator: request.creator.clone(),
			parent: request.parent.clone(),
			process: request.process.clone(),
			sandbox: request.sandbox.clone(),
			token: request.token.clone(),
		},
	);
	let options = crate::control::Options {
		retry: tangram_futures::retry::Options::default(),
		timeout,
	};
	let result = session
		.send_runner_control_request(
			&runner.id,
			scheduler,
			runner.connection_index,
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

fn add(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	capacity.cpus = capacity.cpus.saturating_add(value.cpus);
	capacity.memory = capacity.memory.saturating_add(value.memory);
}

fn available(runner: &Runner) -> tg::runner::Capacity {
	let mut available = runner.capacity.available;
	subtract(&mut available, runner.reserved);
	subtract(&mut available, runner.committed);

	available
}

fn contains(capacity: tg::runner::Capacity, requested: tg::runner::Capacity) -> bool {
	capacity.cpus >= requested.cpus && capacity.memory >= requested.memory
}

fn matches_host(runner: &Runner, request: &EnqueueSandboxRequestArg) -> bool {
	request
		.arg
		.host
		.as_ref()
		.is_none_or(|host| host == &runner.host)
}

fn placeable(runner: &Runner, sandbox: &Sandbox) -> bool {
	let owner_matches = runner
		.owner
		.as_ref()
		.is_none_or(|owner| sandbox.owner_ancestors.contains(owner));
	owner_matches
		&& matches_host(runner, &sandbox.request)
		&& contains(runner.capacity.total, sandbox.capacity)
}

fn score(
	runner: &Runner,
	available: tg::runner::Capacity,
	requested: tg::runner::Capacity,
) -> (u128, u128) {
	let cpu = u128::from(available.cpus - requested.cpus) * 1_000_000
		/ u128::from(runner.capacity.total.cpus.max(1));
	let memory = u128::from(available.memory - requested.memory) * 1_000_000
		/ u128::from(runner.capacity.total.memory.max(1));

	(cpu.max(memory), cpu + memory)
}

fn subtract(capacity: &mut tg::runner::Capacity, value: tg::runner::Capacity) {
	capacity.cpus = capacity.cpus.saturating_sub(value.cpus);
	capacity.memory = capacity.memory.saturating_sub(value.memory);
}
