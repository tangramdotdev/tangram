use {
	super::{
		BorrowableCapacityNotification, CancelSandboxNotification, CreateSandboxRequestArg,
		CreateSandboxResponseOutput, HeartbeatNotification, Reservation, ReservationState,
		Response, ResponseOutput, Runner, Scheduler,
	},
	futures::{FutureExt as _, future::BoxFuture, stream::FuturesUnordered},
	std::collections::{HashMap, HashSet, VecDeque},
	tangram_client::prelude::*,
};

pub(super) struct State {
	pub attempts: FuturesUnordered<BoxFuture<'static, Completion>>,
	pub cancellations: FuturesUnordered<BoxFuture<'static, ()>>,
	blocked: HashSet<(tg::sandbox::Id, tg::runner::Id)>,
	borrowable: HashMap<tg::sandbox::Id, Borrowable>,
	cancelled: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,
	next_sequence: u64,
	queue: VecDeque<Sandbox>,
	sandboxes: HashSet<tg::sandbox::Id, tg::id::BuildHasher>,
	uncertain: HashMap<tg::sandbox::Id, Uncertain, tg::id::BuildHasher>,
}

pub(super) struct Completion {
	result: tg::Result<tg::Result<bool>>,
	runner: tg::runner::Id,
	sandbox: Sandbox,
}

#[derive(Clone)]
struct Sandbox {
	capacity: tg::runner::Capacity,
	request: CreateSandboxRequestArg,
	sequence: u64,
}

struct Borrowable {
	capacity: tg::runner::Capacity,
	runner: tg::runner::Id,
}

struct Placement {
	borrowed: Option<tg::sandbox::Id>,
	runner: tg::runner::Id,
}

struct Uncertain {
	runner: tg::runner::Id,
	sandbox: Sandbox,
}

impl State {
	pub fn new() -> Self {
		Self {
			attempts: FuturesUnordered::new(),
			cancellations: FuturesUnordered::new(),
			blocked: HashSet::new(),
			borrowable: HashMap::new(),
			cancelled: HashSet::default(),
			next_sequence: 0,
			queue: VecDeque::new(),
			sandboxes: HashSet::default(),
			uncertain: HashMap::default(),
		}
	}

	pub async fn handle_create_sandbox_request(
		&mut self,
		scheduler: &Scheduler,
		id: String,
		request: CreateSandboxRequestArg,
	) {
		let result = self.enqueue(scheduler, request);
		let (error, output) = match result {
			Ok(output) => (None, Some(ResponseOutput::CreateSandbox(output))),
			Err(error) => {
				let error = Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				});
				(error, None)
			},
		};
		scheduler
			.send_response(Response { error, id, output })
			.await;
	}

	pub fn handle_borrowable_capacity(&mut self, notification: BorrowableCapacityNotification) {
		self.borrowable.insert(
			notification.parent,
			Borrowable {
				capacity: notification.capacity,
				runner: notification.runner,
			},
		);
	}

	pub fn handle_cancel_sandbox(
		&mut self,
		scheduler: &Scheduler,
		notification: CancelSandboxNotification,
	) {
		self.blocked
			.retain(|(sandbox, _)| sandbox != &notification.sandbox);
		if let Some(position) = self
			.queue
			.iter()
			.position(|sandbox| sandbox.request.sandbox == notification.sandbox)
		{
			self.queue.remove(position);
			self.sandboxes.remove(&notification.sandbox);
			return;
		}
		if let Some(uncertain) = self.uncertain.remove(&notification.sandbox) {
			if let Some(mut runner) = scheduler.runners.get_mut(&uncertain.runner)
				&& let Some(reservation) = runner.reservations.get_mut(&notification.sandbox)
			{
				reservation.state = ReservationState::Succeeded;
			}
			self.cancel(scheduler, notification.sandbox.clone());
			self.sandboxes.remove(&notification.sandbox);
			return;
		}
		if self.sandboxes.contains(&notification.sandbox) {
			self.cancelled.insert(notification.sandbox);
		}
	}

	pub fn handle_heartbeat(
		&mut self,
		scheduler: &Scheduler,
		notification: &HeartbeatNotification,
	) {
		if let Some(mut runner) = scheduler.runners.get_mut(&notification.runner) {
			runner.capacity = notification.capacity;
			runner.heartbeat_at = tokio::time::Instant::now();
			runner
				.reservations
				.retain(|_, reservation| !matches!(reservation.state, ReservationState::Succeeded));
		}
		self.blocked
			.retain(|(_, runner)| runner != &notification.runner);
	}

	pub fn handle_remove_runner(&mut self, scheduler: &Scheduler, runner: &tg::runner::Id) {
		self.borrowable
			.retain(|_, borrowable| &borrowable.runner != runner);
		self.blocked
			.retain(|(_, blocked_runner)| blocked_runner != runner);
		let sandboxes = self
			.uncertain
			.iter()
			.filter(|(_, uncertain)| &uncertain.runner == runner)
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		for id in sandboxes {
			let uncertain = self.uncertain.remove(&id).unwrap();
			if self.cancelled.remove(&id) {
				self.sandboxes.remove(&id);
			} else {
				self.requeue(uncertain.sandbox);
			}
		}
		scheduler.runners.remove(runner);
	}

	pub fn handle_completion(&mut self, scheduler: &Scheduler, completion: Completion) {
		let Completion {
			result,
			runner: runner_id,
			sandbox,
		} = completion;
		let id = sandbox.request.sandbox.clone();
		let cancelled = self.cancelled.remove(&id);
		let mut runner = scheduler.runners.get_mut(&runner_id);
		if let Some(runner) = &mut runner {
			runner.requests = runner.requests.saturating_sub(1);
		}

		match result {
			Ok(Ok(true)) => {
				if let Some(runner) = &mut runner
					&& let Some(reservation) = runner.reservations.get_mut(&id)
				{
					reservation.state = ReservationState::Succeeded;
				}
				self.blocked.retain(|(sandbox, _)| sandbox != &id);
				if cancelled {
					self.cancel(scheduler, id.clone());
				}
				self.sandboxes.remove(&id);
			},
			Ok(Ok(false)) => {
				if let Some(runner) = &mut runner {
					runner.reservations.remove(&id);
				}
				if cancelled {
					self.sandboxes.remove(&id);
				} else {
					self.blocked.insert((id, runner_id));
					self.requeue(sandbox);
				}
			},
			Ok(Err(error)) => {
				tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_id, "the runner failed to create the sandbox");
				if let Some(runner) = &mut runner {
					runner.reservations.remove(&id);
				}
				if cancelled {
					self.sandboxes.remove(&id);
				} else {
					self.blocked.insert((id, runner_id));
					self.requeue(sandbox);
				}
			},
			Err(error) => {
				tracing::error!(error = %error.trace(), sandbox = %id, runner = %runner_id, "failed to create the sandbox on the runner");
				if let Some(runner) = &mut runner
					&& let Some(reservation) = runner.reservations.get_mut(&id)
				{
					reservation.state = if cancelled {
						ReservationState::Succeeded
					} else {
						ReservationState::Uncertain
					};
				}
				if cancelled {
					self.cancel(scheduler, id.clone());
					self.sandboxes.remove(&id);
				} else if runner.is_some() {
					self.uncertain.insert(
						id,
						Uncertain {
							runner: runner_id,
							sandbox,
						},
					);
				} else {
					self.requeue(sandbox);
				}
			},
		}
	}

	pub fn schedule(&mut self, scheduler: &Scheduler) {
		while self.attempts.len() < scheduler.config.max_create_sandbox_requests {
			let Some((position, placement)) = self.find_placement(scheduler) else {
				break;
			};
			let sandbox = self.queue.remove(position).unwrap();
			if let Some(parent) = placement.borrowed {
				self.borrowable.remove(&parent);
			}
			let Some(mut runner) = scheduler.runners.get_mut(&placement.runner) else {
				self.requeue(sandbox);
				continue;
			};
			runner.requests += 1;
			runner.reservations.insert(
				sandbox.request.sandbox.clone(),
				Reservation {
					capacity: sandbox.capacity,
					state: ReservationState::Pending,
				},
			);
			drop(runner);

			let runner = placement.runner;
			let session = scheduler.server.session(&scheduler.server.context);
			let timeout = scheduler.config.create_sandbox_timeout;
			self.attempts.push(
				async move {
					let result = create_sandbox(&session, &runner, &sandbox, timeout).await;
					Completion {
						result,
						runner,
						sandbox,
					}
				}
				.boxed(),
			);
		}
	}

	fn enqueue(
		&mut self,
		scheduler: &Scheduler,
		request: CreateSandboxRequestArg,
	) -> tg::Result<CreateSandboxResponseOutput> {
		if self.sandboxes.contains(&request.sandbox) {
			return Ok(CreateSandboxResponseOutput { enqueued: true });
		}
		if self.queue.len() >= scheduler.config.create_sandbox_queue_capacity {
			return Ok(CreateSandboxResponseOutput { enqueued: false });
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
		let sandbox = Sandbox {
			capacity,
			request,
			sequence: self.next_sequence,
		};
		self.next_sequence = self.next_sequence.wrapping_add(1);
		self.sandboxes.insert(sandbox.request.sandbox.clone());
		self.queue.push_back(sandbox);
		Ok(CreateSandboxResponseOutput { enqueued: true })
	}

	fn find_placement(&self, scheduler: &Scheduler) -> Option<(usize, Placement)> {
		for (position, sandbox) in self.queue.iter().enumerate() {
			if let Some(placement) = self.find_borrowed_placement(scheduler, sandbox) {
				return Some((position, placement));
			}
			if let Some(placement) = self.find_runner_placement(scheduler, sandbox) {
				return Some((position, placement));
			}
		}
		None
	}

	fn find_borrowed_placement(
		&self,
		scheduler: &Scheduler,
		sandbox: &Sandbox,
	) -> Option<Placement> {
		let parent = sandbox.request.parent.as_ref()?;
		let borrowable = self.borrowable.get(parent)?;
		if !contains(borrowable.capacity, sandbox.capacity) {
			return None;
		}
		let runner = scheduler.runners.get(&borrowable.runner)?;
		if !matches_host(&runner, &sandbox.request)
			|| runner.requests >= scheduler.config.max_create_sandbox_requests_per_runner
		{
			return None;
		}
		Some(Placement {
			borrowed: Some(parent.clone()),
			runner: borrowable.runner.clone(),
		})
	}

	fn find_runner_placement(&self, scheduler: &Scheduler, sandbox: &Sandbox) -> Option<Placement> {
		let mut best = None;
		for runner in &scheduler.runners {
			if !matches_host(&runner, &sandbox.request)
				|| runner.requests >= scheduler.config.max_create_sandbox_requests_per_runner
				|| self
					.blocked
					.contains(&(sandbox.request.sandbox.clone(), runner.key().clone()))
			{
				continue;
			}
			let available = available(&runner);
			if !contains(available, sandbox.capacity) {
				continue;
			}
			let score = score(&runner, available, sandbox.capacity);
			if best
				.as_ref()
				.is_none_or(|(_, best_score)| score < *best_score)
			{
				best = Some((runner.key().clone(), score));
			}
		}
		best.map(|(runner, _)| Placement {
			borrowed: None,
			runner,
		})
	}

	fn requeue(&mut self, sandbox: Sandbox) {
		let position = self
			.queue
			.iter()
			.position(|queued| queued.sequence > sandbox.sequence)
			.unwrap_or(self.queue.len());
		self.queue.insert(position, sandbox);
	}

	fn cancel(&mut self, scheduler: &Scheduler, sandbox: tg::sandbox::Id) {
		let session = scheduler.server.session(&scheduler.server.context);
		let timeout = scheduler.config.create_sandbox_timeout;
		self.cancellations
			.push(cancel_sandbox(session, sandbox, timeout).boxed());
	}
}

async fn cancel_sandbox(
	session: crate::Session,
	sandbox: tg::sandbox::Id,
	timeout: std::time::Duration,
) {
	let error = tg::error::Data {
		code: Some(tg::error::Code::Cancellation),
		message: Some("the sandbox was canceled".into()),
		..Default::default()
	};
	let request = tg::sandbox::control::ServerRequestArg::Destroy(
		tg::sandbox::control::DestroyServerRequestArg { error: Some(error) },
	);
	let options = crate::control::Options {
		retry: tangram_futures::retry::Options::default(),
		timeout,
	};
	let result = session
		.send_sandbox_control_request(&sandbox, request, options)
		.await;
	if let Err(error) | Ok(Err(error)) = result {
		tracing::error!(error = %error.trace(), %sandbox, "failed to cancel the sandbox");
	}
}

async fn create_sandbox(
	session: &crate::Session,
	runner: &tg::runner::Id,
	sandbox: &Sandbox,
	timeout: std::time::Duration,
) -> tg::Result<tg::Result<bool>> {
	let request = &sandbox.request;
	let arg = tg::runner::control::ServerRequestArg::CreateSandbox(
		tg::runner::control::CreateSandboxServerRequestArg {
			arg: request.arg.clone(),
			capacity: sandbox.capacity,
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
		.send_runner_control_request(runner, arg, options)
		.await
		.map_err(|error| {
			tg::error!(!error, sandbox = %request.sandbox, %runner, "failed to send the create sandbox request to the runner")
		})?;
	let result = result
		.map_err(|error| {
			tg::error!(!error, sandbox = %request.sandbox, %runner, "the runner create sandbox request failed")
		})
		.and_then(|output| {
			output
				.try_unwrap_create_sandbox()
				.map_err(|_| tg::error!("expected a create sandbox response"))
				.map(|output| output.created)
		});
	Ok(result)
}

fn available(runner: &Runner) -> tg::runner::Capacity {
	let mut available = runner.capacity.available;
	for reservation in runner.reservations.values() {
		available.cpus = available.cpus.saturating_sub(reservation.capacity.cpus);
		available.memory = available.memory.saturating_sub(reservation.capacity.memory);
	}
	available
}

fn contains(capacity: tg::runner::Capacity, requested: tg::runner::Capacity) -> bool {
	capacity.cpus >= requested.cpus && capacity.memory >= requested.memory
}

fn matches_host(runner: &Runner, request: &CreateSandboxRequestArg) -> bool {
	request
		.arg
		.host
		.as_ref()
		.is_none_or(|host| host == &runner.host)
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
