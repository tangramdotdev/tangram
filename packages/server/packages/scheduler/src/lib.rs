use {
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{HashMap, HashSet},
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_messenger::Messenger,
	tangram_server_core::{Server, Session, control::accept::Path},
	tangram_server_scheduler_core::{
		AcquireProcessLeaseRequestArg, AcquireProcessLeaseResponseOutput, AddRunnerRequestArg,
		AddRunnerResponseOutput, BorrowableCapacityNotification, CancelSandboxStopRequestArg,
		CancelSandboxStopResponseOutput, DequeueSandboxRequestArg, DequeueSandboxResponseOutput,
		DestroyRunnerSandboxRequestArg, DestroyRunnerSandboxResponseOutput, EndSandboxRequestArg,
		EndSandboxResponseOutput, EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput,
		HeartbeatAcknowledgement, HeartbeatNotification, Notification,
		ReleaseProcessLeaseRequestArg, ReleaseProcessLeaseResponseOutput, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, RequestArg, ResponseOutput, runner_heartbeat_subject,
		scheduler_notification_subject,
	},
};

mod recovery;
mod request;
mod runner;
mod sandbox;

/// The ID of the only scheduler of a single server, which multi-server mode replaces with a configured ID.
const SINGLE_SERVER_ID: &str = "sch_0000000000000000000000000000";

type Operations = FuturesUnordered<BoxFuture<'static, Operation>>;

type NotificationStream =
	BoxStream<'static, Result<tangram_messenger::Message<Notification>, tangram_messenger::Error>>;

type OwnerAncestors = Arc<HashSet<tg::Id, tg::id::BuildHasher>>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) scheduler_cache_task: Mutex<Option<Task<()>>>,
	pub(crate) schedulers: tangram_server_core::service::Cache<request::Kind>,
	pub(crate) started: tokio::sync::watch::Sender<bool>,
	pub(crate) task: Mutex<Option<Task<()>>>,
}

struct Owned {
	available: Arc<AtomicBool>,
	request: Task<tg::Result<()>>,
	task: Task<tg::Result<()>>,
}

#[derive(Clone)]
struct Scheduler {
	component: Component,
	config: Config,
	id: tg::scheduler::Id,
	sender: request::Sender,
	server: Server,
}

struct SchedulerState {
	accepting: HashMap<tg::sandbox::Id, sandbox::Accepting, tg::id::BuildHasher>,
	operations: Operations,
	owner_ancestor_requests: HashMap<tg::Id, Vec<tg::sandbox::Id>, tg::id::BuildHasher>,
	owner_ancestors: HashMap<tg::Id, OwnerAncestors, tg::id::BuildHasher>,
	parents: sandbox::Parents,
	pending_enqueues: HashMap<tg::sandbox::Id, PendingEnqueue, tg::id::BuildHasher>,
	queue: sandbox::Queue,
	recovery: Option<recovery::Recovery>,
	runners: runner::Runners,
	sandboxes: sandbox::Sandboxes,
}

struct PendingEnqueue {
	ids: Vec<String>,
	recovered: bool,
	request: EnqueueSandboxRequestArg,
}

enum Operation {
	AddRunner {
		registration_index: u64,
		id: String,
		result: tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)>,
		runner: tg::runner::Id,
	},
	CreateSandbox(sandbox::Completion),
	DestroyRunnerSandbox {
		id: String,
		result: tg::Result<bool>,
	},
	DestroySandbox {
		destroy: sandbox::DestroySandbox,
		result: tg::Result<bool>,
	},
	EndSandbox {
		ids: Vec<String>,
		result: tg::Result<()>,
	},
	FinishSandboxStop {
		result: tg::Result<()>,
		sandbox: tg::sandbox::Id,
	},
	LoadOwnerAncestors {
		owner: tg::Id,
		result: tg::Result<Vec<tg::Id>>,
	},
	PersistSandbox {
		result: tg::Result<()>,
		sandbox: tg::sandbox::Id,
	},
	PlacedSandboxes {
		generation: u64,
		keys: Vec<tg::sandbox::Id>,
		lost: Vec<EnqueueSandboxRequestArg>,
	},
	Publish {
		context: &'static str,
		result: tg::Result<()>,
	},
	Recovered {
		statuses: recovery::Statuses,
	},
	RecoveryExpired,
	RemoveRunner {
		id: Option<String>,
		result: tg::Result<RemoveRunnerResponseOutput>,
	},
	Respond {
		id: String,
		result: tg::Result<ResponseOutput>,
	},
	RetrySandbox {
		sandbox: tg::sandbox::Id,
	},
}

enum Event {
	CleanerTick,
	Notification(Notification),
	Operation(Operation),
	Request(request::Request),
	Schedule,
}

#[allow(dead_code)]
#[derive(Clone)]
struct Config {
	create_sandbox_queue_capacity: usize,
	create_sandbox_timeout: Duration,
	default_capacity: tg::runner::Capacity,
	max_create_sandbox_attempts: usize,
	max_create_sandbox_requests: usize,
	max_create_sandbox_requests_per_runner: usize,
	runner_ttl: Duration,
	unplaceable_timeout: Option<Duration>,
}

impl Component {
	#[must_use]
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> Self {
		// Create the started signal, which is already set when this server runs no scheduler task.
		let started = !arg
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Scheduler);
		let (started, _) = tokio::sync::watch::channel(started);

		// Create the task handles and the scheduler cache.
		let scheduler_cache_task = Mutex::new(None);
		let schedulers = tangram_server_core::service::Cache::default();
		let task = Mutex::new(None);

		// Create the state.
		let state = State {
			scheduler_cache_task,
			schedulers,
			started,
			task,
		};

		Self(Arc::new(state))
	}

	fn spawn_scheduler_task(&self, server: &Server) -> Option<Task<()>> {
		if !server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Scheduler)
		{
			return None;
		}
		let config = server.config.scheduler.clone();
		let component = self.clone();
		let server = server.clone();
		let task = Task::spawn(|stopper| async move {
			component.scheduler_task(&server, &config, stopper).await;
		});

		Some(task)
	}

	fn spawn_scheduler_cache_task(&self, server: &Server) -> Task<()> {
		let poll_interval = server.config.scheduler.cache.poll_interval;
		let component = self.clone();
		let server = server.clone();
		Task::spawn(move |stopper| async move {
			component
				.schedulers
				.task(&server, poll_interval, stopper)
				.await;
		})
	}

	fn scheduler_id(
		config: &tangram_server_core::config::Scheduler,
	) -> tg::Result<tg::scheduler::Id> {
		let id = match &config.id {
			Some(id) => id.clone(),
			None => SINGLE_SERVER_ID
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the scheduler ID"))?,
		};

		Ok(id)
	}

	async fn register_scheduler(server: &Server) -> tg::Result<()> {
		if !server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Scheduler)
		{
			return Ok(());
		}
		let id = Self::scheduler_id(&server.config.scheduler)?;
		let scheduler = tangram_index::scheduler::Scheduler::new(id);
		let arg = tangram_index::scheduler::put::Arg { scheduler };
		server
			.index
			.put_scheduler(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to register the scheduler"))?;

		Ok(())
	}

	pub(crate) async fn started(&self) {
		let mut receiver = self.started.subscribe();
		receiver.wait_for(|started| *started).await.ok();
	}

	/// Ask a stopped sandbox's runner to destroy it, which removes its directory, returning whether the runner destroyed it.
	pub(crate) async fn destroy_stopped_runner_sandbox(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		sandbox: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> tg::Result<bool> {
		let request = DestroyRunnerSandboxRequestArg {
			error,
			runner: runner.clone(),
			sandbox: sandbox.clone(),
		};
		let (_, response) = self
			.send_scheduler_request(session, RequestArg::DestroyRunnerSandbox(request))
			.boxed()
			.await?;
		let output = response?
			.try_unwrap_destroy_runner_sandbox()
			.map_err(|_| tg::error!("expected a destroy runner sandbox response"))?;
		Ok(output.destroyed)
	}

	pub(crate) async fn dequeue_sandbox(
		&self,
		session: &Session,
		sandbox: &tg::sandbox::Id,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<bool> {
		let request = RequestArg::DequeueSandbox(DequeueSandboxRequestArg {
			sandbox: sandbox.clone(),
		});
		let response = self
			.send_scheduler_request_to(session, scheduler, request)
			.await
			.map_err(
				|source| tg::error!(!source, %sandbox, %scheduler, "failed to send the dequeue sandbox request"),
			)?
			.map_err(
				|source| tg::error!(!source, %sandbox, %scheduler, "the dequeue sandbox request failed"),
			)?;
		let output = response
			.try_unwrap_dequeue_sandbox()
			.map_err(|_| tg::error!("expected a dequeue sandbox response"))?;

		Ok(output.dequeued)
	}

	/// Acquire a lease of a queued process from its scheduler, which returns `None` if the process's request was sent to a runner.
	pub(crate) async fn acquire_process_lease(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::process::Lease>> {
		let arg = AcquireProcessLeaseRequestArg {
			sandbox: sandbox.clone(),
		};
		let output = self
			.send_scheduler_request_to(session, scheduler, RequestArg::AcquireProcessLease(arg))
			.boxed()
			.await??
			.try_unwrap_acquire_process_lease()
			.map_err(|_| tg::error!("expected an acquire process lease response"))?;

		Ok(output.lease)
	}

	/// Release a lease of a queued process at its scheduler, which returns `None` if the process's request was sent to a runner.
	pub(crate) async fn release_process_lease(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		sandbox: &tg::sandbox::Id,
		lease: tg::process::Lease,
	) -> tg::Result<Option<bool>> {
		let arg = ReleaseProcessLeaseRequestArg {
			lease,
			sandbox: sandbox.clone(),
		};
		let output = self
			.send_scheduler_request_to(session, scheduler, RequestArg::ReleaseProcessLease(arg))
			.boxed()
			.await??
			.try_unwrap_release_process_lease()
			.map_err(|_| tg::error!("expected a release process lease response"))?;

		Ok(output.released)
	}

	/// Ask the scheduler that owns a sandbox to cancel a stop that it holds, returning whether the scheduler holds the sandbox.
	pub(crate) async fn cancel_sandbox_stop(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<bool> {
		let arg = CancelSandboxStopRequestArg {
			sandbox: sandbox.clone(),
		};
		let output = self
			.send_scheduler_request_to(session, scheduler, RequestArg::CancelSandboxStop(arg))
			.boxed()
			.await??
			.try_unwrap_cancel_sandbox_stop()
			.map_err(|_| tg::error!("expected a cancel sandbox stop response"))?;

		Ok(output.held)
	}

	/// Ask the scheduler that owns a sandbox to stop or destroy it before its runner connects, returning whether the scheduler owns it.
	pub(crate) async fn end_sandbox(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		arg: EndSandboxRequestArg,
	) -> tg::Result<bool> {
		let output = self
			.send_scheduler_request_to(session, scheduler, RequestArg::EndSandbox(arg))
			.boxed()
			.await??
			.try_unwrap_end_sandbox()
			.map_err(|_| tg::error!("expected an end sandbox response"))?;

		Ok(output.ended)
	}

	#[tracing::instrument(name = "sandbox.enqueue", level = "debug", skip_all, fields(sandbox = %request.sandbox), err(level = "debug"))]
	pub(crate) async fn enqueue_sandbox(
		&self,
		session: &Session,
		request: EnqueueSandboxRequestArg,
	) -> tg::Result<tg::scheduler::Id> {
		tracing::info!(sandbox = %request.sandbox, "enqueueing the sandbox");
		let sandbox = request.sandbox.clone();

		// Send a sandbox with a parent to its parent's scheduler, and any other sandbox to any scheduler.
		let target = if let Some(parent) = &request.parent {
			let scheduler = request.scheduler.clone().ok_or_else(
				|| tg::error!(%parent, "missing the scheduler for the parent sandbox"),
			)?;
			Some(scheduler)
		} else {
			None
		};
		let arg = RequestArg::EnqueueSandbox(request);
		let (scheduler, response) = match target {
			Some(scheduler) => {
				let response = self
					.send_scheduler_request_to(session, &scheduler, arg)
					.boxed()
					.await?;
				(scheduler, response)
			},
			None => self.send_scheduler_request(session, arg).boxed().await?,
		};
		response?
			.try_unwrap_enqueue_sandbox()
			.map_err(|_| tg::error!("expected an enqueue sandbox response"))?;
		tracing::info!(%sandbox, %scheduler, "enqueued the sandbox");
		tangram_server_core::checkpoint!(session.server, "scheduler.sandbox.enqueued", sandbox = %sandbox)
			.await;

		Ok(scheduler)
	}

	pub(crate) async fn scheduler_task(
		&self,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
		stopper: Stopper,
	) {
		loop {
			let result = self.scheduler_task_inner(server, config, &stopper).await;
			let Err(error) = result else {
				break;
			};
			tracing::error!(error = %error.trace(), "the scheduler task failed");
			tokio::select! {
				() = stopper.wait() => break,
				() = tokio::time::sleep(config.restart_delay) => {},
			}
		}
	}

	async fn scheduler_task_inner(
		&self,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
		stopper: &Stopper,
	) -> tg::Result<()> {
		// Start the scheduler.
		let id = Self::scheduler_id(config)?;
		let Owned {
			available,
			request,
			task,
		} = Scheduler::start(self, server, config, id).await?;

		// Report that the scheduler is subscribed to its requests, which wait for the recovery.
		self.started.send_replace(true);
		if let Err(error) = self.schedulers.refresh(server).await {
			tracing::error!(error = %error.trace(), "failed to refresh the scheduler cache");
		}

		// Wait for the scheduler to fail or for the stop.
		let result = tokio::select! {
			result = task.wait() => Err(Self::scheduler_error(result)),
			result = request.wait() => Err(Self::scheduler_error(result)),
			() = stopper.wait() => Ok(()),
		};

		// Stop accepting requests, so that callers fail over or retry until the scheduler restarts.
		available.store(false, Ordering::SeqCst);

		result
	}

	fn scheduler_error(result: Result<tg::Result<()>, tokio::task::JoinError>) -> tg::Error {
		match result {
			Ok(Ok(())) => tg::error!("the scheduler stopped"),
			Ok(Err(error)) => error,
			Err(error) => tg::error!(!error, "the scheduler task panicked"),
		}
	}
}

impl Scheduler {
	async fn start(
		component: &Component,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
		id: tg::scheduler::Id,
	) -> tg::Result<Owned> {
		// Create the config.
		let request_config = config.request.clone();
		let config = Config {
			create_sandbox_queue_capacity: config.create_sandbox_queue_capacity,
			create_sandbox_timeout: config.create_sandbox_timeout,
			default_capacity: config.default_capacity(),
			max_create_sandbox_attempts: config.max_create_sandbox_attempts,
			max_create_sandbox_requests: config.max_create_sandbox_requests,
			max_create_sandbox_requests_per_runner: config.max_create_sandbox_requests_per_runner,
			runner_ttl: config.runner_ttl,
			unplaceable_timeout: config.unplaceable_timeout,
		};

		// Subscribe to the scheduler's requests and notifications before starting, so that no message sent after the start is lost.
		let endpoint = tangram_server_core::service::Endpoint::<request::Kind>::subscribe(
			server,
			&id,
			request_config.channel_capacity,
			request_config.response_ttl,
		)
		.await?;
		let notifications = server
			.messenger
			.subscribe::<Notification>(scheduler_notification_subject(&id))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the scheduler notifications"
				)
			})?
			.boxed();

		// Create the scheduler.
		let scheduler = Self {
			component: component.clone(),
			config,
			id,
			sender: endpoint.sender(),
			server: server.clone(),
		};

		// Spawn the request task, which sends the accepted requests to the scheduling task through a bounded channel.
		let available = Arc::new(AtomicBool::new(true));
		let (request_sender, request_receiver) =
			tokio::sync::mpsc::channel(request_config.channel_capacity);
		let request = Task::spawn({
			let handler = request::Handler {
				available: available.clone(),
				sender: request_sender,
			};
			let server = server.clone();
			move |stopper| async move {
				endpoint
					.run(&server, handler, request_config.concurrency, stopper)
					.await
			}
		});

		// Spawn the scheduling task.
		let task = Task::spawn(move |_| async move {
			scheduler
				.message_handler_task(notifications, request_receiver)
				.await
		});

		let owned = Owned {
			available,
			request,
			task,
		};

		Ok(owned)
	}

	async fn message_handler_task(
		&self,
		mut notifications: NotificationStream,
		mut requests: tokio::sync::mpsc::Receiver<request::Request>,
	) -> tg::Result<()> {
		let mut state = SchedulerState::new();

		// Recover the accepted requests before handling messages.
		self.recover(&mut state).await?;

		let mut cleaner_interval = self.server.clock.interval(self.config.runner_ttl);
		cleaner_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			let event = self
				.next_event(
					&mut state,
					&mut notifications,
					&mut requests,
					&mut cleaner_interval,
				)
				.await?;
			match event {
				Event::CleanerTick => {
					self.handle_cleaner_tick(&mut state);
				},
				Event::Notification(notification) => {
					self.handle_notification(&mut state, notification);
				},
				Event::Operation(operation) => {
					self.handle_operation(&mut state, operation);
				},
				Event::Request(request) => {
					self.handle_request(&mut state, request);
				},
				Event::Schedule => {
					state.schedule_one(self);
				},
			}
		}
	}

	async fn next_event(
		&self,
		state: &mut SchedulerState,
		notifications: &mut NotificationStream,
		requests: &mut tokio::sync::mpsc::Receiver<request::Request>,
		cleaner_interval: &mut tangram_server_core::clock::Interval,
	) -> tg::Result<Event> {
		let event = tokio::select! {
			_ = cleaner_interval.tick() => Event::CleanerTick,
			result = Self::receive_notification(notifications) => Event::Notification(result?),
			request = requests.recv() => Event::Request(request.ok_or_else(|| tg::error!("the scheduler request task stopped"))?),
			operation = state.operations.next(), if !state.operations.is_empty() => Event::Operation(operation.unwrap()),
			() = tokio::task::yield_now(), if state.can_schedule(self) => Event::Schedule,
		};

		Ok(event)
	}

	async fn receive_notification(
		notifications: &mut NotificationStream,
	) -> tg::Result<Notification> {
		let message = notifications
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to receive a scheduler notification"))?
			.ok_or_else(|| tg::error!("the scheduler notification stream ended"))?;

		Ok(message.payload)
	}

	fn handle_notification(&self, state: &mut SchedulerState, notification: Notification) {
		match notification {
			Notification::BorrowableCapacity(notification) => {
				state.handle_borrowable_capacity(self, notification);
			},
			Notification::Heartbeat(notification) => {
				if state.handle_heartbeat(&notification, self.server.clock.instant()) {
					self.publish_runner_heartbeat_ack(state, &notification);
				}
			},
		}
	}

	fn handle_request(&self, state: &mut SchedulerState, request: request::Request) {
		let id = request.id;
		match request.arg {
			RequestArg::AcquireProcessLease(request) => {
				state.handle_acquire_process_lease_request(self, id, &request);
			},
			RequestArg::AddRunner(request) => {
				state.handle_add_runner_request(self, id, request);
			},
			RequestArg::DequeueSandbox(request) => {
				if let Some(output) = state.dequeue_sandbox(self, id.clone(), request) {
					self.send_dequeue_sandbox_response(state, id, output);
				}
			},
			RequestArg::DestroyRunnerSandbox(request) => {
				state.handle_destroy_runner_sandbox_request(self, id, request);
			},
			RequestArg::CancelSandboxStop(request) => {
				state.handle_cancel_sandbox_stop_request(self, id, &request);
			},
			RequestArg::EndSandbox(request) => {
				state.handle_end_sandbox_request(self, id, request);
			},
			RequestArg::EnqueueSandbox(request) => {
				state.handle_enqueue_sandbox_request(self, Some(id), request, false);
			},
			RequestArg::ReleaseProcessLease(request) => {
				state.handle_release_process_lease_request(self, id, request);
			},
			RequestArg::RemoveRunner(request) => {
				state.handle_remove_runner_request(self, Some(id), request);
			},
		}
	}

	fn handle_operation(&self, state: &mut SchedulerState, operation: Operation) {
		match operation {
			Operation::AddRunner {
				registration_index,
				id,
				result,
				runner,
			} => {
				let current = state
					.runners
					.entries
					.get(&runner)
					.is_some_and(|runner| runner.registration_index == registration_index);
				let result = match result {
					Ok((output, owner)) => {
						if current {
							state.runners.entries.get_mut(&runner).unwrap().owner = owner;
						}
						Ok(output)
					},
					Err(error) => {
						if current {
							let completions = state.remove_runner(self, &runner);
							self.send_dequeue_sandbox_completions(state, completions);
						}
						Err(error)
					},
				};
				let result = result.map(ResponseOutput::AddRunner);
				let response = request::Response::new(id, result);
				self.send_response(state, response);
			},
			Operation::CreateSandbox(completion) => {
				let output = state.handle_create_sandbox_completion(self, completion);
				self.send_dequeue_sandbox_completions(state, output.dequeue_completions);
				if let Some(discarded) = output.discarded {
					self.publish_sandbox_discarded(state, discarded);
				}
				if let Some(sandbox) = output.accepted {
					state.finish_accept(self, &sandbox, Path::Handled, Ok(()));
				}
				if let Some(held) = output.held {
					state.finish_held_end(self, held);
				}
			},
			Operation::DestroyRunnerSandbox { id, result } => {
				let result = result.map(|destroyed| {
					ResponseOutput::DestroyRunnerSandbox(DestroyRunnerSandboxResponseOutput {
						destroyed,
					})
				});
				let response = request::Response::new(id, result);
				self.send_response(state, response);
			},
			Operation::DestroySandbox { destroy, result } => {
				state.handle_destroy_sandbox(self, destroy, result);
			},
			Operation::EndSandbox { ids, result } => {
				for id in ids {
					let result = result.clone().map(|()| {
						ResponseOutput::EndSandbox(EndSandboxResponseOutput { ended: true })
					});
					let response = request::Response::new(id, result);
					self.send_response(state, response);
				}
			},
			Operation::LoadOwnerAncestors { owner, result } => {
				state.handle_load_owner_ancestors_completion(self, owner, result);
			},
			Operation::PersistSandbox { result, sandbox } => {
				state.handle_persist_sandbox_completion(self, &sandbox, result);
			},
			Operation::PlacedSandboxes {
				generation,
				keys,
				lost,
			} => {
				state.handle_placed_sandboxes(self, generation, keys, lost);
			},
			Operation::FinishSandboxStop { result, sandbox } => {
				state.sandboxes.finish_stop(&sandbox);
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), %sandbox, "failed to stop the sandbox once it started");
				}
			},
			Operation::Publish { context, result } => {
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "{context}");
				}
			},
			Operation::Recovered { statuses } => {
				state.handle_recovered(self, statuses);
			},
			Operation::RecoveryExpired => {
				state.handle_recovery_expired(self);
			},
			Operation::RemoveRunner { id, result } => {
				if let Some(id) = id {
					let result = result.map(ResponseOutput::RemoveRunner);
					let response = request::Response::new(id, result);
					self.send_response(state, response);
				} else if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to remove the expired runner");
				}
			},
			Operation::Respond { id, result } => {
				let response = request::Response::new(id, result);
				self.send_response(state, response);
			},
			Operation::RetrySandbox { sandbox } => {
				state.handle_retry_sandbox(&sandbox);
			},
		}
	}

	fn handle_cleaner_tick(&self, state: &mut SchedulerState) {
		state.prune_placed_sandboxes(self);
		let now = self.server.clock.instant();
		let expired = state.runners.expired(now, self.config.runner_ttl);
		for (runner, registration_index) in expired {
			tracing::warn!(%runner, registration_index, ttl_ms = self.config.runner_ttl.as_millis(), "the runner heartbeat expired");
			let request = RemoveRunnerRequestArg {
				registration_index,
				runner,
			};
			state.handle_remove_runner_request(self, None, request);
		}
	}

	fn publish_sandbox_discarded(
		&self,
		state: &mut SchedulerState,
		discarded: sandbox::DiscardedSandbox,
	) {
		// Fail the handoff of the discarded sandbox's acceptance.
		let error = tg::error!(sandbox = %discarded.sandbox, "the sandbox was discarded");
		state.finish_accept(self, &discarded.sandbox, Path::Handled, Err(error));

		let subject = tangram_server_sandbox_common::control::discarded_subject(&discarded.sandbox);
		let error = discarded.error.clone();
		let notification = tangram_server_sandbox_common::control::Discarded {
			error: discarded.error,
		};
		let scheduler = self.clone();
		let server = self.server.clone();
		state.operations.push(
			async move {
				// Record the failure in the sandbox's error field, and then wake the callers.
				let request = discarded.request;
				let result = scheduler.fail_sandbox(&request, error).await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), sandbox = %request.sandbox, "failed to record the discarded sandbox");
				}
				let result = server
					.messenger
					.publish(subject, tangram_messenger::payload::Json(notification))
					.await
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to publish the sandbox discard notification"
						)
					});
				Operation::Publish {
					context: "failed to publish the sandbox discard notification",
					result,
				}
			}
			.boxed(),
		);
	}

	fn send_dequeue_sandbox_completion(
		&self,
		state: &mut SchedulerState,
		completion: sandbox::DequeueCompletion,
	) {
		self.send_dequeue_sandbox_response(state, completion.request, completion.output);
	}

	fn send_dequeue_sandbox_completions(
		&self,
		state: &mut SchedulerState,
		completions: Vec<sandbox::DequeueCompletion>,
	) {
		for completion in completions {
			self.send_dequeue_sandbox_completion(state, completion);
		}
	}

	fn send_dequeue_sandbox_response(
		&self,
		state: &mut SchedulerState,
		id: String,
		output: DequeueSandboxResponseOutput,
	) {
		let response = request::Response::new(id, Ok(ResponseOutput::DequeueSandbox(output)));
		self.send_response(state, response);
	}

	fn publish_runner_heartbeat_ack(
		&self,
		state: &mut SchedulerState,
		notification: &HeartbeatNotification,
	) {
		let subject = runner_heartbeat_subject(&self.id, &notification.runner);
		let acknowledgement = HeartbeatAcknowledgement {
			registration_index: notification.registration_index,
			heartbeat_index: notification.heartbeat_index,
		};
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, acknowledgement)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to acknowledge the runner heartbeat")
					});
				Operation::Publish {
					context: "failed to acknowledge the runner heartbeat",
					result,
				}
			}
			.boxed(),
		);
	}

	fn send_response(&self, _state: &mut SchedulerState, response: request::Response) {
		self.sender
			.send_now(tangram_server_core::service::ClientMessage::Response(
				response,
			));
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn start<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			// Spawn the scheduler cache task.
			let task = self.spawn_scheduler_cache_task(server);
			self.scheduler_cache_task.lock().unwrap().replace(task);

			// Register the scheduler as unavailable until it recovers.
			Self::register_scheduler(server).await?;

			// Spawn the scheduler task.
			let task = self.spawn_scheduler_task(server);
			*self.task.lock().unwrap() = task;

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the scheduler task.
			let task = self.task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the scheduler task panicked");
				}
				tracing::trace!("scheduler task");
			}

			// Stop the scheduler cache task.
			let task = self.scheduler_cache_task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the scheduler cache task panicked");
				}
			}
		}
		.boxed()
	}

	fn abort(&self) {
		let task = self.task.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
		let task = self.scheduler_cache_task.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
	}
}

impl tangram_server_core::scheduler::Component for Component {
	fn destroy_runner_sandbox<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		id: &'a tg::sandbox::Id,
		error: tg::error::Data,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.destroy_runner_sandbox(server, id, error).boxed()
	}

	fn destroy_stopped_runner_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		sandbox: &'a tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> BoxFuture<'a, tg::Result<bool>> {
		self.destroy_stopped_runner_sandbox(session, runner, sandbox, error)
			.boxed()
	}

	fn dequeue_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		sandbox: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<bool>> {
		self.dequeue_sandbox(session, sandbox, scheduler).boxed()
	}

	fn acquire_process_lease<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::process::Lease>>> {
		self.acquire_process_lease(session, scheduler, sandbox)
			.boxed()
	}

	fn release_process_lease<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
		lease: tg::process::Lease,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.release_process_lease(session, scheduler, sandbox, lease)
			.boxed()
	}

	fn cancel_sandbox_stop<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
		sandbox: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<bool>> {
		self.cancel_sandbox_stop(session, scheduler, sandbox)
			.boxed()
	}

	fn end_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
		arg: EndSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<bool>> {
		self.end_sandbox(session, scheduler, arg).boxed()
	}

	fn enqueue_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>> {
		self.enqueue_sandbox(session, request).boxed()
	}

	fn send_scheduler_request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: RequestArg,
	) -> BoxFuture<'a, tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)>> {
		self.send_scheduler_request(session, arg).boxed()
	}

	fn send_scheduler_request_to<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
		arg: RequestArg,
	) -> BoxFuture<'a, tg::Result<tg::Result<ResponseOutput>>> {
		self.send_scheduler_request_to(session, scheduler, arg)
			.boxed()
	}

	fn started(&self) -> BoxFuture<'_, ()> {
		self.started().boxed()
	}

	fn stop_runner_sandbox<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		id: &'a tg::sandbox::Id,
		error: tg::error::Data,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.stop_runner_sandbox(server, id, error).boxed()
	}
}
