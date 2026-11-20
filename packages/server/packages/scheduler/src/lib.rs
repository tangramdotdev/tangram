use {
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_index::prelude::*,
	tangram_messenger::Messenger,
	tangram_server_core::{Server, Session},
	tangram_server_scheduler_core::{
		AddRunnerRequestArg, AddRunnerResponseOutput, AddSandboxParentRequestArg,
		AddSandboxParentResponseOutput, BorrowableCapacityNotification, DequeueSandboxRequestArg,
		DequeueSandboxResponseOutput, DestroyRunnerSandboxRequestArg,
		DestroyRunnerSandboxResponseOutput, EnqueueSandboxRequestArg, EnqueueSandboxResponseOutput,
		HeartbeatAcknowledgement, HeartbeatNotification, Notification, RemoveRunnerRequestArg,
		RemoveRunnerResponseOutput, RequestArg, ResponseOutput, SandboxEndedNotification,
		runner_heartbeat_subject, scheduler_notification_subject,
	},
};

mod holder;
mod offer;
mod placement;
mod queue;
mod record;
mod recovery;
mod request;
mod runner;
mod sandbox;

/// The ID of the only scheduler of a single server, which multi-server mode replaces with a configured ID.
const SINGLE_SERVER_ID: &str = "sch_0000000000000000000000000000";

type Operations = FuturesUnordered<BoxFuture<'static, Operation>>;

type NotificationStream =
	BoxStream<'static, Result<tangram_messenger::Message<Notification>, tangram_messenger::Error>>;

/// The maximum number of sandbox owners whose ancestors a server remembers.
const OWNER_ANCESTORS_CAPACITY: u64 = 65_536;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	/// The ancestors of the owners of the sandboxes that this server enqueued, which is absent if the configuration does not remember them.
	pub(crate) owner_ancestors: Option<moka::future::Cache<tg::Id, Vec<tg::Id>>>,
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
	holder_sender: tokio::sync::mpsc::UnboundedSender<HolderMessage>,
	id: tg::scheduler::Id,
	sender: request::Sender,
	server: Server,
}

/// A message about a sandbox whose holder this scheduler serves.
enum HolderMessage {
	/// The sandbox's control connection was established, so its runner serves its holder.
	Connected { sandbox: tg::sandbox::Id },

	/// A request to the holder of the sandbox's queued process.
	ProcessRequest {
		request: tg::process::control::ServerRequest,
		sandbox: tg::sandbox::Id,
	},

	/// A request to the sandbox's holder.
	Request {
		request: tg::sandbox::control::ServerRequest,
		sandbox: tg::sandbox::Id,
	},
}

struct SchedulerState {
	operations: Operations,
	parents: placement::Parents,
	queue: queue::Queue,
	recovery: Option<recovery::Recovery>,
	runners: runner::Runners,
	sandboxes: sandbox::Sandboxes,
}

enum Operation {
	AddRunner {
		registration_index: u64,
		id: String,
		result: tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)>,
		runner: tg::runner::Id,
	},
	Checkpoint,
	DestroyRunnerSandbox {
		id: String,
		result: tg::Result<bool>,
	},
	DestroySandbox {
		destroy: sandbox::DestroySandbox,
		result: tg::Result<bool>,
	},
	Finished {
		context: &'static str,
		result: tg::Result<()>,
	},
	IndexTerminalSandboxes {
		sandboxes: Vec<(tg::sandbox::Id, u64)>,
	},
	LostSandboxes {
		keys: Vec<tg::sandbox::Id>,
		lost: Vec<EnqueueSandboxRequestArg>,
	},
	OfferSandbox {
		outcome: offer::Outcome,
		placement: placement::Placement,
		sandbox: tg::sandbox::Id,
	},
	PersistSandbox {
		boot: u64,
		result: tg::Result<()>,
		sandbox: tg::sandbox::Id,
	},
	Recovered {
		statuses: recovery::Statuses,
	},
	RecoveryExpired,
	RemoveRunner {
		id: Option<String>,
		result: tg::Result<RemoveRunnerResponseOutput>,
	},
	RetrySandbox {
		sandbox: tg::sandbox::Id,
	},
	RetrySandboxOffer {
		offer: String,
		sandbox: tg::sandbox::Id,
	},
	ServeSandboxHolder {
		result: tg::Result<Vec<Task<tg::Result<()>>>>,
		sandbox: tg::sandbox::Id,
	},
	WriteTerminalSandbox {
		boot: u64,
		sandbox: tg::sandbox::Id,
		written: bool,
	},
}

enum Event {
	CleanerTick,
	HolderMessage(HolderMessage),
	Notification(Notification),
	Operation(Operation),
	Request(request::Request),
	Schedule,
}

#[allow(dead_code)]
#[derive(Clone)]
struct Config {
	default_capacity: tg::runner::Capacity,
	hosts: std::collections::BTreeSet<String>,
	max_offer_failures: usize,
	max_offers: usize,
	max_offers_per_runner: usize,
	offer_retry: tangram_futures::retry::Options,
	offer_timeout: Duration,
	queue_capacity: usize,
	runner_time_to_live: Duration,
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

		// Create the cache of the ancestors of sandbox owners.
		let time_to_live = arg.config.scheduler.owner_ancestors_time_to_live;
		let owner_ancestors = (!time_to_live.is_zero()).then(|| {
			moka::future::Cache::builder()
				.max_capacity(OWNER_ANCESTORS_CAPACITY)
				.time_to_live(time_to_live)
				.build()
		});

		// Create the task handles and the scheduler cache.
		let scheduler_cache_task = Mutex::new(None);
		let schedulers = tangram_server_core::service::Cache::default();
		let task = Mutex::new(None);

		// Create the state.
		let state = State {
			owner_ancestors,
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

	/// Add a parent to a sandbox that a scheduler holds, and return the sandbox's capacity, or none if the scheduler does not hold the sandbox.
	pub(crate) async fn add_sandbox_parent(
		&self,
		session: &Session,
		sandbox: &tg::sandbox::Id,
		parent: &tg::sandbox::Id,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<Option<tg::runner::Capacity>> {
		let request = RequestArg::AddSandboxParent(AddSandboxParentRequestArg {
			parent: parent.clone(),
			sandbox: sandbox.clone(),
		});
		let response = self
			.send_scheduler_request_to(session, scheduler, request)
			.await
			.map_err(
				|source| tg::error!(!source, %sandbox, %scheduler, "failed to send the add sandbox parent request"),
			)?
			.map_err(
				|source| tg::error!(!source, %sandbox, %scheduler, "the add sandbox parent request failed"),
			)?;
		let output = response
			.try_unwrap_add_sandbox_parent()
			.map_err(|_| tg::error!("expected an add sandbox parent response"))?;

		Ok(output.capacity)
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

	/// Get a sandbox owner and its ancestors, which this server remembers for a configured time, so that enqueueing a sandbox does not read the index.
	pub(crate) async fn get_owner_ancestors(
		&self,
		session: &Session,
		owner: &tg::Principal,
	) -> tg::Result<Vec<tg::Id>> {
		let Some(owner) = owner.to_id() else {
			return Ok(Vec::new());
		};
		let Some(cache) = &self.owner_ancestors else {
			return Self::get_owner_ancestors_inner(&session.server, &owner).await;
		};
		let ancestors = cache
			.try_get_with_by_ref(
				&owner,
				Self::get_owner_ancestors_inner(&session.server, &owner),
			)
			.await
			.map_err(|error| (*error).clone())?;

		Ok(ancestors)
	}

	async fn get_owner_ancestors_inner(server: &Server, owner: &tg::Id) -> tg::Result<Vec<tg::Id>> {
		let ancestors = server
			.index
			.try_get_ancestors(owner)
			.await
			.map_err(|error| tg::error!(!error, %owner, "failed to get the owner's ancestors"))?
			.ok_or_else(|| tg::error!(%owner, "failed to find the owner"))?;

		Ok(ancestors)
	}

	/// Enqueue a sandbox with a scheduler that accepts its host, returning the scheduler, or `None` if every scheduler that answers declines the sandbox.
	#[tracing::instrument(name = "sandbox.enqueue", level = "debug", skip_all, fields(sandbox = %request.sandbox), err(level = "debug"))]
	pub(crate) async fn try_enqueue_sandbox(
		&self,
		session: &Session,
		request: EnqueueSandboxRequestArg,
	) -> tg::Result<Option<tg::scheduler::Id>> {
		tracing::info!(sandbox = %request.sandbox, "enqueueing the sandbox");
		let sandbox = request.sandbox.clone();

		// Send a sandbox with a parent to its parent's scheduler, and a start of a stopped sandbox to its runner's scheduler.
		let pinned = if let Some(parent) = &request.parent {
			let scheduler = request.scheduler.clone().ok_or_else(
				|| tg::error!(%parent, "missing the scheduler for the parent sandbox"),
			)?;
			Some(scheduler)
		} else {
			request.scheduler.clone()
		};
		let arg = RequestArg::EnqueueSandbox(request);
		if let Some(scheduler) = pinned {
			let output = self
				.send_scheduler_request_to(session, &scheduler, arg)
				.boxed()
				.await??
				.try_unwrap_enqueue_sandbox()
				.map_err(|_| tg::error!("expected an enqueue sandbox response"))?;
			let output =
				Self::try_enqueue_sandbox_output(session, &sandbox, scheduler, output).await;
			return Ok(output);
		}

		// Get the schedulers, beginning with a random one.
		let mut schedulers = self.schedulers.available();
		if schedulers.is_empty() {
			self.schedulers.refresh(&session.server).await?;
			schedulers = self.schedulers.available();
		}
		if schedulers.is_empty() {
			return Err(tg::error!("no schedulers are available"));
		}
		let len = u64::try_from(schedulers.len()).unwrap();
		let start = usize::try_from(tangram_server_core::service::start() % len).unwrap();
		schedulers.rotate_left(start);

		// Ask each scheduler once until one accepts the sandbox, sharing one retry budget, and move to the next scheduler if one declines the sandbox or is unavailable.
		let retry = session.server.config.scheduler.request.retry.clone().into();
		let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
		let mut answered = false;
		for scheduler in schedulers {
			let output = self
				.try_send_scheduler_request_to(session, &scheduler, arg.clone(), &mut retries)
				.boxed()
				.await?;
			let Some(output) = output else {
				tracing::info!(%sandbox, %scheduler, "the scheduler is unavailable");
				continue;
			};
			answered = true;
			let output = output?
				.try_unwrap_enqueue_sandbox()
				.map_err(|_| tg::error!("expected an enqueue sandbox response"))?;
			let output =
				Self::try_enqueue_sandbox_output(session, &sandbox, scheduler, output).await;
			if output.is_some() {
				return Ok(output);
			}
		}
		if !answered {
			return Err(tg::error!(%sandbox, "no scheduler is available"));
		}

		Ok(None)
	}

	/// Get the scheduler that enqueued the sandbox, or `None` if the scheduler declined it.
	async fn try_enqueue_sandbox_output(
		session: &Session,
		sandbox: &tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
		output: EnqueueSandboxResponseOutput,
	) -> Option<tg::scheduler::Id> {
		match output {
			EnqueueSandboxResponseOutput::Enqueued => {
				tracing::info!(%sandbox, %scheduler, "enqueued the sandbox");
				tangram_server_core::checkpoint!(session.server, "scheduler.sandbox.enqueued", sandbox = %sandbox)
					.await;
				Some(scheduler)
			},
			EnqueueSandboxResponseOutput::Unplaceable => {
				tracing::info!(%sandbox, %scheduler, "the scheduler declined the sandbox");
				None
			},
		}
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
			default_capacity: config.default_capacity(),
			hosts: config.hosts.clone(),
			max_offer_failures: config.max_offer_failures,
			max_offers: config.max_offers,
			max_offers_per_runner: config.max_offers_per_runner,
			offer_retry: config.offer_retry.clone().into(),
			offer_timeout: config.offer_timeout,
			queue_capacity: config.queue_capacity,
			runner_time_to_live: config.runner_time_to_live,
			unplaceable_timeout: config.unplaceable_timeout,
		};

		// Subscribe to the scheduler's requests and notifications before starting, so that no message sent after the start is lost.
		let endpoint = tangram_server_core::service::Endpoint::<request::Kind>::subscribe(
			server,
			&id,
			request_config.channel_capacity,
			request_config.response_time_to_live,
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
		let (holder_sender, holder_receiver) = tokio::sync::mpsc::unbounded_channel();
		let scheduler = Self {
			component: component.clone(),
			config,
			holder_sender,
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
					.boxed()
					.await
			}
		});

		// Spawn the scheduling task.
		let task = Task::spawn(move |_| async move {
			scheduler
				.message_handler_task(notifications, request_receiver, holder_receiver)
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
		mut holder_messages: tokio::sync::mpsc::UnboundedReceiver<HolderMessage>,
	) -> tg::Result<()> {
		let mut state = SchedulerState::new();

		// Recover the accepted requests before handling messages.
		self.recover(&mut state).await?;

		let mut cleaner_interval = self.server.clock.interval(self.config.runner_time_to_live);
		cleaner_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			let event = self
				.next_event(
					&mut state,
					&mut notifications,
					&mut requests,
					&mut holder_messages,
					&mut cleaner_interval,
				)
				.await?;
			match event {
				Event::CleanerTick => {
					self.handle_cleaner_tick(&mut state);
				},
				Event::HolderMessage(message) => {
					state.handle_sandbox_holder_message(self, message);
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
		holder_messages: &mut tokio::sync::mpsc::UnboundedReceiver<HolderMessage>,
		cleaner_interval: &mut tangram_server_core::clock::Interval,
	) -> tg::Result<Event> {
		let event = tokio::select! {
			_ = cleaner_interval.tick() => Event::CleanerTick,
			Some(message) = holder_messages.recv() => Event::HolderMessage(message),
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
			Notification::SandboxEnded(notification) => {
				state.handle_sandbox_ended(self, &notification);
			},
		}
	}

	fn handle_request(&self, state: &mut SchedulerState, request: request::Request) {
		let id = request.id;
		match request.arg {
			RequestArg::AddRunner(request) => {
				state.handle_add_runner_request(self, id, request);
			},
			RequestArg::AddSandboxParent(request) => {
				let output = state.add_sandbox_parent(self, &request);
				let response =
					request::Response::new(id, Ok(ResponseOutput::AddSandboxParent(output)));
				self.send_response(state, response);
			},
			RequestArg::DequeueSandbox(request) => {
				if let Some(output) = state.dequeue_sandbox(self, id.clone(), request) {
					self.send_dequeue_sandbox_response(state, id, output);
				}
			},
			RequestArg::DestroyRunnerSandbox(request) => {
				state.handle_destroy_runner_sandbox_request(self, id, request);
			},
			RequestArg::EnqueueSandbox(request) => {
				state.handle_enqueue_sandbox_request(self, Some(id), request, false);
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
						state.fail_runner_registration(&runner, registration_index);
						Err(error)
					},
				};
				let result = result.map(ResponseOutput::AddRunner);
				let response = request::Response::new(id, result);
				self.send_response(state, response);
			},
			Operation::Checkpoint => {},
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
			Operation::Finished { context, result } => {
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "{context}");
				}
			},
			Operation::IndexTerminalSandboxes { sandboxes } => {
				state.handle_index_terminal_sandboxes(self, sandboxes);
			},
			Operation::LostSandboxes { keys, lost } => {
				state.handle_lost_sandboxes(self, keys, lost);
			},
			Operation::OfferSandbox {
				outcome,
				placement,
				sandbox,
			} => {
				state.handle_offer_outcome(self, &sandbox, &placement, outcome);
			},
			Operation::PersistSandbox {
				boot,
				result,
				sandbox,
			} => {
				state.handle_persist_sandbox_completion(self, &sandbox, boot, result);
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
			Operation::RetrySandbox { sandbox } => {
				state.handle_retry_sandbox(self, &sandbox);
			},
			Operation::RetrySandboxOffer { offer, sandbox } => {
				state.handle_retry_sandbox_offer(self, &sandbox, &offer);
			},
			Operation::ServeSandboxHolder { result, sandbox } => {
				state.handle_serve_sandbox_holder_completion(&sandbox, result);
			},
			Operation::WriteTerminalSandbox {
				boot,
				sandbox,
				written,
			} => {
				state.handle_write_terminal_sandbox(self, sandbox, boot, written);
			},
		}
	}

	fn handle_cleaner_tick(&self, state: &mut SchedulerState) {
		state.prune_sandbox_holders();
		let now = self.server.clock.instant();
		let expired = state.runners.expired(now, self.config.runner_time_to_live);
		for (runner, registration_index) in expired {
			tracing::warn!(%runner, registration_index, time_to_live_ms = self.config.runner_time_to_live.as_millis(), "the runner heartbeat expired");
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
		let subject = tangram_server_sandbox_common::control::discarded_subject(&discarded.sandbox);
		let error = discarded.error.clone();
		let notification = tangram_server_sandbox_common::control::Discarded {
			error: discarded.error,
		};
		let scheduler = self.clone();
		let server = self.server.clone();
		state.operations.push(
			async move {
				// Record the failure in the sandbox's error field, unless the scheduler already began the write, and then wake the callers.
				let request = discarded.request;
				let result = match discarded.write {
					Some(write) => write.await,
					None => scheduler.fail_sandbox(&request, error).await,
				};
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
				Operation::Finished {
					context: "failed to publish the sandbox discard notification",
					result,
				}
			}
			.boxed(),
		);
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
				Operation::Finished {
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

			// End what a single server's previous run left before the server accepts requests, because a single server does not recover its requests.
			if !server.config.advanced.multi_server
				&& server
					.config
					.roles
					.contains(&tangram_server_core::config::Role::Scheduler)
			{
				self.end_previous_run(server).await?;
			}

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
	fn add_sandbox_parent<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		sandbox: &'a tg::sandbox::Id,
		parent: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::Capacity>>> {
		self.add_sandbox_parent(session, sandbox, parent, scheduler)
			.boxed()
	}

	fn destroy_runner_sandbox<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		id: &'a tg::sandbox::Id,
		error: Option<tg::error::Data>,
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

	fn get_owner_ancestors<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		owner: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Vec<tg::Id>>> {
		self.get_owner_ancestors(session, owner).boxed()
	}

	fn select_scheduler<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>> {
		self.select_scheduler(session).boxed()
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

	fn try_enqueue_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<Option<tg::scheduler::Id>>> {
		self.try_enqueue_sandbox(session, request).boxed()
	}
}
