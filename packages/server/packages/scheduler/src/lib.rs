use {
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{self, BoxStream, FuturesUnordered},
	},
	std::{
		collections::{HashMap, HashSet},
		fmt::Display,
		ops::ControlFlow,
		pin::pin,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger,
	tangram_server_core::{Server, Session},
	tangram_server_scheduler_core::{
		Ack, AddRunnerRequestArg, AddRunnerResponseOutput, BorrowableCapacityNotification,
		DequeueSandboxRequestArg, DequeueSandboxResponseOutput, EnqueueSandboxRequestArg,
		EnqueueSandboxResponseOutput, HeartbeatAcknowledgement, HeartbeatNotification, Message,
		Notification, RemoveRunnerRequestArg, RemoveRunnerResponseOutput, Request, RequestArg,
		Response, ResponseOutput, runner_heartbeat_subject, scheduler_heartbeat_subject,
		scheduler_server_subject,
	},
};

mod runner;
mod sandbox;

type Operations = FuturesUnordered<BoxFuture<'static, Operation>>;

type MessageStream =
	BoxStream<'static, Result<tangram_messenger::Message<Message>, tangram_messenger::Error>>;

type OwnerAncestors = Arc<HashSet<tg::Id, tg::id::BuildHasher>>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) started: tokio::sync::watch::Sender<bool>,
	pub(crate) task: Mutex<Option<Task<()>>>,
}

struct Owned {
	task: Task<tg::Result<()>>,
}

#[derive(Clone)]
struct Scheduler {
	component: Component,
	config: Config,
	id: tg::scheduler::Id,
	server: Server,
}

struct SchedulerState {
	operations: Operations,
	owner_ancestor_requests: HashMap<tg::Id, Vec<tg::sandbox::Id>, tg::id::BuildHasher>,
	owner_ancestors: HashMap<tg::Id, OwnerAncestors, tg::id::BuildHasher>,
	parents: sandbox::Parents,
	pending_enqueues: HashMap<tg::sandbox::Id, PendingEnqueue, tg::id::BuildHasher>,
	queue: sandbox::Queue,
	requests: Requests,
	runners: runner::Runners,
	sandboxes: sandbox::Sandboxes,
}

struct PendingEnqueue {
	ids: Vec<String>,
	request: EnqueueSandboxRequestArg,
}

struct Requests {
	active: tangram_server_core::control::requests::Requests<RequestKind>,
	inbox: HashSet<String>,
	outbox: HashMap<String, Response>,
	replies: usize,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum RequestKind {
	AddRunner,
	DequeueSandbox,
	EnqueueSandbox,
	RemoveRunner,
}

enum Operation {
	Acknowledge {
		request: Request,
		result: tg::Result<()>,
	},
	AddRunner {
		connection_index: u64,
		id: String,
		result: tg::Result<(AddRunnerResponseOutput, Option<tg::Id>)>,
		runner: tg::runner::Id,
	},
	CreateSandbox(sandbox::Completion),
	ExpireRequest {
		id: String,
	},
	LoadOwnerAncestors {
		owner: tg::Id,
		result: tg::Result<Vec<tg::Id>>,
	},
	Publish {
		context: &'static str,
		result: tg::Result<()>,
	},
	RemoveRunner {
		id: Option<String>,
		result: tg::Result<RemoveRunnerResponseOutput>,
	},
	Reply {
		result: tg::Result<()>,
	},
	Response {
		id: String,
		result: tg::Result<()>,
	},
	RetrySandbox {
		sandbox: tg::sandbox::Id,
	},
}

enum Event {
	CleanerTick,
	Message(Message),
	Operation(Operation),
	Schedule,
	SchedulerHeartbeatTick,
}

#[allow(dead_code)]
#[derive(Clone)]
struct Config {
	create_sandbox_queue_capacity: usize,
	create_sandbox_timeout: Duration,
	default_capacity: tg::runner::Capacity,
	heartbeat_interval: Duration,
	inbox_ttl: Duration,
	max_create_sandbox_attempts: usize,
	max_create_sandbox_requests: usize,
	max_create_sandbox_requests_per_runner: usize,
	request_concurrency: usize,
	runner_ttl: Duration,
}

struct SchedulerMessageOptions {
	ack_timeout: Duration,
	retry: tangram_futures::retry::Options,
	timeout: Duration,
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

		// Create the task handle.
		let task = Mutex::new(None);

		// Create the state.
		let state = State { started, task };

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
		let task = Task::spawn(|_| async move {
			component.scheduler_task(&server, &config).await;
		});

		Some(task)
	}

	pub(crate) async fn started(&self) {
		let mut receiver = self.started.subscribe();
		receiver.wait_for(|started| *started).await.ok();
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
		let response = tokio::select! {
			result = self.send_scheduler_request_to(session, scheduler, request) => {
				result.map_err(
					|source| tg::error!(!source, %sandbox, %scheduler, "failed to send the dequeue sandbox request"),
				)?
			},
			result = self.scheduler_heartbeat_expired(session, scheduler) => {
				result?;
				return Err(tg::error!(
					code = tg::error::Code::HeartbeatExpiration,
					%sandbox,
					%scheduler,
					"the scheduler heartbeat expired while dequeuing the sandbox"
				));
			},
		}
			.map_err(
				|source| tg::error!(!source, %sandbox, %scheduler, "the dequeue sandbox request failed"),
			)?;
		let output = response
			.try_unwrap_dequeue_sandbox()
			.map_err(|_| tg::error!("expected a dequeue sandbox response"))?;

		Ok(output.dequeued)
	}

	pub(crate) async fn scheduler_heartbeat_expired(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<()> {
		let subject = scheduler_heartbeat_subject(scheduler);
		let heartbeats = session
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					%scheduler,
					"failed to subscribe to the scheduler heartbeat"
				)
			})?;

		let mut heartbeats = pin!(heartbeats);
		let ttl = session.server.config.scheduler.heartbeat_ttl;
		loop {
			match session
				.server
				.clock
				.timeout(ttl, heartbeats.try_next())
				.await
			{
				Ok(Ok(Some(_))) => {},
				Ok(Ok(None)) => {
					return Err(tg::error!(
						%scheduler,
						"the scheduler heartbeat stream ended"
					));
				},
				Ok(Err(source)) => {
					return Err(tg::error!(
						!source,
						%scheduler,
						"failed to receive a scheduler heartbeat"
					));
				},
				Err(_) => {
					return Ok(());
				},
			}
		}
	}

	#[tracing::instrument(name = "sandbox.enqueue", level = "debug", skip_all, fields(sandbox = %request.sandbox), err(level = "debug"))]
	pub(crate) async fn enqueue_sandbox(
		&self,
		session: &Session,
		request: EnqueueSandboxRequestArg,
	) -> tg::Result<tg::scheduler::Id> {
		tracing::info!(sandbox = %request.sandbox, "enqueueing the sandbox");
		let options = session.server.config.scheduler.enqueue_retry.clone().into();
		let (declined_sender, declined) = tokio::sync::watch::channel(None);
		let enqueue = tangram_futures::retry::retry(&options, || {
			let declined_sender = declined_sender.clone();
			let request = request.clone();
			let sandbox = request.sandbox.clone();
			async move {
				let target = if let Some(parent) = &request.parent {
					let scheduler = request.scheduler.clone().ok_or_else(|| {
						Some(tg::error!(
							%parent,
							"missing the scheduler for the parent sandbox"
						))
					})?;
					Some(scheduler)
				} else {
					None
				};
				let (scheduler, response) = match target.as_ref() {
					Some(scheduler) => {
						let response = self
							.send_scheduler_request_to(
								session,
								scheduler,
								RequestArg::EnqueueSandbox(request),
							)
							.boxed()
							.await
							.map_err(Some)?;
						(scheduler.clone(), response)
					},
					None => self
						.send_scheduler_request(session, RequestArg::EnqueueSandbox(request))
						.boxed()
						.await
						.map_err(Some)?,
				};
				let response = response.map_err(Some)?;
				let response = response
					.try_unwrap_enqueue_sandbox()
					.map_err(|_| Some(tg::error!("expected an enqueue sandbox response")))?;
				if response.enqueued {
					tracing::info!(%sandbox, %scheduler, "enqueued the sandbox");
					tangram_server_core::checkpoint!(session.server, "scheduler.sandbox.enqueued", sandbox = %sandbox)
						.await;
					Ok(ControlFlow::Break(scheduler))
				} else {
					tangram_server_core::checkpoint!(
						session.server,
						"scheduler.sandbox.declined",
						sandbox = %sandbox,
						scheduler = %scheduler,
					)
					.await;
					declined_sender.send_if_modified(|declined| {
						if declined.as_ref() == Some(&scheduler) {
							return false;
						}
						declined.replace(scheduler);
						true
					});
					Ok(ControlFlow::Continue(None::<tg::Error>))
				}
			}
			.boxed()
		})
		.boxed();

		// Retry while the scheduler declines the sandbox. Fail a targeted enqueue if its scheduler's heartbeat expires, and leave an untargeted enqueue to the schedulers that remain.
		let result = if request.parent.is_some() {
			tokio::select! {
				result = enqueue => result,
				result = self.declining_scheduler_heartbeat_expired(session, declined, &options) => {
					let scheduler = result?;
					return Err(tg::error!(
						code = tg::error::Code::HeartbeatExpiration,
						sandbox = %request.sandbox,
						%scheduler,
						"the scheduler heartbeat expired"
					));
				},
			}
		} else {
			enqueue.await
		};
		match result {
			Ok(scheduler) => Ok(scheduler),
			Err(Some(error)) => Err(error),
			Err(None) => unreachable!(),
		}
	}

	async fn declining_scheduler_heartbeat_expired(
		&self,
		session: &Session,
		mut declined: tokio::sync::watch::Receiver<Option<tg::scheduler::Id>>,
		options: &tangram_futures::retry::Options,
	) -> tg::Result<tg::scheduler::Id> {
		let mut retries = tangram_futures::retry::stream(options.clone()).boxed();
		loop {
			let scheduler = declined
				.wait_for(Option::is_some)
				.await
				.map_err(|_| tg::error!("the enqueue ended"))?
				.clone()
				.unwrap();
			tokio::select! {
				result = self.scheduler_heartbeat_expired(session, &scheduler) => match result {
					Ok(()) => return Ok(scheduler),
					Err(error) => {
						// Watch the heartbeat again after a failure, so that a messenger error does not end the enqueue.
						tracing::warn!(error = %error.trace(), %scheduler, "failed to watch the scheduler heartbeat");
						if retries.next().await.is_none() {
							return Err(error);
						}
					},
				},
				result = declined.changed() => {
					result.map_err(|_| tg::error!("the enqueue ended"))?;
				},
			}
		}
	}

	pub(crate) async fn send_scheduler_request(
		&self,
		session: &Session,
		arg: RequestArg,
	) -> tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)> {
		self.send_scheduler_request_with_retry(session, None, arg)
			.await
	}

	pub(crate) async fn send_scheduler_request_to(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let (_, response) = self
			.send_scheduler_request_with_retry(session, Some(scheduler), arg)
			.await?;

		Ok(response)
	}

	async fn send_scheduler_request_with_retry(
		&self,
		session: &Session,
		scheduler: Option<&tg::scheduler::Id>,
		arg: RequestArg,
	) -> tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)> {
		let retry = Self::scheduler_message_options(session).retry;
		tangram_futures::retry(&retry, || {
			let arg = arg.clone();
			async move {
				let response = self
					.send_scheduler_request_inner(session, scheduler, arg)
					.boxed()
					.await?;
				if matches!(response.1, Ok(ResponseOutput::Busy)) {
					return Ok(ControlFlow::Continue(tg::error!("the scheduler is busy")));
				}
				Ok(ControlFlow::Break(response))
			}
		})
		.await
	}

	async fn send_scheduler_request_inner(
		&self,
		session: &Session,
		scheduler: Option<&tg::scheduler::Id>,
		arg: RequestArg,
	) -> tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)> {
		let id = tangram_server_core::control::id();
		let request = Request {
			arg,
			id: id.clone(),
		};
		let (scheduler, mut messages) = self
			.send_scheduler_message(session, scheduler, id.clone(), Message::Request(request))
			.await?;
		let timeout = Self::scheduler_message_options(session).timeout;
		let response = tokio::time::timeout(timeout, async {
			loop {
				let message = messages
					.next()
					.await
					.ok_or_else(|| tg::error!("the scheduler message stream ended"))?
					.map_err(|source| {
						tg::error!(!source, "failed to receive the scheduler message")
					})?;
				let Message::Response(message) = message.payload else {
					continue;
				};
				if message.id == id && message.scheduler == scheduler {
					break Ok::<_, tg::Error>(message);
				}
			}
		})
		.await
		.map_err(|_| tg::error!("timed out waiting for the scheduler response"))??;
		session
			.server
			.messenger
			.publish(
				scheduler_server_subject(Some(&scheduler)),
				Message::Ack(Ack {
					id: response.id.clone(),
					scheduler: scheduler.clone(),
				}),
			)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to acknowledge the scheduler response")
			})?;
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Ok((scheduler, Err(error)));
		}
		let output = response
			.output
			.ok_or_else(|| tg::error!("missing scheduler response output"))?;

		Ok((scheduler, Ok(output)))
	}

	pub(crate) async fn send_scheduler_message(
		&self,
		session: &Session,
		scheduler: Option<&tg::scheduler::Id>,
		id: String,
		message: Message,
	) -> tg::Result<(
		tg::scheduler::Id,
		BoxStream<'static, Result<tangram_messenger::Message<Message>, tangram_messenger::Error>>,
	)> {
		let options = Self::scheduler_message_options(session);
		let client_subject = Scheduler::client_subject(&id);
		let expected_scheduler = scheduler.cloned();
		let server = session.server.clone();
		let server_subject = scheduler_server_subject(scheduler);

		let result = tangram_futures::retry::retry(&options.retry, || {
			let client_subject = client_subject.clone();
			let expected_scheduler = expected_scheduler.clone();
			let id = id.clone();
			let message = message.clone();
			let server = server.clone();
			let server_subject = server_subject.clone();
			let timeout = options.ack_timeout;
			async move {
				let mut messages = server
					.messenger
					.subscribe::<Message>(client_subject)
					.await
					.map_err(|source| {
						Some(tg::error!(
							!source,
							"failed to subscribe to the scheduler message"
						))
					})?
					.boxed();

				server
					.messenger
					.publish(server_subject.clone(), message)
					.await
					.map_err(|source| {
						Some(tg::error!(
							!source,
							"failed to publish the scheduler message"
						))
					})?;

				let result = tokio::time::timeout(timeout, async {
					loop {
						let message = messages
							.next()
							.await
							.ok_or_else(|| tg::error!("the scheduler message stream ended"))?
							.map_err(|source| {
								tg::error!(!source, "failed to receive the scheduler message")
							})?;
						let Message::Ack(ack) = message.payload else {
							continue;
						};
						if ack.id == id
							&& expected_scheduler
								.as_ref()
								.is_none_or(|scheduler| scheduler == &ack.scheduler)
						{
							return Ok::<_, tg::Error>((ack.scheduler, messages));
						}
					}
				})
				.await;

				match result {
					Ok(Ok(messages)) => Ok(ControlFlow::Break(messages)),
					Ok(Err(error)) => Err(Some(error)),
					Err(_) => Ok(ControlFlow::Continue(None)),
				}
			}
			.boxed()
		})
		.await;

		match result {
			Ok(output) => Ok(output),
			Err(None) => Err(tg::error!("timed out waiting for the scheduler ack")),
			Err(Some(error)) => Err(error),
		}
	}

	fn scheduler_message_options(session: &Session) -> SchedulerMessageOptions {
		let config = session.server.config.scheduler.clone();
		SchedulerMessageOptions {
			ack_timeout: config.message_ack_timeout,
			retry: config.message_retry.into(),
			timeout: config.message_timeout,
		}
	}

	pub(crate) async fn scheduler_task(
		&self,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
	) {
		loop {
			let result = self.scheduler_task_inner(server, config).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the scheduler task failed");
				tokio::time::sleep(config.restart_delay).await;
			}
		}
	}

	async fn scheduler_task_inner(
		&self,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
	) -> tg::Result<()> {
		let scheduler = Scheduler::start(self, server, config).await?;

		// Report that the scheduler is subscribed to its messages.
		self.started.send_replace(true);

		scheduler.wait().await?;
		Ok(())
	}
}

impl Owned {
	async fn wait(self) -> tg::Result<()> {
		let result = self.task.wait().await;
		if let Err(error) = result {
			return Err(tg::error!(!error, "the scheduler task panicked"));
		}
		Err(tg::error!("the scheduler stopped"))
	}
}

impl Scheduler {
	async fn start(
		component: &Component,
		server: &Server,
		config: &tangram_server_core::config::Scheduler,
	) -> tg::Result<Owned> {
		let id = tg::scheduler::Id::new();

		// Create the config.
		let config = Config {
			create_sandbox_queue_capacity: config.create_sandbox_queue_capacity,
			create_sandbox_timeout: config.create_sandbox_timeout,
			default_capacity: config.default_capacity(),
			heartbeat_interval: config.heartbeat_interval,
			inbox_ttl: config.inbox_ttl,
			max_create_sandbox_attempts: config.max_create_sandbox_attempts,
			max_create_sandbox_requests: config.max_create_sandbox_requests,
			max_create_sandbox_requests_per_runner: config.max_create_sandbox_requests_per_runner,
			request_concurrency: config.request_concurrency,
			runner_ttl: config.runner_ttl,
		};

		// Create the scheduler.
		let scheduler = Self {
			component: component.clone(),
			config,
			id,
			server: server.clone(),
		};
		// Subscribe to the scheduler's messages before starting, so that no message sent after the start is lost.
		let shared_stream = server
			.messenger
			.queue_subscribe::<Message>(scheduler_server_subject(None), "schedulers".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the shared scheduler stream"))?;
		let private_stream = server
			.messenger
			.subscribe::<Message>(scheduler_server_subject(Some(&scheduler.id)))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the private scheduler stream"))?;
		let stream = stream::select(shared_stream, private_stream).boxed();

		let task =
			Task::spawn(move |_| async move { scheduler.message_handler_task(stream).await });
		let owned = Owned { task };

		Ok(owned)
	}

	async fn message_handler_task(&self, mut stream: MessageStream) -> tg::Result<()> {
		let mut state = SchedulerState::new();
		let mut cleaner_interval = self.server.clock.interval(self.config.runner_ttl);
		cleaner_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval);
		heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			let event = self
				.next_event(
					&mut state,
					&mut stream,
					&mut cleaner_interval,
					&mut heartbeat_interval,
				)
				.await?;
			match event {
				Event::CleanerTick => {
					self.handle_cleaner_tick(&mut state);
				},
				Event::Message(message) => {
					self.handle_message(&mut state, message);
				},
				Event::Operation(operation) => {
					self.handle_operation(&mut state, operation);
				},
				Event::Schedule => {
					state.schedule_one(self);
				},
				Event::SchedulerHeartbeatTick => {
					self.publish_scheduler_heartbeat(&mut state);
				},
			}
		}
	}

	async fn next_event(
		&self,
		state: &mut SchedulerState,
		stream: &mut MessageStream,
		cleaner_interval: &mut tangram_server_core::clock::Interval,
		heartbeat_interval: &mut tokio::time::Interval,
	) -> tg::Result<Event> {
		let event = tokio::select! {
			_ = cleaner_interval.tick() => Event::CleanerTick,
			_ = heartbeat_interval.tick() => Event::SchedulerHeartbeatTick,
			result = Self::receive_message(stream) => Event::Message(result?),
			operation = state.operations.next(), if !state.operations.is_empty() => Event::Operation(operation.unwrap()),
			() = tokio::task::yield_now(), if state.can_schedule(self) => Event::Schedule,
		};

		Ok(event)
	}

	async fn receive_message(stream: &mut MessageStream) -> tg::Result<Message> {
		let message = stream
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to receive a scheduler message"))?
			.ok_or_else(|| tg::error!("the scheduler message stream ended"))?;

		Ok(message.payload)
	}

	fn handle_message(&self, state: &mut SchedulerState, message: Message) {
		match message {
			Message::Ack(ack) => {
				self.handle_ack(state, ack);
			},
			Message::Notification(notification) => match notification {
				Notification::BorrowableCapacity(notification) => {
					state.handle_borrowable_capacity(self, notification);
				},
				Notification::Heartbeat(notification) => {
					if state.handle_heartbeat(&notification, self.server.clock.instant()) {
						self.publish_runner_heartbeat_ack(state, &notification);
					}
				},
			},
			Message::Request(request) => {
				self.acknowledge_request(state, request);
			},
			Message::Response(_) => {},
		}
	}

	fn handle_ack(&self, state: &mut SchedulerState, ack: Ack) {
		if ack.scheduler != self.id {
			return;
		}
		if state.requests.outbox.remove(&ack.id).is_none() {
			return;
		}
		let duration = self.config.inbox_ttl;
		let sleep = self.server.clock.sleep(duration);
		state.operations.push(
			async move {
				sleep.await;
				Operation::ExpireRequest { id: ack.id }
			}
			.boxed(),
		);
	}

	fn acknowledge_request(&self, state: &mut SchedulerState, request: Request) {
		let id = request.id.clone();
		if state.requests.active.contains(&id) || state.requests.inbox.contains(&id) {
			let response = state.requests.outbox.get(&id).cloned();
			self.reply_to_request(state, id, response);
			return;
		}
		let kind = match &request.arg {
			RequestArg::AddRunner(_) => RequestKind::AddRunner,
			RequestArg::DequeueSandbox(_) => RequestKind::DequeueSandbox,
			RequestArg::EnqueueSandbox(_) => RequestKind::EnqueueSandbox,
			RequestArg::RemoveRunner(_) => RequestKind::RemoveRunner,
		};
		if !state
			.requests
			.active
			.try_insert(id.clone(), kind, self.config.request_concurrency)
		{
			let response = self.response(id.clone(), Ok(ResponseOutput::Busy));
			self.reply_to_request(state, id, Some(response));
			return;
		}
		let subject = Self::client_subject(&id);
		let message = Message::Ack(Ack {
			id: id.clone(),
			scheduler: self.id.clone(),
		});
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, message)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish a scheduler message"));
				Operation::Acknowledge { request, result }
			}
			.boxed(),
		);
	}

	fn reply_to_request(&self, state: &mut SchedulerState, id: String, response: Option<Response>) {
		// Bound duplicate and busy replies independently of accepted requests.
		if state.requests.replies >= self.config.request_concurrency {
			return;
		}
		state.requests.replies += 1;
		let server = self.server.clone();
		let scheduler = self.id.clone();
		state.operations.push(
			async move {
				let result = async {
					let subject = Self::client_subject(&id);
					let ack = Ack { id, scheduler };
					server
						.messenger
						.publish(subject.clone(), Message::Ack(ack))
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to publish the scheduler ack")
						})?;
					if let Some(response) = response {
						server
							.messenger
							.publish(subject, Message::Response(response))
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the scheduler response")
							})?;
					}
					Ok(())
				}
				.await;
				Operation::Reply { result }
			}
			.boxed(),
		);
	}

	fn handle_acknowledged_request(&self, state: &mut SchedulerState, request: Request) {
		let id = request.id.clone();

		if let Some(response) = state.requests.outbox.get(&id).cloned() {
			self.publish_response(state, response);
			return;
		}

		if !state.requests.inbox.insert(id) {
			return;
		}

		let id = request.id;
		match request.arg {
			RequestArg::AddRunner(request) => {
				state.handle_add_runner_request(self, id, request);
			},
			RequestArg::DequeueSandbox(request) => {
				if let Some(output) = state.dequeue_sandbox(id.clone(), request) {
					self.send_dequeue_sandbox_response(state, id, output);
				}
			},
			RequestArg::EnqueueSandbox(request) => {
				state.handle_enqueue_sandbox_request(self, id, request);
			},
			RequestArg::RemoveRunner(request) => {
				state.handle_remove_runner_request(self, Some(id), request);
			},
		}
	}

	fn handle_operation(&self, state: &mut SchedulerState, operation: Operation) {
		match operation {
			Operation::Acknowledge { request, result } => {
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to publish the ack");
				}
				self.handle_acknowledged_request(state, request);
			},
			Operation::AddRunner {
				connection_index,
				id,
				result,
				runner,
			} => {
				let current = state
					.runners
					.entries
					.get(&runner)
					.is_some_and(|runner| runner.connection_index == connection_index);
				let result = match result {
					Ok((output, owner)) => {
						if current {
							state.runners.entries.get_mut(&runner).unwrap().owner = owner;
						}
						Ok(output)
					},
					Err(error) => {
						if current {
							let completions = state.remove_runner(&runner);
							self.send_dequeue_sandbox_completions(state, completions);
						}
						Err(error)
					},
				};
				let result = result.map(ResponseOutput::AddRunner);
				let response = self.response(id, result);
				self.send_response(state, response);
			},
			Operation::CreateSandbox(completion) => {
				let output = state.handle_create_sandbox_completion(&self.config, completion);
				self.send_dequeue_sandbox_completions(state, output.dequeue_completions);
				if let Some(discarded) = output.discarded {
					self.publish_sandbox_discarded(state, discarded);
				}
			},
			Operation::ExpireRequest { id } => {
				state.requests.inbox.remove(&id);
			},
			Operation::LoadOwnerAncestors { owner, result } => {
				state.handle_load_owner_ancestors_completion(self, owner, result);
			},
			Operation::Publish { context, result } => {
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "{context}");
				}
			},
			Operation::RemoveRunner { id, result } => {
				if let Some(id) = id {
					let result = result.map(ResponseOutput::RemoveRunner);
					let response = self.response(id, result);
					self.send_response(state, response);
				} else if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to remove the expired runner");
				}
			},
			Operation::Reply { result } => {
				state.requests.replies -= 1;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to reply to a scheduler request");
				}
			},
			Operation::Response { id, result } => {
				state.requests.active.remove(&id);
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to publish the scheduler response");
				}
			},
			Operation::RetrySandbox { sandbox } => {
				state.handle_retry_sandbox(&sandbox);
			},
		}
	}

	fn handle_cleaner_tick(&self, state: &mut SchedulerState) {
		let now = self.server.clock.instant();
		let expired = state.runners.expired(now, self.config.runner_ttl);
		for (runner, connection_index) in expired {
			tracing::warn!(%runner, connection_index, ttl_ms = self.config.runner_ttl.as_millis(), "the runner heartbeat expired");
			let request = RemoveRunnerRequestArg {
				connection_index,
				runner,
			};
			state.handle_remove_runner_request(self, None, request);
		}
	}

	fn publish_response(&self, state: &mut SchedulerState, response: Response) {
		let subject = Self::client_subject(&response.id);
		let id = response.id.clone();
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, Message::Response(response))
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to publish the scheduler response")
					});
				Operation::Response { id, result }
			}
			.boxed(),
		);
	}

	fn publish_sandbox_discarded(
		&self,
		state: &mut SchedulerState,
		discarded: sandbox::DiscardedSandbox,
	) {
		let subject = tangram_server_sandbox_core::control::discarded_subject(&discarded.sandbox);
		let discarded = tangram_server_sandbox_core::control::Discarded {
			error: discarded.error,
		};
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, tangram_messenger::payload::Json(discarded))
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
		let response = self.response(id, Ok(ResponseOutput::DequeueSandbox(output)));
		self.send_response(state, response);
	}

	fn publish_scheduler_heartbeat(&self, state: &mut SchedulerState) {
		let id = self.id.clone();
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server.notifications.publish_scheduler_heartbeat(&id).await;
				Operation::Publish {
					context: "failed to publish the scheduler heartbeat",
					result,
				}
			}
			.boxed(),
		);
	}

	fn publish_runner_heartbeat_ack(
		&self,
		state: &mut SchedulerState,
		notification: &HeartbeatNotification,
	) {
		let subject = runner_heartbeat_subject(&self.id, &notification.runner);
		let acknowledgement = HeartbeatAcknowledgement {
			connection_index: notification.connection_index,
			heartbeat_index: notification.heartbeat_index,
		};
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, tangram_messenger::payload::Json(acknowledgement))
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

	fn response(&self, id: String, result: tg::Result<ResponseOutput>) -> Response {
		match result {
			Ok(output) => Response {
				error: None,
				id,
				output: Some(output),
				scheduler: self.id.clone(),
			},
			Err(error) => Response {
				error: Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				id,
				output: None,
				scheduler: self.id.clone(),
			},
		}
	}

	fn send_response(&self, state: &mut SchedulerState, response: Response) {
		state
			.requests
			.outbox
			.insert(response.id.clone(), response.clone());
		self.publish_response(state, response);
	}

	fn client_subject(id: impl Display) -> String {
		format!("scheduler.client.{id}")
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
			// Spawn the scheduler task.
			let task = self.spawn_scheduler_task(server);
			*self.task.lock().unwrap() = task;

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Abort the scheduler task.
			let task = self.task.lock().unwrap().take();
			if let Some(task) = task {
				task.abort();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the scheduler task panicked");
				}
				tracing::trace!("scheduler task");
			}
		}
		.boxed()
	}

	fn abort(&self) {
		let task = self.task.lock().unwrap().take();
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
		reason: &'a str,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.destroy_runner_sandbox(server, id, reason).boxed()
	}

	fn dequeue_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		sandbox: &'a tg::sandbox::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<bool>> {
		self.dequeue_sandbox(session, sandbox, scheduler).boxed()
	}

	fn enqueue_sandbox<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: EnqueueSandboxRequestArg,
	) -> BoxFuture<'a, tg::Result<tg::scheduler::Id>> {
		self.enqueue_sandbox(session, request).boxed()
	}

	fn scheduler_heartbeat_expired<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.scheduler_heartbeat_expired(session, scheduler).boxed()
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
}
