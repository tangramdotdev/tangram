use {
	crate::{Server, Session},
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	indoc::formatdoc,
	std::{
		collections::HashMap,
		fmt::Display,
		ops::{ControlFlow, Deref},
		pin::pin,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Task,
	tangram_messenger::Messenger,
};

mod runner;
mod sandbox;

struct Owned {
	scheduler: Scheduler,
	task: Task<tg::Result<()>>,
}

#[derive(Clone)]
struct Scheduler {
	state: Arc<State>,
}

struct State {
	config: Config,
	id: tg::scheduler::Id,
	inbox: DashMap<String, ()>,
	outbox: DashMap<String, Response>,
	runners: DashMap<tg::runner::Id, Runner>,
	server: Server,
}

struct Runner {
	capacity: tg::runner::control::Capacity,
	heartbeat_at: tokio::time::Instant,
	host: String,
	requests: usize,
	reservations: HashMap<tg::sandbox::Id, Reservation>,
}

struct Reservation {
	capacity: tg::runner::Capacity,
	state: ReservationState,
}

#[derive(Clone, Copy)]
enum ReservationState {
	Pending,
	Succeeded,
	Uncertain,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Message {
	Ack(Ack),
	Notification(Notification),
	Request(Request),
	Response(Response),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Ack {
	pub id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Notification {
	BorrowableCapacity(BorrowableCapacityNotification),
	CancelSandbox(CancelSandboxNotification),
	Heartbeat(HeartbeatNotification),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct BorrowableCapacityNotification {
	pub capacity: tg::runner::Capacity,
	pub parent: tg::sandbox::Id,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CancelSandboxNotification {
	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Request {
	pub arg: RequestArg,
	pub id: String,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum RequestArg {
	AddRunner(AddRunnerRequestArg),
	RemoveRunner(RemoveRunnerRequestArg),
	CreateSandbox(CreateSandboxRequestArg),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Response {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ResponseOutput {
	AddRunner(AddRunnerResponseOutput),
	RemoveRunner(RemoveRunnerResponseOutput),
	CreateSandbox(CreateSandboxResponseOutput),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct HeartbeatNotification {
	pub capacity: tg::runner::control::Capacity,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct AddRunnerRequestArg {
	pub capacity: tg::runner::control::Capacity,
	pub host: String,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct AddRunnerResponseOutput {
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoveRunnerRequestArg {
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoveRunnerResponseOutput {
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxRequestArg {
	pub arg: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub parent: Option<tg::sandbox::Id>,
	pub process: Option<tg::runner::control::Process>,
	pub sandbox: tg::sandbox::Id,
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxResponseOutput {
	pub enqueued: bool,
}

#[allow(dead_code)]
struct Config {
	create_sandbox_queue_capacity: usize,
	create_sandbox_timeout: Duration,
	default_capacity: tg::runner::Capacity,
	inbox_ttl: Duration,
	max_create_sandbox_requests: usize,
	max_create_sandbox_requests_per_runner: usize,
	runner_ttl: Duration,
}

struct SchedulerMessageOptions {
	retry: tangram_futures::retry::Options,
	timeout: Duration,
}

enum Event {
	Cancellation,
	Clean,
	Completion(sandbox::Completion),
	Message(Message),
}

impl Session {
	pub(crate) async fn enqueue_sandbox(&self, request: CreateSandboxRequestArg) -> tg::Result<()> {
		let mut options = self.scheduler_message_options().retry;
		options.max_retries = u64::MAX;
		let result = tangram_futures::retry::retry(&options, || {
			let request = request.clone();
			async move {
				let response = self
					.send_scheduler_request(RequestArg::CreateSandbox(request))
					.await
					.map_err(Some)?
					.map_err(Some)?;
				let response = response
					.try_unwrap_create_sandbox()
					.map_err(|_| Some(tg::error!("expected a create sandbox response")))?;
				if response.enqueued {
					Ok(ControlFlow::Break(()))
				} else {
					Ok(ControlFlow::Continue(None::<tg::Error>))
				}
			}
			.boxed()
		})
		.boxed()
		.await;
		match result {
			Ok(()) => Ok(()),
			Err(Some(error)) => Err(error),
			Err(None) => unreachable!(),
		}
	}

	pub(crate) async fn send_scheduler_request(
		&self,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let id = crate::control::id();
		let request = Request {
			arg,
			id: id.clone(),
		};
		let mut messages = self
			.send_scheduler_message(id.clone(), Message::Request(request))
			.await?;
		loop {
			let message = messages
				.next()
				.await
				.ok_or_else(|| tg::error!("the scheduler message stream ended"))?
				.map_err(|source| tg::error!(!source, "failed to receive the scheduler message"))?;
			let Message::Response(message) = message.payload else {
				continue;
			};
			if message.id != id {
				continue;
			}
			self.server
				.messenger
				.publish(
					Scheduler::server_subject(),
					Message::Ack(Ack { id: message.id }),
				)
				.await
				.inspect_err(|error| {
					tracing::error!(%error, "failed to acknowledge the scheduler response");
				})
				.ok();
			if let Some(error) = message.error {
				let error = tg::Error::try_from(error)
					.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
				break Ok(Err(error));
			}
			let Some(output) = message.output else {
				break Err(tg::error!("missing scheduler response output"));
			};
			break Ok(Ok(output));
		}
	}

	pub(crate) async fn send_scheduler_message(
		&self,
		id: String,
		message: Message,
	) -> tg::Result<
		BoxStream<'static, Result<tangram_messenger::Message<Message>, tangram_messenger::Error>>,
	> {
		let options = self.scheduler_message_options();
		let client_subject = Scheduler::client_subject(&id);
		let server = self.server.clone();
		let server_subject = Scheduler::server_subject();

		let result = tangram_futures::retry::retry(&options.retry, || {
			let client_subject = client_subject.clone();
			let id = id.clone();
			let message = message.clone();
			let server = server.clone();
			let server_subject = server_subject.clone();
			let timeout = options.timeout;
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
						if ack.id == id {
							return Ok::<_, tg::Error>(messages);
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
		})
		.await;

		match result {
			Ok(messages) => Ok(messages),
			Err(None) => Err(tg::error!("timed out waiting for the scheduler ack")),
			Err(Some(error)) => Err(error),
		}
	}

	fn scheduler_message_options(&self) -> SchedulerMessageOptions {
		let config = self.server.config.scheduler.clone().unwrap_or_default();
		SchedulerMessageOptions {
			retry: config.message_retry.into(),
			timeout: config.message_timeout,
		}
	}
}

impl Server {
	pub(crate) async fn scheduler_task(&self, config: &crate::config::Scheduler) {
		loop {
			let result = self.scheduler_task_inner(config).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the scheduler task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn scheduler_task_inner(&self, config: &crate::config::Scheduler) -> tg::Result<()> {
		let scheduler = Scheduler::start(self, config).await?;
		scheduler.wait().await?;
		Ok(())
	}

	/// Get the IDs of all started schedulers from the database.
	pub(crate) async fn get_started_schedulers(&self) -> tg::Result<Vec<tg::scheduler::Id>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		#[derive(Debug, db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::scheduler::Id,
		}
		let statement = formatdoc!(
			"
				select id
				from schedulers
				where status = 'started';
			"
		);
		let result = connection
			.query_all_into::<Row>(statement.into(), db::params![])
			.await;
		drop(connection);
		let schedulers =
			result.map_err(|error| tg::error!(!error, "failed to query started schedulers"))?;
		Ok(schedulers.into_iter().map(|row| row.id).collect())
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
	async fn start(server: &Server, config: &crate::config::Scheduler) -> tg::Result<Owned> {
		let id = tg::scheduler::Id::new();

		// Insert the scheduler into the database.
		server
			.database
			.run(|transaction| {
				let id = id.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into schedulers (id, status)
							values ({p}1, {p}2)
							on conflict (id) do update set status = {p}2;
						"
					);
					let params = db::params![id.to_string(), "started"];
					let result = transaction.execute(statement.into(), params).await;
					crate::database::retry!(result, "failed to execute the statement");
					Ok::<_, tg::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to upsert the scheduler"))?;

		// Create the config.
		let config = Config {
			create_sandbox_queue_capacity: config.create_sandbox_queue_capacity,
			create_sandbox_timeout: config.create_sandbox_timeout,
			default_capacity: tg::runner::Capacity {
				cpus: config.default_cpu,
				memory: config.default_memory,
			},
			inbox_ttl: config.inbox_ttl,
			max_create_sandbox_requests: config.max_create_sandbox_requests,
			max_create_sandbox_requests_per_runner: config.max_create_sandbox_requests_per_runner,
			runner_ttl: config.runner_ttl,
		};

		// Create the scheduler.
		let state = Arc::new(State {
			config,
			id,
			server: server.clone(),
			inbox: DashMap::new(),
			outbox: DashMap::new(),
			runners: DashMap::new(),
		});
		let scheduler = Self { state };

		let task = scheduler.spawn_task();

		let owned = Owned { scheduler, task };

		Ok(owned)
	}

	fn spawn_task(&self) -> Task<tg::Result<()>> {
		let scheduler = self.clone();
		Task::spawn(move |_| async move { scheduler.task().await })
	}

	async fn task(&self) -> tg::Result<()> {
		let message_handler_task = self.spawn_message_handler_task().await?;
		let _heartbeat_task = self.spawn_scheduler_heartbeat_task();
		message_handler_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the scheduler message handler task panicked"))??;
		Ok(())
	}

	async fn spawn_message_handler_task(&self) -> tg::Result<Task<tg::Result<()>>> {
		let stream = self
			.server
			.messenger
			.subscribe::<Message>(Self::server_subject())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the scheduler stream"))?;
		let scheduler = self.clone();
		let task =
			Task::spawn(
				move |_| async move { scheduler.message_handler_task(stream).boxed().await },
			);
		Ok(task)
	}

	fn spawn_scheduler_heartbeat_task(&self) -> Task<()> {
		let scheduler = self.clone();
		Task::spawn(async move |_| {
			scheduler.scheduler_heartbeat_task().await;
		})
	}

	async fn scheduler_heartbeat_task(&self) {
		let interval = Duration::from_secs(1);
		let mut interval = tokio::time::interval(interval);
		loop {
			interval.tick().await;
			let result = self
				.server
				.messenger
				.publish(
					"schedulers.heartbeat".into(),
					tangram_messenger::payload::Json(self.id.clone()),
				)
				.await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to publish the scheduler heartbeat");
			}
		}
	}

	async fn message_handler_task(
		&self,
		stream: impl futures::Stream<
			Item = Result<tangram_messenger::Message<Message>, tangram_messenger::Error>,
		> + Send
		+ 'static,
	) -> tg::Result<()> {
		let mut stream = pin!(stream);
		let mut scheduling = sandbox::State::new();
		let mut cleaner = tokio::time::interval(self.config.runner_ttl);
		cleaner.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			let event = tokio::select! {
				_ = cleaner.tick() => Event::Clean,
				cancellation = scheduling.cancellations.next(), if !scheduling.cancellations.is_empty() => {
					cancellation.unwrap();
					Event::Cancellation
				},
				result = stream.try_next() => {
					let message = result
						.map_err(|error| tg::error!(!error, "failed to receive a scheduler message"))?
						.ok_or_else(|| tg::error!("the scheduler message stream ended"))?;
					Event::Message(message.payload)
				},
				completion = scheduling.attempts.next(), if !scheduling.attempts.is_empty() => {
					Event::Completion(completion.unwrap())
				},
			};
			match event {
				Event::Cancellation => {},
				Event::Clean => {
					self.handle_cleaner_tick(&mut scheduling);
				},
				Event::Completion(completion) => {
					scheduling.handle_completion(self, completion);
				},
				Event::Message(message) => {
					self.handle_message(message, &mut scheduling).boxed().await;
				},
			}
			scheduling.schedule(self);
		}
	}

	async fn handle_message(&self, message: Message, scheduling: &mut sandbox::State) {
		match message {
			Message::Ack(ack) => {
				self.handle_ack(ack);
			},
			Message::Notification(notification) => match notification {
				Notification::BorrowableCapacity(notification) => {
					scheduling.handle_borrowable_capacity(notification);
				},
				Notification::CancelSandbox(notification) => {
					scheduling.handle_cancel_sandbox(self, notification);
				},
				Notification::Heartbeat(notification) => {
					scheduling.handle_heartbeat(self, &notification);
				},
			},
			Message::Request(request) => {
				self.handle_request(request, scheduling).await;
			},
			Message::Response(_) => {},
		}
	}

	fn handle_ack(&self, ack: Ack) {
		self.outbox.remove(&ack.id);
		tokio::spawn({
			let scheduler = self.clone();
			async move {
				tokio::time::sleep(scheduler.config.inbox_ttl).await;
				scheduler.inbox.remove(&ack.id);
			}
		});
	}

	async fn handle_request(&self, request: Request, scheduling: &mut sandbox::State) {
		let id = request.id.clone();
		let subject = Self::client_subject(&id);
		let message = Message::Ack(Ack { id: id.clone() });
		self.server
			.messenger
			.publish(subject, message)
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to publish the ack");
			})
			.ok();

		if let Some(response) = self.outbox.get(&id).map(|entry| entry.value().clone()) {
			self.send_response(response).await;
			return;
		}

		if self.inbox.insert(id, ()).is_some() {
			return;
		}

		let id = request.id;
		match request.arg {
			RequestArg::AddRunner(request) => {
				self.handle_add_runner_request(id, request).await;
			},
			RequestArg::RemoveRunner(request) => {
				scheduling.handle_remove_runner(self, &request.runner);
				self.handle_remove_runner_request(id, request).await;
			},
			RequestArg::CreateSandbox(request) => {
				scheduling
					.handle_create_sandbox_request(self, id, request)
					.await;
			},
		}
	}

	async fn send_response(&self, response: Response) {
		let subject = Self::client_subject(&response.id);
		self.outbox.insert(response.id.clone(), response.clone());
		self.server
			.messenger
			.publish(subject, Message::Response(response))
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to publish the scheduler response");
			})
			.ok();
	}

	fn handle_cleaner_tick(&self, scheduling: &mut sandbox::State) {
		let now = tokio::time::Instant::now();
		let expired = self
			.runners
			.iter()
			.filter(|entry| now.duration_since(entry.heartbeat_at) > self.config.runner_ttl)
			.map(|entry| entry.key().clone())
			.collect::<Vec<_>>();
		for runner in expired {
			scheduling.handle_remove_runner(self, &runner);
			self.handle_expired_runner(runner);
		}
	}

	fn client_subject(id: impl Display) -> String {
		format!("scheduler.client.{id}")
	}

	fn server_subject() -> String {
		"scheduler.server".to_owned()
	}
}

impl Deref for Owned {
	type Target = Scheduler;

	fn deref(&self) -> &Self::Target {
		&self.scheduler
	}
}

impl Deref for Scheduler {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		self.state.as_ref()
	}
}

impl tangram_messenger::Payload for Message {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(message)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}
