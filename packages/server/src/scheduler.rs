use {
	crate::{Server, Session},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	std::{
		collections::{HashMap, HashSet},
		fmt::Display,
		ops::ControlFlow,
		pin::pin,
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
	task: Task<tg::Result<()>>,
}

#[derive(Clone)]
struct Scheduler {
	config: Config,
	id: tg::scheduler::Id,
	server: Server,
}

struct State {
	operations: Operations,
	parents: sandbox::Parents,
	queue: sandbox::Queue,
	requests: Requests,
	runners: runner::Runners,
	sandboxes: sandbox::Sandboxes,
}

type Operations = FuturesUnordered<BoxFuture<'static, Operation>>;

struct Requests {
	inbox: HashSet<String>,
	outbox: HashMap<String, Response>,
}

enum Operation {
	Acknowledge {
		request: Request,
		result: tg::Result<()>,
	},
	AddRunner {
		connection_index: u64,
		id: String,
		result: tg::Result<AddRunnerResponseOutput>,
		runner: tg::runner::Id,
	},
	CancelSandbox,
	CreateSandbox(sandbox::Completion),
	ExpireRequest {
		id: String,
	},
	Publish {
		context: &'static str,
		result: tg::Result<()>,
	},
	RemoveRunner {
		id: Option<String>,
		result: tg::Result<RemoveRunnerResponseOutput>,
	},
}

enum Event {
	Clean,
	Message(Message),
	Operation(Operation),
	Schedule,
	SchedulerHeartbeat,
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
	pub connection_index: u64,
	pub heartbeat_index: u64,
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
	pub connection_index: u64,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoveRunnerRequestArg {
	pub connection_index: u64,
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
#[derive(Clone)]
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

impl Session {
	pub(crate) async fn enqueue_sandbox(&self, request: CreateSandboxRequestArg) -> tg::Result<()> {
		let mut options = self.scheduler_message_options().retry;
		options.max_retries = u64::MAX;
		let result = tangram_futures::retry::retry(&options, || {
			let request = request.clone();
			async move {
				let response = self
					.send_scheduler_request(RequestArg::CreateSandbox(request))
					.boxed()
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
			.boxed()
		})
		.await;

		match result {
			Ok(messages) => Ok(messages),
			Err(None) => Err(tg::error!("timed out waiting for the scheduler ack")),
			Err(Some(error)) => Err(error),
		}
	}

	fn scheduler_message_options(&self) -> SchedulerMessageOptions {
		let config = self.server.config.scheduler.clone();
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

	/// Get the IDs of all started schedulers in the current region from the database.
	pub(crate) async fn get_started_schedulers(&self) -> tg::Result<Vec<tg::scheduler::Id>> {
		let region = self.config.region.clone();
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
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from schedulers
				where (
					(region is null and {p}1 is null) or
					region = {p}1
				) and status = 'started';
			"
		);
		let result = connection
			.query_all_into::<Row>(statement.into(), db::params![region])
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
		let region = server.config.region.clone();

		// Insert the scheduler into the database.
		server
			.database
			.run(|transaction| {
				let id = id.clone();
				let region = region.clone();
				async move {
					let p = transaction.p();
					let statement = formatdoc!(
						"
							insert into schedulers (id, region, status)
							values ({p}1, {p}2, {p}3)
							on conflict (id) do update set region = {p}2, status = {p}3;
						"
					);
					let params = db::params![id.to_string(), region, "started"];
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
		let scheduler = Self {
			config,
			id,
			server: server.clone(),
		};
		let task = scheduler.spawn_task();
		let owned = Owned { task };

		Ok(owned)
	}

	fn spawn_task(self) -> Task<tg::Result<()>> {
		Task::spawn(move |_| async move { self.task().await })
	}

	async fn task(self) -> tg::Result<()> {
		let stream = self
			.server
			.messenger
			.subscribe::<Message>(Self::server_subject())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the scheduler stream"))?;

		self.message_handler_task(stream).await
	}

	async fn message_handler_task(
		&self,
		stream: impl futures::Stream<
			Item = Result<tangram_messenger::Message<Message>, tangram_messenger::Error>,
		> + Send
		+ 'static,
	) -> tg::Result<()> {
		let mut state = State::new();
		let mut stream = pin!(stream);
		let mut cleaner = tokio::time::interval(self.config.runner_ttl);
		cleaner.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
		heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			let event = tokio::select! {
				_ = cleaner.tick() => Event::Clean,
				_ = heartbeat.tick() => Event::SchedulerHeartbeat,
				result = stream.try_next() => {
					let message = result
						.map_err(|error| tg::error!(!error, "failed to receive a scheduler message"))?
						.ok_or_else(|| tg::error!("the scheduler message stream ended"))?;
					Event::Message(message.payload)
				},
				operation = state.operations.next(), if !state.operations.is_empty() => {
					Event::Operation(operation.unwrap())
				},
				() = tokio::task::yield_now(), if state.can_schedule(self) => {
					Event::Schedule
				},
			};
			match event {
				Event::Clean => {
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
				Event::SchedulerHeartbeat => {
					self.publish_scheduler_heartbeat(&mut state);
				},
			}
		}
	}

	fn handle_message(&self, state: &mut State, message: Message) {
		match message {
			Message::Ack(ack) => {
				self.handle_ack(state, ack);
			},
			Message::Notification(notification) => match notification {
				Notification::BorrowableCapacity(notification) => {
					state.handle_borrowable_capacity(self, notification);
				},
				Notification::CancelSandbox(notification) => {
					state.handle_cancel_sandbox(self, notification);
				},
				Notification::Heartbeat(notification) => {
					state.handle_heartbeat(self, &notification);
				},
			},
			Message::Request(request) => {
				self.acknowledge_request(state, request);
			},
			Message::Response(_) => {},
		}
	}

	fn handle_ack(&self, state: &mut State, ack: Ack) {
		state.requests.outbox.remove(&ack.id);
		let duration = self.config.inbox_ttl;
		state.operations.push(
			async move {
				tokio::time::sleep(duration).await;
				Operation::ExpireRequest { id: ack.id }
			}
			.boxed(),
		);
	}

	fn acknowledge_request(&self, state: &mut State, request: Request) {
		let id = request.id.clone();
		let subject = Self::client_subject(&id);
		let message = Message::Ack(Ack { id: id.clone() });
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

	fn handle_acknowledged_request(&self, state: &mut State, request: Request) {
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
			RequestArg::RemoveRunner(request) => {
				state.handle_remove_runner_request(self, Some(id), request);
			},
			RequestArg::CreateSandbox(request) => {
				let result = state.enqueue_sandbox(self, request);
				let result = result.map(ResponseOutput::CreateSandbox);
				let response = Self::response(id, result);
				self.send_response(state, response);
			},
		}
	}

	fn handle_operation(&self, state: &mut State, operation: Operation) {
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
				if current {
					if result.is_ok() {
						state.runners.entries.get_mut(&runner).unwrap().ready = true;
						state.queue.wake();
					} else {
						state.remove_runner(&runner);
					}
				}
				let result = result.map(ResponseOutput::AddRunner);
				let response = Self::response(id, result);
				self.send_response(state, response);
			},
			Operation::CancelSandbox => {},
			Operation::CreateSandbox(completion) => {
				state.handle_create_sandbox_completion(self, completion);
			},
			Operation::ExpireRequest { id } => {
				state.requests.inbox.remove(&id);
			},
			Operation::Publish { context, result } => {
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "{context}");
				}
			},
			Operation::RemoveRunner { id, result } => {
				if let Some(id) = id {
					let result = result.map(ResponseOutput::RemoveRunner);
					let response = Self::response(id, result);
					self.send_response(state, response);
				} else if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to remove the expired runner");
				}
			},
		}
	}

	fn handle_cleaner_tick(&self, state: &mut State) {
		let now = tokio::time::Instant::now();
		let expired = state.runners.expired(now, self.config.runner_ttl);
		for (runner, connection_index) in expired {
			let request = RemoveRunnerRequestArg {
				connection_index,
				runner,
			};
			state.handle_remove_runner_request(self, None, request);
		}
	}

	fn publish_message(
		&self,
		state: &mut State,
		subject: String,
		message: Message,
		context: &'static str,
	) {
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(subject, message)
					.await
					.map_err(|source| tg::error!(!source, "failed to publish a scheduler message"));
				Operation::Publish { context, result }
			}
			.boxed(),
		);
	}

	fn publish_response(&self, state: &mut State, response: Response) {
		let subject = Self::client_subject(&response.id);
		self.publish_message(
			state,
			subject,
			Message::Response(response),
			"failed to publish the scheduler response",
		);
	}

	fn publish_scheduler_heartbeat(&self, state: &mut State) {
		let id = self.id.clone();
		let server = self.server.clone();
		state.operations.push(
			async move {
				let result = server
					.messenger
					.publish(
						"schedulers.heartbeat".into(),
						tangram_messenger::payload::Json(id),
					)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to publish the scheduler heartbeat")
					});
				Operation::Publish {
					context: "failed to publish the scheduler heartbeat",
					result,
				}
			}
			.boxed(),
		);
	}

	fn response(id: String, result: tg::Result<ResponseOutput>) -> Response {
		match result {
			Ok(output) => Response {
				error: None,
				id,
				output: Some(output),
			},
			Err(error) => Response {
				error: Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				id,
				output: None,
			},
		}
	}

	fn send_response(&self, state: &mut State, response: Response) {
		state
			.requests
			.outbox
			.insert(response.id.clone(), response.clone());
		self.publish_response(state, response);
	}

	fn client_subject(id: impl Display) -> String {
		format!("scheduler.client.{id}")
	}

	fn server_subject() -> String {
		"scheduler.server".to_owned()
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
