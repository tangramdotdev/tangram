use {
	crate::{
		Server,
		runner::control::{RunnerControlClientMessage, RunnerControlServerMessage},
	},
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{collections::BTreeSet, pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger,
};

struct Scheduler {
	config: Config,
	responses: DashMap<String, CachedResponse>,
	runners: DashMap<tg::runner::Id, Runner>,
	server: Server,
}

#[derive(Clone, Debug)]
enum CachedResponse {
	Pending,
	Ready(Response),
}

struct Runner {
	cpus: tg::runner::control::Capacity,
	heartbeat_at: tokio::time::Instant,
	host: String,
	memory: tg::runner::control::Capacity,
	permits: tg::runner::control::Capacity,
	sandboxes: BTreeSet<tg::sandbox::Id>,
	#[expect(dead_code)]
	task: Option<Task<()>>,
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
	Register(RegisterNotification),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Request {
	CreateSandbox(CreateSandboxRequest),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Response {
	CreateSandbox(CreateSandboxResponse),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RegisterNotification {
	pub arg: tg::runner::control::Arg,
	pub id: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxRequest {
	pub arg: tg::sandbox::create::Arg,
	pub id: String,
	pub process: Option<tg::process::Id>,
	pub process_token: Option<String>,
	pub sandbox: tg::sandbox::Id,
	pub token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxResponse {
	pub created: bool,
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub sandbox: tg::sandbox::Id,
}

struct Config {
	control_ttl: Duration,
	create_sandbox_timeout: Duration,
	runner_ttl: Duration,
}

impl Response {
	fn id(&self) -> &str {
		match self {
			Self::CreateSandbox(response) => &response.id,
		}
	}

	fn subject(&self) -> String {
		match self {
			Self::CreateSandbox(response) => format!("scheduler.create-sandbox.{}", response.id),
		}
	}
}

impl Server {
	pub(crate) async fn scheduler_task(&self, config: &crate::config::Scheduler) {
		loop {
			if let Err(error) = self.scheduler_task_inner(config).await {
				tracing::error!(error = %error.trace(), "the scheduler task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn scheduler_task_inner(&self, config: &crate::config::Scheduler) -> tg::Result<()> {
		// Create the scheduler.
		let scheduler = Arc::new(Scheduler {
			config: Config {
				control_ttl: config.control_ttl,
				create_sandbox_timeout: config.create_sandbox_timeout,
				runner_ttl: config.runner_ttl,
			},
			server: self.clone(),
			runners: DashMap::new(),
			responses: DashMap::new(),
		});

		// Create the task to dispatch scheduler messages.
		let request_task = Task::spawn({
			let stream = self
				.messenger
				.subscribe::<Message>("scheduler".into())
				.await
				.map_err(|source| tg::error!(!source, "failed to get the scheduler stream"))?;
			let scheduler = scheduler.clone();
			async move |_stop| {
				let mut stream = pin!(stream);
				loop {
					let message = match stream.try_next().await {
						Ok(Some(message)) => message,
						Ok(None) => break,
						Err(error) => {
							tracing::error!(%error, "failed to receive a scheduler message");
							continue;
						},
					};
					match message.payload {
						Message::Ack(ack) => {
							// Mark the response as acked and remove it after the TTL.
							scheduler
								.responses
								.insert(ack.id.clone(), CachedResponse::Pending);
							tokio::spawn({
								let scheduler = scheduler.clone();
								async move {
									tokio::time::sleep(scheduler.config.control_ttl).await;
									scheduler.responses.remove(&ack.id);
								}
							});
						},
						Message::Notification(notification) => match notification {
							Notification::Register(register) => {
								let id = register.id;

								// The handshake retries, so do not spawn a duplicate task.
								if !scheduler.runners.contains_key(&id)
									&& let Err(error) =
										scheduler.spawn_runner_task(&id, register.arg).await
								{
									tracing::error!(%error, "failed to spawn the runner task");
									continue;
								}

								// Ack the runner registration.
								scheduler
									.server
									.messenger
									.publish(
										format!("scheduler.register.{id}"),
										Message::Ack(Ack { id: id.to_string() }),
									)
									.await
									.inspect_err(|error| {
										tracing::error!(%error, "failed to publish the ack");
									})
									.ok();
							},
						},
						Message::Request(Request::CreateSandbox(request)) => {
							// If there is already an entry for the request, then it is a duplicate. Resend the cached response if there is one, otherwise drop the request because it is in flight or acked.
							if let Some(cached) = scheduler.cached_response(&request.id) {
								if let CachedResponse::Ready(response) = cached {
									scheduler.respond(response).await;
								}
								continue;
							}

							// Mark the request as in flight so that duplicates are dropped until the response is ready.
							scheduler
								.responses
								.insert(request.id.clone(), CachedResponse::Pending);

							// Handle each request concurrently so that waiting for a runner's response does not block other requests.
							let scheduler = scheduler.clone();
							tokio::spawn(async move {
								scheduler.handle_create_sandbox(request).await;
							});
						},
						Message::Response(_) => {},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let _watchdog_task = Task::spawn({
			let scheduler = scheduler.clone();
			async move |_stop| {
				scheduler.reap_runners().await;
			}
		});

		// Wait for the request task to exit.
		let request = pin!(request_task.wait());
		let _ = request.await;

		Err(tg::error!("the scheduler stopped"))
	}
}

impl Scheduler {
	fn cached_response(&self, id: &str) -> Option<CachedResponse> {
		self.responses.get(id).map(|entry| entry.value().clone())
	}

	async fn cache_and_respond(&self, response: Response) {
		self.responses.insert(
			response.id().to_owned(),
			CachedResponse::Ready(response.clone()),
		);
		self.respond(response).await;
	}

	async fn respond(&self, response: Response) {
		let subject = response.subject();
		self.server
			.messenger
			.publish(subject, Message::Response(response))
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to publish the scheduler response");
			})
			.ok();
	}

	async fn spawn_runner_task(
		self: &Arc<Self>,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
	) -> tg::Result<()> {
		// Subscribe to the runner's client message stream.
		let client_messages = self
			.server
			.messenger
			.subscribe::<RunnerControlClientMessage>(format!("runners.{id}.client"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the runner client message stream"
				)
			})?
			.boxed();
		let task = Task::spawn({
			let scheduler = self.clone();
			let id = id.clone();
			async move |stop| {
				let stop = stop.wait();
				let future = scheduler
					.clone()
					.runner_task(&id, client_messages)
					.inspect_err(
						|error| tracing::error!(%error, runner = %id, "the runner task failed"),
					)
					.map(|_| ());
				future::select(pin!(future), pin!(stop)).await;
				scheduler.remove_runner(&id);
			}
		});
		let runner = Runner {
			host: arg.host,
			task: Some(task),
			cpus: tg::runner::control::Capacity::default(),
			memory: tg::runner::control::Capacity::default(),
			permits: tg::runner::control::Capacity::default(),
			heartbeat_at: tokio::time::Instant::now(),
			sandboxes: BTreeSet::new(),
		};
		self.runners.insert(id.clone(), runner);
		Ok(())
	}

	async fn reap_runners(&self) {
		let mut interval = tokio::time::interval(self.config.runner_ttl);
		loop {
			interval.tick().await;
			let now = tokio::time::Instant::now();
			let expired = self
				.runners
				.iter()
				.filter(|entry| now.duration_since(entry.heartbeat_at) > self.config.runner_ttl)
				.map(|entry| entry.key().clone())
				.collect::<Vec<_>>();
			for id in expired {
				self.remove_runner(&id);
			}
		}
	}

	async fn handle_create_sandbox(self: Arc<Self>, message: CreateSandboxRequest) {
		let CreateSandboxRequest {
			id,
			sandbox,
			arg,
			process,
			token,
			process_token,
		} = message;

		let deadline = tokio::time::Instant::now() + self.config.create_sandbox_timeout;
		let mut excluded = BTreeSet::new();
		let response = loop {
			if tokio::time::Instant::now() >= deadline {
				break CreateSandboxResponse {
					id: id.clone(),
					sandbox: sandbox.clone(),
					created: false,
					error: None,
				};
			}

			// Find a runner with a matching host.
			let Some(runner) = self.find_runner(arg.host.as_ref(), &excluded) else {
				excluded.clear();
				tokio::time::sleep(Duration::from_millis(10)).await;
				continue;
			};

			// Subscribe to the runner response before publishing so that the response is not missed.
			let dispatch_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
			let Ok(runner_response) = self
				.server
				.messenger
				.subscribe::<RunnerControlClientMessage>(format!(
					"runners.{runner}.client.{dispatch_id}"
				))
				.await
			else {
				tracing::error!("failed to subscribe to the runner response");
				excluded.insert(runner);
				continue;
			};
			let mut runner_response = pin!(runner_response.fuse());

			// Dispatch the sandbox to the runner, republishing with backoff for reliability until the runner responds.
			let subject = format!("runners.{runner}.server");
			let message = tg::runner::control::ServerMessage::Request(
				tg::runner::control::ServerRequest::CreateSandbox(
					tg::runner::control::CreateSandboxServerRequest {
						arg: arg.clone(),
						id: dispatch_id.clone(),
						process: process.clone(),
						process_token: process_token.clone(),
						sandbox: sandbox.clone(),
						token: token.clone(),
					},
				),
			);
			let options = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let mut retries = pin!(tangram_futures::retry::stream(options));
			let runner_response = loop {
				match future::select(runner_response.next(), retries.next()).await {
					future::Either::Left((response, _)) => {
						let Some(response) = response else {
							break None;
						};
						let response = match response {
							Ok(response) => response,
							Err(error) => {
								tracing::error!(%error, "failed to receive the runner response");
								break None;
							},
						};
						let tg::runner::control::ClientMessage::Response(
							tg::runner::control::ClientResponse::CreateSandbox(response),
						) = response.payload.0
						else {
							break None;
						};
						break Some(response);
					},
					future::Either::Right((_tick, _)) => {
						if tokio::time::Instant::now() >= deadline {
							break None;
						}
						self.server
							.messenger
							.publish(subject.clone(), RunnerControlServerMessage(message.clone()))
							.await
							.inspect_err(|error| {
								tracing::error!(%error, "failed to publish the create sandbox request");
							})
							.ok();
					},
				}
			};

			// Ack the runner.
			if runner_response.is_some() {
				self.server
					.messenger
					.publish(
						subject.clone(),
						RunnerControlServerMessage(tg::runner::control::ServerMessage::Ack(
							tg::runner::control::ServerAck { id: dispatch_id },
						)),
					)
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to acknowledge the runner response");
					})
					.ok();
			}

			// Check if the sandbox was actually created, or the runner rejected the request.
			if runner_response.is_some_and(|response| response.created) {
				// Record the sandbox's placement so that processes can be routed to it.
				self.register_sandbox(&sandbox, &runner);
				break CreateSandboxResponse {
					id: id.clone(),
					sandbox: sandbox.clone(),
					created: true,
					error: None,
				};
			}
			excluded.insert(runner);
		};

		self.cache_and_respond(Response::CreateSandbox(response))
			.await;
	}

	fn find_runner(
		&self,
		host: Option<&String>,
		excluded: &BTreeSet<tg::runner::Id>,
	) -> Option<tg::runner::Id> {
		self.runners
			.iter()
			.find(|entry| {
				!excluded.contains(entry.key())
					&& host.is_none_or(|host| host == &entry.host)
					&& entry.permits.used < entry.permits.total
			})
			.map(|entry| entry.key().clone())
	}

	fn remove_runner(&self, id: &tg::runner::Id) {
		if let Some((_, runner)) = self.runners.remove(id) {
			for sandbox in runner.sandboxes {
				// Destroy the sandbox with a heartbeat expiration error so that its processes finish.
				let server = self.server.clone();
				tokio::spawn(async move {
					let session = server.session(&server.context);
					let error = tg::error::Data {
						code: Some(tg::error::Code::HeartbeatExpiration),
						message: Some("heartbeat expired".to_owned()),
						..Default::default()
					};
					let result = session
						.try_destroy_sandbox_local(&sandbox, Some(tg::Either::Left(error)), None)
						.await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), %sandbox, "failed to destroy the sandbox after the runner was lost");
					}
				});
			}
		}
	}

	fn register_sandbox(self: &Arc<Self>, sandbox: &tg::sandbox::Id, runner: &tg::runner::Id) {
		if let Some(mut entry) = self.runners.get_mut(runner) {
			entry.sandboxes.insert(sandbox.clone());
		}
	}

	async fn runner_task(
		self: Arc<Self>,
		id: &tg::runner::Id,
		mut stream: futures::stream::BoxStream<
			'static,
			Result<
				tangram_messenger::Message<RunnerControlClientMessage>,
				tangram_messenger::Error,
			>,
		>,
	) -> tg::Result<()> {
		while let Some(message) = stream.try_next().await.map_err(
			|source| tg::error!(!source, runner = %id, "failed to receive a runner client message"),
		)? {
			match message.payload.0 {
				tg::runner::control::ClientMessage::Ack(_)
				| tg::runner::control::ClientMessage::Response(_) => {},
				tg::runner::control::ClientMessage::Notification(notification) => {
					match notification {
						tg::runner::control::ClientNotification::Heartbeat(heartbeat) => {
							if let Some(mut entry) = self.runners.get_mut(id) {
								entry.cpus = heartbeat.cpus;
								entry.memory = heartbeat.memory;
								entry.permits = heartbeat.permits;
								entry.heartbeat_at = tokio::time::Instant::now();
							}
						},
						tg::runner::control::ClientNotification::SandboxCreated(
							sandbox_created,
						) => {
							self.register_sandbox(&sandbox_created.sandbox, id);
							self.server
								.messenger
								.publish(
									format!("runners.{id}.server"),
									RunnerControlServerMessage(
										tg::runner::control::ServerMessage::Ack(
											tg::runner::control::ServerAck {
												id: sandbox_created.id,
											},
										),
									),
								)
								.await
								.inspect_err(|error| {
									tracing::error!(
										%error,
										"failed to acknowledge the sandbox created message"
									);
								})
								.ok();
						},
						tg::runner::control::ClientNotification::SandboxDestroyed(
							sandbox_destroyed,
						) => {
							if let Some(mut entry) = self.runners.get_mut(id) {
								entry.sandboxes.remove(&sandbox_destroyed.sandbox);
							}
							self.server
								.messenger
								.publish(
									format!("runners.{id}.server"),
									RunnerControlServerMessage(
										tg::runner::control::ServerMessage::Ack(
											tg::runner::control::ServerAck {
												id: sandbox_destroyed.id,
											},
										),
									),
								)
								.await
								.inspect_err(|error| {
									tracing::error!(
										%error,
										"failed to acknowledge the sandbox destroyed message"
									);
								})
								.ok();
						},
					}
				},
				tg::runner::control::ClientMessage::Request(request) => match request {},
			}
		}
		Ok(())
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
