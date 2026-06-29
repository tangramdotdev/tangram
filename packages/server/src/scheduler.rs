use {
	crate::{
		Server,
		runner::control::{RunnerControlClientMessage, RunnerControlServerMessage},
		sandbox::control::SandboxControlServerMessage,
	},
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{collections::BTreeSet, pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger,
};

struct Scheduler {
	control_ttl: Duration,
	create_sandbox_timeout: Duration,
	process_cancellations: DashMap<tg::process::Id, ()>,
	process_dispatches: DashMap<
		String,
		tokio::sync::oneshot::Sender<tg::sandbox::control::SpawnProcessClientResponse>,
	>,
	processes: DashMap<tg::process::Id, tg::runner::Id>,
	responses: DashMap<String, CachedResponse>,
	runners: DashMap<tg::runner::Id, Runner>,
	sandbox_creations: DashMap<
		String,
		tokio::sync::oneshot::Sender<tg::runner::control::CreateSandboxClientResponse>,
	>,
	sandboxes: DashMap<tg::sandbox::Id, tg::runner::Id>,
	server: Server,
	spawn_process_timeout: Duration,
}

struct Runner {
	cpus: tg::runner::control::Capacity,
	heartbeat_at: tokio::time::Instant,
	host: String,
	memory: tg::runner::control::Capacity,
	permits: tg::runner::control::Capacity,
	processes: BTreeSet<tg::process::Id>,
	sandboxes: BTreeSet<tg::sandbox::Id>,
	#[expect(dead_code)]
	task: Option<Task<()>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ListenMessage {
	pub(crate) arg: tg::runner::control::Arg,
	pub(crate) id: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Request {
	Ack(Ack),
	CancelProcess(CancelProcessRequest),
	CreateSandbox(CreateSandboxRequest),
	SandboxInput(SandboxInputRequest),
	SpawnProcess(SpawnProcessRequest),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum Response {
	CreateSandbox(CreateSandboxResponse),
	SpawnProcess(SpawnProcessResponse),
}

#[derive(Clone, Debug)]
enum CachedResponse {
	Pending,
	Ready(Response),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Ack {
	pub(crate) id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxRequest {
	pub(crate) arg: tg::sandbox::create::Arg,
	pub(crate) id: String,
	pub(crate) process: Option<tg::process::Id>,
	pub(crate) process_token: Option<String>,
	pub(crate) sandbox: tg::sandbox::Id,
	pub(crate) token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SpawnProcessRequest {
	pub(crate) id: String,
	pub(crate) process: tg::process::Id,
	pub(crate) process_token: Option<String>,
	pub(crate) sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SpawnProcessResponse {
	pub(crate) error: Option<tg::error::Data>,
	pub(crate) id: String,
	pub(crate) process: tg::process::Id,
	pub(crate) spawned: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SandboxInputRequest {
	pub(crate) message: tg::sandbox::control::ClientMessage,
	pub(crate) sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CancelProcessRequest {
	pub(crate) id: tg::process::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxResponse {
	pub(crate) created: bool,
	pub(crate) error: Option<tg::error::Data>,
	pub(crate) id: String,
	pub(crate) sandbox: tg::sandbox::Id,
}

impl Response {
	fn id(&self) -> &str {
		match self {
			Self::CreateSandbox(response) => &response.id,
			Self::SpawnProcess(response) => &response.id,
		}
	}

	fn subject(&self) -> String {
		match self {
			Self::CreateSandbox(response) => format!("scheduler.create-sandbox.{}", response.id),
			Self::SpawnProcess(response) => format!("scheduler.spawn-process.{}", response.id),
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
			server: self.clone(),
			runners: DashMap::new(),
			processes: DashMap::new(),
			sandboxes: DashMap::new(),
			sandbox_creations: DashMap::new(),
			process_dispatches: DashMap::new(),
			responses: DashMap::new(),
			process_cancellations: DashMap::new(),
			control_ttl: config.control_ttl,
			create_sandbox_timeout: config.create_sandbox_timeout,
			spawn_process_timeout: config.spawn_process_timeout,
		});

		let listener_task = Task::spawn({
			let subject = "scheduler.listen".into();
			let stream = self
				.messenger
				.subscribe::<ListenMessage>(subject)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to get the runner listener stream")
				})?;
			let scheduler = scheduler.clone();
			async move |_stop| {
				let mut stream = pin!(stream);
				loop {
					let message = match stream.try_next().await {
						Ok(Some(message)) => message,
						Ok(None) => break,
						Err(error) => {
							tracing::error!(%error, "failed to receive a runner connection message");
							continue;
						},
					};
					let id = message.payload.id;

					// The handshake retries, so do not spawn a duplicate task.
					if !scheduler.runners.contains_key(&id)
						&& let Err(error) =
							scheduler.spawn_runner_task(&id, message.payload.arg).await
					{
						tracing::error!(%error, "failed to spawn the runner task");
						continue;
					}

					// Acknowledge the runner's connection.
					scheduler
						.server
						.messenger
						.publish(format!("scheduler.listen.{id}"), ())
						.await
						.inspect_err(|error| {
							tracing::error!(%error, "failed to publish the acknowledgement");
						})
						.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Create the task to dispatch runner messages.
		let request_task = Task::spawn({
			let stream = self
				.messenger
				.subscribe::<Request>("scheduler".into())
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
						Request::CreateSandbox(request) => {
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
						Request::SpawnProcess(request) => {
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

							// Handle each request concurrently so that waiting for a sandbox's response does not block other requests.
							let scheduler = scheduler.clone();
							tokio::spawn(async move {
								scheduler.handle_spawn_process(request).await;
							});
						},
						Request::CancelProcess(request) => {
							// Mark the process as cancelled so that the outstanding and any subsequent
							// dispatch loops for it break. Remove the marker after the TTL.
							scheduler
								.process_cancellations
								.insert(request.id.clone(), ());
							tokio::spawn({
								let scheduler = scheduler.clone();
								async move {
									tokio::time::sleep(scheduler.control_ttl).await;
									scheduler.process_cancellations.remove(&request.id);
								}
							});
						},
						Request::SandboxInput(request) => {
							scheduler.handle_sandbox_input(request);
						},
						Request::Ack(ack) => {
							// Mark the response as acked and remove it after the TTL.
							scheduler
								.responses
								.insert(ack.id.clone(), CachedResponse::Pending);
							tokio::spawn({
								let scheduler = scheduler.clone();
								async move {
									tokio::time::sleep(scheduler.control_ttl).await;
									scheduler.responses.remove(&ack.id);
								}
							});
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let _watchdog_task = Task::spawn({
			let scheduler = scheduler.clone();
			let runner_ttl = config.runner_ttl;
			async move |_stop| {
				scheduler.reap_runners(runner_ttl).await;
			}
		});

		// Wait for the listener or request task to exit.
		let listener = pin!(listener_task.wait());
		let request = pin!(request_task.wait());
		future::select(listener, request).await;

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
			.publish(subject, response)
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
			processes: BTreeSet::new(),
			sandboxes: BTreeSet::new(),
		};
		self.runners.insert(id.clone(), runner);
		Ok(())
	}

	async fn reap_runners(&self, runner_ttl: Duration) {
		let mut interval = tokio::time::interval(runner_ttl);
		loop {
			interval.tick().await;
			let now = tokio::time::Instant::now();
			let expired = self
				.runners
				.iter()
				.filter(|entry| now.duration_since(entry.heartbeat_at) > runner_ttl)
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

		let deadline = tokio::time::Instant::now() + self.create_sandbox_timeout;
		let mut excluded = BTreeSet::new();
		let response = loop {
			// If the process was cancelled, then break the loop so that the spawn does not retry.
			if let Some(process) = &process
				&& self.process_cancellations.contains_key(process)
			{
				break CreateSandboxResponse {
					id: id.clone(),
					sandbox: sandbox.clone(),
					created: false,
					error: Some(tg::error::Data {
						message: Some("the process was cancelled".to_owned()),
						..Default::default()
					}),
				};
			}

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

			// Register the dispatch so that the runner's response can be matched.
			let dispatch_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
			let (sender, mut receiver) = tokio::sync::oneshot::channel();
			self.sandbox_creations.insert(dispatch_id.clone(), sender);

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
				match future::select(&mut receiver, retries.next()).await {
					future::Either::Left((response, _)) => break response.ok(),
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

			// Remove the dispatch registration.
			self.sandbox_creations.remove(&dispatch_id);

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
			for process in runner.processes {
				self.processes.remove(&process);
			}
			for sandbox in runner.sandboxes {
				self.sandboxes.remove(&sandbox);

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
		self.sandboxes.insert(sandbox.clone(), runner.clone());
		if let Some(mut entry) = self.runners.get_mut(runner) {
			entry.sandboxes.insert(sandbox.clone());
		}
	}

	async fn handle_spawn_process(self: Arc<Self>, message: SpawnProcessRequest) {
		let SpawnProcessRequest {
			id,
			sandbox,
			process,
			process_token,
		} = message;

		let deadline = tokio::time::Instant::now() + self.spawn_process_timeout;
		let response = loop {
			// If the process was cancelled, then break the loop so that the spawn does not retry.
			if self.process_cancellations.contains_key(&process) {
				break SpawnProcessResponse {
					id: id.clone(),
					process: process.clone(),
					spawned: false,
					error: Some(tg::error::Data {
						message: Some("the process was cancelled".to_owned()),
						..Default::default()
					}),
				};
			}

			if tokio::time::Instant::now() >= deadline {
				break SpawnProcessResponse {
					id: id.clone(),
					process: process.clone(),
					spawned: false,
					error: Some(tg::error::Data {
						message: Some(
							"the sandbox was destroyed before the process could be run".to_owned(),
						),
						..Default::default()
					}),
				};
			}

			// Wait until the sandbox has registered its control stream.
			if !self.sandboxes.contains_key(&sandbox) {
				tokio::time::sleep(Duration::from_millis(10)).await;
				continue;
			}

			// Register the dispatch so that the sandbox's response can be matched.
			let dispatch_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
			let (sender, mut receiver) = tokio::sync::oneshot::channel();
			self.process_dispatches.insert(dispatch_id.clone(), sender);

			// Dispatch the process to the sandbox, republishing with backoff for reliability until the sandbox responds.
			let subject = format!("sandboxes.{sandbox}.server");
			let message = tg::sandbox::control::ServerMessage::Request(
				tg::sandbox::control::ServerRequest::SpawnProcess(
					tg::sandbox::control::SpawnProcessServerRequest {
						id: dispatch_id.clone(),
						process: process.clone(),
						process_token: process_token.clone(),
					},
				),
			);
			let options = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let mut retries = pin!(tangram_futures::retry::stream(options));
			let sandbox_response = loop {
				match future::select(&mut receiver, retries.next()).await {
					future::Either::Left((response, _)) => break response.ok(),
					future::Either::Right((_tick, _)) => {
						if tokio::time::Instant::now() >= deadline {
							break None;
						}
						self.server
							.messenger
							.publish(
								subject.clone(),
								SandboxControlServerMessage(message.clone()),
							)
							.await
							.inspect_err(|error| {
								tracing::error!(%error, "failed to publish the spawn process request");
							})
							.ok();
					},
				}
			};

			// Remove the dispatch registration.
			self.process_dispatches.remove(&dispatch_id);

			// Ack the sandbox.
			if sandbox_response.is_some() {
				self.server
					.messenger
					.publish(
						subject.clone(),
						SandboxControlServerMessage(tg::sandbox::control::ServerMessage::Ack(
							tg::sandbox::control::ServerAck { id: dispatch_id },
						)),
					)
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to acknowledge the sandbox response");
					})
					.ok();
			}

			// Check if the process was actually spawned, or the runner rejected the request.
			if sandbox_response.is_some_and(|response| response.spawned) {
				break SpawnProcessResponse {
					id: id.clone(),
					process: process.clone(),
					spawned: true,
					error: None,
				};
			}

			// The runner did not start the process.
			break SpawnProcessResponse {
				id: id.clone(),
				process: process.clone(),
				spawned: false,
				error: Some(tg::error::Data {
					message: Some("the process could not be started in the sandbox".to_owned()),
					..Default::default()
				}),
			};
		};

		self.cache_and_respond(Response::SpawnProcess(response))
			.await;
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
				tg::runner::control::ClientMessage::Ack(_) => {},
				tg::runner::control::ClientMessage::Notification(
					tg::runner::control::ClientNotification::Heartbeat(heartbeat),
				) => {
					if let Some(mut entry) = self.runners.get_mut(id) {
						entry.cpus = heartbeat.cpus;
						entry.memory = heartbeat.memory;
						entry.permits = heartbeat.permits;
						entry.heartbeat_at = tokio::time::Instant::now();
					}
				},
				tg::runner::control::ClientMessage::Response(
					tg::runner::control::ClientResponse::CreateSandbox(response),
				) => {
					if let Some((_, sender)) = self.sandbox_creations.remove(&response.id) {
						sender.send(response).ok();
					}
				},
				tg::runner::control::ClientMessage::Notification(
					tg::runner::control::ClientNotification::SandboxCreated(sandbox_created),
				) => {
					self.register_sandbox(&sandbox_created.sandbox, id);
					self.server
						.messenger
						.publish(
							format!("runners.{id}.server"),
							RunnerControlServerMessage(tg::runner::control::ServerMessage::Ack(
								tg::runner::control::ServerAck {
									id: sandbox_created.id,
								},
							)),
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
				tg::runner::control::ClientMessage::Notification(
					tg::runner::control::ClientNotification::SandboxDestroyed(sandbox_destroyed),
				) => {
					self.sandboxes
						.remove_if(&sandbox_destroyed.sandbox, |_, runner| runner == id);
					if let Some(mut entry) = self.runners.get_mut(id) {
						entry.sandboxes.remove(&sandbox_destroyed.sandbox);
					}
					self.server
						.messenger
						.publish(
							format!("runners.{id}.server"),
							RunnerControlServerMessage(tg::runner::control::ServerMessage::Ack(
								tg::runner::control::ServerAck {
									id: sandbox_destroyed.id,
								},
							)),
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
		}
		Ok(())
	}

	fn handle_sandbox_input(&self, request: SandboxInputRequest) {
		match request.message {
			tg::sandbox::control::ClientMessage::Ack(_) => {},
			tg::sandbox::control::ClientMessage::Response(
				tg::sandbox::control::ClientResponse::SpawnProcess(response),
			) => {
				if let Some((_, sender)) = self.process_dispatches.remove(&response.id) {
					sender.send(response).ok();
				}
			},
		}
	}
}

impl tangram_messenger::Payload for ListenMessage {
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

impl tangram_messenger::Payload for Request {
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

impl tangram_messenger::Payload for Response {
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
