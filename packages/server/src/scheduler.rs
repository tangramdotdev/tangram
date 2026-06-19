use {
	crate::Server,
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{collections::BTreeSet, pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger,
};

struct Scheduler {
	server: Server,
	runners: DashMap<tg::runner::Id, Runner>,
	processes: DashMap<tg::process::Id, tg::runner::Id>,
	sandboxes: DashMap<tg::sandbox::Id, tg::runner::Id>,
	sandbox_creations:
		DashMap<String, tokio::sync::oneshot::Sender<tg::runner::control::SandboxCreated>>,
	process_dispatches:
		DashMap<String, tokio::sync::oneshot::Sender<tg::runner::control::ProcessSpawned>>,
	create_sandbox_responses: DashMap<String, Option<CreateSandboxResponse>>,
	spawn_process_responses: DashMap<String, Option<SpawnProcessResponse>>,
	process_cancellations: DashMap<tg::process::Id, ()>,
	control_ttl: Duration,
	create_sandbox_timeout: Duration,
	spawn_process_timeout: Duration,
}

struct Runner {
	host: String,
	#[expect(dead_code)]
	task: Option<Task<()>>,
	cpus: tg::runner::control::Capacity,
	memory: tg::runner::control::Capacity,
	permits: tg::runner::control::Capacity,
	heartbeat_at: tokio::time::Instant,
	processes: BTreeSet<tg::process::Id>,
	sandboxes: BTreeSet<tg::sandbox::Id>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ListenMessage {
	pub(crate) id: tg::runner::Id,
	pub(crate) arg: tg::runner::control::Arg,
}

pub(crate) struct InputMessage(pub(crate) tg::runner::control::InputEvent);

pub(crate) struct OutputMessage(pub(crate) tg::runner::control::OutputEvent);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum SchedulerRequest {
	CreateSandbox(CreateSandboxRequest),
	SpawnProcess(SpawnProcessRequest),
	CancelProcess(CancelProcessRequest),
	Ack(SchedulerAck),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum SchedulerResponse {
	CreateSandbox(CreateSandboxResponse),
	SpawnProcess(SpawnProcessResponse),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxRequest {
	pub(crate) request_id: String,
	pub(crate) id: tg::sandbox::Id,
	pub(crate) arg: tg::sandbox::create::Arg,
	pub(crate) process: Option<tg::process::Id>,
	pub(crate) token: Option<String>,
	pub(crate) process_token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SpawnProcessRequest {
	pub(crate) request_id: String,
	pub(crate) sandbox: tg::sandbox::Id,
	pub(crate) id: tg::process::Id,
	pub(crate) process_token: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SpawnProcessResponse {
	pub(crate) request_id: String,
	pub(crate) id: tg::process::Id,
	pub(crate) spawned: bool,
	pub(crate) error: Option<tg::error::Data>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CancelProcessRequest {
	pub(crate) id: tg::process::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SchedulerAck {
	pub(crate) request_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CreateSandboxResponse {
	pub(crate) request_id: String,
	pub(crate) id: tg::sandbox::Id,
	pub(crate) created: bool,
	pub(crate) error: Option<tg::error::Data>,
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
			create_sandbox_responses: DashMap::new(),
			spawn_process_responses: DashMap::new(),
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

		// Create the task to dispatch runner events.
		let request_task = Task::spawn({
			let stream = self
				.messenger
				.subscribe::<SchedulerRequest>("scheduler".into())
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
						SchedulerRequest::CreateSandbox(request) => {
							// If there is already an entry for the request, then it is a duplicate. Resend the cached response if there is one, otherwise drop the request because it is in flight or acked.
							let cached = scheduler
								.create_sandbox_responses
								.get(&request.request_id)
								.map(|entry| entry.value().clone());
							if let Some(cached) = cached {
								if let Some(response) = cached {
									scheduler.respond_create_sandbox(response).await;
								}
								continue;
							}

							// Mark the request as in flight so that duplicates are dropped until the response is ready.
							scheduler
								.create_sandbox_responses
								.insert(request.request_id.clone(), None);

							// Handle each request concurrently so that waiting for a runner's reply does not block other requests.
							let scheduler = scheduler.clone();
							tokio::spawn(async move {
								scheduler.handle_create_sandbox(request).await;
							});
						},
						SchedulerRequest::SpawnProcess(request) => {
							// If there is already an entry for the request, then it is a duplicate. Resend the cached response if there is one, otherwise drop the request because it is in flight or acked.
							let cached = scheduler
								.spawn_process_responses
								.get(&request.request_id)
								.map(|entry| entry.value().clone());
							if let Some(cached) = cached {
								if let Some(response) = cached {
									scheduler.respond_spawn_process(response).await;
								}
								continue;
							}

							// Mark the request as in flight so that duplicates are dropped until the response is ready.
							scheduler
								.spawn_process_responses
								.insert(request.request_id.clone(), None);

							// Handle each request concurrently so that waiting for a runner's reply does not block other requests.
							let scheduler = scheduler.clone();
							tokio::spawn(async move {
								scheduler.handle_spawn_process(request).await;
							});
						},
						SchedulerRequest::CancelProcess(request) => {
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
						SchedulerRequest::Ack(ack) => {
							// Mark the response as acked and remove it after the TTL.
							scheduler
								.create_sandbox_responses
								.insert(ack.request_id.clone(), None);
							scheduler
								.spawn_process_responses
								.insert(ack.request_id.clone(), None);
							tokio::spawn({
								let scheduler = scheduler.clone();
								async move {
									tokio::time::sleep(scheduler.control_ttl).await;
									scheduler.create_sandbox_responses.remove(&ack.request_id);
									scheduler.spawn_process_responses.remove(&ack.request_id);
								}
							});
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn the watchdog task.
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
	async fn spawn_runner_task(
		self: &Arc<Self>,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
	) -> tg::Result<()> {
		// Subscribe to the runner's input stream.
		let stream = self
			.server
			.messenger
			.subscribe::<InputMessage>(format!("runners.{id}.input"))
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to subscribe to the runner input stream")
			})?
			.boxed();
		let task = Task::spawn({
			let scheduler = self.clone();
			let id = id.clone();
			async move |stop| {
				let stop = stop.wait();
				let fut = scheduler
					.runner_task(&id, stream)
					.inspect_err(
						|error| tracing::error!(%error, runner = %id, "the runner task failed"),
					)
					.map(|_| ());
				future::select(pin!(fut), pin!(stop)).await;
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

	async fn handle_create_sandbox(&self, message: CreateSandboxRequest) {
		let CreateSandboxRequest {
			request_id,
			id,
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
					request_id: request_id.clone(),
					id: id.clone(),
					created: false,
					error: Some(tg::error::Data {
						message: Some("the process was cancelled".to_owned()),
						..Default::default()
					}),
				};
			}

			if tokio::time::Instant::now() >= deadline {
				break CreateSandboxResponse {
					request_id: request_id.clone(),
					id: id.clone(),
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

			// Register the dispatch so that the runner's reply can be matched.
			let dispatch_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
			let (sender, mut receiver) = tokio::sync::oneshot::channel();
			self.sandbox_creations.insert(dispatch_id.clone(), sender);

			// Dispatch the sandbox to the runner, republishing with backoff for reliability until the runner replies.
			let subject = format!("runners.{runner}.output");
			let event = tg::runner::control::OutputEvent::CreateSandbox(
				tg::runner::control::CreateSandbox {
					request_id: dispatch_id.clone(),
					id: id.clone(),
					arg: arg.clone(),
					process: process.clone(),
					token: token.clone(),
					process_token: process_token.clone(),
				},
			);
			let options = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let mut retries = pin!(tangram_futures::retry::stream(options));
			let reply = loop {
				match future::select(&mut receiver, retries.next()).await {
					future::Either::Left((reply, _)) => break reply.ok(),
					future::Either::Right((_tick, _)) => {
						if tokio::time::Instant::now() >= deadline {
							break None;
						}
						self.server
							.messenger
							.publish(subject.clone(), OutputMessage(event.clone()))
							.await
							.inspect_err(|error| {
								tracing::error!(%error, "failed to publish the create sandbox event");
							})
							.ok();
					},
				}
			};

			// Remove the dispatch registration.
			self.sandbox_creations.remove(&dispatch_id);

			// Ack the runner.
			if reply.is_some() {
				self.server
					.messenger
					.publish(
						subject.clone(),
						OutputMessage(tg::runner::control::OutputEvent::CreateSandboxAck(
							dispatch_id,
						)),
					)
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to acknowledge the runner's reply");
					})
					.ok();
			}

			// Check if the sandbox was actually created, or the runner rejected the request.
			if reply.is_some_and(|reply| reply.created) {
				// Record the sandbox's placement so that processes can be routed to it.
				self.register_sandbox(&id, &runner);
				break CreateSandboxResponse {
					request_id: request_id.clone(),
					id: id.clone(),
					created: true,
					error: None,
				};
			}
			excluded.insert(runner);
		};

		// Cache and publish the response.
		self.create_sandbox_responses
			.insert(response.request_id.clone(), Some(response.clone()));
		self.respond_create_sandbox(response).await;
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
			}
		}
	}

	fn register_sandbox(&self, sandbox: &tg::sandbox::Id, runner: &tg::runner::Id) {
		self.sandboxes.insert(sandbox.clone(), runner.clone());
		if let Some(mut entry) = self.runners.get_mut(runner) {
			entry.sandboxes.insert(sandbox.clone());
		}
	}

	fn deregister_sandbox(&self, sandbox: &tg::sandbox::Id, runner: &tg::runner::Id) {
		self.sandboxes.remove(sandbox);
		if let Some(mut entry) = self.runners.get_mut(runner) {
			entry.sandboxes.remove(sandbox);
		}
	}

	async fn handle_spawn_process(&self, message: SpawnProcessRequest) {
		let SpawnProcessRequest {
			request_id,
			sandbox,
			id,
			process_token,
		} = message;

		let deadline = tokio::time::Instant::now() + self.spawn_process_timeout;
		let response = loop {
			// If the process was cancelled, then break the loop so that the spawn does not retry.
			if self.process_cancellations.contains_key(&id) {
				break SpawnProcessResponse {
					request_id: request_id.clone(),
					id: id.clone(),
					spawned: false,
					error: Some(tg::error::Data {
						message: Some("the process was cancelled".to_owned()),
						..Default::default()
					}),
				};
			}

			if tokio::time::Instant::now() >= deadline {
				break SpawnProcessResponse {
					request_id: request_id.clone(),
					id: id.clone(),
					spawned: false,
					error: Some(tg::error::Data {
						message: Some(
							"the sandbox was destroyed before the process could be run".to_owned(),
						),
						..Default::default()
					}),
				};
			}

			// Find the runner that hosts the sandbox, retrying until its registration arrives.
			let Some(runner) = self
				.sandboxes
				.get(&sandbox)
				.map(|entry| entry.value().clone())
			else {
				tokio::time::sleep(Duration::from_millis(10)).await;
				continue;
			};

			// Register the dispatch so that the runner's reply can be matched.
			let dispatch_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
			let (sender, mut receiver) = tokio::sync::oneshot::channel();
			self.process_dispatches.insert(dispatch_id.clone(), sender);

			// Dispatch the process to the runner, republishing with backoff for reliability until the runner replies.
			let subject = format!("runners.{runner}.output");
			let event =
				tg::runner::control::OutputEvent::SpawnProcess(tg::runner::control::SpawnProcess {
					request_id: dispatch_id.clone(),
					sandbox: sandbox.clone(),
					id: id.clone(),
					process_token: process_token.clone(),
				});
			let options = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let mut retries = pin!(tangram_futures::retry::stream(options));
			let reply = loop {
				match future::select(&mut receiver, retries.next()).await {
					future::Either::Left((reply, _)) => break reply.ok(),
					future::Either::Right((_tick, _)) => {
						if tokio::time::Instant::now() >= deadline {
							break None;
						}
						self.server
							.messenger
							.publish(subject.clone(), OutputMessage(event.clone()))
							.await
							.inspect_err(|error| {
								tracing::error!(%error, "failed to publish the spawn process event");
							})
							.ok();
					},
				}
			};

			// Remove the dispatch registration.
			self.process_dispatches.remove(&dispatch_id);

			// Ack the runner.
			if reply.is_some() {
				self.server
					.messenger
					.publish(
						subject.clone(),
						OutputMessage(tg::runner::control::OutputEvent::SpawnProcessAck(
							dispatch_id,
						)),
					)
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to acknowledge the runner's reply");
					})
					.ok();
			}

			// Check if the process was actually spawned, or the runner rejected the request.
			if reply.is_some_and(|reply| reply.spawned) {
				break SpawnProcessResponse {
					request_id: request_id.clone(),
					id: id.clone(),
					spawned: true,
					error: None,
				};
			}

			// The runner did not start the process.
			break SpawnProcessResponse {
				request_id: request_id.clone(),
				id: id.clone(),
				spawned: false,
				error: Some(tg::error::Data {
					message: Some("the process could not be started in the sandbox".to_owned()),
					..Default::default()
				}),
			};
		};

		// Cache and publish the response.
		self.spawn_process_responses
			.insert(response.request_id.clone(), Some(response.clone()));
		self.respond_spawn_process(response).await;
	}

	async fn respond_create_sandbox(&self, response: CreateSandboxResponse) {
		let subject = format!("scheduler.create-sandbox.{}", response.request_id);
		let response = SchedulerResponse::CreateSandbox(response);
		self.server
			.messenger
			.publish(subject, response)
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to publish the create sandbox response");
			})
			.ok();
	}

	async fn respond_spawn_process(&self, response: SpawnProcessResponse) {
		let subject = format!("scheduler.spawn-process.{}", response.request_id);
		let response = SchedulerResponse::SpawnProcess(response);
		self.server
			.messenger
			.publish(subject, response)
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to publish the spawn process response");
			})
			.ok();
	}

	async fn runner_task(
		&self,
		id: &tg::runner::Id,
		mut stream: futures::stream::BoxStream<
			'static,
			Result<tangram_messenger::Message<InputMessage>, tangram_messenger::Error>,
		>,
	) -> tg::Result<()> {
		while let Some(message) = stream.try_next().await.map_err(
			|source| tg::error!(!source, runner = %id, "failed to receive runner output message"),
		)? {
			match message.payload.0 {
				tg::runner::control::InputEvent::Heartbeat(heartbeat) => {
					// The heartbeat is the source of truth for the runner's permit usage and liveness.
					if let Some(mut entry) = self.runners.get_mut(id) {
						entry.cpus = heartbeat.cpus;
						entry.memory = heartbeat.memory;
						entry.permits = heartbeat.permits;
						entry.heartbeat_at = tokio::time::Instant::now();
					}
				},
				tg::runner::control::InputEvent::ProcessSpawned(reply) => {
					if let Some((_, sender)) = self.process_dispatches.remove(&reply.request_id) {
						sender.send(reply).ok();
					}
				},
				tg::runner::control::InputEvent::SandboxCreated(reply) => {
					// A sandbox the runner created itself via the local permit fast path registers its
					// placement so that processes can be routed to it.
					if reply.runner {
						self.register_sandbox(&reply.id, id);
					} else if let Some((_, sender)) =
						self.sandbox_creations.remove(&reply.request_id)
					{
						sender.send(reply).ok();
					}
				},
				tg::runner::control::InputEvent::SandboxDestroyed(sandbox) => {
					self.deregister_sandbox(&sandbox, id);
				},
				tg::runner::control::InputEvent::End => break,
			}
		}
		Ok(())
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

impl tangram_messenger::Payload for InputMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}

impl tangram_messenger::Payload for OutputMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}

impl tangram_messenger::Payload for SchedulerRequest {
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

impl tangram_messenger::Payload for SchedulerResponse {
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
