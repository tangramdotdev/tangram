use {
	crate::{Context, Server},
	dashmap::DashMap,
	futures::{StreamExt as _, TryStreamExt as _},
	std::{sync::Arc, time::Duration},
	tangram_client::prelude::*,
};

mod process;
mod progress;
mod sandbox;

pub(crate) use self::sandbox::SpawnSandboxTaskArg;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub value: Option<tg::Value>,
}

#[derive(Clone, Debug)]
enum CachedRunnerResponse {
	Pending,
	Ready(tg::runner::control::CreateSandboxClientResponse),
}

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			if let Err(error) = self.runner_task_inner().await {
				tracing::error!(error = %error.trace(), "the runner task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn runner_task_inner(&self) -> tg::Result<()> {
		// Get the location.
		let location = self
			.config
			.runner
			.as_ref()
			.and_then(|config| config.remote.as_ref())
			.map_or_else(
				|| tg::Location::Local(tg::location::Local::default()),
				|name| {
					tg::Location::Remote(tg::location::Remote {
						name: name.to_owned(),
						region: None,
					})
				},
			);

		// Create the input stream for the messages this runner sends to the scheduler.
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::runner::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();

		// Connect to the scheduler.
		let id = tg::runner::Id::new();
		let arg = tg::runner::control::Arg {
			host: tg::host::current().to_owned(),
			location: Some(location.clone().into()),
		};
		let context = Context {
			principal: tg::Principal::Runner(id.clone()),
			..self.context.clone()
		};
		let session = self.session(&context);
		let mut output = session
			.get_runner_control_stream_with_context(&id, arg, input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the scheduler"))?;

		// Make the input sender available so fast-path sandboxes can register their placement.
		self.runner_input.lock().unwrap().replace(input.clone());

		let heartbeat_interval = self.config.runner.as_ref().map_or_else(
			|| Duration::from_secs(1),
			|config| config.heartbeat_interval,
		);
		let heartbeat_task = tokio::spawn({
			let server = self.clone();
			let input = input.clone();
			async move {
				let mut interval = tokio::time::interval(heartbeat_interval);
				loop {
					interval.tick().await;
					let message = tg::runner::control::ClientMessage::Notification(
						tg::runner::control::ClientNotification::Heartbeat(
							server.runner_heartbeat(),
						),
					);
					match input.try_send(message) {
						Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => (),
						Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
					}
				}
			}
		});

		let lifecycle_task = tokio::spawn({
			let server = self.clone();
			let input = input.clone();
			async move {
				let mut interval = tokio::time::interval(Duration::from_secs(1));
				loop {
					interval.tick().await;
					for message in server
						.runner_lifecycle_messages
						.iter()
						.map(|entry| entry.value().clone())
						.collect::<Vec<_>>()
					{
						if input.send(message).await.is_err() {
							return;
						}
					}
				}
			}
		});

		let control_ttl = self
			.config
			.runner
			.as_ref()
			.map_or(Duration::from_mins(1), |runner| runner.control_ttl);

		let responses: Arc<DashMap<String, CachedRunnerResponse>> = Arc::new(DashMap::new());

		// Process the messages the scheduler sends to this runner.
		while let Some(message) = output
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to receive a runner control message"))?
		{
			match message {
				tg::runner::control::ServerMessage::Request(request) => {
					let id = request.id().to_owned();

					// Duplicate; resend the cached response, or drop if in flight or acked.
					let cached = responses.get(&id).map(|entry| entry.value().clone());
					if let Some(cached) = cached {
						if let CachedRunnerResponse::Ready(response) = cached {
							input
								.send(tg::runner::control::ClientMessage::Response(
									tg::runner::control::ClientResponse::CreateSandbox(response),
								))
								.await
								.ok();
						}
						continue;
					}

					// Mark in flight so duplicates are dropped until the response is ready.
					responses.insert(id.clone(), CachedRunnerResponse::Pending);

					let tg::runner::control::ServerRequest::CreateSandbox(request) = request;

					// Acquire a permit and spawn the sandbox. If none is available, report not created.
					let created =
						if let Ok(permit) = self.sandbox_semaphore.clone().try_acquire_owned() {
							let permit = crate::sandbox::Permit(tg::Either::Left(permit));
							self.spawn_sandbox_task(SpawnSandboxTaskArg {
								id: request.sandbox.clone(),
								location: location.clone(),
								permit,
								process: request.process.clone(),
								process_token: request.process_token.clone(),
								token: request.token.clone(),
							});
							true
						} else {
							false
						};

					// Cache and send the response.
					let response = tg::runner::control::CreateSandboxClientResponse {
						id: id.clone(),
						sandbox: request.sandbox,
						created,
					};
					responses.insert(id, CachedRunnerResponse::Ready(response.clone()));
					input
						.send(tg::runner::control::ClientMessage::Response(
							tg::runner::control::ClientResponse::CreateSandbox(response),
						))
						.await
						.ok();
				},
				tg::runner::control::ServerMessage::Ack(ack) => {
					if self.runner_lifecycle_messages.remove(&ack.id).is_none() {
						responses.insert(ack.id.clone(), CachedRunnerResponse::Pending);
						tokio::spawn({
							let responses = responses.clone();
							async move {
								tokio::time::sleep(control_ttl).await;
								responses.remove(&ack.id);
							}
						});
					}
				},
			}
		}

		heartbeat_task.abort();
		lifecycle_task.abort();

		Ok(())
	}

	fn runner_heartbeat(&self) -> tg::runner::control::HeartbeatClientNotification {
		let total = self
			.config
			.runner
			.as_ref()
			.and_then(|config| config.concurrency)
			.unwrap_or_else(|| {
				std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
			});
		let available = self.sandbox_semaphore.available_permits();
		let used = total.saturating_sub(available);
		tg::runner::control::HeartbeatClientNotification {
			cpus: tg::runner::control::Capacity::default(),
			memory: tg::runner::control::Capacity::default(),
			permits: tg::runner::control::Capacity {
				used: used as u64,
				total: total as u64,
			},
		}
	}
}
