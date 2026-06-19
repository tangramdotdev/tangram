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

		// Create the input stream for the events this runner sends to the scheduler.
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::runner::control::InputEvent>(256);
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
			principal: tg::Principal::Runner,
			..self.context.clone()
		};
		let session = self.session(&context);
		let mut output = session
			.get_runner_control_stream_with_context(&id, arg, input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the scheduler"))?;

		// Make the input sender available so fast-path sandboxes can register their placement.
		self.runner_input.lock().unwrap().replace(input.clone());

		// Spawn a task to send heartbeats, immediately and then on a timer, so the scheduler learns
		// this runner's permit capacity.
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
					let event =
						tg::runner::control::InputEvent::Heartbeat(server.runner_heartbeat());
					if input.send(event).await.is_err() {
						break;
					}
				}
			}
		});

		let control_ttl = self
			.config
			.runner
			.as_ref()
			.map_or(Duration::from_mins(1), |runner| runner.control_ttl);

		// Dedup caches.
		let replies: Arc<DashMap<String, Option<tg::runner::control::SandboxCreated>>> =
			Arc::new(DashMap::new());
		let process_replies: Arc<DashMap<String, Option<tg::runner::control::ProcessSpawned>>> =
			Arc::new(DashMap::new());

		// Process the events the scheduler sends to this runner.
		while let Some(event) = output
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to receive a runner control event"))?
		{
			match event {
				tg::runner::control::OutputEvent::CreateSandbox(event) => {
					// Duplicate; resend the cached reply, or drop if in flight or acked.
					let cached = replies
						.get(&event.request_id)
						.map(|entry| entry.value().clone());
					if let Some(cached) = cached {
						if let Some(reply) = cached {
							input
								.send(tg::runner::control::InputEvent::SandboxCreated(reply))
								.await
								.ok();
						}
						continue;
					}

					// Mark in flight so duplicates are dropped until the reply is ready.
					replies.insert(event.request_id.clone(), None);

					// Acquire a permit and start the sandbox; if none, a local child sandbox took it, so report not created.
					let created =
						if let Ok(permit) = self.sandbox_semaphore.clone().try_acquire_owned() {
							let permit = crate::sandbox::Permit(tg::Either::Left(permit));
							self.spawn_sandbox_task(SpawnSandboxTaskArg {
								id: event.id.clone(),
								location: location.clone(),
								permit,
								process: event.process.clone(),
								process_token: event.process_token.clone(),
								token: event.token.clone(),
							});
							true
						} else {
							false
						};

					// Cache and send the reply.
					let reply = tg::runner::control::SandboxCreated {
						request_id: event.request_id.clone(),
						id: event.id,
						created,
						runner: false,
					};
					replies.insert(event.request_id, Some(reply.clone()));
					input
						.send(tg::runner::control::InputEvent::SandboxCreated(reply))
						.await
						.ok();
				},
				tg::runner::control::OutputEvent::CreateSandboxAck(request_id) => {
					replies.insert(request_id.clone(), None);
					tokio::spawn({
						let replies = replies.clone();
						async move {
							tokio::time::sleep(control_ttl).await;
							replies.remove(&request_id);
						}
					});
				},
				tg::runner::control::OutputEvent::SpawnProcess(event) => {
					// Duplicate; resend the cached reply, or drop if in flight or acked.
					let cached = process_replies
						.get(&event.request_id)
						.map(|entry| entry.value().clone());
					if let Some(cached) = cached {
						if let Some(reply) = cached {
							input
								.send(tg::runner::control::InputEvent::ProcessSpawned(reply))
								.await
								.ok();
						}
						continue;
					}

					// Mark in flight so duplicates are dropped until the reply is ready.
					process_replies.insert(event.request_id.clone(), None);

					// Deliver the process to its sandbox task and report whether it started in a
					// separate task so that waiting for the sandbox does not block other control
					// events.
					let sender = self
						.sandbox_process_senders
						.get(&event.sandbox)
						.map(|entry| entry.value().clone());
					tokio::spawn({
						let input = input.clone();
						let process_replies = process_replies.clone();
						async move {
							// If the sandbox is not present, then report not spawned so the scheduler
							// retries. Otherwise, wait until the sandbox reports whether it started.
							let spawned = match sender {
								Some(sender) => {
									let (started, result) = tokio::sync::oneshot::channel();
									sender
										.send((event.id.clone(), event.process_token.clone(), started))
										.await
										.is_ok() && result.await.unwrap_or(false)
								},
								None => false,
							};

							// Cache and send the reply.
							let reply = tg::runner::control::ProcessSpawned {
								request_id: event.request_id.clone(),
								id: event.id,
								spawned,
							};
							process_replies.insert(event.request_id, Some(reply.clone()));
							input
								.send(tg::runner::control::InputEvent::ProcessSpawned(reply))
								.await
								.ok();
						}
					});
				},
				tg::runner::control::OutputEvent::SpawnProcessAck(request_id) => {
					process_replies.insert(request_id.clone(), None);
					tokio::spawn({
						let process_replies = process_replies.clone();
						async move {
							tokio::time::sleep(control_ttl).await;
							process_replies.remove(&request_id);
						}
					});
				},
				tg::runner::control::OutputEvent::End => break,
			}
		}

		heartbeat_task.abort();

		Ok(())
	}

	fn runner_heartbeat(&self) -> tg::runner::control::Heartbeat {
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
		tg::runner::control::Heartbeat {
			cpus: tg::runner::control::Capacity::default(),
			memory: tg::runner::control::Capacity::default(),
			permits: tg::runner::control::Capacity {
				used: used as u64,
				total: total as u64,
			},
		}
	}
}
