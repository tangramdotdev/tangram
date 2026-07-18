use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future},
	std::{pin::pin, sync::Mutex, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
};

mod capacity;
mod process;
mod progress;
mod sandbox;

mod cleanup;

pub(crate) use self::{
	capacity::Allocation,
	process::ConnectedEvent as ProcessConnected,
	sandbox::{SandboxIdentity, SpawnSandboxTaskArg, Started as SandboxStarted},
};

pub mod control;

pub struct Runner {
	pub state: State,
	pub task: Mutex<Option<Task<()>>>,
}

pub struct State {
	pub capacity: self::capacity::Pool,
	pub id: Mutex<Option<tg::runner::Id>>,
	pub processes: crate::process::Map,
	pub reservations: self::capacity::Reservations,
	pub sandboxes: crate::sandbox::Map,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub value: Option<tg::Value>,
}

impl Runner {
	pub fn new(capacity: tg::runner::Capacity) -> Self {
		let state = State {
			capacity: self::capacity::Pool::new(capacity),
			id: Mutex::new(None),
			processes: crate::process::Map::default(),
			reservations: self::capacity::Reservations::new(),
			sandboxes: crate::sandbox::Map::default(),
		};
		let task = Mutex::new(None);
		Self { state, task }
	}
}

type RunnerSender =
	crate::control::Sender<tg::runner::control::ServerMessage, tg::runner::control::ClientMessage>;

impl Session {
	pub(crate) async fn runner_task(&self, id: tg::runner::Id, stopper: Stopper) {
		self.server
			.runner
			.state
			.id
			.lock()
			.unwrap()
			.replace(id.clone());
		loop {
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			let run_future = self.runner_task_inner(&id, stopper.clone());
			let run_future = pin!(run_future);
			let future::Either::Right((result, _)) = future::select(stop_future, run_future).await
			else {
				break;
			};
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the runner task failed");
				let stop_future = stopper.wait();
				let stop_future = pin!(stop_future);
				let sleep_future = tokio::time::sleep(Duration::from_secs(1));
				let sleep_future = pin!(sleep_future);
				if matches!(
					future::select(stop_future, sleep_future).await,
					future::Either::Left(_)
				) {
					break;
				}
			}
		}

		// Stop retaining finished sandbox state and wait for running sandboxes to finish.
		self.server.sandbox_tasks.stop_all();
		let results = self.server.sandbox_tasks.wait().await;
		for result in results {
			if let Err(error) = result
				&& !error.is_cancelled()
			{
				tracing::error!(?error, "a sandbox task panicked");
			}
		}
	}

	async fn runner_task_inner(&self, id: &tg::runner::Id, stopper: Stopper) -> tg::Result<()> {
		// Get the location.
		let location = self
			.server
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

		// Get the runner control stream.
		let control = self.run_get_runner_control_stream(id, &location).await?;

		// Handle the runner control stream.
		self.run_handle_runner_control_stream(id, location, control, stopper)
			.boxed()
			.await?;

		Ok(())
	}

	async fn run_get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		location: &tg::Location,
	) -> tg::Result<
		crate::control::Stream<
			tg::runner::control::ServerMessage,
			tg::runner::control::ClientMessage,
		>,
	> {
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::runner::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let heartbeat = self.create_runner_heartbeat(0);
		let host = tg::host::current().to_owned();
		let location = Some(location.clone().into());
		let arg = tg::runner::control::Arg {
			heartbeat,
			host,
			id: id.clone(),
			location,
		};
		let output_stream = self
			.get_runner_control_stream_all(arg, input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the scheduler"))?
			.boxed();
		let stream =
			crate::control::Stream::new(output_stream, input, crate::control::stream_options());
		Ok(stream)
	}

	async fn run_handle_runner_control_stream(
		&self,
		_runner: &tg::runner::Id,
		location: tg::Location,
		mut control: crate::control::Stream<
			tg::runner::control::ServerMessage,
			tg::runner::control::ClientMessage,
		>,
		stopper: Stopper,
	) -> tg::Result<()> {
		let sender = control.sender();

		// Spawn the heartbeat task.
		let _heartbeat_task = self.spawn_runner_heartbeat_task(sender.clone());

		// Process the messages the scheduler sends to this runner.
		loop {
			let receive = control.recv();
			let receive = pin!(receive);
			let stop = stopper.wait();
			let stop = pin!(stop);
			let result = future::select(receive, stop).await;
			let message = match result {
				future::Either::Left((result, _)) => result.map_err(|source| {
					tg::error!(!source, "failed to receive a runner control message")
				})?,
				future::Either::Right(_) => break,
			};
			let Some(message) = message else {
				break;
			};

			// Get the request.
			let tg::runner::control::ServerMessage::Request(message) = message else {
				unreachable!();
			};
			let id = message.id;
			let tg::runner::control::ServerRequestArg::CreateSandbox(request) = message.arg;

			let requested = request.capacity;

			// Attempt to immediately acquire capacity. If none is available, respond indicating that the sandbox was not created.
			let Some(allocation) = self.try_acquire_scheduled_sandbox_capacity(
				request.borrowed,
				request.parent.as_ref(),
				requested,
			) else {
				let output =
					tg::runner::control::CreateSandboxClientResponseOutput { created: false };
				let message = Self::create_runner_control_response(
					id.clone(),
					Ok(tg::runner::control::ClientResponseOutput::CreateSandbox(
						output,
					)),
				);
				sender.send(message).await.ok();
				continue;
			};
			// Spawn the sandbox task.
			let sandbox = request.sandbox.clone();
			let Some(token) = request.token else {
				let message = Self::create_runner_control_response(
					id.clone(),
					Err(tg::error!(%sandbox, "missing the sandbox authentication token")),
				);
				sender.send(message).await.ok();
				continue;
			};
			let identity = Some(SandboxIdentity {
				id: sandbox.clone(),
				token,
			});
			let task = self.server.spawn_sandbox_task(SpawnSandboxTaskArg {
				allocation,
				arg: request.arg,
				creator: request.creator,
				identity,
				location: location.clone(),
				process: request.process,
			});

			// Send the response.
			let output = tg::runner::control::CreateSandboxClientResponseOutput { created: true };
			let message = Self::create_runner_control_response(
				id.clone(),
				Ok(tg::runner::control::ClientResponseOutput::CreateSandbox(
					output,
				)),
			);
			sender.send(message).await.ok();

			// Spawn a task to send the sandbox destroyed notification.
			tokio::spawn({
				let sender = sender.clone();
				async move {
					task.destroyed.await.ok();
					let id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
					let notification = tg::runner::control::ClientNotification::SandboxDestroyed(
						tg::runner::control::SandboxDestroyedClientNotification { id, sandbox },
					);
					let message = tg::runner::control::ClientMessage::Notification(notification);
					sender.send(message).await.ok();
				}
			});
		}

		Ok(())
	}

	fn spawn_runner_heartbeat_task(&self, sender: RunnerSender) -> Task<()> {
		let heartbeat_interval = self.server.config.runner.as_ref().map_or_else(
			|| Duration::from_secs(1),
			|config| config.heartbeat_interval,
		);
		Task::spawn({
			let session = self.clone();
			move |_| async move {
				session
					.runner_heartbeat_task(sender, heartbeat_interval)
					.await;
			}
		})
	}

	async fn runner_heartbeat_task(&self, sender: RunnerSender, interval: Duration) {
		let mut interval = tokio::time::interval(interval);
		let mut index = 1;
		loop {
			tokio::select! {
				_ = interval.tick() => {},
				() = self.server.runner.state.capacity.wait_for_change() => {},
			}
			let message = tg::runner::control::ClientMessage::Notification(
				tg::runner::control::ClientNotification::Heartbeat(
					self.create_runner_heartbeat(index),
				),
			);
			index = index.wrapping_add(1);
			let result = sender.send(message).await;
			if result.is_err() {
				break;
			}
		}
	}

	fn create_runner_heartbeat(
		&self,
		index: u64,
	) -> tg::runner::control::HeartbeatClientNotification {
		let capacity = self.server.runner.state.capacity.get();
		tg::runner::control::HeartbeatClientNotification { capacity, index }
	}

	fn create_runner_control_response(
		id: String,
		result: tg::Result<tg::runner::control::ClientResponseOutput>,
	) -> tg::runner::control::ClientMessage {
		let (error, output) = match result {
			Ok(output) => {
				let error = None;
				let output = Some(output);
				(error, output)
			},
			Err(error) => {
				let error = Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				});
				let output = None;
				(error, output)
			},
		};
		tg::runner::control::ClientMessage::Response(tg::runner::control::ClientResponse {
			error,
			id,
			output,
		})
	}
}

impl State {
	pub fn id(&self) -> Option<tg::runner::Id> {
		self.id.lock().unwrap().clone()
	}

	pub fn started_process_count(&self) -> u64 {
		self.sandboxes
			.iter()
			.map(|sandbox| {
				sandbox
					.processes
					.values()
					.filter(|process| process.data.status == tg::process::Status::Started)
					.count()
			})
			.sum::<usize>()
			.try_into()
			.unwrap()
	}

	pub fn try_get_sandbox(&self, id: &tg::sandbox::Id) -> Option<tg::sandbox::get::Output> {
		self.sandboxes.get(id).map(|sandbox| sandbox.data.clone())
	}

	pub fn try_get_process(&self, id: &tg::process::Id) -> Option<tg::process::Data> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		Some(process.data.clone())
	}

	pub fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> Option<tg::process::control::GetChildrenClientResponseOutput> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		let children = process.data.children.as_deref().unwrap_or_default();
		let children_length = u64::try_from(children.len()).unwrap();
		let start = usize::try_from(position.min(children_length)).unwrap();
		let end = usize::try_from(position.saturating_add(length).min(children_length)).unwrap();
		Some(tg::process::control::GetChildrenClientResponseOutput {
			children: children[start..end].to_vec(),
			length: children_length,
			status: process.data.status,
		})
	}

	pub fn try_update_process<T>(
		&self,
		id: &tg::process::Id,
		update: impl FnOnce(&mut crate::process::State) -> T,
	) -> Option<T> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let mut sandbox = self.sandboxes.get_mut(&sandbox)?;
		let process = sandbox.processes.get_mut(id)?;
		Some(update(process))
	}

	pub fn try_get_process_sandbox(&self, id: &tg::process::Id) -> Option<tg::sandbox::Id> {
		self.processes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
	}
}
