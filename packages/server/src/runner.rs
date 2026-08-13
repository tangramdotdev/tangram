use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, future},
	std::{
		ops::ControlFlow,
		pin::pin,
		sync::{
			Mutex,
			atomic::{AtomicU64, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::{Stopper, Task},
};

pub(crate) mod capacity;
pub(crate) mod process;
pub(crate) mod sandbox;

pub mod control;
pub mod create;
pub mod delete;
pub mod list;
pub mod token;

type RunnerSender =
	crate::control::Sender<tg::runner::control::ServerMessage, tg::runner::control::ClientMessage>;

#[derive(Clone, Copy, Debug)]
pub(super) struct Config {
	pub capacity: tg::runner::Capacity,
	pub sandbox_pool_size: usize,
}

pub struct Runner {
	sandbox_pool: self::sandbox::Pool,
	state: State,
	task: Mutex<Option<Task<()>>>,
}

pub struct State {
	capacity: self::capacity::Pool,
	id: Mutex<Option<tg::runner::Id>>,
	next_sandbox_index: AtomicU64,
	process_tokens: dashmap::DashMap<String, tokio::sync::watch::Receiver<Option<tg::process::Id>>>,
	processes: crate::process::Map,
	reservations: self::capacity::Reservations,
	sandboxes: crate::sandbox::Sandboxes,
	scheduler: tokio::sync::watch::Sender<Option<tg::scheduler::Id>>,
}

impl Runner {
	#[must_use]
	pub fn new(config: Config) -> Self {
		let (scheduler, _) = tokio::sync::watch::channel(None);
		let state = State {
			capacity: self::capacity::Pool::new(config.capacity),
			id: Mutex::new(None),
			next_sandbox_index: AtomicU64::new(1),
			process_tokens: dashmap::DashMap::new(),
			processes: crate::process::Map::default(),
			reservations: self::capacity::Reservations::new(),
			sandboxes: crate::sandbox::Sandboxes::default(),
			scheduler,
		};
		let task = Mutex::new(None);
		let sandbox_pool = self::sandbox::Pool::new(config.sandbox_pool_size);
		Self {
			sandbox_pool,
			state,
			task,
		}
	}

	#[must_use]
	pub(crate) fn state(&self) -> &State {
		&self.state
	}

	#[must_use]
	pub(crate) fn task(&self) -> &Mutex<Option<Task<()>>> {
		&self.task
	}
}

impl Session {
	pub(crate) async fn authorize_runner_owner(&self, owner: Option<&tg::Id>) -> tg::Result<()> {
		self.verify_request_from_host()?;
		let Some(owner) = owner else {
			if matches!(self.context.principal, tg::Principal::Root) {
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		};
		let permission = Self::admin_permission_for_resource(owner)?;
		let authorized = self
			.authorize(owner.clone(), permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		if !authorized {
			return Err(tg::error!("unauthorized"));
		}

		Ok(())
	}

	pub(crate) async fn resolve_runner_owner(
		&self,
		owner: &tg::principal::Selector,
	) -> tg::Result<tg::Principal> {
		let owner = match owner {
			tg::principal::Selector::Principal(principal) => {
				tg::authorization::subject::Selector::Subject(principal.try_to_subject()?)
			},
			tg::principal::Selector::Specifier(specifier) => {
				tg::authorization::subject::Selector::Specifier(specifier.clone())
			},
		};
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let owner = owner.clone();
				async move { Self::resolve_runner_owner_with_transaction(transaction, &owner).await }
					.boxed()
			})
			.await
	}

	async fn resolve_runner_owner_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		owner: &tg::authorization::subject::Selector,
	) -> tg::Result<ControlFlow<tg::Principal, crate::database::Error>> {
		let owner = match Self::resolve_subject_with_transaction(transaction, owner).await? {
			ControlFlow::Break(owner) => owner,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		.ok_or_else(|| tg::error!("failed to resolve the runner owner"))?;
		let owner = match owner {
			tg::authorization::Subject::Group(id) => tg::Principal::Group(id),
			tg::authorization::Subject::Organization(id) => tg::Principal::Organization(id),
			tg::authorization::Subject::User(id) => tg::Principal::User(id),
			_ => return Err(tg::error!("invalid runner owner")),
		};

		Ok(ControlFlow::Break(owner))
	}

	pub(crate) async fn try_get_runner_data(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tg::runner::Data>> {
		let runner = runner.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let runner = runner.clone();
				async move { Self::try_get_runner_data_with_transaction(transaction, &runner).await }
					.boxed()
			})
			.await
	}

	async fn try_get_runner_data_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Option<tg::runner::Data>, crate::database::Error>> {
		#[derive(tangram_database::row::Deserialize)]
		struct Row {
			created_at: i64,

			#[tangram_database(as = "Option<tangram_database::value::FromStr>")]
			owner: Option<tg::Id>,
		}

		let p = transaction.p();
		let statement = format!("select created_at, owner from runners where id = {p}1;");
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				tangram_database::params![runner.to_string()],
			)
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let data = row
			.map(|row| {
				let owner = row.owner.map(Self::runner_owner_from_id).transpose()?;
				Ok::<_, tg::Error>(tg::runner::Data {
					created_at: row.created_at,
					id: runner.clone(),
					owner,
				})
			})
			.transpose()?;

		Ok(ControlFlow::Break(data))
	}

	pub(crate) fn runner_owner_from_id(owner: tg::Id) -> tg::Result<tg::Principal> {
		match owner.kind() {
			tg::id::Kind::Group => Ok(tg::Principal::Group(owner.try_into()?)),
			tg::id::Kind::Organization => Ok(tg::Principal::Organization(owner.try_into()?)),
			tg::id::Kind::User => Ok(tg::Principal::User(owner.try_into()?)),
			_ => Err(tg::error!("invalid runner owner")),
		}
	}

	pub(crate) async fn runner_task(&self, id: tg::runner::Id, stopper: Stopper) {
		self.server
			.runner
			.state
			.id
			.lock()
			.unwrap()
			.replace(id.clone());
		self.start_sandbox_pool();
		loop {
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			let run_future = self.runner_task_inner(&id, stopper.clone());
			let run_future = pin!(run_future);
			let future::Either::Right((result, _)) = future::select(stop_future, run_future).await
			else {
				break;
			};
			self.server.runner.state.set_scheduler(None);
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

		// Stop the sandbox pool.
		self.stop_sandbox_pool().await;

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

	pub(crate) fn start_sandbox_pool(&self) {
		self.server.runner.sandbox_pool.start(self);
	}

	pub(crate) async fn stop_sandbox_pool(&self) {
		self.server.runner.sandbox_pool.stop().await;
	}

	async fn runner_task_inner(&self, id: &tg::runner::Id, stopper: Stopper) -> tg::Result<()> {
		// Get the location.
		let location = self.server.config.runner.remote.as_ref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|name| {
				tg::Location::Remote(tg::location::Remote {
					name: name.to_owned(),
					region: None,
				})
			},
		);

		// Get the runner control stream.
		let (output, control) = self.run_get_runner_control_stream(id, &location).await?;
		tracing::info!(
			runner = %id,
			scheduler = %output.scheduler,
			%location,
			"connected to the scheduler"
		);
		self.server
			.runner
			.state
			.set_scheduler(Some(output.scheduler));

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
	) -> tg::Result<(
		tg::runner::control::Output,
		crate::control::Stream<
			tg::runner::control::ServerMessage,
			tg::runner::control::ClientMessage,
		>,
	)> {
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::runner::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let heartbeat = self.create_runner_heartbeat(0);
		let host = tg::host::current().to_owned();
		let location = Some(location.clone().into());
		let scheduler_ttl = self.server.config.runner.scheduler_ttl;
		let arg = tg::runner::control::Arg {
			heartbeat,
			host,
			id: id.clone(),
			location,
			scheduler_ttl,
		};
		let (output, output_stream) = self
			.get_runner_control_stream_with_context(arg, input_stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the scheduler"))?;
		let output_stream = output_stream.boxed();
		let stream =
			crate::control::Stream::new(output_stream, input, crate::control::stream_options());
		Ok((output, stream))
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
			let receive_future = control.recv();
			let receive_future = pin!(receive_future);
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			let result = future::select(receive_future, stop_future).await;
			let message = match result {
				future::Either::Left((result, _)) => result.map_err(|source| {
					tg::error!(!source, "failed to receive a runner control message")
				})?,
				future::Either::Right(_) => break,
			};
			let Some(message) = message else {
				break;
			};

			let message = match message {
				tg::runner::control::ServerMessage::Request(message) => message,
				tg::runner::control::ServerMessage::Ack(_)
				| tg::runner::control::ServerMessage::Response(_) => unreachable!(),
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
			let task = self
				.server
				.spawn_sandbox_task(self::sandbox::SpawnSandboxTaskArg {
					allocation,
					arg: request.arg,
					creator: request.creator,
					id: Some(sandbox.clone()),
					location: location.clone(),
					process: request.process,
					token: Some(token),
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
			Task::spawn({
				let sender = sender.clone();
				move |_| async move {
					let mut events = task.events;
					while let Some(event) = events.recv().await {
						match event {
							Ok(self::sandbox::Event::Destroyed) | Err(_) => break,
							Ok(self::sandbox::Event::Ready(_)) => {},
						}
					}
					let id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
					let notification = tg::runner::control::ClientNotification::SandboxDestroyed(
						tg::runner::control::SandboxDestroyedClientNotification { id, sandbox },
					);
					let message = tg::runner::control::ClientMessage::Notification(notification);
					sender.send(message).await.ok();
				}
			})
			.detach();
		}

		Ok(())
	}

	fn spawn_runner_heartbeat_task(&self, sender: RunnerSender) -> Task<()> {
		let heartbeat_interval = self.server.config.runner.heartbeat_interval;
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

	#[must_use]
	fn create_runner_heartbeat(
		&self,
		index: u64,
	) -> tg::runner::control::HeartbeatClientNotification {
		let capacity = self.server.runner.state.capacity.get();
		tg::runner::control::HeartbeatClientNotification { capacity, index }
	}

	#[must_use]
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
	#[must_use]
	pub(crate) fn capacity(&self) -> &self::capacity::Pool {
		&self.capacity
	}

	#[must_use]
	pub(crate) fn processes(&self) -> &crate::process::Map {
		&self.processes
	}

	#[must_use]
	pub(crate) fn process_tokens(
		&self,
	) -> &dashmap::DashMap<String, tokio::sync::watch::Receiver<Option<tg::process::Id>>> {
		&self.process_tokens
	}

	#[must_use]
	pub(crate) fn reservations(&self) -> &self::capacity::Reservations {
		&self.reservations
	}

	#[must_use]
	pub(crate) fn sandboxes(&self) -> &crate::sandbox::Sandboxes {
		&self.sandboxes
	}

	pub async fn wait_for_scheduler(&self) -> tg::scheduler::Id {
		let mut scheduler = self.scheduler.subscribe();
		loop {
			if let Some(scheduler) = scheduler.borrow_and_update().clone() {
				return scheduler;
			}
			scheduler.changed().await.unwrap();
		}
	}

	pub async fn wait_for_scheduler_change(
		&self,
		current: &tg::scheduler::Id,
	) -> tg::scheduler::Id {
		let mut scheduler = self.scheduler.subscribe();
		loop {
			if let Some(scheduler) = scheduler.borrow_and_update().clone()
				&& scheduler != *current
			{
				return scheduler;
			}
			scheduler.changed().await.unwrap();
		}
	}

	pub fn set_scheduler(&self, scheduler: Option<tg::scheduler::Id>) {
		self.scheduler.send_replace(scheduler);
	}

	#[must_use]
	fn create_sandbox_index(&self) -> u64 {
		let index = self.next_sandbox_index.fetch_add(1, Ordering::Relaxed);
		assert_ne!(index, u64::MAX, "exhausted the sandbox indexes");

		index
	}

	#[must_use]
	pub fn id(&self) -> Option<tg::runner::Id> {
		self.id.lock().unwrap().clone()
	}

	#[must_use]
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

	#[must_use]
	pub fn try_get_sandbox(&self, id: &tg::sandbox::Id) -> Option<tg::sandbox::get::Output> {
		self.sandboxes
			.get_by_id(id)
			.map(|sandbox| sandbox.data.clone())
	}

	#[must_use]
	pub fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: u64,
		length: u64,
	) -> Option<crate::sandbox::processes::Output> {
		let sandbox = self.sandboxes.get_by_id(id)?;
		let mut processes = sandbox.processes.keys().cloned().collect::<Vec<_>>();
		processes.sort();
		let processes_length = u64::try_from(processes.len()).unwrap();
		let start = usize::try_from(position.min(processes_length)).unwrap();
		let end = usize::try_from(position.saturating_add(length).min(processes_length)).unwrap();
		Some(crate::sandbox::processes::Output {
			length: processes_length,
			processes: processes[start..end].to_vec(),
			status: sandbox.data.status,
		})
	}

	#[must_use]
	pub fn try_get_process(&self, id: &tg::process::Id) -> Option<tg::process::Data> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		Some(process.data())
	}

	#[must_use]
	pub fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> Option<tg::process::control::GetChildrenClientResponseOutput> {
		let sandbox = self.try_get_process_sandbox(id)?;
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let process = sandbox.processes.get(id)?;
		let children_length = u64::try_from(process.children.len()).unwrap();
		let start = usize::try_from(position.min(children_length)).unwrap();
		let end = usize::try_from(position.saturating_add(length).min(children_length)).unwrap();
		let children = process
			.children
			.get_range(start..end)
			.unwrap()
			.values()
			.map(|child| child.data.clone())
			.collect();
		Some(tg::process::control::GetChildrenClientResponseOutput {
			children,
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
		let mut sandbox = self.sandboxes.get_mut_by_id(&sandbox)?;
		let process = sandbox.processes.get_mut(id)?;
		Some(update(process))
	}

	#[must_use]
	pub fn try_get_process_sandbox(&self, id: &tg::process::Id) -> Option<tg::sandbox::Id> {
		self.processes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
	}
}
