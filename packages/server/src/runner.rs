use {
	crate::{Session, Shutdown},
	futures::{
		FutureExt as _, StreamExt as _, future, future::BoxFuture, stream::FuturesUnordered,
	},
	std::{
		ops::ControlFlow,
		pin::pin,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::{Stopper, Task},
};

#[cfg(test)]
mod tests;

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

type CreateControlConnection<T> = Arc<dyn Fn() -> BoxFuture<'static, tg::Result<T>> + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub(super) struct Config {
	pub capacity: tg::runner::Capacity,
	pub process_control_connection_pool_size: usize,
	pub process_control_connection_pool_ttl: Duration,
	pub sandbox_control_connection_pool_size: usize,
	pub sandbox_control_connection_pool_ttl: Duration,
	pub sandbox_pool_size: usize,
}

struct ControlConnectionPoolEntry<T> {
	connection: Mutex<Option<T>>,
}

struct ControlConnectionPool<T: Send + 'static> {
	name: &'static str,
	notify: Arc<tokio::sync::Notify>,
	pool: Mutex<Option<tangram_pool::Pool<ControlConnectionPoolEntry<T>, tg::Error>>>,
	size: usize,
	task: Mutex<Option<Task<()>>>,
	ttl: Duration,
}

pub struct Runner {
	process_control_connection_pool: ControlConnectionPool<self::process::ProcessControlConnection>,
	sandbox_control_connection_pool: ControlConnectionPool<self::sandbox::SandboxControlConnection>,
	sandbox_pool: self::sandbox::Pool,
	state: State,
	task: Mutex<Option<Task<()>>>,
}

pub struct State {
	capacity: self::capacity::Pool,
	id: Mutex<Option<tg::runner::Id>>,
	next_sandbox_index: AtomicU64,
	process_for_token: dashmap::DashMap<String, (u64, tg::process::Id)>,
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
			process_for_token: dashmap::DashMap::new(),
			processes: crate::process::Map::default(),
			reservations: self::capacity::Reservations::new(),
			sandboxes: crate::sandbox::Sandboxes::default(),
			scheduler,
		};
		let task = Mutex::new(None);
		let process_control_connection_pool = ControlConnectionPool::new(
			"process",
			config.process_control_connection_pool_size,
			config.process_control_connection_pool_ttl,
		);
		let sandbox_control_connection_pool = ControlConnectionPool::new(
			"sandbox",
			config.sandbox_control_connection_pool_size,
			config.sandbox_control_connection_pool_ttl,
		);
		let sandbox_pool = self::sandbox::Pool::new(config.sandbox_pool_size);
		Self {
			process_control_connection_pool,
			sandbox_control_connection_pool,
			sandbox_pool,
			state,
			task,
		}
	}

	#[must_use]
	fn process_control_connection_pool(
		&self,
	) -> &ControlConnectionPool<self::process::ProcessControlConnection> {
		&self.process_control_connection_pool
	}

	#[must_use]
	fn sandbox_control_connection_pool(
		&self,
	) -> &ControlConnectionPool<self::sandbox::SandboxControlConnection> {
		&self.sandbox_control_connection_pool
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

impl<T: Send + 'static> ControlConnectionPool<T> {
	#[must_use]
	fn new(name: &'static str, size: usize, ttl: Duration) -> Self {
		Self {
			name,
			notify: Arc::new(tokio::sync::Notify::new()),
			pool: Mutex::new(None),
			size,
			task: Mutex::new(None),
			ttl,
		}
	}

	fn start<F, Fut>(&self, create: F)
	where
		F: Fn() -> Fut + Send + Sync + 'static,
		Fut: Future<Output = tg::Result<T>> + Send + 'static,
	{
		let create: CreateControlConnection<T> = Arc::new(move || create().boxed());
		let create_for_pool = create.clone();
		let options = tangram_pool::Options {
			max: self.size.max(1),
			min: 0,
			shared: 1,
			ttl: Some(self.ttl),
		};
		let pool = tangram_pool::Pool::new(options, move || {
			let create = create_for_pool.clone();
			async move {
				let connection = create().await?;
				Ok(ControlConnectionPoolEntry {
					connection: Mutex::new(Some(connection)),
				})
			}
		});
		self.pool.lock().unwrap().replace(pool.clone());
		if self.size == 0 || self.ttl.is_zero() {
			return;
		}
		let interval = (self.ttl / 2).max(Duration::from_millis(10));
		let name = self.name;
		let notify = self.notify.clone();
		let size = self.size;
		let task = Task::spawn(move |stopper| async move {
			loop {
				while pool.available() < size {
					let result = tokio::select! {
						biased;
						() = stopper.wait() => return,
						result = create() => result,
					};
					let connection = match result {
						Ok(connection) => connection,
						Err(error) => {
							tracing::warn!(
								error = %error.trace(),
								kind = name,
								"failed to add a control connection to the pool",
							);
							break;
						},
					};
					let entry = ControlConnectionPoolEntry {
						connection: Mutex::new(Some(connection)),
					};
					pool.add(entry);
				}
				tokio::select! {
					() = notify.notified() => {},
					() = stopper.wait() => break,
					() = tokio::time::sleep(interval) => {},
				}
			}
		});
		self.task.lock().unwrap().replace(task);
	}

	async fn take(&self) -> tg::Result<T> {
		let pool = self
			.pool
			.lock()
			.unwrap()
			.clone()
			.ok_or_else(|| tg::error!("the control connection pool is not running"))?;
		let guard = pool.get_exclusive(tangram_pool::Priority::High).await?;
		let connection = guard
			.connection
			.lock()
			.unwrap()
			.take()
			.expect("the control connection pool entry was empty");
		guard.discard();
		self.notify.notify_one();

		Ok(connection)
	}

	async fn shutdown(&self) {
		let task = self.task.lock().unwrap().take();
		if let Some(task) = task {
			task.stop();
			task.wait().await.ok();
		}
		if let Some(pool) = self.pool.lock().unwrap().take() {
			pool.clear();
		}
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
		self.start_control_connection_pools();
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

		let shutdown = self
			.server
			.shutdown
			.borrow()
			.expect("the shutdown mode was not set");

		// Stop the pools.
		self.shutdown_control_connection_pools().await;
		self.shutdown_sandbox_pool(shutdown).await;

		// Shut down the sandbox tasks.
		let sandboxes = match shutdown {
			Shutdown::Interrupt => {
				self.server.sandbox_tasks.stop_all();
				Vec::new()
			},
			Shutdown::Terminate => {
				let sandboxes = self
					.server
					.runner
					.state
					.sandboxes
					.iter()
					.filter_map(|sandbox| {
						for process in sandbox.processes.iter() {
							process.stopper.stop();
						}
						let id = sandbox.id.clone();
						let sandbox = sandbox.sandbox.clone()?;

						Some((id, sandbox))
					})
					.collect::<Vec<_>>();
				self.server.sandbox_tasks.abort_all();

				sandboxes
			},
		};
		let wait_future = self.server.sandbox_tasks.wait();
		let terminate_future = sandboxes
			.into_iter()
			.map(|(id, sandbox)| async move {
				if let Err(error) = sandbox.destroy().await {
					tracing::error!(?id, error = %error.trace(), "failed to terminate a sandbox");
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>();
		let (results, ()) = tokio::join!(wait_future, terminate_future);
		for result in results {
			if let Err(error) = result
				&& !error.is_cancelled()
			{
				tracing::error!(?error, "a sandbox task panicked");
			}
		}
	}

	fn start_control_connection_pools(&self) {
		let context = crate::Context {
			token: self.server.config.runner.token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		self.server
			.runner
			.process_control_connection_pool()
			.start(move || {
				let session = session.clone();
				async move { session.create_process_control_connection().await }
			});
		let session = self.server.session(&context);
		self.server
			.runner
			.sandbox_control_connection_pool()
			.start(move || {
				let session = session.clone();
				async move { session.create_sandbox_control_connection().await }
			});
	}

	async fn shutdown_control_connection_pools(&self) {
		self.server
			.runner
			.process_control_connection_pool()
			.shutdown()
			.await;
		self.server
			.runner
			.sandbox_control_connection_pool()
			.shutdown()
			.await;
	}

	pub(crate) fn start_sandbox_pool(&self) {
		self.server.runner.sandbox_pool.start(self);
	}

	pub(crate) async fn shutdown_sandbox_pool(&self, shutdown: Shutdown) {
		self.server.runner.sandbox_pool.shutdown(shutdown).await;
	}

	pub(crate) async fn stop_sandbox_pool(&self) {
		self.shutdown_sandbox_pool(Shutdown::Interrupt).await;
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
		let (output, control) = self
			.run_get_runner_control_stream(id, &location)
			.boxed()
			.await?;
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
			.boxed()
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
			let receive_future = control.recv_with_ack();
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
				sender.send(message).await?;
				continue;
			};
			// Spawn the sandbox task.
			let sandbox = request.sandbox.clone();
			let Some(token) = request.token else {
				let message = Self::create_runner_control_response(
					id.clone(),
					Err(tg::error!(%sandbox, "missing the sandbox authentication token")),
				);
				sender.send(message).await?;
				continue;
			};
			let _ = self
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
			sender.send(message).await?;
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
	pub(crate) fn process_for_token(&self) -> &dashmap::DashMap<String, (u64, tg::process::Id)> {
		&self.process_for_token
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
					.iter()
					.filter(|process| process.value().data.status == tg::process::Status::Started)
					.count()
			})
			.sum::<usize>()
			.try_into()
			.unwrap()
	}

	#[must_use]
	pub fn try_get_sandbox(&self, id: &tg::sandbox::Id) -> Option<tg::sandbox::get::Output> {
		Some(self.sandboxes.get_by_id(id)?.data())
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
		let sandbox = self.sandboxes.get_by_id(&sandbox)?;
		let mut process = sandbox.processes.get_mut(id)?;
		Some(update(&mut process))
	}

	#[must_use]
	pub fn try_get_process_sandbox(&self, id: &tg::process::Id) -> Option<tg::sandbox::Id> {
		self.processes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
	}

	pub(crate) fn remove_process(&self, id: &tg::process::Id) {
		let Some((_, sandbox)) = self.processes.remove(id) else {
			return;
		};
		if let Some(sandbox) = self.sandboxes.get_by_id(&sandbox) {
			sandbox.processes.remove(id);
		}
	}

	pub(crate) fn remove_sandbox(&self, id: &tg::sandbox::Id) {
		let Some(index) = self.sandboxes.get_by_id(id).map(|sandbox| *sandbox.key()) else {
			return;
		};
		if let Some(sandbox) = self.sandboxes.remove(index) {
			for process in sandbox.processes.iter() {
				self.processes.remove(process.key());
			}
			sandbox.processes.clear();
		}
	}
}
