use {
	futures::{
		FutureExt as _, StreamExt as _, future,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		ops::ControlFlow,
		pin::pin,
		sync::{Arc, Mutex, atomic::Ordering},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::{Stopper, Task},
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{Server, Session, Shutdown},
};

mod assets;

pub mod control;
pub mod create;
pub mod delete;
pub mod list;
pub mod process;
pub mod sandbox;
pub mod token;

pub type RunnerSender = tangram_server_core::control::Sender<
	tg::runner::control::ServerMessage,
	tg::runner::control::ClientMessage,
>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub control: tokio::sync::watch::Sender<Option<RunnerSender>>,

	#[cfg(target_os = "linux")]
	pub ip_pool: tangram_sandbox::network::ip::Pool,

	pub sandbox_assets: crate::assets::Assets,

	pub sandbox_pool: crate::sandbox::pool::Pool,

	pub sandbox_tasks: tangram_futures::task::Map<String, ()>,

	pub state: tangram_server_runner_core::State,

	pub task: Mutex<Option<Task<()>>>,
}

impl Component {
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Self> {
		// Create the runner state, warming sandboxes only on runners.
		let config = Self::create_config(arg.config, arg.path)?;
		let sandbox_pool = crate::sandbox::pool::Pool::new(config.sandbox_pool_size);
		let state = tangram_server_runner_core::State::new(config);

		// Create the IP pool.
		#[cfg(target_os = "linux")]
		let ip_pool = tangram_sandbox::network::ip::Pool::new(
			arg.config
				.sandbox
				.network
				.ip_ranges
				.iter()
				.map(|range| (range.min.to_bits(), range.max.to_bits())),
		);

		// Create the sandbox tasks.
		let sandbox_tasks = tangram_futures::task::Map::default();

		// Prepare the runner's reusable sandbox assets.
		let sandbox_assets = crate::assets::Assets::new(arg.path, arg.tangram_path, arg.config)?;

		// Create the task handle.
		let task = Mutex::new(None);

		// Create the runner control sender.
		let (control, _) = tokio::sync::watch::channel(None);

		// Create the state.
		let state = State {
			control,

			#[cfg(target_os = "linux")]
			ip_pool,

			sandbox_assets,

			sandbox_pool,

			sandbox_tasks,

			state,

			task,
		};
		let component = Self(Arc::new(state));

		Ok(component)
	}

	fn create_config(
		config: &tangram_server_core::Config,
		#[cfg_attr(not(target_os = "linux"), expect(unused_variables))] path: &std::path::Path,
	) -> tg::Result<tangram_server_runner_core::Config> {
		// Get the available parallelism.
		let parallelism =
			std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);

		// Compute the capacity.
		let capacity = if config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			let runner = &config.runner;
			let cpus = runner
				.cpus
				.unwrap_or_else(|| u64::try_from(parallelism).unwrap());
			#[cfg(target_os = "linux")]
			let (physical_memory, physical_storage) = tangram_sandbox::capacity::detect(path)?;
			#[cfg(not(target_os = "linux"))]
			let (physical_memory, physical_storage) = (1_073_741_824_u64.saturating_mul(cpus), 0);
			let memory = runner.memory.unwrap_or(physical_memory * 3 / 4);
			let storage = runner.storage.unwrap_or(physical_storage * 3 / 4);
			if memory > physical_memory || storage > physical_storage {
				return Err(tg::error!(
					"the runner resource capacity exceeds its available backing"
				));
			}
			let memory = tg::sandbox::Resource::new(
				memory,
				memory
					.checked_mul(runner.overcommit.memory)
					.ok_or_else(|| tg::error!("the memory overcommit capacity overflowed"))?,
			);
			let storage = tg::sandbox::Resource::new(
				storage,
				storage
					.checked_mul(runner.overcommit.storage)
					.ok_or_else(|| tg::error!("the storage overcommit capacity overflowed"))?,
			);

			let max = cpus
				.checked_mul(runner.overcommit.cpu)
				.and_then(|value| value.checked_mul(1000))
				.ok_or_else(|| tg::error!("the CPU capacity is too large"))?;
			let max = tg::sandbox::Amount::from_millis(max);
			if cpus > u64::try_from(parallelism).unwrap() {
				return Err(tg::error!(
					"the runner CPU capacity exceeds its available hardware threads"
				));
			}
			let min = cpus
				.checked_mul(1000)
				.ok_or_else(|| tg::error!("the CPU capacity is too large"))?;
			let min = tg::sandbox::Amount::from_millis(min);
			let sandboxes = runner.sandboxes.unwrap_or(cpus);
			if sandboxes == 0 {
				return Err(tg::error!(
					"the runner sandbox capacity must be greater than zero"
				));
			}
			let capacity = tg::runner::Capacity {
				cpu: tg::sandbox::Cpu::new(min, max),
				memory,
				sandboxes,
				storage,
			};
			if capacity.cpu.max.is_none_or(|max| max.millis() == 0) {
				return Err(tg::error!(
					"the runner CPU capacity must be greater than zero"
				));
			}

			if capacity.memory.max.is_none_or(|max| max == 0) {
				return Err(tg::error!(
					"the runner memory capacity must be greater than zero"
				));
			}

			capacity
		} else {
			tg::runner::Capacity::default()
		};

		// Create the config, warming sandboxes only on runners.
		let sandbox_pool_size = if config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			config.runner.sandbox_pool_size
		} else {
			0
		};
		let runner_config = tangram_server_runner_core::Config {
			capacity,

			#[cfg(target_os = "linux")]
			filesystem_project_ids: config.runner.resources.filesystem_project_ids,

			overcommit: config.runner.overcommit,

			sandbox_pool_size,
		};

		Ok(runner_config)
	}

	fn spawn_runner_task(&self, server: &Server) -> Option<Task<()>> {
		if !server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			return None;
		}
		let id = server
			.config
			.runner
			.id
			.clone()
			.unwrap_or_else(tg::runner::Id::new);
		let context = tangram_server_core::context::Context {
			principal: tg::Principal::Runner(id.clone()),
			..server.context.clone()
		};
		let session = server.session(&context);
		let component = self.clone();
		let task = Task::spawn(|stopper| async move {
			// Wait for this server's scheduler to subscribe, so that the runner's first scheduler message is not lost, unless the server stops first.
			tokio::select! {
				biased;
				() = session.server.components.scheduler.started() => {},
				() = stopper.wait() => return,
			}
			component.runner_task(&session, id, stopper).boxed().await;
		});

		Some(task)
	}

	pub(crate) fn stop_runner_task(&self) {
		if let Some(task) = self.task.lock().unwrap().as_ref() {
			task.stop();
		}
	}
}

impl Component {
	pub(crate) async fn authorize_runner_owner(
		&self,
		session: &Session,
		owner: Option<&tg::Id>,
	) -> tg::Result<()> {
		session.verify_request_from_host()?;
		let Some(owner) = owner else {
			if matches!(session.context.principal, tg::Principal::Root) {
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		};
		let permission = tangram_server_grant_common::admin_permission_for_resource(owner)?;
		let authorized = session
			.authorize(owner.clone(), permission)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(permission);
		if !authorized {
			return Err(tg::error!("unauthorized"));
		}

		Ok(())
	}

	pub(crate) async fn resolve_runner_owner(
		&self,
		session: &Session,
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
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let owner = owner.clone();
				let session = session.clone();
				async move {
					Self::resolve_runner_owner_with_transaction(&session, transaction, &owner).await
				}
				.boxed()
			})
			.await
	}

	async fn resolve_runner_owner_with_transaction(
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		owner: &tg::authorization::subject::Selector,
	) -> tg::Result<ControlFlow<tg::Principal, tangram_server_core::database::Error>> {
		let owner = match session
			.server
			.components
			.grant
			.resolve_subject_with_transaction(transaction, owner)
			.await?
		{
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

	pub async fn try_get_runner_data(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tg::runner::Data>> {
		let runner = runner.clone();
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let runner = runner.clone();
				async move { Self::try_get_runner_data_with_transaction(transaction, &runner).await }
					.boxed()
			})
			.await
	}

	async fn try_get_runner_data_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Option<tg::runner::Data>, tangram_server_core::database::Error>> {
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
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
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

	pub(crate) async fn runner_task(
		&self,
		session: &Session,
		id: tg::runner::Id,
		stopper: Stopper,
	) {
		session
			.server
			.components
			.runner
			.state()
			.id
			.lock()
			.unwrap()
			.replace(id.clone());
		self.start_sandbox_pool(session);
		let mut cleanup = true;
		let mut retries = None::<BoxStream<'static, ()>>;
		loop {
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			let started = tokio::time::Instant::now();
			let run_future = self.runner_task_inner(session, &id, &mut cleanup, stopper.clone());
			let run_future = pin!(run_future);
			let future::Either::Right((result, _)) = future::select(stop_future, run_future).await
			else {
				break;
			};
			session.server.components.runner.state().set_scheduler(None);

			// Reconnect at once after a connection that lasted a heartbeat interval ends, with backoff while the scheduler cannot be reached or a connection ends sooner, and after the restart delay after a failure.
			let lasted = started.elapsed() >= session.server.config.runner.heartbeat_interval;
			let delay = match result {
				Ok(ControlFlow::Break(())) if lasted => {
					tracing::info!(runner = %id, "the runner control connection ended");
					retries = None;
					continue;
				},
				Ok(ControlFlow::Break(())) => {
					tracing::warn!(runner = %id, "the runner control connection ended before a heartbeat interval");
					let retries = retries.get_or_insert_with(|| {
						tangram_futures::retry::stream(
							tangram_server_core::Server::client_stream_retry(),
						)
						.boxed()
					});
					retries.next().boxed()
				},
				Ok(ControlFlow::Continue(error)) => {
					tracing::warn!(error = %error.trace(), runner = %id, "failed to reach the scheduler");
					let retries = retries.get_or_insert_with(|| {
						tangram_futures::retry::stream(
							tangram_server_core::Server::client_stream_retry(),
						)
						.boxed()
					});
					retries.next().boxed()
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "the runner task failed");
					retries = None;
					tokio::time::sleep(session.server.config.runner.restart_delay)
						.map(|()| Some(()))
						.boxed()
				},
			};
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			if matches!(
				future::select(stop_future, delay).await,
				future::Either::Left(_)
			) {
				break;
			}
		}

		let shutdown = session
			.server
			.shutdown
			.borrow()
			.expect("the shutdown mode was not set");

		// Stop the pools.
		self.shutdown_sandbox_pool(shutdown).await;

		// Shut down the sandbox tasks.
		let sandboxes = match shutdown {
			Shutdown::Interrupt => {
				self.sandbox_tasks.stop_all();
				Vec::new()
			},
			Shutdown::Terminate => {
				let sandboxes = session
					.server
					.components
					.runner
					.state()
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
				self.sandbox_tasks.abort_all();

				sandboxes
			},
		};
		let wait_future = self.sandbox_tasks.wait();
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

	pub fn start_sandbox_pool(&self, session: &Session) {
		self.sandbox_pool.start(self, session);
	}

	pub(crate) async fn shutdown_sandbox_pool(&self, shutdown: Shutdown) {
		self.sandbox_pool.shutdown(shutdown).await;
	}

	pub async fn stop_sandbox_pool(&self) {
		self.shutdown_sandbox_pool(Shutdown::Interrupt).await;
	}

	async fn runner_task_inner(
		&self,
		session: &Session,
		id: &tg::runner::Id,
		cleanup: &mut bool,
		stopper: Stopper,
	) -> tg::Result<ControlFlow<(), tg::Error>> {
		// Get the location.
		let location = session.server.config.runner.remote.as_ref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|name| {
				tg::Location::Remote(tg::location::Remote {
					name: name.to_owned(),
					region: None,
				})
			},
		);

		// Get the runner control stream, continuing if the scheduler could not be reached.
		let (header, mut control) = match self
			.run_get_runner_control_stream(session, id, &location, *cleanup)
			.boxed()
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		tracing::info!(runner = %id, scheduler = %header.scheduler, previous_sandboxes = header.sandboxes.len(), "connected the runner to the scheduler");

		// Destroy the previous sandboxes before accepting work.
		let mut index = 1;
		if *cleanup {
			self.cleanup_runner_sandboxes(session, header.sandboxes, &mut control, &mut index)
				.boxed()
				.await?;
			*cleanup = false;
		}
		tracing::info!(runner = %id, "the runner is ready to accept work");
		session
			.server
			.components
			.runner
			.state()
			.set_scheduler(Some(header.scheduler));

		// Handle new sandbox requests.
		self.run_handle_runner_control_stream(session, location, control, index, stopper)
			.boxed()
			.await?;

		Ok(ControlFlow::Break(()))
	}

	async fn cleanup_runner_sandboxes(
		&self,
		session: &Session,
		sandboxes: Vec<tg::sandbox::Id>,
		control: &mut tangram_server_core::control::Stream<
			tg::runner::control::ServerMessage,
			tg::runner::control::ClientMessage,
		>,
		index: &mut u64,
	) -> tg::Result<()> {
		let sender = control.sender();
		let destroy = self.destroy_runner_sandboxes(sandboxes, sender.clone());
		let mut destroy = pin!(destroy);
		let mut interval = tokio::time::interval(session.server.config.runner.heartbeat_interval);
		loop {
			let message = tokio::select! {
				result = &mut destroy => {
					result?;
					break;
				},
				_ = interval.tick() => {
					let heartbeat = Self::create_runner_heartbeat(session, *index, true);
					let message = tg::runner::control::ClientMessage::Notification(
						tg::runner::control::ClientNotification::Heartbeat(heartbeat),
					);
					*index = index.wrapping_add(1);
					sender.send(message).await?;
					continue;
				},
				message = control.recv_with_ack() => message?,
			};
			let Some(message) = message else {
				return Err(tg::error!(
					"the runner control stream ended during startup cleanup"
				));
			};
			let request = match message {
				tg::runner::control::ServerMessage::Ack(_) => unreachable!(),

				tg::runner::control::ServerMessage::Request(request) => request,

				// The control stream has already acknowledged duplicate responses.
				tg::runner::control::ServerMessage::Response(_) => continue,
			};
			let tg::runner::control::ServerRequestArg::CreateSandbox(_) = request.arg;
			let output = tg::runner::control::CreateSandboxClientResponseOutput { created: false };
			let message = Self::create_runner_control_response(
				request.id,
				Ok(tg::runner::control::ClientResponseOutput::CreateSandbox(
					output,
				)),
			);
			sender.send(message).await?;
		}

		Ok(())
	}

	async fn destroy_runner_sandboxes(
		&self,
		sandboxes: Vec<tg::sandbox::Id>,
		sender: RunnerSender,
	) -> tg::Result<()> {
		for sandbox in sandboxes {
			let arg = tg::runner::control::DestroySandboxClientRequestArg {
				error: None,
				sandbox,
			};
			match Self::request_runner_sandbox_destroy(&sender, arg).await? {
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => {
					return Err(tg::error!(
						!error,
						"failed to destroy the previous runner sandbox"
					));
				},
			}
		}

		Ok(())
	}

	/// Ask the scheduler to destroy a sandbox, waiting for a runner control stream and retrying after the stream is lost.
	pub(crate) async fn destroy_sandbox_with_runner_control(
		&self,
		arg: tg::runner::control::DestroySandboxClientRequestArg,
		stopper: &Stopper,
	) -> tg::Result<()> {
		let mut control = self.control.subscribe();
		loop {
			let sender = tokio::select! {
				result = control.wait_for(Option::is_some) => result
					.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?
					.clone()
					.unwrap(),
				() = stopper.wait() => {
					return Err(tg::error!("the runner stopped before the sandbox was destroyed"));
				},
			};
			match Self::request_runner_sandbox_destroy(&sender, arg.clone()).await? {
				ControlFlow::Break(()) => return Ok(()),
				ControlFlow::Continue(error) => {
					tracing::warn!(error = %error.trace(), sandbox = %arg.sandbox, "failed to send the destroy sandbox request");
					control.mark_unchanged();
					tokio::select! {
						result = control.changed() => result.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?,
						() = stopper.wait() => {
							return Err(tg::error!("the runner stopped before the sandbox was destroyed"));
						},
					}
				},
			}
		}
	}

	async fn request_runner_sandbox_destroy(
		sender: &RunnerSender,
		arg: tg::runner::control::DestroySandboxClientRequestArg,
	) -> tg::Result<ControlFlow<(), tg::Error>> {
		let request = tg::runner::control::ClientRequest {
			arg: tg::runner::control::ClientRequestArg::DestroySandbox(arg),
			id: tangram_server_core::control::id(),
		};
		let response = sender
			.request(
				tg::runner::control::ClientMessage::Request(request),
				tangram_server_core::control::Priority::High,
			)
			.await;
		let response = match response {
			Ok(response) => response.await,
			Err(error) => return Ok(ControlFlow::Continue(error)),
		};
		let Ok(response) = response else {
			let error = tg::error!("the runner control response stream ended");
			return Ok(ControlFlow::Continue(error));
		};
		let tg::runner::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a runner control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error).map_err(|error| {
				tg::error!(!error, "failed to deserialize the runner control error")
			})?;
			return Err(tg::error!(!error, "the destroy sandbox request failed"));
		}
		let Some(tg::runner::control::ServerResponseOutput::DestroySandbox(_)) = response.output
		else {
			return Err(tg::error!("expected a destroy sandbox response"));
		};

		Ok(ControlFlow::Break(()))
	}

	async fn run_get_runner_control_stream(
		&self,
		session: &Session,
		id: &tg::runner::Id,
		location: &tg::Location,
		cleanup: bool,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				tangram_server_core::control::Stream<
					tg::runner::control::ServerMessage,
					tg::runner::control::ClientMessage,
				>,
			),
			tg::Error,
		>,
	> {
		let capacity = session.server.config.runner.control_channel_capacity;
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::runner::control::ClientMessage>(capacity);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let heartbeat = Self::create_runner_heartbeat(session, 0, cleanup);
		let host = tg::host::current().to_owned();
		let location = Some(location.clone().into());
		let scheduler_ttl = session.server.config.runner.scheduler_ttl;
		let arg = tg::runner::control::Arg {
			heartbeat,
			host,
			id: id.clone(),
			location,
			scheduler_ttl,
		};
		let output = self
			.get_runner_control_stream_with_context(session, arg, input_stream)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the scheduler"))?;
		let (header, output_stream) = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => {
				let error = tg::error!(!error, "failed to connect to the scheduler");
				return Ok(ControlFlow::Continue(error));
			},
		};
		let output_stream = output_stream.boxed();
		let stream = tangram_server_core::control::Stream::new(
			output_stream,
			input,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		Ok(ControlFlow::Break((header, stream)))
	}

	async fn run_handle_runner_control_stream(
		&self,
		session: &Session,
		location: tg::Location,
		mut control: tangram_server_core::control::Stream<
			tg::runner::control::ServerMessage,
			tg::runner::control::ClientMessage,
		>,
		mut index: u64,
		stopper: Stopper,
	) -> tg::Result<()> {
		let sender = control.sender();

		// Publish the sender while the stream handles requests, so that sandbox tasks can report destruction through it.
		self.control.send_replace(Some(sender.clone()));
		let _control = scopeguard::guard(&self.control, |control| {
			control.send_replace(None);
		});

		let mut interval = tokio::time::interval(session.server.config.runner.heartbeat_interval);

		// Process the messages the scheduler sends to this runner.
		loop {
			let message = tokio::select! {
				() = async {
					tokio::select! {
						_ = interval.tick() => {},
						() = session.server.components.runner.state().capacity.wait_for_change() => {},
					}
				} => {
					let heartbeat = Self::create_runner_heartbeat(session, index, false);
					let message = tg::runner::control::ClientMessage::Notification(
						tg::runner::control::ClientNotification::Heartbeat(heartbeat),
					);
					index = index.wrapping_add(1);
					sender.send(message).await?;
					continue;
				},
				message = control.recv_with_ack() => message.map_err(|source| {
					tg::error!(!source, "failed to receive a runner control message")
				})?,
				() = stopper.wait() => break,
			};
			let Some(message) = message else {
				break;
			};

			let message = match message {
				tg::runner::control::ServerMessage::Ack(_) => unreachable!(),

				tg::runner::control::ServerMessage::Request(message) => message,

				// The control stream has already acknowledged duplicate responses.
				tg::runner::control::ServerMessage::Response(_) => continue,
			};
			let id = message.id;
			let tg::runner::control::ServerRequestArg::CreateSandbox(request) = message.arg;

			let requested = request.capacity;

			// Decline work if capacity is unavailable.
			let allocation = session
				.server
				.components
				.process
				.try_acquire_scheduled_sandbox_capacity(
					session,
					request.borrowed,
					request.parent.as_ref(),
					requested,
				);
			let Some(allocation) = allocation else {
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
			let _ = self.spawn_sandbox_task(
				&session.server,
				tangram_server_runner_core::sandbox::SpawnSandboxTaskArg {
					allocation,
					arg: request.arg,
					creator: request.creator,
					id: Some(sandbox.clone()),
					location: location.clone(),
					process: request.process,
					token: Some(token),
				},
			);

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

	#[must_use]
	fn create_runner_heartbeat(
		session: &Session,
		index: u64,
		cleanup: bool,
	) -> tg::runner::control::HeartbeatClientNotification {
		// Publish the snapshot index before reading the state so later capacity hints can reject older snapshots.
		session
			.server
			.components
			.runner
			.state()
			.heartbeat_index
			.store(index, Ordering::Release);
		let capacity = if cleanup {
			tg::runner::control::Capacity::default()
		} else {
			session.server.components.runner.state().capacity.get()
		};
		tracing::debug!(target: "tangram_server::runner::control", index, cleanup, available_cpu_min = capacity.available.cpu.min.millis(), available_cpu_max = ?capacity.available.cpu.max.map(tg::sandbox::Amount::millis), available_memory = ?capacity.available.memory, "sending the runner heartbeat");
		let sandboxes = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.iter()
			.filter(|sandbox| !cleanup && sandbox.allocation.is_some())
			.map(|sandbox| sandbox.id.clone())
			.collect();
		tg::runner::control::HeartbeatClientNotification {
			capacity,
			index,
			sandboxes,
		}
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

impl Component {
	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["runners"]) => self.create_runner_request(session, request).await,
			(http::Method::GET, ["runners"]) => self.list_runners_request(session, request).await,
			(http::Method::DELETE, ["runners", runner]) => {
				self.try_delete_runner_request(session, request, runner)
					.await
			},
			(http::Method::POST, ["runners", runner, "tokens"]) => {
				self.create_runner_token_request(session, request, runner)
					.await
			},
			(http::Method::GET, ["runners", runner, "tokens"]) => {
				self.list_runner_tokens_request(session, request, runner)
					.await
			},
			(http::Method::DELETE, ["runners", runner, "tokens", token]) => {
				self.try_delete_runner_token_request(session, request, runner, token)
					.await
			},
			(http::Method::POST, ["runners", "control"]) => {
				self.get_runner_control_stream_request(session, request)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn start<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			// Spawn the runner task.
			let task = self.spawn_runner_task(server);
			*self.task.lock().unwrap() = task;

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the runner task.
			let task = self.task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the runner task panicked");
				}
				tracing::trace!("runner task");
			}

			// Abort the sandbox tasks.
			self.sandbox_tasks.abort_all();
			let results = self.sandbox_tasks.wait().await;
			for result in results {
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "a sandbox task panicked");
				}
			}
			tracing::trace!("sandbox tasks");
		}
		.boxed()
	}

	fn abort(&self) {
		self.sandbox_tasks.abort_all();
	}
}

impl tangram_server_core::runner::Component for Component {
	fn try_get_runner_data<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::Data>>> {
		self.try_get_runner_data(session, runner).boxed()
	}

	fn guest_path_for_host_path(
		&self,
		session: &tangram_server_core::Session,
		path: &std::path::Path,
	) -> tg::Result<std::path::PathBuf> {
		Self::guest_path_for_host_path(session, path)
	}

	fn host_path_for_guest_path(
		&self,
		session: &tangram_server_core::Session,
		path: &std::path::Path,
	) -> tg::Result<std::path::PathBuf> {
		Self::host_path_for_guest_path(session, path)
	}

	fn host_path_for_guest_path_with_store_path(
		&self,
		session: &tangram_server_core::Session,
		path: &std::path::Path,
		store_path: &std::path::Path,
	) -> tg::Result<std::path::PathBuf> {
		Self::host_path_for_guest_path_with_store_path(session, path, store_path)
	}

	fn resolve_sandbox_backend(
		&self,
		server: &tangram_server_core::Server,
	) -> tg::Result<tangram_sandbox::Backend> {
		self.resolve_sandbox_backend(server)
	}

	fn send_runner_control_request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		scheduler: &'a tg::scheduler::Id,
		connection_index: u64,
		arg: tg::runner::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>>> {
		self.send_runner_control_request(session, runner, scheduler, connection_index, arg, options)
			.boxed()
	}

	fn spawn_sandbox_task<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		arg: tangram_server_runner_core::sandbox::SpawnSandboxTaskArg,
	) -> tangram_server_runner_core::sandbox::SpawnSandboxTaskOutput {
		self.spawn_sandbox_task(server, arg)
	}

	fn start_sandbox_pool<'a>(&'a self, session: &'a tangram_server_core::Session) {
		self.start_sandbox_pool(session);
	}

	fn state(&self) -> &tangram_server_runner_core::State {
		&self.state
	}

	fn stop_runner_task(&self) {
		self.stop_runner_task();
	}

	fn stop_sandbox_pool(&self) -> BoxFuture<'_, ()> {
		self.stop_sandbox_pool().boxed()
	}

	fn try_get_process_inner_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
	) -> Option<String> {
		Self::try_get_process_inner_token(session, id)
	}

	fn try_get_sandbox_control_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<tangram_server_sandbox_core::control::local::Local> {
		self.try_get_sandbox_control_runner(id, location)
	}

	fn try_get_sandbox_runner(
		&self,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> Option<tangram_server_core::runner::Runner> {
		self.try_get_sandbox_runner(id, location)
	}

	fn create_runner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::runner::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::create::Output>> {
		self.create_runner(session, arg).boxed()
	}

	fn create_runner_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::create::Output>> {
		self.create_runner_token(session, runner, arg).boxed()
	}

	fn try_delete_runner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_runner(session, runner, arg).boxed()
	}

	fn try_delete_runner_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_runner_token(session, runner, token, arg)
			.boxed()
	}

	fn list_runners<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::runner::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::list::Output>> {
		self.list_runners(session, arg).boxed()
	}

	fn list_runner_tokens<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::list::Output>> {
		self.list_runner_tokens(session, runner, arg).boxed()
	}

	fn get_runner_control_stream<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::runner::control::Header,
					BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
				),
				tg::Error,
			>,
		>,
	> {
		self.get_runner_control_stream_with_context(session, arg, stream)
			.boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
