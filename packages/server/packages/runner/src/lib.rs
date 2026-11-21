use {
	futures::{
		FutureExt as _, StreamExt as _, future,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
		pin::pin,
		sync::{Arc, Mutex, atomic::Ordering},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::{Stopper, Task},
	tangram_http::body::Boxed as BoxBody,
	tangram_index::prelude::*,
	tangram_server_core::{Server, Session, Shutdown},
};

mod assets;

pub mod control;
pub mod create;
pub mod delete;
pub mod get;
pub mod list;
pub mod process;
pub mod sandbox;
pub mod token;

/// The ID of the only runner of a single server, which a configured ID replaces.
const SINGLE_SERVER_ID: &str = "rnr_0000000000000000000000000000";

pub type RunnerSender = tangram_server_core::control::Sender<
	tg::runner::control::ServerMessage,
	tg::runner::control::ClientMessage,
>;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub control: tokio::sync::watch::Sender<Option<RunnerSender>>,

	/// The boots whose tasks run, with the processes that each accepted, which the runner reports when it connects to its scheduler.
	pub(crate) held: dashmap::DashMap<tg::sandbox::Id, crate::sandbox::Held, tg::id::BuildHasher>,

	/// The replies to recent offers of boots, by request ID, so that an offer that the scheduler sends again is answered with its reply, even after the boot that it created ended. The replies outlast the scheduler's ttl of the runner, after which the scheduler no longer sends an offer again.
	pub offers: tangram_server_core::holder::Replies<tg::runner::control::ClientMessage>,

	#[cfg(target_os = "linux")]
	pub ip_pool: tangram_sandbox::network::ip::Pool,

	/// The tasks that push the objects of finished processes to their locations, which a graceful stop waits for.
	pub push_tasks: tangram_futures::task::Map<tg::process::Id, ()>,

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

		// Create the push and sandbox tasks.
		let push_tasks = tangram_futures::task::Map::default();
		let sandbox_tasks = tangram_futures::task::Map::default();

		// Prepare the runner's reusable sandbox assets.
		let sandbox_assets = crate::assets::Assets::new(arg.path, arg.tangram_path, arg.config)?;

		// Create the task handle.
		let task = Mutex::new(None);

		// Create the runner control sender.
		let (control, _) = tokio::sync::watch::channel(None);

		// Create the state.
		let offers = tangram_server_core::holder::Replies::new(
			arg.config.control.reply_capacity,
			arg.config.scheduler.runner_time_to_live,
			arg.config.control.request_timeout,
		);
		let held = dashmap::DashMap::default();
		let state = State {
			control,
			held,
			offers,

			#[cfg(target_os = "linux")]
			ip_pool,

			push_tasks,

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
		let id = self.local_runner_id(server)?;
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
		let authorized = self.is_runner_owner_authorized(session, owner).await?;
		if !authorized {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	pub(crate) async fn is_runner_owner_authorized(
		&self,
		session: &Session,
		owner: Option<&tg::Id>,
	) -> tg::Result<bool> {
		session.verify_request_from_host()?;
		let Some(owner) = owner else {
			let authorized = matches!(session.context.principal, tg::Principal::Root);
			return Ok(authorized);
		};
		let permission = tangram_server_grant_common::admin_permission_for_resource(owner)?;
		let authorized = session
			.authorize(owner.clone(), permission)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(permission);

		Ok(authorized)
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
		// Read the runner from the read connection.
		let options = db::ConnectionOptions::default();
		let data = Self::try_get_runner_data_with_options(session, runner, options).await?;
		if data.is_some() {
			return Ok(data);
		}

		// Read the runner from the write connection, because a new runner may not have reached the read connection.
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			..Default::default()
		};
		let data = Self::try_get_runner_data_with_options(session, runner, options).await?;

		Ok(data)
	}

	async fn try_get_runner_data_with_options(
		session: &Session,
		runner: &tg::runner::Id,
		options: db::ConnectionOptions,
	) -> tg::Result<Option<tg::runner::Data>> {
		session
			.server
			.database
			.run_with_options(options, |transaction| {
				let runner = runner.clone();
				async move { Self::try_get_runner_data_with_transaction(transaction, &runner).await }
					.boxed()
			})
			.await
	}

	pub(crate) async fn try_get_runner_data_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		runner: &tg::runner::Id,
	) -> tg::Result<ControlFlow<Option<tg::runner::Data>, tangram_server_core::database::Error>>
	{
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

	pub async fn set_runner_scheduler(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
		scheduler: &tg::scheduler::Id,
	) -> tg::Result<()> {
		let arg = tangram_index::runner::put::Arg {
			id: runner.clone(),
			scheduler: scheduler.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutRunner(Box::new(arg))],
		};
		session.server.index.write(arg).await??;
		Ok(())
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

		// Get the sandboxes that the previous run left directories for, before the pool starts, which the first connection reports.
		let previous = match Self::sandbox_directory_ids(session).await {
			Ok(previous) => previous,
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to get the previous sandboxes");
				Vec::new()
			},
		};
		self.start_sandbox_pool(session);

		// Report the stopped sandboxes' storage usage and destroy the expired ones periodically, until the runner stops.
		let _stopped_sandbox_task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let stopper = stopper.clone();
			move |_| async move {
				component.stopped_sandbox_task(&session, stopper).await;
			}
		});

		let control_stopper = Stopper::new();
		let mut first = true;
		let mut drained = false;
		let mut retries = None::<BoxStream<'static, ()>>;
		loop {
			let stop_future = stopper.wait();
			let stop_future = pin!(stop_future);
			let started = tokio::time::Instant::now();
			let run_future = self.runner_task_inner(
				session,
				&id,
				&previous,
				&mut first,
				stopper.clone(),
				control_stopper.clone(),
			);
			let mut run_future = pin!(run_future);
			let (result, _) = match future::select(stop_future, run_future.as_mut()).await {
				future::Either::Left(((), _)) => {
					// Drain the sandbox tasks while the control stream keeps running, and then end the control stream with a message that tells the server that the runner shut down gracefully and holds nothing, so that the server removes the runner at once. A runner that terminates leaves its removal to its ttl and its next connection.
					let shutdown = session
						.server
						.shutdown
						.borrow()
						.expect("the shutdown mode was not set");
					if matches!(shutdown, Shutdown::Interrupt) {
						let drain_future = self.shutdown_sandbox_tasks(shutdown);
						let drain_future = pin!(drain_future);
						if let future::Either::Right((_, drain_future)) =
							future::select(drain_future, run_future).await
						{
							drain_future.await;
						}
						drained = true;
						if let Some(sender) = self.control.borrow().clone() {
							let message = tg::runner::control::ClientMessage::Notification(
								tg::runner::control::ClientNotification::End,
							);
							sender.try_send_untracked(message);
						}
					}
					control_stopper.stop();
					break;
				},
				future::Either::Right(output) => output,
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

		// Shut down the sandbox tasks, unless they drained before the control stream ended.
		if !drained {
			let shutdown = session
				.server
				.shutdown
				.borrow()
				.expect("the shutdown mode was not set");
			self.shutdown_sandbox_tasks(shutdown).await;
		}
	}

	/// Stop the pools and shut down the sandbox tasks, waiting for them to end.
	async fn shutdown_sandbox_tasks(&self, shutdown: Shutdown) {
		// Stop the pools.
		self.shutdown_sandbox_pool(shutdown).await;

		// Shut down the sandbox tasks.
		let sandboxes = match shutdown {
			Shutdown::Interrupt => {
				self.sandbox_tasks.stop_all();
				Vec::new()
			},
			Shutdown::Terminate => {
				let sandboxes = self
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
				self.sandbox_tasks.abort_all();

				sandboxes
			},
		};
		let wait_future = self.sandbox_tasks.wait();
		let terminate_future = sandboxes
			.into_iter()
			.map(|(id, sandbox)| async move {
				if let Err(error) = sandbox.stop().await {
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

	/// Get the ID of this server's runner, if the server has the runner role, which is the configured ID or the single-server ID.
	#[must_use]
	pub fn local_runner_id(&self, server: &Server) -> Option<tg::runner::Id> {
		if !server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Runner)
		{
			return None;
		}
		if let Some(id) = server.config.runner.id.clone() {
			return Some(id);
		}
		let id = SINGLE_SERVER_ID.parse().unwrap();
		Some(id)
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
		previous: &[tg::sandbox::Id],
		first: &mut bool,
		stopper: Stopper,
		control_stopper: Stopper,
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

		// Get the sandboxes that the runner has directories for and does not hold, which are the previous run's on the first connection and the ones on disk afterwards. Leave out a sandbox that the runner still has state for, whose end may not be recorded yet.
		let stopped = if *first {
			previous.to_vec()
		} else {
			match Self::sandbox_directory_ids(session).await {
				Ok(stopped) => stopped,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to get the sandbox directories");
					Vec::new()
				},
			}
		};
		let stopped = stopped
			.into_iter()
			.filter(|sandbox| {
				self.state.sandboxes.get_by_id(sandbox).is_none()
					&& !self.state.starting.contains_key(sandbox)
			})
			.collect::<Vec<_>>();

		// Get the runner control stream, continuing if the scheduler could not be reached.
		let (header, control) = match self
			.run_get_runner_control_stream(session, id, &location, *first, &stopped)
			.boxed()
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		tracing::info!(runner = %id, scheduler = %header.scheduler, ended_sandboxes = header.ended.len(), "connected the runner to the scheduler");

		// Remember the replies to offers for longer than the scheduler keeps the runner, so that the scheduler never sends an offer again after its reply is forgotten.
		self.offers
			.set_time_to_live(header.runner_time_to_live.saturating_mul(2));

		// Keep or remove the directories of the sandboxes that the runner does not hold as the scheduler decided before accepting work, and give up the reported boots that the index records as ended.
		let index = 1;
		self.finish_sandbox_directories(session, &stopped, &header, *first)
			.await?;
		*first = false;
		self.yield_runner_sandboxes(&header);
		tracing::info!(runner = %id, "the runner is ready to accept work");
		session
			.server
			.components
			.runner
			.state()
			.set_scheduler(Some(header.scheduler));

		// Handle new sandbox requests.
		self.run_handle_runner_control_stream(
			session,
			location,
			control,
			index,
			stopper,
			control_stopper,
		)
		.boxed()
		.await?;

		Ok(ControlFlow::Break(()))
	}

	/// Remove the directory of each reported sandbox that the runner does not hold and that the scheduler does not keep. On the first connection, also reserve the storage of each kept directory, and record the stop of each sandbox in a remote that the connection's takeover stopped in this runner's local index.
	async fn finish_sandbox_directories(
		&self,
		session: &Session,
		stopped: &[tg::sandbox::Id],
		header: &tg::runner::control::Header,
		first: bool,
	) -> tg::Result<()> {
		// Remove the directories that the scheduler does not keep, failing the first connection if one cannot be removed, and logging the failure on a later connection, which keeps running sandboxes.
		for sandbox in stopped {
			if header.kept.contains(sandbox) {
				continue;
			}
			let result = self
				.remove_stopped_sandbox_directory(session, sandbox)
				.await;
			match result {
				Ok(()) => {},
				Err(error) if first => return Err(error),
				Err(error) => {
					tracing::error!(error = %error.trace(), %sandbox, "failed to remove the sandbox directory");
				},
			}
		}

		// Reserve the storage of the kept directories and record the stops on the first connection only, because before a later connection the runner already holds the storage of its kept directories.
		if !first {
			return Ok(());
		}
		let kept = stopped
			.iter()
			.filter(|sandbox| header.kept.contains(sandbox))
			.cloned()
			.collect::<Vec<_>>();
		let indexed = session.server.index.try_get_sandboxes(&kept).await?;
		for (sandbox, indexed) in std::iter::zip(&kept, indexed) {
			let storage = indexed
				.and_then(|indexed| indexed.data)
				.and_then(|data| data.data.storage);
			self.keep_stopped_sandbox_directory(session, sandbox, storage);
			if header.stopped.contains(sandbox) {
				self.record_previous_sandbox_stop(session, sandbox).await?;
			}
		}

		Ok(())
	}

	/// Give up each reported boot that the index records as ended or replaced by a later boot, such as by a takeover, without writing anything more for it. Stop a running boot locally, keeping its directory if the scheduler keeps it, and destroy it otherwise, which removes its directory.
	fn yield_runner_sandboxes(&self, header: &tg::runner::control::Header) {
		let sandboxes = header
			.ended
			.iter()
			.filter_map(|id| {
				// Stop answering the servers and writing for the boot at once, because the boot ended in the index and the server may already hold the sandbox's next boot.
				let mut sandbox = self.state.sandboxes.get_mut_by_id(id)?;
				sandbox.yielded = true;
				if sandbox.status.is_stopped_or_destroyed() {
					return None;
				}
				let request = if header.kept.contains(id) {
					tg::sandbox::control::ServerRequestArg::Stop(
						tg::sandbox::control::StopServerRequestArg {},
					)
				} else {
					tg::sandbox::control::ServerRequestArg::Destroy(
						tg::sandbox::control::DestroyServerRequestArg { error: None },
					)
				};
				Some((sandbox.id.clone(), sandbox.control_sender.clone(), request))
			})
			.collect::<Vec<_>>();
		for (id, control_sender, request) in sandboxes {
			tracing::warn!(sandbox = %id, "giving up a sandbox that the server took over");
			tokio::spawn(async move {
				let result = async {
					let _response = control_sender.send_request(request).await?.await??;
					Ok::<_, tg::Error>(())
				}
				.await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), sandbox = %id, "failed to give up the sandbox");
				}
			});
		}
	}

	/// Destroy a stopped sandbox in a task, so that the control stream keeps receiving the response to the sandbox's destroy report.
	fn spawn_destroy_stopped_sandbox_task(
		&self,
		session: &Session,
		id: String,
		arg: tg::runner::control::DestroySandboxServerRequestArg,
		sender: &RunnerSender,
	) {
		let component = self.clone();
		let session = session.clone();
		let sender = sender.clone();
		tokio::spawn(async move {
			let result = component
				.destroy_stopped_sandbox(&session, arg, &sender)
				.await
				.map(tg::runner::control::ClientResponseOutput::DestroySandbox);
			let message = Self::create_runner_control_response(id, result);
			if let Err(error) = sender.send(message).await {
				tracing::error!(error = %error.trace(), "failed to send the destroy sandbox response");
			}
		});
	}

	/// Report a sandbox that this runner started on the shortcut path to its scheduler through the runner control stream, and report it again on the next stream after the scheduler rejects it, such as for a stream that a reconnect replaced.
	pub(crate) async fn report_shortcut_sandbox_with_runner_control(
		&self,
		sandbox: &tg::sandbox::Id,
		boot: u64,
		proof: tg::sandbox::id::Proof,
		stopper: &Stopper,
	) -> tg::Result<()> {
		let arg = tg::runner::control::ClientRequestArg::AddSandbox(
			tg::runner::control::AddSandboxClientRequestArg {
				boot,
				proof,
				sandbox: sandbox.clone(),
			},
		);
		let mut control = self.control.subscribe();
		loop {
			control.mark_unchanged();
			let error = match self.report_with_runner_control(arg.clone(), stopper).await {
				Ok(output) => {
					output
						.try_unwrap_add_sandbox()
						.map_err(|_| tg::error!("expected an add sandbox response"))?;
					return Ok(());
				},
				Err(error) => error,
			};
			tracing::warn!(error = %error.trace(), %sandbox, "failed to report the sandbox to the scheduler");
			tokio::select! {
				biased;
				result = control.changed() => result.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?,
				() = stopper.wait() => {
					return Err(tg::error!(%sandbox, "the runner stopped before the sandbox was reported"));
				},
			}
		}
	}

	/// Report the end of a boot that this runner held to its scheduler through the runner control stream.
	pub(crate) async fn report_ended_sandbox_with_runner_control(
		&self,
		arg: tg::runner::control::EndSandboxClientRequestArg,
		stopper: &Stopper,
	) -> tg::Result<()> {
		let sandbox = arg.sandbox.clone();
		let arg = tg::runner::control::ClientRequestArg::EndSandbox(arg);
		self.report_with_runner_control(arg, stopper)
			.await
			.map_err(
				|error| tg::error!(!error, %sandbox, "failed to report the end of the sandbox"),
			)?
			.try_unwrap_end_sandbox()
			.map_err(|_| tg::error!("expected an end sandbox response"))?;

		Ok(())
	}

	/// Report processes that the runner accepted for a boot that the scheduler offered them into and that cannot run to the runner's scheduler, which finishes them as failed with the error.
	pub(crate) async fn report_finished_processes_with_runner_control(
		&self,
		sandbox: &tg::sandbox::Id,
		boot: u64,
		processes: &[tg::runner::control::Process],
		error: tg::Either<tg::error::Data, tg::error::Id>,
		stopper: &Stopper,
	) -> tg::Result<()> {
		let arg = tg::runner::control::ClientRequestArg::FinishProcesses(
			tg::runner::control::FinishProcessesClientRequestArg {
				boot,
				error,
				processes: processes.to_vec(),
				sandbox: sandbox.clone(),
			},
		);
		self.report_with_runner_control(arg, stopper)
			.await
			.map_err(|error| tg::error!(!error, "failed to report the finished processes"))?
			.try_unwrap_finish_processes()
			.map_err(|_| tg::error!("expected a finish processes response"))?;

		Ok(())
	}

	/// Send a report through the runner control stream and return the server's response, waiting for a runner control stream and sending the report again after the stream is lost, or after a backoff or a new stream if the server asks for the report again because it cannot decide it yet. Send the report on a stream that is connected even after the stopper is stopped, so that a report made during a graceful drain precedes the runner's `End`, and give up only if the stopper is stopped while no stream is connected. An error in the response is the server's refusal, after which the report is not sent again.
	async fn report_with_runner_control(
		&self,
		arg: tg::runner::control::ClientRequestArg,
		stopper: &Stopper,
	) -> tg::Result<tg::runner::control::ServerResponseOutput> {
		let mut control = self.control.subscribe();
		let retry = tangram_futures::retry::Options::default();
		let mut attempt = 0;
		loop {
			// Wait for a runner control stream.
			let sender = tokio::select! {
				biased;
				result = control.wait_for(Option::is_some) => result
					.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?
					.clone()
					.unwrap(),
				() = stopper.wait() => {
					return Err(tg::error!("the runner stopped without a runner control stream"));
				},
			};

			// Send the report, and return the response.
			let request = tg::runner::control::ClientRequest {
				arg: arg.clone(),
				id: tangram_server_core::control::id(),
			};
			let response = sender
				.request(
					tg::runner::control::ClientMessage::Request(request),
					tangram_server_core::control::Priority::High,
				)
				.await;
			let response = match response {
				Ok(response) => response.await.ok(),
				Err(_) => None,
			};
			if let Some(response) = response {
				let tg::runner::control::ServerMessage::Response(response) = response else {
					return Err(tg::error!("expected a runner control response"));
				};

				// Send the report again after a backoff or on a new stream if the server cannot decide it yet.
				if response.retry.is_some() {
					let delay = tangram_futures::retry::delay_for_attempt(attempt, &retry);
					attempt += 1;
					control.mark_unchanged();
					tokio::select! {
						biased;
						result = control.changed() => result.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?,
						() = tokio::time::sleep(delay) => {},
					}
					continue;
				}

				if let Some(error) = response.error {
					let error = tg::Error::try_from(error).map_err(|error| {
						tg::error!(!error, "failed to deserialize the runner control error")
					})?;
					return Err(error);
				}
				let output = response
					.output
					.ok_or_else(|| tg::error!("missing the runner control response output"))?;
				return Ok(output);
			}

			// Wait for the stream to change after it is lost.
			tracing::warn!("failed to send the report through the runner control stream");
			control.mark_unchanged();
			tokio::select! {
				biased;
				result = control.changed() => result.map_err(|error| tg::error!(!error, "the runner control sender was dropped"))?,
				() = stopper.wait() => {
					return Err(tg::error!("the runner stopped without a runner control stream"));
				},
			}
		}
	}

	pub(crate) async fn request_runner_sandbox_destroy(
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

	pub(crate) async fn request_runner_sandbox_usage(
		sender: &RunnerSender,
		arg: tg::runner::control::ReportSandboxUsageClientRequestArg,
	) -> tg::Result<
		ControlFlow<tg::runner::control::ReportSandboxUsageServerResponseOutput, tg::Error>,
	> {
		let request = tg::runner::control::ClientRequest {
			arg: tg::runner::control::ClientRequestArg::ReportSandboxUsage(arg),
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
			return Err(tg::error!(
				!error,
				"the report sandbox usage request failed"
			));
		}
		let Some(tg::runner::control::ServerResponseOutput::ReportSandboxUsage(output)) =
			response.output
		else {
			return Err(tg::error!("expected a report sandbox usage response"));
		};

		Ok(ControlFlow::Break(output))
	}

	async fn run_get_runner_control_stream(
		&self,
		session: &Session,
		id: &tg::runner::Id,
		location: &tg::Location,
		first: bool,
		stopped: &[tg::sandbox::Id],
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
		let heartbeat = Self::create_runner_heartbeat(session, 0, first);
		let host = tg::host::current().to_owned();
		let location = Some(location.clone().into());

		// Report every sandbox that is running or starting, with the processes that its scheduler offered into the boot and that the runner holds, and the proof for a boot on the shortcut path, so that a restarted scheduler can tell which requests it already placed and cancel the boots and processes that the index does not have.
		let mut sandboxes = self
			.state
			.sandboxes
			.iter()
			.filter(|sandbox| !sandbox.status.is_stopped_or_destroyed())
			.map(|sandbox| (sandbox.id.clone(), sandbox.boot))
			.collect::<BTreeMap<_, _>>();
		for entry in &self.state.starting {
			sandboxes
				.entry(entry.key().clone())
				.or_insert(*entry.value());
		}
		let sandboxes = sandboxes
			.into_iter()
			.map(|(id, boot)| {
				let (processes, proof) = self
					.held
					.get(&id)
					.filter(|held| held.boot == boot)
					.map_or_default(|held| held.report());
				tg::runner::control::ArgSandbox {
					boot,
					id,
					processes,
					proof,
				}
			})
			.collect::<Vec<_>>();

		// Report the sandboxes with directories that the runner does not hold and the sandboxes that it runs on every connection, so that the scheduler decides which directories the runner keeps, and, on the first connection after the runner starts, ends the sandboxes that the runner no longer holds.
		let directories = stopped
			.iter()
			.cloned()
			.chain(sandboxes.iter().map(|sandbox| sandbox.id.clone()))
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect();
		let scheduler_time_to_live = session.server.config.runner.scheduler_time_to_live;
		let arg = tg::runner::control::Arg {
			directories,
			first,
			heartbeat,
			host,
			id: id.clone(),
			location,
			sandboxes,
			scheduler_time_to_live,
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
		control_stopper: Stopper,
	) -> tg::Result<()> {
		let sender = control.sender();

		// Publish the sender while the stream handles requests, so that sandbox tasks can report destruction through it.
		self.control.send_replace(Some(sender.clone()));
		let _control = std::mem::DropGuard::new(&self.control, |control| {
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
				() = control_stopper.wait() => break,
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
			let request = match message.arg {
				tg::runner::control::ServerRequestArg::OfferSandbox(request) => request,
				tg::runner::control::ServerRequestArg::DestroySandbox(arg) => {
					self.spawn_destroy_stopped_sandbox_task(session, id.clone(), arg, &sender);
					continue;
				},
			};
			tangram_server_core::checkpoint!(session.server, "runner.control.offer_sandbox", sandbox = %request.sandbox).await;

			// Answer an offer that the scheduler sent again with the reply to the offer, without acting on it again, so that a boot that ended is not created again.
			if let tangram_server_core::holder::Begin::Replied(message) = self.offers.begin(&id) {
				tangram_server_core::checkpoint!(session.server, "runner.control.offer_sandbox.replied", sandbox = %request.sandbox).await;
				sender.send(message).await?;
				continue;
			}

			// Acquire the storage of a stopped sandbox that starts again, which the scheduler places without it because this runner keeps it reserved.
			let mut requested = request.capacity;
			if request.boot > 0 {
				requested.storage = request.arg.storage.unwrap_or_default();
			}

			// Join a boot that this runner already holds, and ignore an older boot, because a scheduler can send a boot again after it restarts or resends a request.
			let state = session.server.components.runner.state();
			let joined = state
				.starting
				.get(&request.sandbox)
				.is_some_and(|boot| *boot >= request.boot)
				|| state
					.sandboxes
					.get_by_id(&request.sandbox)
					.is_some_and(|sandbox| sandbox.boot >= request.boot);
			if joined {
				let output =
					tg::runner::control::OfferSandboxClientResponseOutput { accepted: true };
				let message = Self::create_runner_control_response(
					id.clone(),
					Ok(tg::runner::control::ClientResponseOutput::OfferSandbox(
						output,
					)),
				);
				self.offers.finish(&id, message.clone());
				sender.send(message).await?;
				continue;
			}

			// Decline work while the runner drains its sandboxes before it shuts down.
			if stopper.stopped() {
				let output =
					tg::runner::control::OfferSandboxClientResponseOutput { accepted: false };
				let message = Self::create_runner_control_response(
					id.clone(),
					Ok(tg::runner::control::ClientResponseOutput::OfferSandbox(
						output,
					)),
				);
				self.offers.finish(&id, message.clone());
				sender.send(message).await?;
				continue;
			}

			// Reject a request without the sandbox's authentication token.
			let sandbox = request.sandbox.clone();
			let Some(token) = request.token else {
				let message = Self::create_runner_control_response(
					id.clone(),
					Err(tg::error!(%sandbox, "missing the sandbox authentication token")),
				);
				self.offers.finish(&id, message.clone());
				sender.send(message).await?;
				continue;
			};

			// Decline work if capacity is unavailable. A start of a stopped sandbox releases the storage that its directory keeps reserved before it acquires its capacity, and reserves the storage again if the capacity is unavailable.
			let stopped_storage = (request.boot > 0)
				.then(|| self.state.stopped_storage.remove(&request.sandbox))
				.flatten()
				.map(|(_, storage)| storage.capacity());
			let allocation = session
				.server
				.components
				.process
				.try_acquire_scheduled_sandbox_capacity(
					session,
					request.lender.as_ref(),
					request.parent.as_ref(),
					requested,
				);
			if allocation.is_none()
				&& let Some(capacity) = stopped_storage
			{
				match self.state.capacity.try_acquire(capacity) {
					Some(storage) => {
						self.state
							.stopped_storage
							.insert(request.sandbox.clone(), storage);
					},
					None => {
						tracing::warn!(%sandbox, "failed to reserve the stopped sandbox's storage again");
					},
				}
			}
			let Some(allocation) = allocation else {
				let output =
					tg::runner::control::OfferSandboxClientResponseOutput { accepted: false };
				let message = Self::create_runner_control_response(
					id.clone(),
					Ok(tg::runner::control::ClientResponseOutput::OfferSandbox(
						output,
					)),
				);
				self.offers.finish(&id, message.clone());
				sender.send(message).await?;
				continue;
			};

			// Spawn the sandbox task.
			state.starting.insert(sandbox.clone(), request.boot);
			let _ = self.spawn_sandbox_task(
				&session.server,
				tangram_server_runner_core::sandbox::SpawnSandboxTaskArg {
					allocation,
					arg: request.arg,
					boot: request.boot,
					creator: request.creator,
					id: Some(sandbox.clone()),
					location: location.clone(),
					owner: request.owner,
					process: request.process,
					public: false,
					scheduled: true,
					sequence: request.sequence + 2,
					token: Some(token),
				},
			);
			tangram_server_core::checkpoint!(session.server, "runner.control.offer_sandbox.accepted", sandbox = %sandbox).await;

			// Send the response.
			let output = tg::runner::control::OfferSandboxClientResponseOutput { accepted: true };
			let message = Self::create_runner_control_response(
				id.clone(),
				Ok(tg::runner::control::ClientResponseOutput::OfferSandbox(
					output,
				)),
			);
			self.offers.finish(&id, message.clone());
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
			(http::Method::GET, ["runners", runner]) => {
				self.try_get_runner_request(session, request, runner).await
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
			(http::Method::GET, ["runners", runner, "tokens", token]) => {
				self.try_get_runner_token_request(session, request, runner, token)
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
			// Reclaim the directories of stopped sandboxes before accepting work.
			if server
				.config
				.roles
				.contains(&tangram_server_core::config::Role::Runner)
			{
				self.reclaim_sandbox_directories(server).await;
			}

			// Spawn the runner task.
			let task = self.spawn_runner_task(server);
			*self.task.lock().unwrap() = task;

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, ()> {
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

			// Wait for the pushes of finished processes' objects while the components that they use still run, and abandon the pushes that the push drain timeout outlasts.
			let timeout = server.config.runner.push_drain_timeout;
			if tokio::time::timeout(timeout, self.push_tasks.wait())
				.await
				.is_err()
			{
				for process in self.push_tasks.keys() {
					tracing::warn!(%process, "abandoned the push of a finished process's objects");
				}
				self.push_tasks.abort_all();
			}
			tracing::trace!("push tasks");

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

	fn set_runner_scheduler<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		scheduler: &'a tg::scheduler::Id,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.set_runner_scheduler(session, runner, scheduler)
			.boxed()
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

	fn local_runner_id(&self, server: &tangram_server_core::Server) -> Option<tg::runner::Id> {
		self.local_runner_id(server)
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
		connection: u64,
		id: String,
		arg: tg::runner::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> BoxFuture<'a, tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>>> {
		self.send_runner_control_request(session, runner, scheduler, connection, id, arg, options)
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

	fn try_get_runner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		arg: tg::runner::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::get::Output>>> {
		self.try_get_runner(session, runner, arg).boxed()
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

	fn try_get_runner_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::runner::token::get::Output>>> {
		self.try_get_runner_token(session, runner, token, arg)
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
