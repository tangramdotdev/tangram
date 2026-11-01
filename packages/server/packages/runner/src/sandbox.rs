use {
	self::pool::CreateSandboxOutput,
	super::process::{Event as ProcessEvent, SpawnProcessTaskArg, SpawnProcessTaskOutput},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::{BoxStream, FuturesUnordered},
	},
	std::{collections::BTreeMap, pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::TryExt as _,
		task::{Stopper, Task},
	},
	tangram_server_runner_core::{
		process::ConnectedEvent,
		sandbox::{Event, ReadyEvent, SpawnSandboxTaskArg, SpawnSandboxTaskOutput},
	},
	tangram_server_sandbox_core::control::local::{Message, Reply},
	tokio::task::JoinSet,
	tokio_stream::{StreamMap, wrappers::UnboundedReceiverStream},
	tracing::Instrument as _,
	{
		crate::Component,
		tangram_server_core::{
			Server, Session,
			context::{Context, Origin},
			temp::Temp,
		},
	},
};

mod control;
mod get;
mod listener;
mod path;

pub(crate) mod pool;

type ConnectionReady = futures::future::Shared<BoxFuture<'static, tg::Result<()>>>;

type PendingProcess = BoxFuture<
	'static,
	(
		tg::Result<ConnectedEvent>,
		tokio::sync::mpsc::UnboundedReceiver<tg::Result<ProcessEvent>>,
		Option<Reply>,
	),
>;

type SandboxControlSender = tangram_server_core::control::Sender<
	tg::sandbox::control::ServerMessage,
	tg::sandbox::control::ClientMessage,
>;

struct SpawnSandboxArg {
	arg: SpawnSandboxTaskArg,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	stopper: Stopper,
}

struct SandboxTaskArg {
	allocation: tangram_server_runner_core::capacity::Allocation,
	arg: tg::sandbox::create::Arg,
	boot: u64,
	creator: tg::Principal,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	location: tg::Location,
	owner: tg::Principal,
	process: Option<tg::runner::control::Process>,
	proof: Option<tg::sandbox::id::Proof>,
	stopper: Stopper,
	token: Option<String>,
}

struct GetSandboxControlStreamArg<'a> {
	boot: u64,
	create: tg::sandbox::create::Arg,
	created_at: i64,
	creator: tg::Principal,
	id: &'a tg::sandbox::Id,
	input_stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	location: &'a tg::Location,
	owner: tg::Principal,
	proof: Option<tg::sandbox::id::Proof>,
}

struct ConnectedSandboxControl {
	requests:
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	token: String,
}

struct DestroyFailedSandboxArg<'a> {
	arg: tg::sandbox::create::Arg,
	boot: u64,
	connection: ConnectedSandboxControl,
	created_at: i64,
	creator: tg::Principal,
	error: &'a tg::Error,
	id: &'a tg::sandbox::Id,
	input: tokio::sync::mpsc::Sender<tg::sandbox::control::ClientMessage>,
	location: &'a tg::Location,
	owner: tg::Principal,
}

struct SandboxTaskInnerArg {
	connected: ConnectionReady,
	control: control::Control,
	create_output: CreateSandboxOutput,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	processes: Arc<tangram_server_process_core::Processes>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RunSandboxTaskArg {
	connected: ConnectionReady,
	control: control::Control,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	guest_url: tangram_uri::Uri,
	id: tg::sandbox::Id,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	processes: Arc<tangram_server_process_core::Processes>,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RetainSandboxTaskArg {
	control: Option<control::Control>,
	id: tg::sandbox::Id,
	process_tasks: JoinSet<tg::Result<()>>,
	stopper: Stopper,
}

enum Directory {
	/// The directory of a stopped sandbox, which starts again on its files.
	Existing {
		#[cfg(target_os = "linux")]
		filesystem_project_id: Option<tangram_server_runner_core::project::Id>,
		path: std::path::PathBuf,
	},

	/// A new directory for a sandbox's first boot.
	New { path: std::path::PathBuf },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Teardown {
	Destroy,
	Stop,
}

impl Component {
	pub(crate) fn resolve_sandbox_backend(
		&self,
		server: &Server,
	) -> tg::Result<tangram_sandbox::Backend> {
		let backend = match &server.config.runner.backend {
			tangram_server_core::config::RunnerBackend::Builtin(_) => {
				tangram_sandbox::Backend::Builtin
			},
			tangram_server_core::config::RunnerBackend::Bubblewrap(config) => {
				tangram_sandbox::Backend::Bubblewrap {
					path: config.path.clone(),
				}
			},
			tangram_server_core::config::RunnerBackend::CloudHypervisor(config) => {
				let image_path = self
					.sandbox_assets
					.image
					.clone()
					.ok_or_else(|| tg::error!("the cloud-hypervisor image is unavailable"))?;
				let snapshot = self.sandbox_assets.snapshot.clone();
				tangram_sandbox::Backend::CloudHypervisor(tangram_sandbox::CloudHypervisor {
					cloud_hypervisor_path: config.cloud_hypervisor_path.clone(),
					dax: config.dax.map(|dax| dax.window_size as u64),
					image_path,
					kernel_path: config.kernel_path.clone(),
					max_cpu: config.max_cpu,
					max_memory: config.max_memory,
					network_interface_timeout: config.network_interface_timeout,
					ready_timeout: config.ready_timeout,
					serial_connect_interval: config.serial_connect_interval,
					serial_connect_timeout: config.serial_connect_timeout,
					snapshot,
					snapshot_cpu: config.snapshot_cpu,
					snapshot_memory: config.snapshot_memory,
				})
			},
			tangram_server_core::config::RunnerBackend::Gvisor(config) => {
				tangram_sandbox::Backend::Gvisor {
					path: config.path.clone(),
					platform: config.platform,
				}
			},
			tangram_server_core::config::RunnerBackend::Seatbelt => {
				tangram_sandbox::Backend::Seatbelt
			},
		};
		if cfg!(target_os = "linux") && matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the seatbelt backend requires macos"));
		}
		if cfg!(target_os = "macos") && !matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the selected backend requires linux"));
		}
		Ok(backend)
	}

	pub fn spawn_sandbox_task(
		&self,
		server: &Server,
		arg: SpawnSandboxTaskArg,
	) -> SpawnSandboxTaskOutput {
		let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let task_id = tangram_server_core::control::id();
		let mut task = self.sandbox_tasks.spawn(task_id, |stopper| {
			let component = self.clone();
			let server = server.clone();
			async move {
				// Run the sandbox task.
				let session = server.session(&server.context);
				let arg = SpawnSandboxArg {
					arg,
					event_sender,
					stopper,
				};
				component.sandbox_task(&session, arg).boxed().await;
			}
			.in_current_span()
		});
		task.detach();
		SpawnSandboxTaskOutput {
			events: event_receiver,
		}
	}
}

impl Component {
	async fn handle_destroyed_sandbox_control_request(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		message: Message,
	) -> tg::Result<()> {
		let result = match message.arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) => {
				Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: false },
				))
			},
			tg::sandbox::control::ServerRequestArg::Get(_) => session
				.server
				.components
				.runner
				.state()
				.try_get_sandbox(id)
				.map(|data| {
					tg::sandbox::control::ClientResponseOutput::Get(
						tg::sandbox::control::GetClientResponseOutput { data },
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => session
				.server
				.components
				.runner
				.state()
				.sandboxes
				.get_by_id(id)
				.map(|sandbox| {
					tg::sandbox::control::ClientResponseOutput::GetProcesses(
						sandbox.processes(arg.position, arg.length),
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => {
				if Self::sandbox_status(session, id).is_some_and(|status| !status.is_destroyed()) {
					Err(tg::error!(%id, "the sandbox is not started"))
				} else {
					Err(tg::error!(%id, "the sandbox was destroyed"))
				}
			},
			tg::sandbox::control::ServerRequestArg::Stop(_) => {
				if Self::sandbox_status(session, id).is_some_and(|status| !status.is_destroyed()) {
					Ok(tg::sandbox::control::ClientResponseOutput::Stop(
						tg::sandbox::control::StopClientResponseOutput { stopped: false },
					))
				} else {
					Err(tg::error!(%id, "the sandbox was destroyed"))
				}
			},
		};
		message
			.sender
			.send(result)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))?;
		Ok(())
	}

	#[tracing::instrument(name = "sandbox.lifecycle", level = "info", skip_all, fields(runner = self.state.id().as_ref().map(ToString::to_string), sandbox = arg.arg.id.as_ref().map(ToString::to_string)))]
	async fn sandbox_task(&self, session: &Session, arg: SpawnSandboxArg) {
		let SpawnSandboxArg {
			arg,
			event_sender,
			stopper,
		} = arg;

		// Use the sandbox's ID and token from its assignment, or, on the shortcut path, derive the ID and receive the token in the control connection's header.
		let (id, token, proof) = match self.create_sandbox_identity(arg.id, arg.token) {
			Ok(identity) => identity,
			Err(error) => {
				tracing::error!(error = %error.trace(), "the sandbox task failed");
				event_sender.send(Err(error)).ok();
				return;
			},
		};

		// Run the sandbox task.
		let task_arg = SandboxTaskArg {
			allocation: arg.allocation,
			arg: arg.arg,
			boot: arg.boot,
			creator: arg.creator,
			event_sender: event_sender.clone(),
			id: id.clone(),
			location: arg.location,
			owner: arg.owner,
			process: arg.process,
			proof,
			stopper: stopper.clone(),
			token,
		};
		let boot = task_arg.boot;
		let result = if session.server.shutdown.borrow().is_some() {
			Err(tg::error!("the server is shutting down"))
		} else {
			std::panic::AssertUnwindSafe(self.sandbox_task_with_id(session, task_arg).boxed())
				.catch_unwind()
				.await
				.unwrap_or_else(|_| Err(tg::error!("the sandbox task panicked")))
		};
		self.state
			.booting
			.remove_if(&id, |_, booting| *booting == boot);
		let Err(error) = result else {
			return;
		};
		tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox task failed");
		event_sender.send(Err(error.clone())).ok();

		// Leave a restarted sandbox for its start to report as stopped, because its directory is intact.
		if boot > 0 {
			return;
		}

		// Report the sandbox's destruction through the runner control stream, so that a sandbox that failed without reporting its destruction does not remain started.
		let arg = tg::runner::control::DestroySandboxClientRequestArg {
			error: Some(Self::sandbox_error_data(session, &error)),
			sandbox: id.clone(),
		};
		if let Err(error) = self
			.destroy_sandbox_with_runner_control(arg, &stopper)
			.await
		{
			tracing::error!(error = %error.trace(), sandbox = %id, "failed to destroy the failed sandbox");
		}
	}

	fn create_sandbox_identity(
		&self,
		id: Option<tg::sandbox::Id>,
		token: Option<String>,
	) -> tg::Result<(
		tg::sandbox::Id,
		Option<String>,
		Option<tg::sandbox::id::Proof>,
	)> {
		let runner = self
			.state
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let identity = match (id, token) {
			(Some(id), Some(token)) => (id, Some(token), None),
			(None, None) => {
				let (id, proof) = tg::sandbox::Id::with_runner(&runner);
				(id, None, Some(proof))
			},
			_ => {
				return Err(tg::error!(
					"the sandbox id and token must be provided together"
				));
			},
		};
		Ok(identity)
	}

	async fn sandbox_task_with_id(&self, session: &Session, arg: SandboxTaskArg) -> tg::Result<()> {
		let SandboxTaskArg {
			allocation,
			arg,
			boot,
			creator,
			event_sender,
			id,
			location,
			owner,
			process,
			proof,
			stopper,
			token,
		} = arg;
		#[cfg(target_os = "linux")]
		let mut allocation = allocation;

		// Limit each resource without a maximum to the capacity that the sandbox was allocated, which is its lender's capacity when it borrows.
		let mut arg = arg;
		tangram_server_sandbox_common::limit_sandbox_resources(&mut arg, allocation.capacity());

		let runner = session
			.server
			.components
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let shortcut = proof.is_some();
		let (token_sender, token_receiver) = tokio::sync::watch::channel(token.clone());
		let context = if let Some(token) = &token {
			Context {
				principal: tg::Principal::Sandbox(id.clone()),
				token: Some(token.clone()),
				..session.context.clone()
			}
		} else {
			Context {
				principal: tg::Principal::Runner(runner),
				token: session.server.config.runner.token.clone(),
				..session.context.clone()
			}
		};
		let connection_session = session.server.session(&context);

		// Create the sandbox concurrently with its control stream, starting a restarted sandbox on its directory after the previous boot's state is gone.
		let create_future = if boot > 0 {
			let component = self.clone();
			let session = session.clone();
			let arg = arg.clone();
			let id = id.clone();
			async move {
				session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.wait_settled(&id)
					.await;
				let path = Self::sandbox_path(&session, &id);
				if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
					return Err(tg::error!(%id, "the sandbox was destroyed"));
				}
				let directory = Directory::Existing {
					#[cfg(target_os = "linux")]
					filesystem_project_id: component
						.state
						.stopped_project_ids
						.remove(&id)
						.map(|(_, project_id)| project_id),
					path,
				};
				component
					.create_sandbox_inner(&session, arg, Some(directory))
					.await
			}
			.boxed()
		} else {
			self.create_sandbox_with_pool(session, arg.clone(), &id)
				.boxed()
		};
		let created_at = session.server.clock.unix_timestamp();
		let capacity = session.server.config.runner.sandbox_channel_capacity;
		let (input, input_receiver) = tokio::sync::mpsc::channel(capacity);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();

		// Connect the control stream in a task, so that the connection proceeds while nothing polls it.
		let connect_task = Task::spawn({
			let component = self.clone();
			let create = arg.clone();
			let creator = creator.clone();
			let id = id.clone();
			let location = location.clone();
			let owner = owner.clone();
			move |_| async move {
				let connect_arg = GetSandboxControlStreamArg {
					boot,
					create,
					created_at,
					creator,
					id: &id,
					input_stream,
					location: &location,
					owner,
					proof,
				};
				let connection = component
					.get_sandbox_control_stream(&connection_session, connect_arg)
					.await?;

				// Send the token from the header, so that the sandbox's requests stop claiming the sandbox with the runner's token.
				token_sender.send_replace(Some(connection.token.clone()));

				Ok::<_, tg::Error>(connection)
			}
		});
		let mut connect_future: BoxFuture<'static, tg::Result<ConnectedSandboxControl>> =
			async move {
				connect_task.wait().await.map_err(|error| {
					tg::error!(!error, "the sandbox control connect task panicked")
				})?
			}
			.boxed();
		let mut create_future = pin!(create_future);
		let mut connection = None;
		let create_output = loop {
			tokio::select! {
				result = &mut create_future => break result,
				result = &mut connect_future, if connection.is_none() => {
					connection = Some(result?);
				},
			}
		};

		// Report a failure to create the sandbox as its destruction, so that the sandbox does not remain started after its control connection is available.
		let create_output = match create_output {
			Ok(output) => output,
			Err(error) => {
				tracing::error!(error = %error.trace(), sandbox = %id, "failed to create the sandbox");
				let connection = match connection {
					Some(connection) => connection,
					None => connect_future.await?,
				};
				let arg = DestroyFailedSandboxArg {
					arg,
					boot,
					connection,
					created_at,
					creator,
					error: &error,
					id: &id,
					input,
					location: &location,
					owner,
				};
				if let Err(source) = self.destroy_failed_sandbox(session, arg).boxed().await {
					tracing::error!(error = %source.trace(), sandbox = %id, "failed to report the failed sandbox");
				}
				return Err(error);
			},
		};
		tracing::Span::current().record("sandbox", tracing::field::display(&id));
		tracing::info!(runner = self.state.id().as_ref().map(ToString::to_string), sandbox = %id, "created the sandbox");
		let process = process
			.map(|process| Self::prepare_process(process, &id))
			.transpose()?;
		let context = Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: token.clone(),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let sandbox_create = shortcut.then(|| tg::process::control::SandboxCreate {
			arg: arg.clone(),
			created_at,
			creator: creator.clone(),
			owner: owner.clone(),
			runner: self.state.id(),
		});

		// Take the sandbox's CPU minimum from its lender, and record its CPU share so that its borrowers can take theirs from it.
		#[cfg(target_os = "linux")]
		{
			let result = create_output
				.sandbox
				.set_cpu_lender(allocation.cpu_lender())
				.await;
			if let Err(error) = result {
				tracing::warn!(error = %error.trace(), "failed to set the sandbox CPU lender");
			}
			allocation.set_cpu_share(create_output.sandbox.cpu_share().await);
		}

		// Store the identified sandbox state before starting any processes.
		let capacity = allocation.capacity();
		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		let index = create_output.sandbox.index();
		let processes = Arc::new(tangram_server_process_core::Processes::default());
		let control_channel_capacity = session.server.config.sandbox.control_channel_capacity;
		let (control_sender, control_receiver) =
			tangram_server_sandbox_core::control::local::Local::new(control_channel_capacity);
		let entry = tangram_server_runner_core::sandbox::State {
			allocation: Some(allocation),

			authorization_tokens: tg::authorization::Tokens::default(),

			boot,

			capacity,

			changed: tokio::sync::watch::channel(()).0,

			control_sender,

			create: arg,

			creator,

			error: None,

			id: id.clone(),

			location: location.clone(),

			owner,

			process_ids: indexmap::IndexMap::default(),

			processes: processes.clone(),

			sandbox: Some(create_output.sandbox.clone()),

			status: tg::sandbox::Status::Started,

			token: token_receiver,

			tokens: BTreeMap::new(),

			usage: None,

			#[cfg(target_os = "linux")]
			vfs: create_output.vfs.as_ref().map(|vfs| vfs.provider()),

			#[cfg(not(target_os = "linux"))]
			vfs: None,
		};
		self.state.sandboxes.insert(index, entry);
		let server = session.server.clone();
		let id_for_cleanup = id.clone();
		scopeguard::defer! {
			drop(server.components.runner.state().sandboxes.remove(index));

			// Let tests observe that the runner discarded the sandbox state.
			if server.checkpoints.is_some() {
				let server = server.clone();
				let id = id_for_cleanup.clone();
				tokio::spawn(async move {
					tangram_server_core::checkpoint!(server, "runner.sandbox.state.removed", sandbox = %id).await;
				});
			}
		}
		tangram_server_core::checkpoint!(session.server, "runner.sandbox.state.inserted", index, sandbox = %id)
			.await;

		// Bind the pooled VFS to this sandbox before starting any processes.
		#[cfg(target_os = "linux")]
		if let Some(vfs) = &create_output.vfs {
			vfs.provider()
				.set_principal(tg::Principal::Sandbox(id.clone()));
		}

		// Begin accounting before starting any workload processes.
		create_output.sandbox.activate().await?;

		// Spawn the process before waiting for the control stream.
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();
		let (sandbox_ready_sender, sandbox_ready_receiver) = tokio::sync::oneshot::channel();
		let process_task_output = process.map(|process| {
			let arg = SpawnProcessTaskArg {
				guest_url: &create_output.guest_url,
				location: location.clone(),
				process,
				process_stopper: &process_stopper,
				process_tasks: &mut process_tasks,
				processes: processes.clone(),
				retention_stopper: stopper.clone(),
				sandbox: &create_output.sandbox,
				sandbox_create,
				sandbox_ready_receiver: Some(sandbox_ready_receiver),
			};
			self.spawn_process_task(&session, arg)
		});

		// Drive the control stream alongside local requests.
		let (connected_sender, connected_receiver) = tokio::sync::oneshot::channel();

		// Let a process on the shortcut path start without waiting for the control stream, because its start creates the sandbox and the sandbox's control service verifies the runner.
		let mut sandbox_ready_sender = Some(sandbox_ready_sender);
		if shortcut && let Some(sender) = sandbox_ready_sender.take() {
			sender.send(()).ok();
		}
		let requests = futures::stream::once(async move {
			let connection = if let Some(connection) = connection {
				connection
			} else {
				connect_future.await?
			};
			if let Some(sender) = sandbox_ready_sender {
				sender.send(()).ok();
			}
			connected_sender.send(()).ok();
			Ok::<_, tg::Error>(connection.requests)
		})
		.try_flatten()
		.boxed();
		let control = tangram_server_core::control::Stream::new_reconnecting(
			requests,
			input,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		let control = control::Control::new(control, control_receiver);
		let connected = async move {
			connected_receiver
				.await
				.map_err(|_| tg::error!("the sandbox failed before connecting"))
		}
		.boxed()
		.shared();
		let state = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get(index)
			.expect("the sandbox state was not found")
			.data();
		let arg = SandboxTaskInnerArg {
			connected,
			control,
			create_output,
			event_sender,
			id: id.clone(),
			location: location.clone(),
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			state,
			stopper,
		};
		self.sandbox_task_inner(&session, arg).boxed().await
	}

	#[tracing::instrument(
		name = "sandbox.acquire",
		level = "debug",
		skip_all,
		err(level = "debug")
	)]
	async fn create_sandbox_with_pool(
		&self,
		session: &Session,
		arg: tg::sandbox::create::Arg,
		id: &tg::sandbox::Id,
	) -> tg::Result<CreateSandboxOutput> {
		if let Some(task) = self.sandbox_pool.take(&arg, self, session) {
			match task.wait().await {
				Ok(Ok(mut output)) => {
					// Link the sandbox's path to the pooled sandbox's temp and keep the temp, so that a crash or termination leaves the sandbox's files where its next boot finds them.
					let path = Self::sandbox_path(session, id);
					let result = async {
						tokio::fs::create_dir_all(path.parent().unwrap()).await?;
						tokio::fs::symlink(output.temp.path(), &path).await
					}
					.await;
					match result {
						Ok(()) => output.temp.set_preserve(true),
						Err(error) => {
							tracing::warn!(%error, sandbox = %id, "failed to link the sandbox directory");
						},
					}

					tangram_server_core::checkpoint!(
						session.server,
						"runner.sandbox.pool.take",
						index = output.sandbox.index(),
						path = %output.temp.path().display(),
						pid = output.sandbox.pid(),
					)
					.await;
					tracing::debug!(
						index = output.sandbox.index(),
						"claimed a sandbox from the pool"
					);

					return Ok(output);
				},
				Ok(Err(error)) => {
					tracing::warn!(
						error = %error.trace(),
						"failed to claim a sandbox from the pool; falling back to cold creation",
					);
				},
				Err(error) => {
					tracing::warn!(
						?error,
						"the sandbox pool task panicked; falling back to cold creation",
					);
				},
			}
		}

		tracing::debug!("creating a sandbox after a pool miss");

		let directory = Directory::New {
			path: Self::sandbox_path(session, id),
		};
		self.create_sandbox_inner(session, arg, Some(directory))
			.await
	}

	#[tracing::instrument(
		name = "sandbox.create",
		level = "debug",
		skip_all,
		err(level = "debug")
	)]
	async fn create_sandbox_inner(
		&self,
		session: &Session,
		mut arg: tg::sandbox::create::Arg,
		mut directory: Option<Directory>,
	) -> tg::Result<CreateSandboxOutput> {
		let isolation = self.resolve_sandbox_backend(&session.server)?;
		let mut resources = session
			.server
			.config
			.runner
			.backend
			.resources(&session.server.config.runner.resources);
		if matches!(
			&isolation,
			tangram_sandbox::Backend::Builtin
				| tangram_sandbox::Backend::Bubblewrap { .. }
				| tangram_sandbox::Backend::Gvisor { .. }
		) && session.server.config.runner.backend.harden()
		{
			if matches!(arg.network.as_ref(), Some(tg::sandbox::Network::Host)) {
				return Err(tg::error!(
					"host networking is not allowed for hardened container isolation"
				));
			}
			if arg.mounts.iter().any(|mount| !mount.readonly) {
				return Err(tg::error!(
					"writable mounts are not allowed for hardened container isolation"
				));
			}
		}

		#[cfg(target_os = "linux")]
		self.sandbox_assets
			.ensure_cloud_hypervisor(&session.server, &isolation)
			.await?;

		let rootfs_path = self.sandbox_assets.root.clone();

		// Enforce the sandbox storage maximum only when storage limits are enabled, because enforcement requires a filesystem with project quotas.
		if !session.server.config.runner.backend.storage_limits() {
			arg.storage = None;
		}

		// Apply the sandbox storage maximum.
		if let Some(storage) = arg.storage {
			storage.validate()?;
			if !isolation.uses_linux_namespaces() {
				return Err(tg::error!("storage bounds require container isolation"));
			}
			if let Some(max) = storage.max {
				resources.max_filesystem_size = Some(max);
			}
		}

		// Allocate the filesystem project ID, reusing the one that a restarted sandbox's directory already has.
		#[cfg(target_os = "linux")]
		let filesystem_project_id = match &isolation {
			backend
				if backend.uses_linux_namespaces()
					&& (resources.max_filesystem_inodes.is_some()
						|| resources.max_filesystem_size.is_some()) =>
			{
				let reused = match directory.as_mut() {
					#[cfg(target_os = "linux")]
					Some(Directory::Existing {
						filesystem_project_id,
						..
					}) => filesystem_project_id.take(),
					Some(Directory::New { .. }) | None => None,
				};
				let project_id = match reused {
					Some(project_id) => project_id,
					None => self.state.create_filesystem_project_id()?,
				};
				resources.filesystem_project_id = Some(project_id.value());
				Some(project_id)
			},
			_ => None,
		};

		// Create the temp at the sandbox's directory, keeping an existing directory even if the start fails.
		let mut temp = match &directory {
			Some(Directory::Existing { path, .. }) => {
				let mut temp = Temp::with_path(&session.server, path.clone());
				temp.set_preserve(true);
				temp
			},
			Some(Directory::New { path }) => Temp::with_path(&session.server, path.clone()),
			None => Temp::new(&session.server),
		};
		#[cfg(target_os = "linux")]
		if let Some(project_id) = filesystem_project_id {
			temp.set_filesystem_project_id(project_id);
		}
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Create the sandbox index.
		let index = self.state.create_sandbox_index();

		// Start an unbound per-sandbox VFS that denies access until its principal is set.
		#[cfg(target_os = "linux")]
		let (vfs, vfs_task, vfs_mount, fuse_sendfd) = match &isolation {
			tangram_sandbox::Backend::CloudHypervisor(vm) => {
				let socket = temp.path().join("vfs.sock");
				let vfs = session
					.server
					.components
					.vfs
					.start_virtiofs(
						&session.server,
						&socket,
						vm.dax,
						Origin::Sandbox(index),
						None,
					)
					.await
					.map_err(|error| tg::error!(!error, %index, "failed to start the store VFS"))?;
				(Some(vfs), None, None, None)
			},
			tangram_sandbox::Backend::Builtin
			| tangram_sandbox::Backend::Bubblewrap { .. }
			| tangram_sandbox::Backend::Gvisor { .. }
				if session.server.checkouts_enabled() && session.server.config.vfs.is_some() =>
			{
				let mount_path = temp.path().join("store");
				tokio::fs::create_dir_all(&mount_path)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to create the store mount directory")
					})?;

				if matches!(isolation, tangram_sandbox::Backend::Builtin) {
					// Create the socket pair over which the sandbox sends the mounted FUSE descriptor.
					let (sendfd, recvfd) = rustix::net::socketpair(
						rustix::net::AddressFamily::UNIX,
						rustix::net::SocketType::STREAM,
						rustix::net::SocketFlags::CLOEXEC,
						None,
					)
					.map_err(|error| tg::error!(!error, "failed to create the FUSE socket pair"))?;

					// Start the VFS concurrently, because it blocks until the sandbox mounts the filesystem and sends the descriptor.
					let mut options = session.server.config.vfs.clone().unwrap_or_default();
					options.sqpoll = false;
					let vfs_task = Task::spawn({
						let server = session.server.clone();
						let mount_path = mount_path.clone();
						move |_| {
							async move {
								let arg = tangram_server_core::vfs::MountArg {
									kind: tangram_server_core::vfs::Kind::Fuse,
									options,
									origin: Origin::Sandbox(index),
									path: &mount_path,
									principal: None,
									recvfd: Some(recvfd),
								};
								server.components.vfs.mount(&server, arg).await
							}
							.in_current_span()
						}
					});

					(None, Some(vfs_task), Some(mount_path), Some(sendfd))
				} else {
					let mut options = session.server.config.vfs.clone().unwrap_or_default();
					options.sqpoll = false;
					let arg = tangram_server_core::vfs::MountArg {
						kind: tangram_server_core::vfs::Kind::Fuse,
						options,
						origin: Origin::Sandbox(index),
						path: &mount_path,
						principal: None,
						recvfd: None,
					};
					let vfs = session
						.server
						.components
						.vfs
						.mount(&session.server, arg)
						.await
						.map_err(|error| tg::error!(!error, "failed to start the store VFS"))?;
					(Some(vfs), None, Some(mount_path), None)
				}
			},
			_ => (None, None, None, None),
		};

		// Create the listener.
		let listener_port = match &session.server.config.runner.backend {
			tangram_server_core::config::RunnerBackend::CloudHypervisor(config) => {
				Some(config.listener_port)
			},
			_ => None,
		};
		let (listener, guest_url, tangram_socket_path) =
			Self::run_create_listener(temp.path(), &isolation, &resources, listener_port)
				.await
				.map_err(
					|error| tg::error!(!error, %index, "failed to create the sandbox listener"),
				)?;

		// Create the sandbox with a readonly store mount.
		let store_path = session.server.store_path();
		#[cfg(target_os = "linux")]
		let store_source = vfs_mount.clone().unwrap_or_else(|| store_path.clone());
		#[cfg(not(target_os = "linux"))]
		let store_source = store_path.clone();
		let mut mounts = arg.mounts;
		mounts.push(tg::sandbox::Mount {
			readonly: true,
			source: store_source.clone(),
			target: store_path.clone(),
		});
		let network = match arg.network {
			None => None,
			Some(tg::sandbox::Network::Default) => Some(tangram_sandbox::Network::Default),
			Some(tg::sandbox::Network::Bridge(bridge)) => {
				Some(tangram_sandbox::Network::Bridge(tangram_sandbox::Bridge {
					ports: bridge.ports,
				}))
			},
			Some(tg::sandbox::Network::Host) => Some(tangram_sandbox::Network::Host),
		};
		let config = &session.server.config.sandbox;
		let arg = tangram_sandbox::Arg {
			backend: isolation,

			#[cfg(target_os = "linux")]
			cgroup_cleanup_interval: config.cgroup_cleanup_interval,

			#[cfg(target_os = "linux")]
			cgroup_cleanup_timeout: config.cgroup_cleanup_timeout,

			connect_timeout: config.connect_timeout,

			cpu: arg.cpu,

			#[cfg(target_os = "linux")]
			cpu_affinity: session.server.config.runner.cpu_affinity,

			#[cfg(target_os = "linux")]
			cpu_shares: Some(self.state.cpu_shares.clone()),

			#[cfg(target_os = "linux")]
			cpu_threads: Some(self.state.cpu_threads.clone()),

			dns: config.network.dns.clone(),

			#[cfg(target_os = "linux")]
			firewall: match config.network.firewall {
				tangram_server_core::config::SandboxNetworkFirewall::Iptables => {
					tangram_sandbox::network::Firewall::Iptables
				},
				tangram_server_core::config::SandboxNetworkFirewall::Nft => {
					tangram_sandbox::network::Firewall::Nft
				},
			},

			#[cfg(target_os = "linux")]
			fuse_fd: fuse_sendfd.map(Arc::new),

			hostname: arg.hostname,

			http2: config.http2,

			identity: session.server.path.clone(),

			index,

			#[cfg(target_os = "linux")]
			ip_pool: self.ip_pool.clone(),

			memory: arg.memory,

			memory_sampling_interval: session.server.config.runner.memory_sampling_interval,

			mounts,

			network,

			#[cfg(target_os = "linux")]
			network_startup_timeout: config.network.startup_timeout,

			nice: config.nice,

			path: temp.path().to_owned(),

			request_timeout: config.request_timeout,

			resources,

			rootfs_path,

			shutdown_timeout: config.shutdown_timeout,

			storage: arg.storage,

			#[cfg(target_os = "linux")]
			storage_pool: Some(self.state.storage_pool.clone()),

			store_path: store_source,

			tangram_path: session.server.tangram_path.clone(),

			tangram_socket_path,
		};
		tracing::debug!(sandbox_index = index, "creating the physical sandbox");
		let sandbox = tangram_sandbox::Sandbox::start(arg)
			.await
			.map_err(|error| tg::error!(!error, %index, "failed to create the sandbox"))?;

		// Wait for the per-sandbox VFS, which finishes starting once the sandbox mounts the filesystem and sends the FUSE descriptor.
		#[cfg(target_os = "linux")]
		let vfs = match vfs_task {
			None => vfs,
			Some(vfs_task) => {
				let vfs = vfs_task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the VFS startup task panicked"))?
					.map_err(|error| tg::error!(!error, %index, "failed to start the store VFS"))?;
				Some(vfs)
			},
		};

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = session.server.clone();
			let listener_config = tangram_server_core::config::HttpListener {
				tls: None,
				url: guest_url.clone(),
			};
			move |stopper| {
				async move {
					server
						.serve(listener, listener_config, Origin::Sandbox(index), stopper)
						.await;
				}
				.in_current_span()
			}
		});
		// Keep a new directory once the sandbox is created, so that a crash or termination leaves its files where its next boot finds them.
		if let Some(Directory::New { .. }) = directory {
			temp.set_preserve(true);
		}

		let output = CreateSandboxOutput {
			guest_url,

			sandbox,

			serve_task,

			temp,

			#[cfg(target_os = "linux")]
			vfs,
		};

		Ok(output)
	}

	async fn sandbox_task_inner(
		&self,
		session: &Session,
		arg: SandboxTaskInnerArg,
	) -> tg::Result<()> {
		let SandboxTaskInnerArg {
			connected,
			control,
			create_output,
			event_sender,
			id,
			location,
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			state,
			stopper,
		} = arg;
		let CreateSandboxOutput {
			guest_url,
			sandbox,
			serve_task,
			temp,
			#[cfg(target_os = "linux")]
			mut vfs,
		} = create_output;

		let arg = RunSandboxTaskArg {
			connected,
			control,
			event_sender,
			guest_url,
			id: id.clone(),
			location,
			process_stopper,
			process_task_output,
			process_tasks,
			processes,
			sandbox,
			serve_task,
			state,
			stopper,
		};
		let mut temp = Some(temp);
		#[cfg(target_os = "linux")]
		let result = self
			.run_sandbox_task(session, arg, &mut temp, &mut vfs)
			.boxed()
			.await;
		#[cfg(not(target_os = "linux"))]
		let result = self.run_sandbox_task(session, arg, &mut temp).boxed().await;

		// Stop the VFS after an early sandbox task failure.
		#[cfg(target_os = "linux")]
		if let Some(vfs) = vfs {
			vfs.stop();
			vfs.wait().await;
		}

		// Remove the directory after an early sandbox task failure, which ends the sandbox without a stop or destroy.
		if let Some(temp) = temp {
			Self::remove_sandbox_directory(session, &id, temp);
		}

		let arg = result?;

		self.retain_sandbox_task(session, arg).boxed().await
	}

	#[tracing::instrument(name = "sandbox.run", level = "debug", skip_all, fields(sandbox = %arg.id), err(level = "debug"))]
	async fn run_sandbox_task(
		&self,
		session: &Session,
		arg: RunSandboxTaskArg,
		temp: &mut Option<Temp>,
		#[cfg(target_os = "linux")] vfs: &mut Option<Box<dyn tangram_server_vfs_core::Server>>,
	) -> tg::Result<RetainSandboxTaskArg> {
		let RunSandboxTaskArg {
			connected,
			mut control,
			event_sender,
			guest_url,
			id,
			location,
			process_stopper,
			process_task_output,
			mut process_tasks,
			processes,
			sandbox,
			serve_task,
			state,
			stopper,
		} = arg;

		let sender = control.sender();
		let process_stopper = scopeguard::guard(process_stopper, |stopper| stopper.stop());

		// Create the process events.
		let mut process_events = StreamMap::new();

		// Create the timer.
		let mut timer_future: Option<BoxFuture<'static, ()>> = None;
		let initial_process = process_task_output.is_some();
		let ttl = state.data.ttl;

		// Observe process initialization without blocking either control transport.
		let mut pending = FuturesUnordered::new();
		if let Some(output) = process_task_output {
			pending.push(wait_for_process_attachion(output, None));
		}
		let mut ready = false;
		let mut ready_connection = connected.clone();

		// Wait for the sandbox process to exit.
		let mut sandbox_wait = {
			let sandbox = sandbox.clone();
			async move { sandbox.wait().await }.boxed()
		};
		let mut failure = None;
		let mut teardown = Teardown::Destroy;
		let mut stopping = false;

		// Run the sandbox until it should be stopped or destroyed, destroying it with the error if the loop fails.
		let result = async {
			loop {
				let current_timer_future = timer_future.as_mut().map_or_else(
					|| future::pending().left_future(),
					|timer_future| timer_future.as_mut().right_future(),
				);

				tokio::select! {
					result = &mut ready_connection, if !initial_process && !ready => {
						result?;
						if pending.is_empty() && process_events.is_empty() && let Some(ttl) = ttl {
							timer_future.replace(session.server.clock.sleep(ttl).boxed());
						}
						let event = ReadyEvent { connected_event: None, sandbox: id.clone() };
						event_sender.send(Ok(Event::Ready(event))).ok();
						tracing::info!(sandbox = %id, "the sandbox is ready");
						ready = true;
					},
					output = pending.next(), if !pending.is_empty() => {
						let (result, events, reply) = output.unwrap();
						let connected_event = match result {
							Ok(event) => event,
							Err(error) => {
								if let Some(reply) = reply {
									reply.send(Err(error.clone())).await?;
								}
								tracing::error!(error = %error.trace(), sandbox = %id, "failed to start a sandbox process");
								if !ready {
									event_sender.send(Err(error)).ok();
								}
								break;
							},
						};
						process_events.insert(connected_event.process.node.clone(), UnboundedReceiverStream::new(events));
						if let Some(reply) = reply {
							let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
								lease: connected_event.lease,
								process: connected_event.process,
							};
							reply.send(Ok(tg::sandbox::control::ClientResponseOutput::SpawnProcess(output))).await?;
						} else {
							let event = ReadyEvent { connected_event: Some(connected_event), sandbox: id.clone() };
							event_sender.send(Ok(Event::Ready(event))).ok();
							tracing::info!(sandbox = %id, "the sandbox is ready");
							ready = true;
						}
					},
					message = control.recv() => {
						// Get the message.
						let message = message
							.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
						let Some(message) = message else {
							break;
						};
						let mut end = false;
						let result = match message.arg {
							tg::sandbox::control::ServerRequestArg::Destroy(request) => {
								Self::cancel_sandbox_processes(session, &id, request.error)?;
								end = true;
								Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
									tg::sandbox::control::DestroyClientResponseOutput {
										destroyed: true,
									},
								))
							},
							tg::sandbox::control::ServerRequestArg::Get(_) => {
								let data = session
									.server
									.components
									.runner
									.state()
									.try_get_sandbox(&id)
									.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
								let output = tg::sandbox::control::GetClientResponseOutput { data };
								Ok(tg::sandbox::control::ClientResponseOutput::Get(output))
							},
							tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => session.server.components.runner.state().sandboxes.get_by_id(&id)
								.map(|sandbox| tg::sandbox::control::ClientResponseOutput::GetProcesses(sandbox.processes(arg.position, arg.length)))
								.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
							tg::sandbox::control::ServerRequestArg::SpawnProcess(request) => {
								timer_future.take();

								// Spawn the process task.
								let process = Self::prepare_process(request.process, &id)?;
								let arg = SpawnProcessTaskArg {
									guest_url: &guest_url,
									location: location.clone(),
									process,
									process_stopper: &process_stopper,
									process_tasks: &mut process_tasks,
									processes: processes.clone(),
									retention_stopper: stopper.clone(),
									sandbox: &sandbox,
									sandbox_create: None,
									sandbox_ready_receiver: None,
								};
								let task = self.spawn_process_task(session, arg);
								pending.push(wait_for_process_attachion(task, Some(message.sender)));
								continue;
							},
							tg::sandbox::control::ServerRequestArg::Stop(_) => {
								Self::cancel_sandbox_processes(session, &id, None)?;
								end = true;
								teardown = Teardown::Stop;
								Ok(tg::sandbox::control::ClientResponseOutput::Stop(
									tg::sandbox::control::StopClientResponseOutput {
										stopped: true,
									},
								))
							},
						};
						message.sender
							.send(result)
							.await
							.map_err(|error| {
								tg::error!(!error, "failed to send the sandbox control response")
							})?;
						if end {
							break;
						}
					},

					// If the runner is shutting down gracefully, then stop the sandbox, or destroy it if its ttl is zero, once its processes finish.
					() = stopper.wait(), if !stopping => {
						stopping = true;
						teardown = Teardown::with_ttl(ttl);
						if pending.is_empty() && process_events.is_empty() {
							break;
						}
					},

					// Handle an underlying process event.
					event = process_events.next(), if !process_events.is_empty() => {
						let Some((process, event)) = event else {
							break;
						};
						match event? {
							ProcessEvent::Buffered | ProcessEvent::Released => {
								process_events.remove(&process);
								if stopping && process_events.is_empty() && pending.is_empty() {
									break;
								}
								if ready && process_events.is_empty() && pending.is_empty() && let Some(ttl) = ttl {
									timer_future.replace(session.server.clock.sleep(ttl).boxed());
								}
							},
							ProcessEvent::Connected(_) => {
								return Err(tg::error!(%process, "received a duplicate process connected event"));
							},
							ProcessEvent::Exited => {},
						}
					},

					// Reap a process task after its retained state expires.
					output = process_tasks.join_next(), if !process_tasks.is_empty() => {
						let result = output
							.unwrap()
							.map_err(|error| tg::error!(!error, "a process task panicked"))?
							.map_err(|error| tg::error!(!error, "a process task failed"));
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), sandbox = %id, "a sandbox process failed");
							if !ready {
								event_sender.send(Err(error)).ok();
							}
							break;
						}
					},

					// If the sandbox process exits, then break and destroy the sandbox with the error that describes its exit.
					result = &mut sandbox_wait => {
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox process exited");
							failure = Some(error);
						}
						break;
					},

					// If the timer fires, then break and destroy the sandbox.
					() = current_timer_future => {
						break;
					},
				}
			}

			Ok::<_, tg::Error>(())
		}
		.await;
		if let Err(error) = result {
			tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
			failure.get_or_insert(error);
		}
		if failure.is_some() {
			teardown = Teardown::Destroy;
		}

		let destroy = async {
			match teardown {
				Teardown::Destroy => tracing::info!(sandbox = %id, "destroying the sandbox"),
				Teardown::Stop => {
					tracing::info!(sandbox = %id, "stopping the sandbox");
					let mut state = session
						.server
						.components
						.runner
						.state()
						.sandboxes
						.get_mut_by_id(&id)
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
					state.status = tg::sandbox::Status::Stopping;
					state.changed.send_replace(());
				},
			}

			// Stop and await the underlying processes.
			process_stopper.stop();
			while let Some((result, events, reply)) = pending.next().await {
				if let Some(reply) = reply {
					reply
						.send(Err(tg::error!(%id, "the sandbox was destroyed")))
						.await?;
				}
				if let Ok(event) = result {
					process_events.insert(event.process.node, UnboundedReceiverStream::new(events));
				}
			}
			while let Some((process, event)) = process_events.next().await {
				match event? {
					ProcessEvent::Buffered | ProcessEvent::Released => {
						process_events.remove(&process);
					},
					ProcessEvent::Connected(_) => {
						return Err(
							tg::error!(%process, "received a duplicate process connected event"),
						);
					},
					ProcessEvent::Exited => {},
				}
			}

			let allocation = {
				let mut state = session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state
					.allocation
					.take()
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox allocation"))?
			};

			// Stop the VFS while the sandbox still owns its mount namespace.
			#[cfg(target_os = "linux")]
			if let Some(vfs) = vfs.take() {
				vfs.stop();
				vfs.wait().await;
			}

			// Stop or destroy the sandbox while retaining its process and control state.
			let output = match teardown {
				Teardown::Destroy => sandbox.destroy().await.map_err(
					|error| tg::error!(!error, %id, "failed to destroy the sandbox process"),
				)?,
				Teardown::Stop => sandbox.stop().await.map_err(
					|error| tg::error!(!error, %id, "failed to stop the sandbox process"),
				)?,
			};

			let usage = output.usage;

			// Release the sandbox's capacity after its cgroup is stopped and accounted.
			tangram_server_core::checkpoint!(
				session.server,
				"runner.sandbox.capacity.release",
				sandbox = %id,
			)
			.await;
			drop(allocation.lock().await.take());
			let mut state = session
				.server
				.components
				.runner
				.state()
				.sandboxes
				.get_mut_by_id(&id)
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			state.usage = Some(usage);
			drop(state);

			// Stop and await the serve task.
			serve_task.stop();
			serve_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

			// Remove the directory of a destroyed sandbox in the background.
			if let Teardown::Destroy = teardown
				&& let Some(temp) = temp.take()
			{
				Self::remove_sandbox_directory(session, &id, temp);
			}

			// Keep the directory of a stopped sandbox, so that it can start again on its data.
			if let Teardown::Stop = teardown
				&& let Some(temp) = temp.take()
			{
				let path = Self::sandbox_path(session, &id);
				let project_id = temp.persist(&path).await.map_err(
					|error| tg::error!(!error, %id, "failed to keep the sandbox directory"),
				)?;
				#[cfg(target_os = "linux")]
				if let Some(project_id) = project_id {
					self.state
						.stopped_project_ids
						.insert(id.clone(), project_id);
				}
				#[cfg(not(target_os = "linux"))]
				let _ = project_id;
			}

			let (data, processes) = {
				let mut state = session
					.server
					.components
					.runner
					.state()
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state.status = match teardown {
					Teardown::Destroy => tg::sandbox::Status::Destroyed,
					Teardown::Stop => tg::sandbox::Status::Stopped,
				};
				state.error = failure
					.as_ref()
					.map(|error| Self::sandbox_error_data(session, error));
				state.changed.send_replace(());
				state.sandbox.take();
				(
					state.data(),
					state.process_ids.keys().cloned().collect::<Vec<_>>(),
				)
			};
			drop(sandbox);

			// Register the sandbox with its owner before indexing or reporting its stop or destruction.
			connected.await?;
			self.index_remote_sandbox(
				session,
				&id,
				&location,
				session.server.clock.unix_timestamp(),
				Some(&data),
				Some(&processes),
			)
			.await?;

			match teardown {
				Teardown::Destroy => {
					Self::request_sandbox_destroy(&sender, &id, data, processes).await?;
					tangram_server_core::checkpoint!(
						session.server,
						"runner.sandbox.destroyed",
						sandbox = %id,
					)
					.await;
					tracing::info!(sandbox = %id, "destroyed the sandbox");
				},
				Teardown::Stop => {
					Self::request_sandbox_stop(&sender, &id, data, processes).await?;
					tangram_server_core::checkpoint!(
						session.server,
						"runner.sandbox.stopped",
						sandbox = %id,
					)
					.await;
					tracing::info!(sandbox = %id, "stopped the sandbox");
				},
			}
			event_sender.send(Ok(Event::Destroyed)).ok();

			Ok::<_, tg::Error>(())
		};
		let mut destroy = destroy
			.instrument(tracing::debug_span!("sandbox.destroy", sandbox = %id))
			.boxed();
		loop {
			tokio::select! {
				result = &mut destroy => {
					result?;
					break;
				},
				message = control.recv() => {
					if let Some(message) = message? {
						self.handle_destroyed_sandbox_control_request(session, &id, message).await?;
					}
				},
			}
		}
		drop(destroy);

		// Close the control stream of a stopped sandbox, so that the index serves it and its next boot's control stream is the only one.
		let control = match teardown {
			Teardown::Destroy => Some(control),
			Teardown::Stop => None,
		};
		let output = RetainSandboxTaskArg {
			control,
			id,
			process_tasks,
			stopper,
		};

		Ok(output)
	}

	#[tracing::instrument(name = "sandbox.retain", level = "debug", skip_all, fields(sandbox = %arg.id), err(level = "debug"))]
	async fn retain_sandbox_task(
		&self,
		session: &Session,
		arg: RetainSandboxTaskArg,
	) -> tg::Result<()> {
		let RetainSandboxTaskArg {
			control,
			id,
			mut process_tasks,
			stopper,
		} = arg;

		// Await the process tasks while they retain their state and control streams.
		let Some(mut control) = control else {
			while let Some(result) = process_tasks.join_next().await {
				result
					.map_err(|error| tg::error!(!error, "a process task panicked"))?
					.map_err(|error| tg::error!(!error, "a process task failed"))?;
			}
			return Ok(());
		};
		while !process_tasks.is_empty() {
			tokio::select! {
				result = process_tasks.join_next() => {
					result.unwrap()
						.map_err(|error| tg::error!(!error, "a process task panicked"))?
						.map_err(|error| tg::error!(!error, "a process task failed"))?;
				},
				message = control.recv() => {
					if let Some(message) = message? {
						self.handle_destroyed_sandbox_control_request(session, &id, message).await?;
					}
				},
			}
		}

		// Retain the sandbox state and control stream.
		let retention_ttl = session.server.config.runner.sandbox_state_ttl;
		let retention_future = session.server.clock.sleep(retention_ttl);
		let mut retention_future = pin!(retention_future);
		loop {
			tokio::select! {
				() = &mut retention_future => break,
				() = stopper.wait() => break,
				message = control.recv() => {
					let message = message
						.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
					if let Some(message) = message {
						self.handle_destroyed_sandbox_control_request(session, &id, message).await?;
					}
				},
			}
		}

		Ok(())
	}

	async fn destroy_failed_sandbox(
		&self,
		session: &Session,
		arg: DestroyFailedSandboxArg<'_>,
	) -> tg::Result<()> {
		let DestroyFailedSandboxArg {
			arg,
			boot,
			connection,
			created_at,
			creator,
			error,
			id,
			input,
			location,
			owner,
		} = arg;

		// Create the failed sandbox's data, which is stopped rather than destroyed when a restart fails, because its directory is intact.
		let error = Self::sandbox_error_data(session, error);
		let status = if boot > 0 {
			tg::sandbox::Status::Stopped
		} else {
			tg::sandbox::Status::Destroyed
		};
		let data = tg::sandbox::Data {
			boot,
			cpu: arg.cpu,
			creator,
			error: Some(error),
			hostname: arg.hostname,
			id: id.clone(),
			memory: arg.memory,
			mounts: arg.mounts,
			network: arg.network,
			owner,
			status,
			storage: arg.storage,
			ttl: arg.ttl,
			usage: None,
		};
		let data = tg::sandbox::get::Output {
			data,
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};

		// Create the control stream.
		let context = Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: Some(connection.token.clone()),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let (_, local) = tokio::sync::mpsc::channel(1);
		let control = tangram_server_core::control::Stream::new_reconnecting(
			connection.requests,
			input,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
		let mut control = control::Control::new(control, local);
		let sender = control.sender();

		// Index and report the failure while answering the server's requests.
		let destroy = async {
			self.index_remote_sandbox(&session, id, location, created_at, Some(&data), Some(&[]))
				.await?;
			if status.is_stopped() {
				Self::request_sandbox_stop(&sender, id, data.clone(), Vec::new()).await?;
			} else {
				Self::request_sandbox_destroy(&sender, id, data.clone(), Vec::new()).await?;
			}
			Ok::<_, tg::Error>(())
		};
		let mut destroy = pin!(destroy);
		loop {
			tokio::select! {
				result = &mut destroy => {
					result?;
					break;
				},
				message = control.recv() => {
					if let Some(message) = message? {
						self.handle_destroyed_sandbox_control_request(&session, id, message).await?;
					}
				},
			}
		}

		Ok(())
	}

	async fn request_sandbox_destroy(
		sender: &SandboxControlSender,
		id: &tg::sandbox::Id,
		data: tg::sandbox::get::Output,
		processes: Vec<tg::process::Id>,
	) -> tg::Result<()> {
		let request_id = tangram_server_core::control::id();
		let request =
			tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
				arg: tg::sandbox::control::ClientRequestArg::Destroy(
					tg::sandbox::control::DestroyClientRequestArg { data, processes },
				),
				id: request_id,
			});
		let response = sender
			.request(request, tangram_server_core::control::Priority::High)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the destroy sandbox request"),
			)?;
		let response = response
			.await
			.map_err(|_| tg::error!("the sandbox control response stream ended"))?;
		let tg::sandbox::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a sandbox control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Err(tg::error!(!error, "the destroy sandbox request failed"));
		}
		response
			.output
			.ok_or_else(|| tg::error!("missing the destroy sandbox response output"))?
			.try_unwrap_destroy()
			.map_err(|_| tg::error!("expected a destroy sandbox response"))?;

		Ok(())
	}

	async fn request_sandbox_stop(
		sender: &SandboxControlSender,
		id: &tg::sandbox::Id,
		data: tg::sandbox::get::Output,
		processes: Vec<tg::process::Id>,
	) -> tg::Result<()> {
		let request_id = tangram_server_core::control::id();
		let request =
			tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
				arg: tg::sandbox::control::ClientRequestArg::Stop(
					tg::sandbox::control::StopClientRequestArg { data, processes },
				),
				id: request_id,
			});
		let response = sender
			.request(request, tangram_server_core::control::Priority::High)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to send the stop sandbox request"))?;
		let response = response
			.await
			.map_err(|_| tg::error!("the sandbox control response stream ended"))?;
		let tg::sandbox::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a sandbox control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Err(tg::error!(!error, "the stop sandbox request failed"));
		}
		response
			.output
			.ok_or_else(|| tg::error!("missing the stop sandbox response output"))?
			.try_unwrap_stop()
			.map_err(|_| tg::error!("expected a stop sandbox response"))?;

		Ok(())
	}

	fn cancel_sandbox_processes(
		session: &Session,
		id: &tg::sandbox::Id,
		error: Option<tg::error::Data>,
	) -> tg::Result<()> {
		let error = error.unwrap_or_else(|| tg::error::Data {
			code: Some(tg::error::Code::Cancellation),
			message: Some("the process was canceled".into()),
			..Default::default()
		});
		let sandbox = session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_by_id(id)
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
		for mut process in sandbox.processes.iter_mut() {
			if !process.value().data.status.is_finished() {
				process.value_mut().finish.get_or_insert(
					tg::process::control::FinishServerRequestArg {
						error: Some(error.clone()),
						exit: 1,
					},
				);
				process.stopper.stop();
			}
		}
		Ok(())
	}

	/// Destroy a stopped sandbox on this runner by removing its directory, releasing its filesystem project ID, and reporting its destruction, or destroy a boot of it that is running.
	pub(crate) async fn destroy_stopped_sandbox(
		&self,
		session: &Session,
		arg: tg::runner::control::DestroySandboxServerRequestArg,
		sender: &crate::RunnerSender,
	) -> tg::Result<tg::runner::control::DestroySandboxClientResponseOutput> {
		let tg::runner::control::DestroySandboxServerRequestArg { error, sandbox: id } = arg;

		// Destroy a boot that is running through its control, which reports its destruction.
		let control_sender = self
			.state
			.sandboxes
			.get_by_id(&id)
			.filter(|sandbox| !sandbox.status.is_settled())
			.map(|sandbox| sandbox.control_sender.clone());
		if let Some(control_sender) = control_sender {
			let request = tg::sandbox::control::ServerRequestArg::Destroy(
				tg::sandbox::control::DestroyServerRequestArg { error },
			);
			let response = control_sender.send_request(request).await?;
			let destroyed = response
				.await??
				.try_unwrap_destroy()
				.map_err(|_| tg::error!("expected a destroy sandbox response"))?
				.destroyed;
			let output = tg::runner::control::DestroySandboxClientResponseOutput { destroyed };
			return Ok(output);
		}

		// Remove the directory and release the filesystem project ID.
		let path = Self::sandbox_path(session, &id);
		let mut temp = Temp::with_path(&session.server, path);
		if let Err(error) = temp.remove().await {
			return Err(tg::error!(!error, %id, "failed to remove the sandbox directory"));
		}
		#[cfg(target_os = "linux")]
		if let Some((_, project_id)) = self.state.stopped_project_ids.remove(&id) {
			project_id.release();
		}

		// Report the destruction, which marks the sandbox destroyed.
		let arg = tg::runner::control::DestroySandboxClientRequestArg {
			error: error.map(tg::Either::Left),
			sandbox: id.clone(),
		};
		match Self::request_runner_sandbox_destroy(sender, arg).await? {
			std::ops::ControlFlow::Break(()) => {},
			std::ops::ControlFlow::Continue(error) => {
				return Err(tg::error!(!error, %id, "failed to report the sandbox's destruction"));
			},
		}
		let output = tg::runner::control::DestroySandboxClientResponseOutput { destroyed: true };

		Ok(output)
	}

	/// Remove a sandbox's directory in the background, along with the link at the sandbox's path to a pooled sandbox's temp.
	fn remove_sandbox_directory(session: &Session, id: &tg::sandbox::Id, mut temp: Temp) {
		let link = Self::sandbox_path(session, id);
		let id = id.clone();
		tokio::spawn(async move {
			if let Err(error) = temp.remove().await {
				tracing::error!(%error, sandbox = %id, "failed to remove the sandbox directory");
			}
			if tokio::fs::symlink_metadata(&link)
				.await
				.is_ok_and(|metadata| metadata.file_type().is_symlink())
			{
				tokio::fs::remove_file(&link).await.ok();
			}
		});
	}

	fn sandbox_status(session: &Session, id: &tg::sandbox::Id) -> Option<tg::sandbox::Status> {
		session
			.server
			.components
			.runner
			.state()
			.sandboxes
			.get_by_id(id)
			.map(|sandbox| sandbox.status)
	}

	/// Move pooled sandboxes' temps to their sandboxes' paths, and reserve the filesystem project IDs of the sandboxes' directories.
	pub(crate) async fn reclaim_sandbox_directories(&self, server: &Server) {
		let path = server.path.join("sandboxes");
		let mut entries = match tokio::fs::read_dir(&path).await {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
			Err(error) => {
				tracing::error!(%error, "failed to read the sandbox directories");
				return;
			},
		};
		loop {
			let entry = match entries.next_entry().await {
				Ok(Some(entry)) => entry,
				Ok(None) => break,
				Err(error) => {
					tracing::error!(%error, "failed to read a sandbox directory");
					break;
				},
			};
			let Some(id) = entry
				.file_name()
				.to_str()
				.and_then(|name| name.parse::<tg::sandbox::Id>().ok())
			else {
				continue;
			};
			let path = entry.path();

			// Move a pooled sandbox's temp to the sandbox's path, replacing the link that pointed at it, or remove a link whose temp is gone.
			let metadata = match tokio::fs::symlink_metadata(&path).await {
				Ok(metadata) => metadata,
				Err(error) => {
					tracing::error!(%error, sandbox = %id, "failed to read the sandbox directory");
					continue;
				},
			};
			if metadata.file_type().is_symlink() {
				let result = async {
					let target = tokio::fs::read_link(&path).await?;
					tokio::fs::remove_file(&path).await?;
					if tokio::fs::try_exists(&target).await? {
						tokio::fs::rename(&target, &path).await?;
						Ok::<_, std::io::Error>(true)
					} else {
						Ok(false)
					}
				}
				.await;
				match result {
					Ok(true) => {},
					Ok(false) => continue,
					Err(error) => {
						tracing::error!(%error, sandbox = %id, "failed to move the sandbox directory");
						continue;
					},
				}
			}

			// Reserve the directory's filesystem project ID.
			#[cfg(target_os = "linux")]
			match tangram_sandbox::filesystem_project_id(&path) {
				Ok(Some(value)) => {
					if let Some(project_id) = self.state.filesystem_project_ids.acquire_value(value)
					{
						self.state.stopped_project_ids.insert(id, project_id);
					}
				},
				Ok(None) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), sandbox = %id, "failed to read the sandbox filesystem project");
				},
			}
		}
	}

	/// Decide what happens to each sandbox that the previous run left a directory for, using the local index, and remove the directories of the sandboxes to destroy.
	pub(crate) async fn finish_previous_sandboxes(
		&self,
		session: &Session,
	) -> tg::Result<Vec<(tg::sandbox::Id, Teardown)>> {
		// Get the sandboxes with directories.
		let path = session.server.path.join("sandboxes");
		let mut ids = Vec::new();
		let mut entries = match tokio::fs::read_dir(&path).await {
			Ok(entries) => entries,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the sandbox directories"));
			},
		};
		while let Some(entry) = entries
			.next_entry()
			.await
			.map_err(|error| tg::error!(!error, "failed to read a sandbox directory"))?
		{
			if let Some(id) = entry
				.file_name()
				.to_str()
				.and_then(|name| name.parse::<tg::sandbox::Id>().ok())
			{
				ids.push(id);
			}
		}
		if ids.is_empty() {
			return Ok(Vec::new());
		}

		// Get the sandboxes' local records after pending indexing, so that a record is not stale.
		session
			.index(tg::index::Arg::default())
			.await?
			.try_last()
			.await?;
		let indexed = session.server.index.try_get_sandboxes(&ids).await?;

		// Stop a sandbox that was running with a nonzero ttl, keep a stopped sandbox, and destroy the others. Remove the directory of a sandbox that is not recorded or is destroyed now, and remove the directory of a sandbox to destroy after its destruction is reported, so that a restart reports a destruction that was not.
		let mut sandboxes = Vec::new();
		for (id, indexed) in std::iter::zip(ids, indexed) {
			let teardown = match indexed.map(|indexed| indexed.data) {
				None => None,
				Some(None) => Some(Teardown::Destroy),
				Some(Some(data)) => match data.data.status {
					tg::sandbox::Status::Destroyed => None,
					tg::sandbox::Status::Started
					| tg::sandbox::Status::Starting
					| tg::sandbox::Status::Stopping => Some(Teardown::with_ttl(data.data.ttl)),
					tg::sandbox::Status::Stopped => continue,
				},
			};
			let Some(teardown) = teardown else {
				self.remove_previous_sandbox_directory(session, &id).await?;
				continue;
			};
			sandboxes.push((id, teardown));
		}

		Ok(sandboxes)
	}

	/// Remove a previous sandbox's directory and release its filesystem project ID.
	pub(crate) async fn remove_previous_sandbox_directory(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let mut temp = Temp::with_path(&session.server, Self::sandbox_path(session, id));
		temp.remove()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to remove the sandbox directory"))?;
		#[cfg(target_os = "linux")]
		if let Some((_, project_id)) = self.state.stopped_project_ids.remove(id) {
			project_id.release();
		}

		Ok(())
	}

	/// Get the path of the directory that holds a sandbox's data while it is stopped.
	fn sandbox_path(session: &Session, id: &tg::sandbox::Id) -> std::path::PathBuf {
		session.server.path.join("sandboxes").join(id.to_string())
	}

	fn sandbox_error_data(
		session: &Session,
		error: &tg::Error,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		let mut error = error.to_data_or_id();
		if !session.server.config.advanced.internal_error_locations
			&& let tg::Either::Left(error) = &mut error
		{
			error.remove_internal_locations();
		}
		error
	}

	async fn get_sandbox_control_stream(
		&self,
		session: &Session,
		arg: GetSandboxControlStreamArg<'_>,
	) -> tg::Result<ConnectedSandboxControl> {
		let GetSandboxControlStreamArg {
			boot,
			create,
			created_at,
			creator,
			id,
			input_stream,
			location,
			owner,
			proof,
		} = arg;
		let runner = session
			.server
			.components
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				boot,
				cpu: create.cpu,
				creator: creator.clone(),
				error: None,
				hostname: create.hostname.clone(),
				id: id.clone(),
				memory: create.memory,
				mounts: create.mounts.clone(),
				network: create.network.clone(),
				owner: owner.clone(),
				status: tg::sandbox::Status::Started,
				storage: create.storage,
				ttl: create.ttl,
				usage: None,
			},
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let arg = tg::sandbox::control::Arg {
			boot,
			create: Some(create),
			created_at: Some(created_at),
			creator,
			id: Some(id.clone()),
			location: Some(location.clone().into()),
			owner,
			proof,
			runner: Some(runner),
		};
		let (header, control) = self
			.connect_sandbox_control(session, arg, input_stream)
			.await?;
		if header.id != *id {
			return Err(
				tg::error!(actual = %header.id, expected = %id, "the server returned an invalid sandbox"),
			);
		}
		let token = header
			.token
			.or_else(|| session.context.token.clone())
			.ok_or_else(
				|| tg::error!(id = %header.id, "missing the sandbox authentication token"),
			)?;
		self.index_remote_sandbox(session, &header.id, location, created_at, Some(&data), None)
			.await?;
		let connection = ConnectedSandboxControl {
			requests: control,
			token,
		};

		Ok(connection)
	}

	#[tracing::instrument(name = "sandbox.connect", level = "debug", skip_all, fields(sandbox = arg.id.as_ref().map(ToString::to_string)), err(level = "debug"))]
	async fn connect_sandbox_control(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		input: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	)> {
		let id = arg.id.clone();
		let shortcut = arg.proof.is_some();
		tangram_server_core::checkpoint!(session.server, "runner.sandbox.control.connect", sandbox = ?id, shortcut = %shortcut).await;
		let reconnect_context = session.context.clone();
		let reconnect_server = session.server.clone();
		let reconnect = move |header: &tg::sandbox::control::Header| {
			let token = header.token.clone().or(reconnect_context.token.clone());
			let context = Context {
				principal: tg::Principal::Sandbox(header.id.clone()),
				token,
				..reconnect_context
			};

			reconnect_server.session(&context)
		};
		let (header, control) = session
			.get_sandbox_control_stream_all(arg, input, reconnect)
			.boxed()
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					?id,
					"failed to connect to the sandbox control stream"
				)
			})?;

		Ok((header, control.boxed()))
	}

	async fn index_remote_sandbox(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		location: &tg::Location,
		created_at: i64,
		data: Option<&tg::sandbox::get::Output>,
		processes: Option<&[tg::process::Id]>,
	) -> tg::Result<()> {
		if !location.is_remote() {
			return Ok(());
		}
		let data = data.cloned().map(|mut data| {
			data.tokens.clear();
			data
		});
		let touched_at = session.server.clock.unix_timestamp();
		let sandbox = tangram_index::sandbox::put::Arg {
			account: None,
			created_at,
			data,
			id: id.clone(),
			location: Some(location.clone()),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes: processes.map(<[_]>::to_vec),
			runner: None,
			touched_at,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(sandbox)],
		};
		session
			.server
			.components
			.indexer
			.index_batch(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to index the remote sandbox"))?;

		Ok(())
	}

	fn prepare_process(
		mut arg: tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<tg::runner::control::Process> {
		if arg.id.is_none() {
			arg.data.sandbox = Some(sandbox.clone());
		} else if arg.data.sandbox.as_ref() != Some(sandbox) {
			let process = arg.id.as_ref();
			return Err(tg::error!(
				?process,
				sandbox = %sandbox,
				"the process is not in the sandbox"
			));
		}
		Ok(arg)
	}
}

impl Teardown {
	pub(crate) fn with_ttl(ttl: Option<std::time::Duration>) -> Self {
		if ttl.is_some_and(|ttl| ttl.is_zero()) {
			Self::Destroy
		} else {
			Self::Stop
		}
	}
}

fn wait_for_process_attachion(
	output: SpawnProcessTaskOutput,
	sender: Option<Reply>,
) -> PendingProcess {
	let mut events = output.events;
	async move {
		let result = async {
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!("the process event sender was dropped"))??;
			let ProcessEvent::Connected(event) = event else {
				return Err(tg::error!("expected the process connected event"));
			};
			Ok(event)
		}
		.await;
		(result, events, sender)
	}
	.boxed()
}
