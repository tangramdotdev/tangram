use {
	super::process::{
		ConnectedEvent, Event as ProcessEvent, SpawnProcessTaskArg, SpawnProcessTaskOutput,
	},
	crate::{
		Context, Origin, Server, Session,
		sandbox::control::local::{Message, Reply},
		temp::Temp,
	},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::{BoxStream, FuturesUnordered},
	},
	std::{collections::BTreeMap, pin::pin, sync::Arc, time::Instant},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
	tokio_stream::{StreamMap, wrappers::UnboundedReceiverStream},
};

mod control;
#[cfg(target_os = "linux")]
mod linux;
mod listener;
mod pool;

pub(super) use self::pool::Pool;

type ConnectionReady = futures::future::Shared<BoxFuture<'static, tg::Result<()>>>;

type PendingProcess = BoxFuture<
	'static,
	(
		tg::Result<ConnectedEvent>,
		tokio::sync::mpsc::UnboundedReceiver<tg::Result<ProcessEvent>>,
		Option<Reply>,
	),
>;

type SandboxControlSender = crate::control::Sender<
	tg::sandbox::control::ServerMessage,
	tg::sandbox::control::ClientMessage,
>;

pub(crate) struct SpawnSandboxTaskArg {
	pub allocation: crate::runner::capacity::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub id: Option<tg::sandbox::Id>,
	pub location: tg::Location,
	pub process: Option<tg::runner::control::Process>,
	pub token: Option<String>,
}

#[must_use]
pub(crate) struct SpawnSandboxTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

struct SandboxTaskArg {
	allocation: crate::runner::capacity::Allocation,
	arg: tg::sandbox::create::Arg,
	creator: Option<tg::Principal>,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: Option<tg::sandbox::Id>,
	location: tg::Location,
	process: Option<tg::runner::control::Process>,
	stopper: Stopper,
	token: Option<String>,
}

struct CreateSandboxOutput {
	guest_url: tangram_uri::Uri,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	temp: Temp,
	#[cfg(target_os = "linux")]
	vfs: Option<crate::vfs::Server>,
	#[cfg(target_os = "linux")]
	vfs_principal: Option<Arc<std::sync::Mutex<Option<tg::Principal>>>>,
}

struct ConnectedSandboxControl {
	requests:
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
}

pub(super) struct SandboxControlConnection {
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	id: tg::sandbox::Id,
	token: String,
}

enum SandboxControlConnectionKind {
	Pooled(SandboxControlConnection),
	Standard(ConnectedSandboxControl),
}

pub(crate) enum Event {
	Destroyed,
	Ready(ReadyEvent),
}

#[derive(Clone, Debug)]
pub(crate) struct ReadyEvent {
	pub connected_event: Option<ConnectedEvent>,
	pub sandbox: tg::sandbox::Id,
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
	processes: Arc<crate::process::Processes>,
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
	processes: Arc<crate::process::Processes>,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	started_at: Instant,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RetainSandboxTaskArg {
	control: control::Control,
	id: tg::sandbox::Id,
	process_tasks: JoinSet<tg::Result<()>>,
	stopper: Stopper,
}

impl Server {
	pub(crate) fn spawn_sandbox_task(&self, arg: SpawnSandboxTaskArg) -> SpawnSandboxTaskOutput {
		let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let task_id = crate::control::id();
		let mut task = self.sandbox_tasks.spawn(task_id, |stopper| {
			let server = self.clone();
			async move {
				// Run the sandbox task.
				let session = server.session(&server.context);
				let arg = SandboxTaskArg {
					allocation: arg.allocation,
					arg: arg.arg,
					creator: arg.creator,
					event_sender: event_sender.clone(),
					id: arg.id,
					location: arg.location,
					process: arg.process,
					stopper,
					token: arg.token,
				};
				let result = if server.shutdown.borrow().is_some() {
					Err(tg::error!("the server is shutting down"))
				} else {
					session.sandbox_task(arg).boxed().await
				};
				if let Err(error) = &result {
					event_sender.send(Err(error.clone())).ok();
				}
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "the sandbox task failed");
				}
			}
		});
		task.detach();
		SpawnSandboxTaskOutput {
			events: event_receiver,
		}
	}
}

impl Session {
	async fn handle_destroyed_sandbox_control_request(
		&self,
		id: &tg::sandbox::Id,
		message: Message,
	) -> tg::Result<()> {
		let result = match message.arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) => {
				Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
					tg::sandbox::control::DestroyClientResponseOutput { destroyed: false },
				))
			},
			tg::sandbox::control::ServerRequestArg::Get(_) => self
				.server
				.runner
				.state
				.try_get_sandbox(id)
				.map(|data| {
					tg::sandbox::control::ClientResponseOutput::Get(
						tg::sandbox::control::GetClientResponseOutput { data },
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => self
				.server
				.runner
				.state
				.sandboxes
				.get_by_id(id)
				.map(|sandbox| {
					tg::sandbox::control::ClientResponseOutput::GetProcesses(
						sandbox.processes(arg.position, arg.length),
					)
				})
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox")),
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => {
				Err(tg::error!(%id, "the sandbox was destroyed"))
			},
		};
		message
			.sender
			.send(result)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))?;
		Ok(())
	}

	async fn sandbox_task(&self, arg: SandboxTaskArg) -> tg::Result<()> {
		let SandboxTaskArg {
			allocation,
			arg,
			creator,
			event_sender,
			id,
			location,
			process,
			stopper,
			token,
		} = arg;
		let identity = match (id, token) {
			(Some(id), Some(token)) => Some((id, token)),
			(None, None) => None,
			_ => {
				return Err(tg::error!(
					"the sandbox id and token must be provided together"
				));
			},
		};
		let context = if let Some((id, token)) = &identity {
			Context {
				principal: tg::Principal::Sandbox(id.clone()),
				token: Some(token.clone()),
				..self.context.clone()
			}
		} else {
			let runner = self
				.server
				.runner
				.state
				.id()
				.ok_or_else(|| tg::error!("missing the runner id"))?;
			let token = self.server.config.runner.token.clone();
			Context {
				principal: tg::Principal::Runner(runner),
				token,
				..self.context.clone()
			}
		};
		let connection_session = self.server.session(&context);

		// Create the sandbox concurrently with its control stream.
		let create_future = self.create_sandbox_with_pool(arg.clone());
		let created_at = self.server.clock.unix_timestamp()?;
		let control_data = tg::sandbox::control::Data {
			arg: arg.clone(),
			creator: creator.clone(),
		};
		let (input, input_receiver) = tokio::sync::mpsc::channel(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let connect_future: BoxFuture<'static, tg::Result<SandboxControlConnectionKind>> =
			if let Some((id, _)) = &identity {
				let control_data = control_data.clone();
				let id = id.clone();
				let location = location.clone();
				async move {
					let connection = connection_session
						.get_sandbox_control_stream(
							Some(&id),
							&location,
							created_at,
							control_data,
							input_stream,
						)
						.await?;

					Ok(SandboxControlConnectionKind::Standard(connection))
				}
				.boxed()
			} else {
				let server = self.server.clone();
				async move {
					crate::checkpoint!(server, "runner.sandbox.control.acquire").await;
					let connection = server
						.runner
						.sandbox_control_connection_pool()
						.take()
						.await?;

					Ok(SandboxControlConnectionKind::Pooled(connection))
				}
				.boxed()
			};
		let mut create_future = pin!(create_future);
		let mut connect_future = connect_future.boxed();
		let mut connection = None;
		let create_output = loop {
			tokio::select! {
				result = &mut create_future => break result?,
				result = &mut connect_future, if connection.is_none() => {
					connection = Some(result?);
				},
			}
		};

		// Resolve the shortcut identity before activating the physical sandbox.
		let shortcut = identity.is_none();
		let (id, token) = if let Some(identity) = identity {
			identity
		} else {
			let connection_ = if let Some(connection) = connection.take() {
				connection
			} else {
				connect_future.as_mut().await?
			};
			let SandboxControlConnectionKind::Pooled(connection_) = connection_ else {
				unreachable!();
			};
			let create = tg::sandbox::control::CreateClientRequestArg {
				created_at,
				data: control_data.clone(),
			};
			let request_id = crate::control::id();
			let request =
				tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
					arg: tg::sandbox::control::ClientRequestArg::Create(create),
					id: request_id,
				});
			let response = connection_
				.control
				.sender()
				.request(request, crate::control::Priority::High)
				.await?;
			let sandbox_stopper = stopper.clone();
			crate::checkpoint!(self.server, "runner.sandbox.control.create.sent", sandbox = %connection_.id).await;
			let mut create_task = Task::spawn(move |_| async move {
				let result = async {
					let response = response
						.await
						.map_err(|_| tg::error!("the sandbox control response stream ended"))?;
					let tg::sandbox::control::ServerMessage::Response(response) = response else {
						return Err(tg::error!("expected a sandbox control create response"));
					};
					if let Some(error) = response.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the error")
						})?;
						return Err(error);
					}
					response
						.output
						.ok_or_else(|| tg::error!("missing the sandbox control create response"))?
						.try_unwrap_create()
						.map_err(|_| tg::error!("expected a sandbox control create response"))?;

					Ok::<_, tg::Error>(())
				}
				.await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to create the sandbox control connection");
					sandbox_stopper.stop();
				}
			});
			create_task.detach();
			let identity = (connection_.id.clone(), connection_.token.clone());
			connection = Some(SandboxControlConnectionKind::Pooled(connection_));
			identity
		};
		let process = process
			.map(|process| Self::prepare_process(process, &id))
			.transpose()?;
		let context = Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: Some(token.clone()),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let sandbox_initialization = shortcut.then(|| tg::process::control::Sandbox {
			created_at,
			data: control_data.clone(),
			runner: self.server.runner.state.id(),
			token: token.clone(),
		});

		// Store the identified sandbox state before starting any processes.
		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		let index = create_output.sandbox.index();
		let processes = Arc::new(crate::process::Processes::default());
		let (control_sender, control_receiver) = crate::sandbox::control::local::Local::new();
		let entry = crate::sandbox::State {
			allocation: Some(allocation),
			authorization_tokens: tg::authorization::Tokens::default(),
			changed: tokio::sync::watch::channel(()).0,
			control_sender,
			data: control_data,
			id: id.clone(),
			location: location.clone(),
			process_ids: indexmap::IndexMap::default(),
			processes: processes.clone(),
			sandbox: Some(create_output.sandbox.clone()),
			status: tg::sandbox::Status::Started,
			token,
			tokens: BTreeMap::new(),
			usage: None,
		};
		self.server.runner.state.sandboxes.insert(index, entry);
		let server = self.server.clone();
		scopeguard::defer! {
			server.runner.state.sandboxes.remove(index);
		}
		crate::checkpoint!(self.server, "runner.sandbox.state.inserted", index, sandbox = %id)
			.await;

		// Bind the pooled VFS to this sandbox before starting any processes.
		#[cfg(target_os = "linux")]
		if let Some(principal) = &create_output.vfs_principal {
			principal
				.lock()
				.unwrap()
				.replace(tg::Principal::Sandbox(id.clone()));
		}

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
				sandbox_initialization,
				sandbox_ready_receiver: Some(sandbox_ready_receiver),
			};
			self.spawn_process_task(arg)
		});

		// Drive the control stream alongside local requests.
		let (connected_sender, connected_receiver) = tokio::sync::oneshot::channel();
		let control = match connection {
			Some(SandboxControlConnectionKind::Pooled(connection)) => {
				sandbox_ready_sender.send(()).ok();
				connected_sender.send(()).ok();
				connection.control
			},
			connection => {
				let requests = futures::stream::once(async move {
					let connection = match connection {
						Some(SandboxControlConnectionKind::Standard(connection)) => connection,
						Some(SandboxControlConnectionKind::Pooled(_)) => unreachable!(),
						None => {
							let SandboxControlConnectionKind::Standard(connection) =
								connect_future.await?
							else {
								unreachable!();
							};
							connection
						},
					};
					sandbox_ready_sender.send(()).ok();
					connected_sender.send(()).ok();
					Ok::<_, tg::Error>(connection.requests)
				})
				.try_flatten()
				.boxed();
				crate::control::Stream::new_reconnecting(
					requests,
					input,
					crate::control::stream_options(),
				)
			},
		};
		let control = control::Control::new(control, control_receiver);
		let connected = async move {
			connected_receiver
				.await
				.map_err(|_| tg::error!("the sandbox failed before connecting"))
		}
		.boxed()
		.shared();
		let state = self
			.server
			.runner
			.state
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
		let result = session.sandbox_task_inner(arg).boxed().await;
		if let Err(error) = &result {
			tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
			let mut error = error.to_data_or_id();
			if !session.server.config.advanced.internal_error_locations
				&& let tg::Either::Left(error) = &mut error
			{
				error.remove_internal_locations();
			}
			let arg = tg::sandbox::destroy::Arg {
				error: Some(error),
				location: Some(location.into()),
			};
			match session.try_destroy_sandbox(&id, arg).boxed().await {
				Ok(Some(_)) => {},
				Ok(None) => {
					tracing::error!(sandbox = %id, "failed to find the sandbox after the sandbox failed");
				},
				Err(error) => {
					tracing::error!(
						error = %error.trace(),
						sandbox = %id,
						"failed to destroy the sandbox after the sandbox failed"
					);
				},
			}
		}

		result
	}

	async fn create_sandbox_with_pool(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<CreateSandboxOutput> {
		if let Some(task) = self.server.runner.sandbox_pool.take(&arg, self) {
			match task.wait().await {
				Ok(Ok(output)) => {
					crate::checkpoint!(
						self.server,
						"runner.sandbox.pool.take",
						index = output.sandbox.index(),
						path = %output.temp.path().display(),
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

		self.create_sandbox_inner(arg).await
	}

	async fn create_sandbox_inner(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<CreateSandboxOutput> {
		let isolation = match &arg.isolation {
			Some(tg::sandbox::Isolation::Container) => {
				let container = self
					.server
					.config()
					.sandbox
					.isolation
					.container
					.as_ref()
					.ok_or_else(|| tg::error!("container isolation is not configured"))?;
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation {
					max_pids: container.max_pids,
				})
			},
			Some(tg::sandbox::Isolation::Seatbelt) => {
				tangram_sandbox::Isolation::Seatbelt(tangram_sandbox::SeatbeltIsolation::default())
			},
			Some(tg::sandbox::Isolation::Vm) => {
				#[cfg(target_os = "linux")]
				{
					let vm = self
						.server
						.config()
						.sandbox
						.isolation
						.vm
						.as_ref()
						.ok_or_else(|| tg::error!("missing vm configuration"))?;
					let kernel_path = vm.kernel_path.clone();
					let image_path = self.server.sandbox_vm_image.clone().ok_or_else(|| {
						tg::error!(
							"vm isolation requested but no image path was configured; check the server config"
						)
					})?;
					let snapshot = Some(
						vm.snapshot
							.clone()
							.unwrap_or_else(|| self.server.vm_snapshot_path()),
					);
					tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation {
						cloud_hypervisor_path: vm.cloud_hypervisor_path.clone(),
						dax: vm.dax.map(|dax| dax.window_size as u64),
						image_path,
						kernel_path,
						max_cpu: vm.max_cpu,
						max_memory: vm.max_memory,
						snapshot,
						snapshot_cpu: vm.snapshot_cpu,
						snapshot_memory: vm.snapshot_memory,
					})
				}
				#[cfg(target_os = "macos")]
				{
					return Err(tg::error!("vm isolation is not supported on macos"));
				}
			},
			None => self.server.resolve_sandbox_isolation()?,
		};

		#[cfg(target_os = "linux")]
		self.ensure_vm_isolation(&isolation).await?;

		let rootfs_path = match &isolation {
			tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Vm(_) => {
				self.server.sandbox_container_root.clone()
			},
			tangram_sandbox::Isolation::Seatbelt(_) => self.server.sandbox_seatbelt_root.clone(),
		};

		// Create the temp.
		let temp = Temp::new(&self.server);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Create the sandbox index.
		let index = self.server.runner.state.create_sandbox_index();

		// Start an unbound per-sandbox VFS that denies access until its principal is set.
		#[cfg(target_os = "linux")]
		let principal = Arc::new(std::sync::Mutex::new(None));
		#[cfg(target_os = "linux")]
		let (vfs, vfs_task, vfs_mount, fuse_sendfd) = match &isolation {
			tangram_sandbox::Isolation::Vm(vm) => {
				let socket = temp.path().join("vfs.sock");
				let vfs = crate::vfs::Server::start_virtiofs(
					&self.server,
					&socket,
					vm.dax,
					Origin::Sandbox(index),
					principal.clone(),
				)
				.await
				.map_err(|error| tg::error!(!error, %index, "failed to start the store VFS"))?;
				(Some(vfs), None, None, None)
			},
			tangram_sandbox::Isolation::Container(_)
				if self.server.checkouts_enabled() && self.server.config.vfs.is_some() =>
			{
				let mount_path = temp.path().join("store");
				tokio::fs::create_dir_all(&mount_path)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to create the store mount directory")
					})?;

				// Create the socket pair over which the sandbox sends the mounted FUSE descriptor.
				let (sendfd, recvfd) = rustix::net::socketpair(
					rustix::net::AddressFamily::UNIX,
					rustix::net::SocketType::STREAM,
					rustix::net::SocketFlags::CLOEXEC,
					None,
				)
				.map_err(|error| tg::error!(!error, "failed to create the FUSE socket pair"))?;

				// Start the VFS concurrently, because it blocks until the sandbox mounts the filesystem and sends the descriptor.
				let mut options = self.server.config.vfs.clone().unwrap_or_default();
				options.sqpoll = false;
				let vfs_task = Task::spawn({
					let server = self.server.clone();
					let principal = principal.clone();
					let mount_path = mount_path.clone();
					move |_| async move {
						crate::vfs::Server::start(
							&server,
							crate::vfs::Kind::Fuse,
							&mount_path,
							options,
							Origin::Sandbox(index),
							principal,
							Some(recvfd),
						)
						.await
					}
				});
				(None, Some(vfs_task), Some(mount_path), Some(sendfd))
			},
			_ => (None, None, None, None),
		};

		// Create the listener.
		let (listener, guest_url, tangram_socket_path) =
			Server::run_create_listener(temp.path(), &isolation)
				.await
				.map_err(
					|error| tg::error!(!error, %index, "failed to create the sandbox listener"),
				)?;

		// Create the sandbox with a readonly store mount.
		let store_path = self.server.store_path();
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
		let arg = tangram_sandbox::Arg {
			cpu: arg.cpu,
			dns: self.server.config.sandbox.network.dns.clone(),
			#[cfg(target_os = "linux")]
			firewall: match self.server.config.sandbox.network.firewall {
				crate::config::SandboxNetworkFirewall::Iptables => {
					tangram_sandbox::Firewall::Iptables
				},
				crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
			},
			#[cfg(target_os = "linux")]
			fuse_fd: fuse_sendfd.map(Arc::new),
			hostname: arg.hostname,
			identity: self.server.path.clone(),
			index,
			#[cfg(target_os = "linux")]
			ip_pool: self.server.ip_pool.clone(),
			isolation,
			memory: arg.memory,
			mounts,
			network,
			nice: self.server.config.sandbox.nice,
			path: temp.path().to_owned(),
			rootfs_path,
			store_path: store_source,
			tangram_path: self.server.tangram_path.clone(),
			tangram_socket_path,
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
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
		#[cfg(target_os = "linux")]
		let vfs_principal = vfs.is_some().then_some(principal);

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = self.server.clone();
			let listener_config = crate::config::HttpListener {
				tls: None,
				url: guest_url.clone(),
			};
			move |stopper| async move {
				server
					.serve(listener, listener_config, Origin::Sandbox(index), stopper)
					.await;
			}
		});
		let output = CreateSandboxOutput {
			guest_url,
			sandbox,
			serve_task,
			temp,
			#[cfg(target_os = "linux")]
			vfs,
			#[cfg(target_os = "linux")]
			vfs_principal,
		};

		Ok(output)
	}

	async fn sandbox_task_inner(&self, arg: SandboxTaskInnerArg) -> tg::Result<()> {
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
			#[cfg(target_os = "linux")]
				vfs_principal: _,
		} = create_output;

		let started_at = Instant::now();
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
			started_at,
			state,
			stopper,
		};
		#[cfg(target_os = "linux")]
		let result = self.run_sandbox_task(arg, &mut vfs).boxed().await;
		#[cfg(not(target_os = "linux"))]
		let result = self.run_sandbox_task(arg).boxed().await;

		// Stop the VFS after an early sandbox task failure.
		#[cfg(target_os = "linux")]
		if let Some(vfs) = vfs {
			vfs.stop();
			vfs.wait().await;
		}
		drop(temp);

		let arg = result?;

		self.retain_sandbox_task(arg).boxed().await
	}

	async fn run_sandbox_task(
		&self,
		arg: RunSandboxTaskArg,
		#[cfg(target_os = "linux")] vfs: &mut Option<crate::vfs::Server>,
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
			started_at,
			state,
			stopper,
		} = arg;

		let sender = control.sender();
		let process_stopper = scopeguard::guard(process_stopper, |stopper| stopper.stop());

		// Create the process events.
		let mut process_events = StreamMap::new();

		// Create the timer.
		let mut timer_future: Option<BoxFuture<'static, ()>> = None;
		let reusable = process_task_output.is_none();
		let ttl = state.data.ttl;

		// Observe process initialization without blocking either control transport.
		let mut pending = FuturesUnordered::new();
		if let Some(output) = process_task_output {
			pending.push(wait_for_process_connection(output, None));
		}
		let mut ready = false;
		let mut ready_connection = connected.clone();

		loop {
			let current_timer_future = timer_future.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer_future| timer_future.as_mut().right_future(),
			);
			tokio::select! {
				result = &mut ready_connection, if reusable && !ready => {
					result?;
					if pending.is_empty() && process_events.is_empty() && let Some(ttl) = ttl {
						timer_future.replace(tokio::time::sleep(ttl).boxed());
					}
					let event = ReadyEvent { connected_event: None, sandbox: id.clone() };
					event_sender.send(Ok(Event::Ready(event))).ok();
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
					let mut destroy = false;
					let result = match message.arg {
						tg::sandbox::control::ServerRequestArg::Destroy(request) => {
							let error = request.error.unwrap_or_else(|| tg::error::Data {
								code: Some(tg::error::Code::Cancellation),
								message: Some("the process was canceled".into()),
								..Default::default()
							});
							let sandbox = self
								.server
								.runner
								.state
								.sandboxes
								.get_by_id(&id)
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
							destroy = true;
							Ok(tg::sandbox::control::ClientResponseOutput::Destroy(
								tg::sandbox::control::DestroyClientResponseOutput {
									destroyed: true,
								},
							))
						},
						tg::sandbox::control::ServerRequestArg::Get(_) => {
							let data = self
								.server
								.runner
								.state
								.try_get_sandbox(&id)
								.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
							let output = tg::sandbox::control::GetClientResponseOutput { data };
							Ok(tg::sandbox::control::ClientResponseOutput::Get(output))
						},
						tg::sandbox::control::ServerRequestArg::GetProcesses(arg) => self.server.runner.state.sandboxes.get_by_id(&id)
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
								sandbox_initialization: None,
								sandbox_ready_receiver: None,
							};
							let task = self.spawn_process_task(arg);
							pending.push(wait_for_process_connection(task, Some(message.sender)));
							continue;
						},
					};
					message.sender
						.send(result)
						.await
						.map_err(|error| {
							tg::error!(!error, "failed to send the sandbox control response")
						})?;
					if destroy {
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
							if ready && process_events.is_empty() && pending.is_empty() {
								if !reusable {
									break;
								}
								if let Some(ttl) = ttl {
									timer_future.replace(tokio::time::sleep(ttl).boxed());
								}
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

				// If the timer fires, then break and destroy the sandbox.
				() = current_timer_future => {
					break;
				},
			}
		}

		let destroy = async {
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

			// Release the sandbox's capacity once all of its underlying processes have exited.
			crate::checkpoint!(
				self.server,
				"runner.sandbox.capacity.release",
				sandbox = %id,
			)
			.await;
			let allocation = {
				let mut state = self
					.server
					.runner
					.state
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state
					.allocation
					.take()
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox allocation"))?
			};
			let usage = {
				let mut allocation = allocation.lock().await;
				let duration = started_at.elapsed();
				let usage = allocation
					.as_ref()
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox allocation"))?
					.usage(duration)?;
				drop(allocation.take());

				usage
			};
			let mut state = self
				.server
				.runner
				.state
				.sandboxes
				.get_mut_by_id(&id)
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			state.usage = Some(tg::sandbox::Usage {
				cpu: usage.cpu,
				memory: usage.memory,
			});
			drop(state);

			// Stop the VFS while the sandbox still owns its mount namespace.
			#[cfg(target_os = "linux")]
			if let Some(vfs) = vfs.take() {
				vfs.stop();
				vfs.wait().await;
			}

			// Destroy the sandbox while retaining its process and control state.
			sandbox.destroy().await.map_err(
				|error| tg::error!(!error, %id, "failed to destroy the sandbox process"),
			)?;

			// Stop and await the serve task.
			serve_task.stop();
			serve_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

			let (data, processes) = {
				let mut state = self
					.server
					.runner
					.state
					.sandboxes
					.get_mut_by_id(&id)
					.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
				state.status = tg::sandbox::Status::Destroyed;
				state.changed.send_replace(());
				state.sandbox.take();
				(
					state.data(),
					state.process_ids.keys().cloned().collect::<Vec<_>>(),
				)
			};
			drop(sandbox);

			// Register the sandbox with its owner before indexing or reporting destruction.
			connected.await?;
			self.index_remote_sandbox(
				&id,
				&location,
				self.server.clock.unix_timestamp()?,
				Some(&data),
				Some(&processes),
			)
			.await?;

			let request_id = crate::control::id();
			let request =
				tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
					arg: tg::sandbox::control::ClientRequestArg::Destroy(
						tg::sandbox::control::DestroyClientRequestArg { data, processes },
					),
					id: request_id.clone(),
				});
			let response = sender
				.request(request, crate::control::Priority::High)
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

			crate::checkpoint!(
				self.server,
				"runner.sandbox.destroyed",
				sandbox = %id,
			)
			.await;
			event_sender.send(Ok(Event::Destroyed)).ok();
			Ok::<_, tg::Error>(())
		};
		let mut destroy = destroy.boxed();
		loop {
			tokio::select! {
				result = &mut destroy => {
					result?;
					break;
				},
				message = control.recv() => {
					if let Some(message) = message? {
						self.handle_destroyed_sandbox_control_request(&id, message).await?;
					}
				},
			}
		}
		drop(destroy);

		let output = RetainSandboxTaskArg {
			control,
			id,
			process_tasks,
			stopper,
		};

		Ok(output)
	}

	async fn retain_sandbox_task(&self, arg: RetainSandboxTaskArg) -> tg::Result<()> {
		let RetainSandboxTaskArg {
			mut control,
			id,
			mut process_tasks,
			stopper,
		} = arg;

		// Await the process tasks while they retain their state and control streams.
		while !process_tasks.is_empty() {
			tokio::select! {
				result = process_tasks.join_next() => {
					result.unwrap()
						.map_err(|error| tg::error!(!error, "a process task panicked"))?
						.map_err(|error| tg::error!(!error, "a process task failed"))?;
				},
				message = control.recv() => {
					if let Some(message) = message? {
						self.handle_destroyed_sandbox_control_request(&id, message).await?;
					}
				},
			}
		}

		// Retain the sandbox state and control stream.
		let retention_ttl = self.server.config.runner.sandbox_state_ttl;
		let retention_future = tokio::time::sleep(retention_ttl);
		let mut retention_future = pin!(retention_future);
		loop {
			tokio::select! {
				() = &mut retention_future => break,
				() = stopper.wait() => break,
				message = control.recv() => {
					let message = message
						.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
					if let Some(message) = message {
						self.handle_destroyed_sandbox_control_request(&id, message).await?;
					}
				},
			}
		}

		Ok(())
	}

	pub(super) async fn create_sandbox_control_connection(
		&self,
	) -> tg::Result<SandboxControlConnection> {
		let location = self.server.config.runner.remote.as_ref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|name| {
				tg::Location::Remote(tg::location::Remote {
					name: name.clone(),
					region: None,
				})
			},
		);
		let runner = self
			.server
			.runner
			.state
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let (input, input_receiver) = tokio::sync::mpsc::channel(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let arg = tg::sandbox::control::Arg {
			create: false,
			created_at: None,
			data: None,
			id: None,
			location: Some(location.into()),
			runner: Some(runner),
		};
		let (output, requests) = self.connect_sandbox_control(arg, input_stream).await?;
		let token = output.token.ok_or_else(
			|| tg::error!(id = %output.id, "missing the sandbox authentication token"),
		)?;
		let control = crate::control::Stream::new_reconnecting(
			requests,
			input,
			crate::control::stream_options(),
		);
		let connection = SandboxControlConnection {
			control,
			id: output.id,
			token,
		};

		Ok(connection)
	}

	async fn get_sandbox_control_stream(
		&self,
		id: Option<&tg::sandbox::Id>,
		location: &tg::Location,
		created_at: i64,
		data: tg::sandbox::control::Data,
		input_stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<ConnectedSandboxControl> {
		let runner = self
			.server
			.runner
			.state
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let arg = tg::sandbox::control::Arg {
			create: true,
			created_at: Some(created_at),
			data: Some(data),
			id: id.cloned(),
			location: Some(location.clone().into()),
			runner: Some(runner),
		};
		let (output, control) = self.connect_sandbox_control(arg, input_stream).await?;
		if let Some(id) = id
			&& output.id != *id
		{
			return Err(
				tg::error!(actual = %output.id, expected = %id, "the server returned an invalid sandbox"),
			);
		}
		output
			.token
			.or_else(|| id.and(self.context.token.clone()))
			.ok_or_else(
				|| tg::error!(id = %output.id, "missing the sandbox authentication token"),
			)?;
		self.index_remote_sandbox(&output.id, location, created_at, None, None)
			.await?;
		let connection = ConnectedSandboxControl { requests: control };

		Ok(connection)
	}

	async fn connect_sandbox_control(
		&self,
		arg: tg::sandbox::control::Arg,
		input: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	)> {
		let id = arg.id.clone();
		crate::checkpoint!(self.server, "runner.sandbox.control.connect", sandbox = ?id).await;
		let reconnect_context = self.context.clone();
		let reconnect_server = self.server.clone();
		let reconnect = move |output: &tg::sandbox::control::Output| {
			let token = output.token.clone().or(reconnect_context.token.clone());
			let context = Context {
				principal: tg::Principal::Sandbox(output.id.clone()),
				token,
				..reconnect_context
			};

			reconnect_server.session(&context)
		};
		let (output, control) = self
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

		Ok((output, control.boxed()))
	}

	async fn index_remote_sandbox(
		&self,
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
		let touched_at = self.server.clock.unix_timestamp()?;
		let sandbox = tangram_index::sandbox::put::Arg {
			account: None,
			created_at,
			data,
			id: id.clone(),
			location: Some(location.clone()),
			processes: processes.map(<[_]>::to_vec),
			runner: None,
			touched_at,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(sandbox)],
		};
		self.server
			.index_batch(arg)
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

fn wait_for_process_connection(
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
