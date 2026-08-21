use {
	super::process::{
		ConnectedEvent, Event as ProcessEvent, SpawnProcessTaskArg, SpawnProcessTaskOutput,
	},
	crate::{Context, Origin, Server, Session, temp::Temp},
	futures::{FutureExt as _, StreamExt as _, future},
	std::{collections::BTreeMap, pin::pin, sync::Arc, time::Instant},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
	tokio_stream::{StreamMap, wrappers::UnboundedReceiverStream},
};

#[cfg(target_os = "linux")]
mod linux;
mod listener;
mod pool;

pub(super) use self::pool::Pool;

type SandboxControlSender = crate::control::Sender<
	tg::sandbox::control::ServerMessage,
	tg::sandbox::control::ClientMessage,
>;

pub(crate) struct SpawnSandboxTaskArg {
	pub allocation: crate::runner::capacity::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub command_push_task_arg: Option<crate::process::CommandPushTaskArg>,
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
	command_push_task_arg: Option<crate::process::CommandPushTaskArg>,
	creator: Option<tg::Principal>,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: Option<tg::sandbox::Id>,
	location: tg::Location,
	process: Option<tg::runner::control::Process>,
	stopper: Stopper,
	token: Option<String>,
}

struct CreateSandboxArg {
	arg: tg::sandbox::create::Arg,
	expected_id: Option<tg::sandbox::Id>,
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
	allocation: crate::runner::capacity::Allocation,
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	create_output: CreateSandboxOutput,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	sandbox_id_sender: tokio::sync::oneshot::Sender<tg::sandbox::Id>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
	token: String,
}

struct RunSandboxTaskArg {
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	guest_url: tangram_uri::Uri,
	id: tg::sandbox::Id,
	location: tg::Location,
	process_stopper: Stopper,
	process_task_output: Option<SpawnProcessTaskOutput>,
	process_tasks: JoinSet<tg::Result<()>>,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	started_at: Instant,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RetainSandboxTaskArg {
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	id: tg::sandbox::Id,
	process_tasks: JoinSet<tg::Result<()>>,
	sender: SandboxControlSender,
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
					command_push_task_arg: arg.command_push_task_arg,
					creator: arg.creator,
					event_sender: event_sender.clone(),
					id: arg.id,
					location: arg.location,
					process: arg.process,
					stopper,
					token: arg.token,
				};
				let result = session.sandbox_task(arg).boxed().await;
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
	#[must_use]
	fn sandbox_control_response(
		id: String,
		result: tg::Result<tg::sandbox::control::ClientResponseOutput>,
	) -> tg::sandbox::control::ClientMessage {
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
		tg::sandbox::control::ClientMessage::Response(tg::sandbox::control::ClientResponse {
			error,
			id,
			output,
		})
	}

	async fn handle_destroyed_sandbox_control_request(
		&self,
		id: &tg::sandbox::Id,
		request: tg::sandbox::control::ServerRequest,
		sender: &SandboxControlSender,
	) -> tg::Result<()> {
		let result = match request.arg {
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
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => {
				Err(tg::error!(%id, "the sandbox was destroyed"))
			},
		};
		let response = Self::sandbox_control_response(request.id, result);
		sender
			.send(response)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the sandbox control response"))?;
		Ok(())
	}

	async fn sandbox_task(&self, arg: SandboxTaskArg) -> tg::Result<()> {
		let SandboxTaskArg {
			allocation,
			arg,
			command_push_task_arg,
			creator,
			event_sender,
			id: expected_id,
			location,
			process,
			stopper,
			token,
		} = arg;
		let context = match (&expected_id, &token) {
			(Some(id), Some(token)) => Context {
				principal: tg::Principal::Sandbox(id.clone()),
				token: Some(token.clone()),
				..self.context.clone()
			},
			(None, None) => {
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
			},
			_ => {
				return Err(tg::error!(
					"the sandbox id and token must be provided together"
				));
			},
		};
		let connection_session = self.server.session(&context);

		// Create the sandbox concurrently with its control stream.
		let create_future = self.create_sandbox_with_pool(CreateSandboxArg {
			arg: arg.clone(),
			expected_id: expected_id.clone(),
		});
		let created_at = self.server.clock.unix_timestamp()?;
		let data = tg::sandbox::control::Data {
			arg: arg.clone(),
			creator: creator.clone(),
		};
		let connect_future = {
			let expected_id = expected_id.clone();
			let location = location.clone();
			async move {
				connection_session
					.get_sandbox_control_stream(expected_id.as_ref(), &location, created_at, data)
					.await
			}
		};
		let mut create_future = pin!(create_future);
		let mut connect_future = pin!(connect_future);
		let mut connection = None;
		let create_output = loop {
			tokio::select! {
				result = &mut create_future => break result?,
				result = &mut connect_future, if connection.is_none() => {
					connection = Some(result?);
				},
			}
		};

		// Spawn the process before waiting for the control stream.
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();
		let (sandbox_id_sender, sandbox_id_receiver) = tokio::sync::oneshot::channel();
		let process_task_output = process.map(|process| {
			self.spawn_process_task(SpawnProcessTaskArg {
				command_push_task_arg,
				guest_url: &create_output.guest_url,
				location: location.clone(),
				process,
				process_stopper: &process_stopper,
				process_tasks: &mut process_tasks,
				retention_stopper: stopper.clone(),
				sandbox: &create_output.sandbox,
				sandbox_id_receiver: Some(sandbox_id_receiver),
			})
		});

		let connection = match connection {
			Some(connection) => Ok(connection),
			None => connect_future.await,
		};
		let connection = connection.and_then(|(output, control)| {
			if let Some(expected_id) = &expected_id
				&& output.id != *expected_id
			{
				return Err(tg::error!(
					actual = %output.id,
					expected = %expected_id,
					"the server returned an invalid sandbox"
				));
			}
			let id = output.id;
			let token = output
				.token
				.or(token)
				.ok_or_else(|| tg::error!(%id, "missing the sandbox authentication token"))?;

			Ok((control, id, token))
		});
		let (control, id, token) = match connection {
			Ok(connection) => connection,
			Err(error) => {
				drop(sandbox_id_sender);
				process_stopper.stop();
				while process_tasks.join_next().await.is_some() {}

				return Err(error);
			},
		};
		let state = tg::sandbox::get::Output {
			cpu: arg.cpu,
			creator,
			hostname: arg.hostname,
			id: id.clone(),
			isolation: arg.isolation,
			location: Some(location.clone()),
			memory: arg.memory,
			mounts: arg.mounts,
			network: arg.network,
			owner: arg.owner,
			status: tg::sandbox::Status::Started,
			tokens: tg::authorization::Tokens::default(),
			ttl: arg.ttl,
			usage: None,
		};
		let context = Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: Some(token.clone()),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let result = session
			.sandbox_task_inner(SandboxTaskInnerArg {
				allocation,
				control,
				create_output,
				event_sender,
				id: id.clone(),
				location: location.clone(),
				process_stopper,
				process_task_output,
				process_tasks,
				sandbox_id_sender,
				state,
				stopper,
				token,
			})
			.boxed()
			.await;
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
			if let Err(error) = session.destroy_sandbox(&id, arg).boxed().await {
				tracing::error!(
					error = %error.trace(),
					sandbox = %id,
					"failed to destroy the sandbox after the sandbox failed"
				);
			}
		}

		result
	}

	async fn create_sandbox_with_pool(
		&self,
		arg: CreateSandboxArg,
	) -> tg::Result<CreateSandboxOutput> {
		let expected_id = arg.expected_id.clone();
		if let Some(task) = self.server.runner.sandbox_pool.take(&arg.arg, self) {
			match task.wait().await {
				Ok(Ok(output)) => {
					tracing::debug!(?expected_id, "claimed a sandbox from the pool");

					return Ok(output);
				},
				Ok(Err(error)) => {
					tracing::warn!(
						error = %error.trace(),
						?expected_id,
						"failed to claim a sandbox from the pool; falling back to cold creation",
					);
				},
				Err(error) => {
					tracing::warn!(
						?error,
						?expected_id,
						"the sandbox pool task panicked; falling back to cold creation",
					);
				},
			}
		}

		self.create_sandbox_inner(arg).await
	}

	async fn create_sandbox_inner(&self, arg: CreateSandboxArg) -> tg::Result<CreateSandboxOutput> {
		let CreateSandboxArg { arg, expected_id } = arg;

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
				.map_err(|error| {
					tg::error!(!error, ?expected_id, "failed to start the store VFS")
				})?;
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
				.map_err(|error| {
					tg::error!(
						!error,
						?expected_id,
						"failed to create the sandbox listener"
					)
				})?;

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
			.map_err(|error| tg::error!(!error, ?expected_id, "failed to create the sandbox"))?;

		// Wait for the per-sandbox VFS, which finishes starting once the sandbox mounts the filesystem and sends the FUSE descriptor.
		#[cfg(target_os = "linux")]
		let vfs = match vfs_task {
			None => vfs,
			Some(vfs_task) => {
				let vfs = vfs_task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the VFS startup task panicked"))?
					.map_err(|error| {
						tg::error!(!error, ?expected_id, "failed to start the store VFS")
					})?;
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
			allocation,
			control,
			create_output,
			event_sender,
			id,
			location,
			process_stopper,
			process_task_output,
			process_tasks,
			sandbox_id_sender,
			state,
			stopper,
			token,
		} = arg;
		let CreateSandboxOutput {
			guest_url,
			sandbox,
			serve_task,
			temp,
			#[cfg(target_os = "linux")]
			mut vfs,
			#[cfg(target_os = "linux")]
			vfs_principal,
		} = create_output;

		let started_at = Instant::now();
		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		let index = sandbox.index();
		self.server.runner.state.sandboxes.insert(
			index,
			id.clone(),
			crate::sandbox::State {
				allocation: Some(allocation),
				command_push_tasks: BTreeMap::new(),
				data: state.clone(),
				processes: crate::process::Processes::default(),
				sandbox: Some(sandbox.clone()),
				token: Some(token.clone()),
				tokens: BTreeMap::new(),
			},
		);
		scopeguard::defer! {
			self.server.runner.state.sandboxes.remove(index);
		}

		// Bind the per-sandbox VFS before releasing the process task.
		#[cfg(target_os = "linux")]
		if let Some(vfs_principal) = &vfs_principal {
			vfs_principal
				.lock()
				.unwrap()
				.replace(tg::Principal::Sandbox(id.clone()));
		}

		sandbox_id_sender.send(id.clone()).ok();
		let arg = RunSandboxTaskArg {
			control,
			event_sender,
			guest_url,
			id: id.clone(),
			location,
			process_stopper,
			process_task_output,
			process_tasks,
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

		self.retain_sandbox_task(arg).await
	}

	async fn run_sandbox_task(
		&self,
		arg: RunSandboxTaskArg,
		#[cfg(target_os = "linux")] vfs: &mut Option<crate::vfs::Server>,
	) -> tg::Result<RetainSandboxTaskArg> {
		let RunSandboxTaskArg {
			mut control,
			event_sender,
			guest_url,
			id,
			location,
			process_stopper,
			process_task_output,
			mut process_tasks,
			sandbox,
			serve_task,
			started_at,
			state,
			stopper,
		} = arg;

		let sender = control.sender();

		// Create the process events.
		let mut process_events = StreamMap::new();

		// Create the timer.
		let mut timer_future = None;
		let reusable = process_task_output.is_none();
		let ttl = state.ttl;

		let connected_event = if let Some(process_task_output) = process_task_output {
			let mut events = process_task_output.events;
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!(%id, "the process event sender was dropped"))??;
			let ProcessEvent::Connected(connected_event) = event else {
				return Err(tg::error!(%id, "expected the process connected event"));
			};
			process_events.insert(
				connected_event.process.clone(),
				UnboundedReceiverStream::new(events),
			);
			Some(connected_event)
		} else if let Some(ttl) = ttl {
			timer_future.replace(tokio::time::sleep(ttl).boxed());
			None
		} else {
			None
		};
		event_sender
			.send(Ok(Event::Ready(ReadyEvent {
				connected_event,
				sandbox: id.clone(),
			})))
			.ok();

		loop {
			let current_timer_future = timer_future.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer_future| timer_future.as_mut().right_future(),
			);
			tokio::select! {
				message = control.recv() => {
					// Get the message.
					let message = message
						.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
					let Some(message) = message else {
						break;
					};
					let request = match message {
						tg::sandbox::control::ServerMessage::Request(request) => request,
						tg::sandbox::control::ServerMessage::Ack(_)
						| tg::sandbox::control::ServerMessage::Response(_) => unreachable!(),
						tg::sandbox::control::ServerMessage::Notification(notification) => match notification {},
					};
					let request_id = request.id;
					let mut destroy = false;
					let result = match request.arg {
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
						tg::sandbox::control::ServerRequestArg::SpawnProcess(request) => {
							timer_future.take();

							// Spawn the process task.
							let process = Self::prepare_process(request.process, &id)?;
							let task = self.spawn_process_task(SpawnProcessTaskArg {
								command_push_task_arg: None,
								guest_url: &guest_url,
								location: location.clone(),
								process,
								process_stopper: &process_stopper,
								process_tasks: &mut process_tasks,
								retention_stopper: stopper.clone(),
								sandbox: &sandbox,
								sandbox_id_receiver: None,
							});
							let mut events = task.events;
							let event = events
								.recv()
								.await
								.ok_or_else(|| {
									tg::error!(%id, "the process event sender was dropped")
								})??;
							let ProcessEvent::Connected(connected_event) = event else {
								return Err(tg::error!(%id, "expected the process connected event"));
							};
							process_events.insert(
								connected_event.process.clone(),
								UnboundedReceiverStream::new(events),
							);
							let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
								grant: connected_event.grant,
								lease: connected_event.lease,
								process: connected_event.process,
							};
							Ok(tg::sandbox::control::ClientResponseOutput::SpawnProcess(output))
						},
					};
					let message = Self::sandbox_control_response(request_id, result);
					sender
						.send(message)
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
							if process_events.is_empty() {
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
					output
						.unwrap()
						.map_err(|error| tg::error!(!error, "a process task panicked"))?
						.map_err(|error| tg::error!(!error, "a process task failed"))?;
				},

				// If the timer fires, then break and destroy the sandbox.
				() = current_timer_future => {
					break;
				},
			}
		}

		// Stop and await the underlying processes.
		process_stopper.stop();
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
		state.data.usage = Some(tg::sandbox::get::Usage {
			cpu: usage.cpu,
			memory: usage.memory,
		});
		drop(state);

		// Stop and await the serve task.
		serve_task.stop();
		serve_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

		// Stop the VFS while the sandbox still owns its mount namespace.
		#[cfg(target_os = "linux")]
		if let Some(vfs) = vfs.take() {
			vfs.stop();
			vfs.wait().await;
		}

		// Destroy the sandbox while retaining its process and control state.
		sandbox
			.destroy()
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox process"))?;

		let data = {
			let mut state = self
				.server
				.runner
				.state
				.sandboxes
				.get_mut_by_id(&id)
				.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
			state.data.status = tg::sandbox::Status::Destroyed;
			state.sandbox.take();
			state.data.clone()
		};
		drop(sandbox);

		let request_id = crate::control::id();
		let request =
			tg::sandbox::control::ClientMessage::Request(tg::sandbox::control::ClientRequest {
				arg: tg::sandbox::control::ClientRequestArg::Destroy(
					tg::sandbox::control::DestroyClientRequestArg { data },
				),
				id: request_id.clone(),
			});
		sender.send(request).await.map_err(
			|error| tg::error!(!error, %id, "failed to send the destroy sandbox request"),
		)?;
		loop {
			let message = control
				.recv()
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to receive a sandbox control message"),
				)?
				.ok_or_else(|| tg::error!(%id, "the sandbox control stream ended"))?;
			match message {
				tg::sandbox::control::ServerMessage::Response(response)
					if response.id == request_id =>
				{
					if let Some(error) = response.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the error")
						})?;
						return Err(tg::error!(!error, "the destroy sandbox request failed"));
					}
					let output = response
						.output
						.ok_or_else(|| tg::error!("missing destroy sandbox response output"))?;
					output
						.try_unwrap_destroy()
						.map_err(|_| tg::error!("expected a destroy sandbox response"))?;
					break;
				},
				tg::sandbox::control::ServerMessage::Request(request) => {
					self.handle_destroyed_sandbox_control_request(&id, request, &sender)
						.await?;
				},
				tg::sandbox::control::ServerMessage::Ack(_) => unreachable!(),
				tg::sandbox::control::ServerMessage::Notification(notification) => {
					match notification {}
				},
				tg::sandbox::control::ServerMessage::Response(_) => {},
			}
		}
		crate::checkpoint!(
			self.server,
			"runner.sandbox.destroyed",
			sandbox = %id,
		)
		.await;
		event_sender.send(Ok(Event::Destroyed)).ok();

		let output = RetainSandboxTaskArg {
			control,
			id,
			process_tasks,
			sender,
			stopper,
		};

		Ok(output)
	}

	async fn retain_sandbox_task(&self, arg: RetainSandboxTaskArg) -> tg::Result<()> {
		let RetainSandboxTaskArg {
			mut control,
			id,
			mut process_tasks,
			sender,
			stopper,
		} = arg;

		// Await the process tasks while they retain their state and control streams.
		while let Some(result) = process_tasks.join_next().await {
			result
				.map_err(|error| tg::error!(!error, "a process task panicked"))?
				.map_err(|error| tg::error!(!error, "a process task failed"))?;
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
					let Some(message) = message else {
						retention_future.await;
						break;
					};
					match message {
						tg::sandbox::control::ServerMessage::Request(request) => {
							self.handle_destroyed_sandbox_control_request(&id, request, &sender)
								.await?;
						},
						tg::sandbox::control::ServerMessage::Ack(_) => unreachable!(),
						tg::sandbox::control::ServerMessage::Notification(notification) => match notification {},
						tg::sandbox::control::ServerMessage::Response(_) => {},
					}
				},
			}
		}

		Ok(())
	}

	async fn get_sandbox_control_stream(
		&self,
		id: Option<&tg::sandbox::Id>,
		location: &tg::Location,
		created_at: i64,
		data: tg::sandbox::control::Data,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		crate::control::Stream<
			tg::sandbox::control::ServerMessage,
			tg::sandbox::control::ClientMessage,
		>,
	)> {
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::sandbox::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let runner = self
			.server
			.runner
			.state
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let arg = tg::sandbox::control::Arg {
			created_at: Some(created_at),
			data: Some(data),
			id: id.cloned(),
			location: Some(location.clone().into()),
			runner: Some(runner),
		};
		let (output, control) = self
			.get_sandbox_control_stream_all(arg, input_stream)
			.boxed()
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					?id,
					"failed to connect to the sandbox control stream"
				)
			})?;
		let control =
			crate::control::Stream::new(control.boxed(), input, crate::control::stream_options());
		Ok((output, control))
	}

	fn prepare_process(
		mut arg: tg::runner::control::Process,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<tg::runner::control::Process> {
		if arg.id.is_none() {
			arg.data.sandbox = sandbox.clone();
		} else if arg.data.sandbox != *sandbox {
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
