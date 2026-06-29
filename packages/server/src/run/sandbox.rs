use {
	super::process::SpawnProcessTaskArg,
	crate::{Context, Server, Session, temp::Temp},
	dashmap::DashMap,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_index::prelude::*,
	tokio::task::JoinSet,
};

mod listener;

pub(crate) struct SpawnSandboxTaskArg {
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
	pub permit: crate::sandbox::Permit,
	pub process: Option<tg::process::Id>,
	pub process_token: Option<String>,
	pub token: Option<String>,
}

struct CreateProcessSessionOutput {
	process: tg::Process,
	process_token: String,
	session: Session,
}

#[derive(Clone, Debug)]
enum CachedSandboxResponse {
	Pending,
	Ready(tg::sandbox::control::SpawnProcessClientResponse),
}

impl Server {
	pub(crate) fn spawn_sandbox_task(&self, task: SpawnSandboxTaskArg) {
		self.sandbox_tasks
			.spawn(task.id.clone(), |_| {
				let server = self.clone();
				let id = task.id;
				async move {
					// Create the session.
					let context = Context {
						..server.context.clone()
					};
					let context = Context {
						principal: tg::Principal::Sandbox(id.clone()),
						..context
					};
					let session = server.session(&context);

					// Run the sandbox task.
					let result = session
						.sandbox_task(
							&id,
							task.location.clone(),
							task.permit,
							task.process,
							task.token.clone(),
							task.process_token,
						)
						.boxed()
						.await;

					// If the sandbox task fails, then destroy the sandbox with an error.
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
						let mut error = error.to_data_or_id();
						if !session.server.config.advanced.internal_error_locations
							&& let tg::Either::Left(error) = &mut error
						{
							error.remove_internal_locations();
						}
						let arg = tg::sandbox::destroy::Arg {
							error: Some(error),
							location: Some(task.location.into()),
						};
						let result = session.destroy_sandbox(&id, arg).await;
						if let Err(error) = result {
							tracing::error!(
								error = %error.trace(),
								sandbox = %id,
								"failed to destroy the sandbox after the sandbox failed"
							);
						}
					}
				}
			})
			.detach();
	}
}

impl Session {
	async fn sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		location: tg::Location,
		permit: crate::sandbox::Permit,
		process: Option<tg::process::Id>,
		token: Option<String>,
		process_token: Option<String>,
	) -> tg::Result<()> {
		// Get the sandbox.
		let state = self
			.try_get_sandbox(
				id,
				tg::sandbox::get::Arg {
					location: Some(location.clone().into()),
				},
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
		let Some(state) = state else {
			return Ok(());
		};
		if state.status.is_destroyed() {
			return Ok(());
		}

		let isolation = match &state.isolation {
			Some(tg::sandbox::Isolation::Container) => {
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation::default())
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
						kernel_path,
						max_cpu: vm.max_cpu,
						max_memory: vm.max_memory,
						image_path,
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
		if let tangram_sandbox::Isolation::Vm(vm) = &isolation
			&& let Some(snapshot_path) = vm.snapshot.as_deref()
		{
			let mut image_created = self.server.sandbox_vm_image_lock.lock().await;
			if !*image_created {
				let _file_lock = acquire_vm_lock(&self.server.path).await?;
				let arg = tangram_sandbox::vm::image::Arg {
					image_path: vm.image_path.clone(),
					path: self.server.sandbox_container_root.clone(),
					tangram_path: self.server.tangram_path.clone(),
				};
				let created =
					tokio::task::spawn_blocking(move || tangram_sandbox::vm::image::ensure(&arg))
						.await
						.map_err(|error| tg::error!(!error, "the vm image task panicked"))??;
				if created {
					std::fs::remove_dir_all(snapshot_path).ok();
					std::fs::remove_file(snapshot_path).ok();
				}
				*image_created = true;
			}
			self.server
				.ensure_vm_snapshot(snapshot_path, &vm.kernel_path, vm)
				.await?;
		}

		let rootfs_path = match &isolation {
			tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Vm(_) => {
				self.server.sandbox_container_root.clone()
			},
			tangram_sandbox::Isolation::Seatbelt(_) => self.server.sandbox_seatbelt_root.clone(),
		};

		// Associate the permit with the sandbox.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.server.sandbox_permits.insert(id.clone(), permit);
		scopeguard::defer! {
			self.server.sandbox_permits.remove(id);
		}

		// Create the temp.
		let temp = Temp::new(&self.server);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Start the per-sandbox virtiofsd serving artifacts. Kept as a local so the daemon
		// stops and the socket file is removed when the sandbox task ends.
		#[cfg(target_os = "linux")]
		let _vfs = if let tangram_sandbox::Isolation::Vm(vm) = &isolation {
			let socket = temp.path().join("vfs.sock");
			let options = self.server.config.vfs.unwrap_or_default();
			let vfs = crate::vfs::Server::start_virtiofs(&self.server, options, &socket, vm.dax)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to start the artifacts vfs"))?;
			Some(vfs)
		} else {
			None
		};

		// Create the listener.
		let (listener, guest_url, tangram_socket_path) =
			Server::run_create_listener(temp.path(), &isolation)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to create the tangram listener"),
				)?;

		// Create the sandbox. Include the artifacts directory as a readonly mount.
		let artifacts_path = self.server.artifacts_path();
		let mut mounts = state.mounts.clone();
		mounts.push(tg::sandbox::Mount {
			readonly: true,
			source: artifacts_path.clone(),
			target: artifacts_path.clone(),
		});
		let network = match state.network.clone() {
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
			artifacts_path,
			cpu: state.cpu,
			creator: state.creator.clone(),
			dns: self.server.config.sandbox.network.dns.clone(),
			#[cfg(target_os = "linux")]
			firewall: match self.server.config.sandbox.network.firewall {
				crate::config::SandboxNetworkFirewall::Iptables => {
					tangram_sandbox::Firewall::Iptables
				},
				crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
			},
			hostname: state.hostname.clone(),
			id: id.clone(),
			identity: self.server.path.clone(),
			#[cfg(target_os = "linux")]
			ip_pool: self.server.ip_pool.clone(),
			isolation,
			location: location.clone(),
			memory: state.memory,
			mounts,
			network,
			nice: self.server.config.sandbox.nice,
			owner: state.owner.clone(),
			path: temp.path().to_owned(),
			rootfs_path,
			tangram_path: self.server.tangram_path.clone(),
			tangram_socket_path,
			token: token.clone(),
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to create the sandbox"))?;

		self.server.sandboxes.insert(id.clone(), sandbox.clone());
		scopeguard::defer! {
			self.server.sandboxes.remove(id);
		}

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = self.server.clone();
			let listener_config = crate::config::HttpListener {
				tls: None,
				url: guest_url.clone(),
			};
			move |stopper| async move {
				server.serve(listener, listener_config, true, stopper).await;
			}
		});

		// Spawn the heartbeat task.
		let heartbeat_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let location = location.clone();
			move |stopper| async move {
				session
					.sandbox_heartbeat_task(&id, &location, stopper)
					.await
			}
		});

		let arg = tg::sandbox::status::Arg {
			location: Some(location.clone().into()),
			timeout: None,
		};
		let status = self
			.get_sandbox_status(id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox status stream"))?;
		let mut status = pin!(status);

		// Create the process tasks.
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();

		// Create the timer.
		let mut timer = None;
		let ttl = state.ttl;

		// Connect to the sandbox control stream before registering the sandbox's placement so
		// processes can be routed to it.
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::sandbox::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let arg = tg::sandbox::control::Arg {
			location: Some(location.clone().into()),
		};
		let control = self
			.get_sandbox_control_stream_all(id, arg, input_stream)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to connect to the sandbox control stream"),
			)?;
		let mut control = pin!(control);
		self.register_sandbox_with_scheduler(id).await;
		let control_ttl = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(Duration::from_mins(1), |runner| runner.control_ttl);
		let responses: Arc<DashMap<String, CachedSandboxResponse>> = Arc::new(DashMap::new());

		// If the sandbox was created with a process, then start it. Otherwise, start the timer. A
		// failure to start the process does not tear down the sandbox.
		match process {
			Some(process) => {
				match self
					.create_process_session(
						process,
						&location,
						id,
						state.creator.clone(),
						process_token,
					)
					.await
				{
					Ok(output) => {
						output.session.spawn_process_task(SpawnProcessTaskArg {
							guest_url: &guest_url,
							process: output.process,
							process_token: output.process_token,
							process_stopper: &process_stopper,
							process_tasks: &mut process_tasks,
							sandbox: &sandbox,
						});
					},
					Err(error) => {
						tracing::error!(error = %error.trace(), %id, "failed to start the process");
						if let Some(ttl) = ttl {
							timer.replace(tokio::time::sleep(ttl).boxed());
						}
					},
				}
			},
			None => {
				if let Some(ttl) = ttl {
					timer.replace(tokio::time::sleep(ttl).boxed());
				}
			},
		}

		let mut destroy = false;
		loop {
			let timer_future = timer.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer| timer.as_mut().right_future(),
			);
			tokio::select! {
				// If a process is dispatched to the sandbox, then start it and report whether it
				// started. A failure to start does not tear down the sandbox.
				message = control.try_next() => {
					let message = message
						.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
					let Some(message) = message else {
						break;
					};
					let request = match message {
						tg::sandbox::control::ServerMessage::Request(request) => request,
						tg::sandbox::control::ServerMessage::Ack(ack) => {
								responses.insert(ack.id.clone(), CachedSandboxResponse::Pending);
								tokio::spawn({
									let responses = responses.clone();
									async move {
										tokio::time::sleep(control_ttl).await;
										responses.remove(&ack.id);
									}
								});
								continue;
						}
					};
					let request_id = request.id().to_owned();

					// Duplicate; resend the cached response, or drop if in flight or acked.
					let cached = responses
						.get(&request_id)
						.map(|entry| entry.value().clone());
						if let Some(cached) = cached {
							if let CachedSandboxResponse::Ready(response) = cached {
								input
									.send(tg::sandbox::control::ClientMessage::Response(
										tg::sandbox::control::ClientResponse::SpawnProcess(response),
									))
									.await
									.ok();
							}
						continue;
					}

					// Mark in flight so duplicates are dropped until the response is ready.
					responses.insert(request_id.clone(), CachedSandboxResponse::Pending);

					let tg::sandbox::control::ServerRequest::SpawnProcess(request) = request;

					timer.take();
					match self
						.create_process_session(
							request.process.clone(),
							&location,
							id,
							state.creator.clone(),
							request.process_token.clone(),
						)
						.await
					{
						Ok(output) => {
							output.session.spawn_process_task(SpawnProcessTaskArg {
								guest_url: &guest_url,
								process: output.process,
								process_token: output.process_token,
								process_stopper: &process_stopper,
								process_tasks: &mut process_tasks,
								sandbox: &sandbox,
							});
							let response = tg::sandbox::control::SpawnProcessClientResponse {
								id: request_id.clone(),
								process: request.process,
								spawned: true,
							};
							responses.insert(request_id, CachedSandboxResponse::Ready(response.clone()));
							input
								.send(tg::sandbox::control::ClientMessage::Response(
									tg::sandbox::control::ClientResponse::SpawnProcess(response),
								))
								.await
								.ok();
						},
						Err(error) => {
							tracing::error!(error = %error.trace(), %id, "failed to start the dispatched process");
							let response = tg::sandbox::control::SpawnProcessClientResponse {
								id: request_id.clone(),
								process: request.process,
								spawned: false,
							};
							responses.insert(request_id, CachedSandboxResponse::Ready(response.clone()));
							input
								.send(tg::sandbox::control::ClientMessage::Response(
									tg::sandbox::control::ClientResponse::SpawnProcess(response),
								))
								.await
								.ok();
							if process_tasks.is_empty()
								&& let Some(ttl) = ttl
							{
								timer.replace(tokio::time::sleep(ttl).boxed());
							}
						},
					}
				},

				// If the sandbox is destroyed or its status cannot be read, then break.
				result = status.try_next() => {
					let option = match result {
						Ok(option) => option,
						Err(error) => {
							tracing::error!(error = %error.trace(), %id, "failed to read the sandbox status");
							break;
						},
					};
					let Some(status) = option else {
						break;
					};
					if status.is_destroyed() {
						break;
					}
				},

				// If a process finishes and there are no processes, then start the timer. A process
				// task panic or failure is logged so that it does not tear down the sandbox.
				output = process_tasks.join_next(), if !process_tasks.is_empty() => {
					match output.unwrap() {
						Ok(Ok(())) => (),
						Ok(Err(error)) => {
							tracing::error!(error = %error.trace(), %id, "a process task failed");
						},
						Err(error) => {
							tracing::error!(%error, %id, "a process task panicked");
						},
					}
					if process_tasks.is_empty() && let Some(ttl) = ttl {
						timer.replace(tokio::time::sleep(ttl).boxed());
					}
				},

				// If the timer fires, then break and destroy the sandbox.
				() = timer_future => {
					destroy = true;
					break;
				},
			}
		}

		// Deregister the sandbox's placement with the scheduler.
		self.deregister_sandbox_with_scheduler(id).await;

		// Destroy the sandbox.
		if destroy {
			let arg = tg::sandbox::destroy::Arg {
				error: None,
				location: Some(location.clone().into()),
			};
			self.destroy_sandbox(id, arg).await?;
		}

		// Stop and await the process tasks.
		process_stopper.stop();
		while let Some(result) = process_tasks.join_next().await {
			result
				.map_err(|error| tg::error!(!error, "a process task panicked"))?
				.map_err(|error| tg::error!(!error, "a process task failed"))?;
		}

		// Stop and await the serve task.
		serve_task.stop();
		serve_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

		// Stop and await the heartbeat task.
		heartbeat_task.stop();
		heartbeat_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the heartbeat task panicked"))?
			.map_err(|error| tg::error!(!error, "the heartbeat task failed"))?;

		Ok(())
	}

	async fn create_process_session(
		&self,
		id: tg::process::Id,
		location: &tg::Location,
		sandbox: &tg::sandbox::Id,
		_creator: Option<tg::Principal>,
		token: Option<String>,
	) -> tg::Result<CreateProcessSessionOutput> {
		let process = tg::Process::new(
			id.clone(),
			tg::process::Options {
				location: Some(location.clone().into()),
				..Default::default()
			},
		);
		let token = match token {
			Some(token) => token,
			None => self.server.create_process_token(&id).await.map_err(
				|error| tg::error!(!error, process = %id, "failed to create the process token"),
			)?,
		};
		let state = process
			.load_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, process = %id, "failed to load the process"))?;
		if state.sandbox != *sandbox {
			return Err(tg::error!(
				process = %id,
				sandbox = %sandbox,
				"the process is not in the sandbox"
			));
		}
		let mut context = self.context.clone();
		context.principal = tg::Principal::Process(id.clone());
		let session = Session::new(self.server.clone(), context);
		session
			.spawn_grant_process_command_task(&process, &id, location)
			.await?;
		Ok(CreateProcessSessionOutput {
			process,
			process_token: token,
			session,
		})
	}

	async fn spawn_grant_process_command_task(
		&self,
		process: &tg::Process,
		id: &tg::process::Id,
		location: &tg::Location,
	) -> tg::Result<()> {
		if !location.is_remote() {
			return Ok(());
		}

		let command = process
			.command_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command"))?;

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now + time_to_live;
		let principal = tg::grant::Principal::Process(id.clone());
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		let permissions = tg::grant::permission::Set::from_permission(permission);
		let put_grant = tangram_index::grant::put::Arg {
			created_at: now,
			creator: Some(tg::Principal::Process(id.clone())),
			expires_at: Some(expires_at),
			permissions,
			principal,
			resource: command.id().into(),
		};

		self.server
			.index_tasks
			.spawn(|_| {
				let server = self.server.clone();
				async move {
					let arg = tangram_index::batch::Arg {
						put_grants: vec![put_grant],
						..Default::default()
					};
					let result = server.index.batch(arg).await;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to grant the process command");
					}
				}
			})
			.detach();

		Ok(())
	}

	async fn register_sandbox_with_scheduler(&self, id: &tg::sandbox::Id) {
		let message_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
		let message = tg::runner::control::ClientMessage::Notification(
			tg::runner::control::ClientNotification::SandboxCreated(
				tg::runner::control::SandboxCreatedClientNotification {
					id: message_id.clone(),
					sandbox: id.clone(),
				},
			),
		);
		self.server
			.runner_lifecycle_messages
			.insert(message_id, message.clone());
		let sender = self.server.runner_input.lock().unwrap().clone();
		if let Some(sender) = sender {
			sender.send(message).await.ok();
		}
	}

	async fn deregister_sandbox_with_scheduler(&self, id: &tg::sandbox::Id) {
		let created = self
			.server
			.runner_lifecycle_messages
			.iter()
			.filter_map(|entry| match entry.value() {
				tg::runner::control::ClientMessage::Notification(
					tg::runner::control::ClientNotification::SandboxCreated(message),
				) if message.sandbox == *id => Some(entry.key().clone()),
				_ => None,
			})
			.collect::<Vec<_>>();
		for message in created {
			self.server.runner_lifecycle_messages.remove(&message);
		}

		let message_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());
		let message = tg::runner::control::ClientMessage::Notification(
			tg::runner::control::ClientNotification::SandboxDestroyed(
				tg::runner::control::SandboxDestroyedClientNotification {
					id: message_id.clone(),
					sandbox: id.clone(),
				},
			),
		);
		self.server
			.runner_lifecycle_messages
			.insert(message_id, message.clone());
		let sender = self.server.runner_input.lock().unwrap().clone();
		if let Some(sender) = sender {
			sender.send(message).await.ok();
		}
	}

	async fn sandbox_heartbeat_task(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::Location,
		stopper: Stopper,
	) -> tg::Result<()> {
		let config = self.server.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::sandbox::heartbeat::Arg {
				location: Some(location.clone().into()),
			};
			let result = self.heartbeat_sandbox(id, arg).await;
			if let Ok(output) = result
				&& output.status.is_destroyed()
			{
				break;
			}
			let sleep = tokio::time::sleep(config.heartbeat_interval);
			match future::select(pin!(sleep), pin!(stopper.wait())).await {
				future::Either::Left(_) => (),
				future::Either::Right(_) => break,
			}
		}
		Ok(())
	}
}

#[cfg(target_os = "linux")]
impl Server {
	async fn ensure_vm_snapshot(
		&self,
		snapshot_path: &std::path::Path,
		kernel_path: &std::path::Path,
		vm: &tangram_sandbox::VmIsolation,
	) -> tg::Result<()> {
		if snapshot_path.exists() {
			return Ok(());
		}
		let _guard = self.sandbox_vm_snapshot_lock.lock().await;
		if snapshot_path.exists() {
			return Ok(());
		}
		let _file_lock = acquire_vm_lock(&self.path).await?;
		if snapshot_path.exists() {
			return Ok(());
		}
		if let Some(parent) = snapshot_path.parent() {
			tokio::fs::create_dir_all(parent).await.map_err(|error| {
				tg::error!(!error, path = %parent.display(), "failed to create the vm snapshot parent directory")
			})?;
		}
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to create the vm snapshot temp directory")
			})?;
		let _vfs = {
			let socket = temp.path().join("vfs.sock");
			let options = self.config.vfs.unwrap_or_default();
			crate::vfs::Server::start_virtiofs(self, options, &socket, vm.dax)
				.await
				.map_err(|error| tg::error!(!error, "failed to start the artifacts vfs"))?
		};
		let image_path = self.sandbox_vm_image.as_ref().ok_or_else(|| {
			tg::error!(
				"cannot create the vm snapshot without an image; ensure vm isolation is configured"
			)
		})?;
		let snapshot_id = tg::sandbox::Id::new();
		tracing::info!(
			snapshot = %snapshot_path.display(),
			sandbox = %snapshot_id,
			"creating vm snapshot",
		);
		let firewall = match self.config.sandbox.network.firewall {
			crate::config::SandboxNetworkFirewall::Iptables => tangram_sandbox::Firewall::Iptables,
			crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
		};
		let mut command = tokio::process::Command::new(&self.tangram_path);
		command
			.arg("sandbox")
			.arg("vm")
			.arg("run")
			.arg("--create-snapshot")
			.arg(snapshot_path)
			.arg("--id")
			.arg(snapshot_id.to_string())
			.arg("--artifacts-path")
			.arg(self.artifacts_path())
			.arg("--firewall")
			.arg(firewall.to_string())
			.arg("--kernel-path")
			.arg(kernel_path)
			.arg("--max-cpu")
			.arg(vm.max_cpu.to_string())
			.arg("--max-memory")
			.arg(vm.max_memory.to_string())
			.arg("--rootfs-path")
			.arg(&self.sandbox_container_root)
			.arg("--image-path")
			.arg(image_path)
			.arg("--snapshot-cpu")
			.arg(vm.snapshot_cpu.to_string())
			.arg("--snapshot-memory")
			.arg(vm.snapshot_memory.to_string())
			.arg("--tangram-path")
			.arg(&self.tangram_path)
			.arg("--path")
			.arg(temp.path())
			.arg("--url")
			.arg("http+vsock://2:6748");
		if let Some(dax) = vm.dax {
			command.arg("--dax").arg(dax.to_string());
		}
		if let Some(path) = &vm.cloud_hypervisor_path {
			command.arg("--cloud-hypervisor-path").arg(path);
		}
		let status = command
			.status()
			.await
			.map_err(|error| tg::error!(!error, "failed to spawn the snapshot process"))?;
		if !status.success() {
			return Err(tg::error!(
				%status,
				snapshot = %snapshot_path.display(),
				"the snapshot process exited with a non-zero status",
			));
		}
		tracing::info!(
			snapshot = %snapshot_path.display(),
			"vm snapshot created",
		);
		Ok(())
	}
}

#[cfg(target_os = "linux")]
async fn acquire_vm_lock(data_dir: &std::path::Path) -> tg::Result<std::fs::File> {
	let lock_path = data_dir.join(".tangram/vm.lock");
	tokio::task::spawn_blocking(move || -> tg::Result<std::fs::File> {
		if let Some(parent) = lock_path.parent() {
			std::fs::create_dir_all(parent).map_err(
				|error| tg::error!(!error, path = %parent.display(), "failed to create the vm lock parent"),
			)?;
		}
		let lock = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(&lock_path)
			.map_err(
				|error| tg::error!(!error, path = %lock_path.display(), "failed to open the vm lock"),
			)?;
		let ret = unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to acquire the vm lock"));
		}
		Ok(lock)
	})
	.await
	.map_err(|error| tg::error!(!error, "the vm lock task panicked"))?
}
