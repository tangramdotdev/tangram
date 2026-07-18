use {
	super::process::{ConnectedEvent, Event as ProcessEvent, SpawnProcessTaskArg},
	crate::{Context, Server, Session, temp::Temp},
	futures::{FutureExt as _, StreamExt as _, future},
	std::{collections::HashMap, pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
	tokio_stream::{StreamMap, wrappers::UnboundedReceiverStream},
};

#[cfg(target_os = "linux")]
mod linux;
mod listener;

type SandboxControlSender = crate::control::Sender<
	tg::sandbox::control::ServerMessage,
	tg::sandbox::control::ClientMessage,
>;

pub(crate) struct SpawnSandboxTaskArg {
	pub allocation: crate::runner::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub identity: Option<SandboxIdentity>,
	pub location: tg::Location,
	pub process: Option<tg::runner::control::Process>,
}

#[must_use]
pub(crate) struct SpawnSandboxTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SandboxIdentity {
	pub id: tg::sandbox::Id,
	pub token: String,
}

struct SandboxTaskArg {
	allocation: crate::runner::Allocation,
	arg: tg::sandbox::create::Arg,
	creator: Option<tg::Principal>,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	identity: Option<SandboxIdentity>,
	location: tg::Location,
	process: Option<tg::runner::control::Process>,
	stopper: Stopper,
}

pub(crate) enum Event {
	Destroy,
	Start(StartedEvent),
}

#[derive(Clone, Debug)]
pub(crate) struct StartedEvent {
	pub process: Option<ConnectedEvent>,
	pub sandbox: tg::sandbox::Id,
}

struct SandboxTaskInnerArg {
	allocation: crate::runner::Allocation,
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	data: tg::sandbox::get::Output,
	event_sender: tokio::sync::mpsc::UnboundedSender<tg::Result<Event>>,
	id: tg::sandbox::Id,
	location: tg::Location,
	process: Option<tg::runner::control::Process>,
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
	process: Option<tg::runner::control::Process>,
	sandbox: tangram_sandbox::Sandbox,
	serve_task: Task<()>,
	state: tg::sandbox::get::Output,
	stopper: Stopper,
}

struct RetainSandboxTaskArg {
	control: crate::control::Stream<
		tg::sandbox::control::ServerMessage,
		tg::sandbox::control::ClientMessage,
	>,
	id: tg::sandbox::Id,
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
					creator: arg.creator,
					event_sender: event_sender.clone(),
					identity: arg.identity,
					location: arg.location,
					process: arg.process,
					stopper,
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
			creator,
			event_sender,
			identity,
			location,
			process,
			stopper,
		} = arg;
		let context = if let Some(identity) = &identity {
			Context {
				principal: tg::Principal::Sandbox(identity.id.clone()),
				token: Some(identity.token.clone()),
				..self.context.clone()
			}
		} else {
			let runner = self
				.server
				.runner
				.state
				.id()
				.ok_or_else(|| tg::error!("missing the runner id"))?;
			let token = self
				.server
				.config
				.runner
				.as_ref()
				.and_then(|runner| runner.token.clone());
			Context {
				principal: tg::Principal::Runner(runner),
				token,
				..self.context.clone()
			}
		};
		let connection_session = self.server.session(&context);
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let data = tg::sandbox::control::Data {
			arg: arg.clone(),
			creator: creator.clone(),
		};
		let (output, control) = connection_session
			.get_sandbox_control_stream(identity.as_ref(), &location, created_at, data)
			.await?;
		if let Some(identity) = &identity
			&& output.id != identity.id
		{
			return Err(tg::error!(
				actual = %output.id,
				expected = %identity.id,
				"the server returned an invalid sandbox"
			));
		}
		let id = output.id;
		let token = output
			.token
			.or_else(|| identity.map(|identity| identity.token))
			.ok_or_else(|| tg::error!(%id, "missing the sandbox authentication token"))?;
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
			ttl: arg.ttl,
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
				data: state,
				event_sender,
				id: id.clone(),
				location: location.clone(),
				process,
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

	async fn sandbox_task_inner(&self, arg: SandboxTaskInnerArg) -> tg::Result<()> {
		let SandboxTaskInnerArg {
			allocation,
			control,
			data: state,
			event_sender,
			id,
			location,
			process,
			stopper,
			token,
		} = arg;

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

		// Start the per-sandbox vfs if necessary.
		#[cfg(target_os = "linux")]
		let _vfs = if let tangram_sandbox::Isolation::Vm(vm) = &isolation {
			let socket = temp.path().join("vfs.sock");
			let vfs = crate::vfs::Server::start_virtiofs(&self.server, &socket, vm.dax)
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
			token: Some(token.clone()),
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to create the sandbox"))?;

		let allocation = Arc::new(tokio::sync::Mutex::new(Some(allocation)));
		self.server.runner.state.sandboxes.insert(
			id.clone(),
			crate::sandbox::State {
				allocation: Some(allocation),
				data: state.clone(),
				processes: HashMap::new(),
				sandbox: Some(sandbox.clone()),
				token: Some(token.clone()),
			},
		);
		scopeguard::defer! {
			self.server.runner.state.sandboxes.remove(&id);
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
		let arg = RunSandboxTaskArg {
			control,
			event_sender,
			guest_url,
			id: id.clone(),
			location,
			process,
			sandbox,
			serve_task,
			state,
			stopper,
		};

		self.run_sandbox_task(arg).boxed().await
	}

	async fn run_sandbox_task(&self, arg: RunSandboxTaskArg) -> tg::Result<()> {
		let RunSandboxTaskArg {
			mut control,
			event_sender,
			guest_url,
			id,
			location,
			process,
			sandbox,
			serve_task,
			state,
			stopper,
		} = arg;

		let sender = control.sender();

		// Create the process tasks.
		let mut process_events = StreamMap::new();
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();

		// Create the timer.
		let mut timer = None;
		let ttl = state.ttl;

		let process = if let Some(process) = process {
			let process = Self::prepare_process(process, &id)?;
			let task = self.spawn_process_task(SpawnProcessTaskArg {
				guest_url: &guest_url,
				location: location.clone(),
				process,
				process_stopper: &process_stopper,
				process_tasks: &mut process_tasks,
				retention_stopper: stopper.clone(),
				sandbox: &sandbox,
			});
			let mut events = task.events;
			let event = events
				.recv()
				.await
				.ok_or_else(|| tg::error!(%id, "the process event sender was dropped"))??;
			let ProcessEvent::Connect(event) = event else {
				return Err(tg::error!(%id, "expected the process connect event"));
			};
			process_events.insert(event.process.clone(), UnboundedReceiverStream::new(events));
			Some(event)
		} else if let Some(ttl) = ttl {
			timer.replace(tokio::time::sleep(ttl).boxed());
			None
		} else {
			None
		};
		event_sender
			.send(Ok(Event::Start(StartedEvent {
				process,
				sandbox: id.clone(),
			})))
			.ok();

		loop {
			let timer_future = timer.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer| timer.as_mut().right_future(),
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
							let mut sandbox = self
								.server
								.runner
								.state
								.sandboxes
								.get_mut(&id)
								.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
							for process in sandbox.processes.values_mut() {
								if !process.data.status.is_finished() {
									process.finish.get_or_insert(
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
							timer.take();

							// Spawn the process task.
							let process = Self::prepare_process(request.process, &id)?;
							let task = self.spawn_process_task(SpawnProcessTaskArg {
								guest_url: &guest_url,
								location: location.clone(),
								process,
								process_stopper: &process_stopper,
								process_tasks: &mut process_tasks,
								retention_stopper: stopper.clone(),
								sandbox: &sandbox,
							});
							let mut events = task.events;
							let event = events
								.recv()
								.await
								.ok_or_else(|| {
									tg::error!(%id, "the process event sender was dropped")
								})??;
							let ProcessEvent::Connect(event) = event else {
								return Err(tg::error!(%id, "expected the process connect event"));
							};
							process_events.insert(
								event.process.clone(),
								UnboundedReceiverStream::new(events),
							);
							let output = tg::sandbox::control::SpawnProcessClientResponseOutput {
								grant: event.grant,
								lease: event.lease,
								process: event.process,
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

				// Destroy the sandbox when its last underlying process exits.
				event = process_events.next(), if !process_events.is_empty() => {
					let Some((process, event)) = event else {
						break;
					};
					match event? {
						ProcessEvent::Connect(_) => {
							return Err(tg::error!(%process, "received a duplicate process connect event"));
						},
						ProcessEvent::Exit => {
							process_events.remove(&process);
							if process_events.is_empty() {
								break;
							}
						},
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
				() = timer_future => {
					break;
				},
			}
		}

		// Stop and await the underlying processes.
		process_stopper.stop();
		while let Some((process, event)) = process_events.next().await {
			match event? {
				ProcessEvent::Connect(_) => {
					return Err(tg::error!(%process, "received a duplicate process connect event"));
				},
				ProcessEvent::Exit => {
					process_events.remove(&process);
				},
			}
		}

		// Release the sandbox's capacity once all of its underlying processes have exited.
		if let Some(mut state) = self.server.runner.state.sandboxes.get_mut(&id) {
			state.allocation.take();
		}

		// Stop and await the serve task.
		serve_task.stop();
		serve_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

		let data = {
			let mut state = self
				.server
				.runner
				.state
				.sandboxes
				.get_mut(&id)
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
		event_sender.send(Ok(Event::Destroy)).ok();

		// Await the process tasks while they retain their state and control streams.
		while let Some(result) = process_tasks.join_next().await {
			result
				.map_err(|error| tg::error!(!error, "a process task panicked"))?
				.map_err(|error| tg::error!(!error, "a process task failed"))?;
		}
		let arg = RetainSandboxTaskArg {
			control,
			id,
			sender,
			stopper,
		};

		self.retain_sandbox_task(arg).await
	}

	async fn retain_sandbox_task(&self, arg: RetainSandboxTaskArg) -> tg::Result<()> {
		let RetainSandboxTaskArg {
			mut control,
			id,
			sender,
			stopper,
		} = arg;

		let retention_ttl = self
			.server
			.config
			.runner
			.as_ref()
			.unwrap()
			.sandbox_state_ttl;
		let retention = tokio::time::sleep(retention_ttl);
		let mut retention = pin!(retention);
		loop {
			tokio::select! {
				() = &mut retention => break,
				() = stopper.wait() => break,
				message = control.recv() => {
					let message = message
						.map_err(|error| tg::error!(!error, %id, "failed to receive a sandbox control message"))?;
					let Some(message) = message else {
						retention.await;
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
		identity: Option<&SandboxIdentity>,
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
			id: identity.map(|identity| identity.id.clone()),
			location: Some(location.clone().into()),
			runner: Some(runner),
		};
		let (output, control) = self
			.get_sandbox_control_stream_all(arg, input_stream)
			.boxed()
			.await
			.map_err(|error| {
				let id = identity.map(|identity| &identity.id);
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
		if arg.identity.is_none() {
			arg.data.sandbox = sandbox.clone();
		} else if arg.data.sandbox != *sandbox {
			let process = arg.identity.as_ref().map(|identity| &identity.id);
			return Err(tg::error!(
				?process,
				sandbox = %sandbox,
				"the process is not in the sandbox"
			));
		}
		Ok(arg)
	}
}
