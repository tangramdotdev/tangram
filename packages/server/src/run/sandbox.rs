use {
	crate::{SandboxPermit, Server, context::Context, temp::Temp},
	futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{path::Path, pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
};

impl Server {
	pub(crate) fn spawn_sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		location: tg::Location,
		permit: SandboxPermit,
		process: Option<tg::process::Id>,
	) {
		if self.sandbox_tasks.try_get_id(id).is_some() {
			return;
		}
		self.sandbox_tasks
			.spawn(id.clone(), |_| {
				let server = self.clone();
				let id = id.clone();
				async move { server.sandbox_task(&id, location, permit, process).await }
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the sandbox task failed");
					})
					.map(|_| ())
			})
			.detach();
	}

	async fn sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		location: tg::Location,
		permit: SandboxPermit,
		process: Option<tg::process::Id>,
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
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox"))?;
		let Some(state) = state else {
			return Ok(());
		};
		if state.status.is_finished() {
			return Ok(());
		}
		let isolation = self.resolve_sandbox_isolation()?;

		// Associate the permit with the sandbox.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.sandbox_permits.insert(id.clone(), permit);
		scopeguard::defer! {
			self.sandbox_permits.remove(id);
		}

		// Create the temp.
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Create the listener.
		let (listener, guest_uri) = Self::run_create_listener(temp.path(), &isolation)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to create the tangram listener"))?;

		// Create the sandbox. Include the artifacts directory as a readonly mount.
		let artifacts_path = self.artifacts_path();
		let mut mounts = state.mounts.clone();
		mounts.push(tg::sandbox::Mount {
			source: artifacts_path.clone(),
			target: artifacts_path.clone(),
			readonly: true,
		});
		let (host_ip, guest_ip) = match &isolation {
			tangram_sandbox::Isolation::Container(container)
				if matches!(container.net, tangram_sandbox::Net::Bridge(_)) =>
			{
				(None, Some(self.allocate_guest_ip()?))
			},
			tangram_sandbox::Isolation::Container(_) => (
				Some(self.allocate_guest_ip()?),
				Some(self.allocate_guest_ip()?),
			),
			_ => (None, None),
		};
		let arg = tangram_sandbox::Arg {
			artifacts_path,
			cpu: state.cpu,
			host_ip: host_ip.as_ref().map(|ip| ip.addr),
			guest_ip: guest_ip.as_ref().map(|ip| ip.addr),
			hostname: state.hostname.clone(),
			id: id.clone(),
			isolation,
			memory: state.memory,
			mounts,
			network: state.network,
			path: temp.path().to_owned(),
			rootfs_path: self.sandbox_rootfs.clone(),
			tangram_path: self.tangram_path.clone(),
			user: state.user.clone(),
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to create the sandbox"))?;

		// TODO: starting here, async move. Capture all state.
		self.sandboxes.insert(id.clone(), sandbox.clone());
		scopeguard::defer! {
			self.sandboxes.remove(id);
			drop(host_ip);
			drop(guest_ip);
		}

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = self.clone();
			let config = crate::config::HttpListener {
				url: guest_uri.clone(),
				tls: None,
			};
			let context = Context {
				sandbox: Some(id.clone()),
				..Default::default()
			};
			|stop| async move {
				server.serve(listener, config, context, stop).await;
			}
		});

		// Spawn the heartbeat task.
		let heartbeat_task = Task::spawn({
			let server = self.clone();
			let id = id.clone();
			let location = location.clone();
			move |stopper| async move { server.sandbox_heartbeat_task(&id, &location, stopper).await }
		});

		let status = self
			.get_sandbox_status(
				id,
				tg::sandbox::status::Arg {
					location: Some(location.clone().into()),
				},
			)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the sandbox status stream"),
			)?;
		let mut status = pin!(status);

		let process_stopper = Stopper::new();
		let mut process_tasks = JoinSet::new();

		let ttl = (state.ttl != i64::MAX as u64).then(|| Duration::from_secs(state.ttl));
		let mut timer = None;

		if let Some(process) = process {
			self.spawn_sandbox_process_task(
				&mut process_tasks,
				&process_stopper,
				process.clone(),
				&location,
				&sandbox,
				&guest_uri,
			);
		} else if let Some(ttl) = ttl {
			timer.replace(tokio::time::sleep(ttl).boxed());
		}

		loop {
			let timer_future = timer.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer| timer.as_mut().right_future(),
			);
			tokio::select! {
				output = self.dequeue_sandbox_process(id, &location) => {
					let output = output.map_err(|source| tg::error!(!source, "failed to dequeue a process"))?;
					timer.take();
					self.spawn_sandbox_process_task(
						&mut process_tasks,
						&process_stopper,
						output.process.clone(),
						&location,
						&sandbox,
						&guest_uri,
					);
				},
				result = status.try_next() => {
					let option = result.map_err(|source| tg::error!(!source, "failed to read the sandbox status"))?;
					let Some(status) = option else {
						break;
					};
					if status.is_finished() {
						break;
					}
				},
				_ = process_tasks.join_next(), if !process_tasks.is_empty() => {
					if process_tasks.is_empty() && let Some(ttl) = ttl {
						timer.replace(tokio::time::sleep(ttl).boxed());
					}
				},
				() = timer_future => {
					let arg = tg::sandbox::finish::Arg {
						location: Some(location.clone().into()),
					};
					self.finish_sandbox(id, arg).await.ok();
					timer.take();
				},
			}
		}

		process_stopper.stop();
		while process_tasks.join_next().await.is_some() {}

		serve_task.stop();
		serve_task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the serve task panicked"))?;

		heartbeat_task.stop();
		heartbeat_task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the heartbeat task panicked"))?
			.map_err(|source| tg::error!(!source, "the heartbeat task failed"))?;

		self.finish_unfinished_processes_in_sandbox(
			id,
			&location,
			tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the process was canceled".into()),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, %id, "failed to finish unfinished processes"))?;

		Ok(())
	}

	async fn dequeue_sandbox_process(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::location::Location,
	) -> tg::Result<tg::sandbox::process::queue::Output> {
		loop {
			let arg = tg::sandbox::process::queue::Arg {
				location: Some(location.clone().into()),
			};
			match self.try_dequeue_sandbox_process(id, arg).await {
				Ok(Some(output)) => return Ok(output),
				Ok(None) => (),
				Err(error) => {
					tracing::trace!(error = %error.trace(), sandbox = %id, ?location, "failed to dequeue a process");
				},
			}
		}
	}

	async fn run_create_listener(
		root_path: &Path,
		isolation: &tangram_sandbox::Isolation,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		#[cfg(target_os = "linux")]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) => {
					Self::run_create_unix_listener(root_path).await
				},
				tangram_sandbox::Isolation::Seatbelt(_) => {
					Err(tg::error!("seatbelt isolation is not supported on linux"))
				},

				tangram_sandbox::Isolation::Vm(_) => {
					#[cfg(not(feature = "vsock"))]
					{
						Err(tg::error!("vsock is not enabled"))
					}
					#[cfg(feature = "vsock")]
					{
						let port = 8476;
						let socket = format!(
							"{}_{port}",
							tangram_sandbox::vm::run::CLOUD_HYPERVISOR_VSOCK_SOCKET_NAME
						);
						let path = root_path.join("vm").join(socket);
						tokio::fs::create_dir_all(path.parent().unwrap())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to create the vm directory")
							})?;
						let url = tangram_uri::Uri::builder()
							.scheme("http+unix")
							.authority(&path.display().to_string())
							.path("")
							.build()
							.map_err(|source| {
								tg::error!(source = source, "failed to build the socket URL")
							})?;
						let listener = Server::listen(&url).await.map_err(
							|source| tg::error!(!source, url = %url, "failed to listen"),
						)?;
						let guest_uri = format!(
							"http+vsock://{}:{port}",
							tangram_sandbox::vm::VMADDR_CID_HOST
						)
						.parse::<tangram_uri::Uri>()
						.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?;
						Ok((listener, guest_uri))
					}
				},
			}
		}

		#[cfg(not(target_os = "linux"))]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) => {
					Err(tg::error!("container isolation is not supported on macos"))
				},
				tangram_sandbox::Isolation::Seatbelt(_) => {
					Self::run_create_unix_listener(root_path).await
				},
				tangram_sandbox::Isolation::Vm(_) => {
					Err(tg::error!("vm isolation is not supported on macos"))
				},
			}
		}
	}

	async fn run_create_unix_listener(
		root_path: &Path,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		let host_socket_path =
			tangram_sandbox::Sandbox::host_tangram_socket_path_from_root(root_path);
		let guest_socket_path =
			tangram_sandbox::Sandbox::guest_tangram_socket_path_from_root(root_path);
		let max_socket_path_len = if cfg!(target_os = "macos") {
			100
		} else {
			usize::MAX
		};
		let host_socket_path_string = host_socket_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid socket path"))?;
		let guest_socket_path_string = guest_socket_path
			.to_str()
			.ok_or_else(|| tg::error!("invalid socket path"))?;

		tokio::fs::create_dir_all(host_socket_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
		let url = if host_socket_path_string.len() <= max_socket_path_len {
			tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(host_socket_path_string)
				.path("")
				.build()
				.map_err(|source| tg::error!(source = source, "failed to build the socket URL"))?
		} else {
			"http://localhost:0"
				.parse::<tangram_uri::Uri>()
				.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
		};
		let listener = Server::listen(&url)
			.await
			.map_err(|source| tg::error!(!source, "failed to listen"))?;
		let guest_uri = match &listener {
			crate::http::Listener::Unix(_) => tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(guest_socket_path_string)
				.path("")
				.build()
				.map_err(|source| tg::error!(source = source, "failed to build the guest URL"))?,
			crate::http::Listener::Tcp(tcp) => {
				let port = tcp
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the listener address"))?
					.port();
				format!("http://localhost:{port}")
					.parse::<tangram_uri::Uri>()
					.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
			},
			#[cfg(feature = "vsock")]
			crate::http::Listener::Vsock(_vsock) => return Err(tg::error!("unreachable")),
		};
		Ok((listener, guest_uri))
	}

	async fn sandbox_heartbeat_task(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::location::Location,
		stopper: Stopper,
	) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::sandbox::heartbeat::Arg {
				location: Some(location.clone().into()),
			};
			let result = self.heartbeat_sandbox(id, arg).await;
			if let Ok(output) = result
				&& output.status.is_finished()
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

	fn spawn_sandbox_process_task(
		&self,
		process_tasks: &mut JoinSet<tg::Result<()>>,
		process_stopper: &Stopper,
		process: tg::process::Id,
		location: &tg::location::Location,
		sandbox: &tangram_sandbox::Sandbox,
		guest_uri: &tangram_uri::Uri,
	) {
		let server = self.clone();
		let process = tg::Process::new(
			process,
			Some(location.clone().into()),
			None,
			None,
			None,
			None,
		);
		let sandbox = sandbox.clone();
		let guest_uri = guest_uri.clone();
		let stopper = process_stopper.clone();
		process_tasks.spawn(async move {
			server
				.run_process_task(&process, sandbox, guest_uri, stopper)
				.await
				.inspect_err(|error| {
					tracing::error!(error = %error.trace(), process = %process.id(), "the process task failed");
				})
		});
	}
}
