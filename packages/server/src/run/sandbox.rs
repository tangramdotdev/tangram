use {
	crate::{SandboxPermit, Server, context::Context, run::ProcessTaskMap, temp::Temp},
	futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{path::Path, pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
};

impl Server {
	pub(crate) fn spawn_sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		remote: Option<String>,
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
				async move { server.sandbox_task(&id, remote, permit, process).await }
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
		remote: Option<String>,
		permit: SandboxPermit,
		process: Option<tg::process::Id>,
	) -> tg::Result<()> {
		// Get the sandbox.
		let remotes = remote.as_ref().map(|remote| vec![remote.clone()]);
		let state = self
			.try_get_sandbox(
				id,
				tg::sandbox::get::Arg {
					local: None,
					remotes: remotes.clone(),
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
		let (listener, guest_uri) = Self::run_create_listener(temp.path())
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
		let sandbox_arg = tangram_sandbox::SpawnArg {
			artifacts_path,
			hostname: state.hostname.clone(),
			mounts,
			network: state.network,
			path: temp.path().to_owned(),
			rootfs_path: self.sandbox_rootfs.clone(),
			tangram_path: self.tangram_path.clone(),
			user: state.user.clone(),
		};
		let sandbox = tangram_sandbox::Sandbox::new(sandbox_arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to create the sandbox"))?;
		self.sandboxes.insert(id.clone(), sandbox.clone());
		scopeguard::defer! {
			self.sandboxes.remove(id);
		}

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let server = self.clone();
			let context = Context {
				sandbox: Some(id.clone()),
				..Default::default()
			};
			|stop| async move {
				server.serve(listener, context, stop).await;
			}
		});

		// Spawn the heartbeat task.
		let heartbeat_task = Task::spawn({
			let server = self.clone();
			let id = id.clone();
			let remote = remote.clone();
			move |stopper| async move {
				server
					.sandbox_heartbeat_task(&id, remote.as_deref(), stopper)
					.await
			}
		});

		let status = self
			.get_sandbox_status(
				id,
				tg::sandbox::status::Arg {
					local: None,
					remotes: remotes.clone(),
				},
			)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the sandbox status stream"),
			)?;
		let mut status = pin!(status);

		let process_tasks = ProcessTaskMap::default();
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<tg::process::Id>();

		let ttl = (state.ttl != i64::MAX as u64).then(|| Duration::from_secs(state.ttl));
		let mut timer = None;

		if let Some(process) = process {
			self.spawn_sandbox_process_task(
				&process_tasks,
				&sender,
				process.clone(),
				remote.as_ref(),
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
				output = self.dequeue_sandbox_process(id, remote.as_deref()) => {
					let output = output.map_err(|source| tg::error!(!source, "failed to dequeue a process"))?;
					timer.take();
					self.spawn_sandbox_process_task(
						&process_tasks,
						&sender,
						output.process.clone(),
						remote.as_ref(),
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
				id = receiver.recv() => {
					let Some(_) = id else {
						break;
					};
					if process_tasks.is_empty() && let Some(ttl) = ttl {
						timer.replace(tokio::time::sleep(ttl).boxed());
					}
				},
				() = timer_future => {
					let arg = tg::sandbox::finish::Arg {
						local: None,
						remotes: remotes.clone(),
					};
					self.finish_sandbox(id, arg).await.ok();
					timer.take();
				},
			}
		}

		process_tasks.stop_all();
		drop(sender);
		process_tasks.wait().await;

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
			remote.as_deref(),
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
		remote: Option<&str>,
	) -> tg::Result<tg::sandbox::process::queue::Output> {
		loop {
			let arg = tg::sandbox::process::queue::Arg {
				local: None,
				remotes: remote.map(|remote| vec![remote.to_owned()]),
			};
			match self.try_dequeue_sandbox_process(id, arg).await {
				Ok(Some(output)) => return Ok(output),
				Ok(None) => (),
				Err(error) => {
					tracing::trace!(error = %error.trace(), sandbox = %id, remote = ?remote, "failed to dequeue a process");
				},
			}
		}
	}

	async fn run_create_listener(
		root_path: &Path,
	) -> tg::Result<(crate::http::Listener, tangram_uri::Uri)> {
		let host_socket_path = if cfg!(target_os = "linux") {
			root_path.join("upper/opt/tangram/socket")
		} else {
			root_path.join("tg")
		};
		let guest_socket_path = if cfg!(target_os = "linux") {
			Path::new("/opt/tangram/socket").to_owned()
		} else {
			host_socket_path.clone()
		};
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
			crate::http::Listener::Vsock(vsock) => {
				let addr = vsock
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get the listener address"))?;
				format!("http+vsock://{}:{}", addr.cid(), addr.port())
					.parse::<tangram_uri::Uri>()
					.map_err(|source| tg::error!(source = source, "failed to parse the URL"))?
			},
		};
		Ok((listener, guest_uri))
	}

	async fn sandbox_heartbeat_task(
		&self,
		id: &tg::sandbox::Id,
		remote: Option<&str>,
		stopper: Stopper,
	) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::sandbox::heartbeat::Arg {
				local: None,
				remotes: remote.map(|remote| vec![remote.to_owned()]),
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
		process_tasks: &ProcessTaskMap,
		sender: &tokio::sync::mpsc::UnboundedSender<tg::process::Id>,
		process: tg::process::Id,
		remote: Option<&String>,
		sandbox: &tangram_sandbox::Sandbox,
		guest_uri: &tangram_uri::Uri,
	) {
		let server = self.clone();
		let sender = sender.clone();
		let process = tg::Process::new(process, None, remote.cloned(), None, None, None);
		let sandbox = sandbox.clone();
		let guest_uri = guest_uri.clone();
		process_tasks
			.spawn(process.id().clone(), move |stopper| async move {
				let process_id = process.id().clone();
				let _guard =
					scopeguard::guard((sender, process_id.clone()), |(sender, process_id)| {
						sender.send(process_id).ok();
					});
				server
					.run_process_task(&process, sandbox, guest_uri, stopper)
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), process = %process.id(), "the process task failed");
					})
			})
			.detach();
	}
}
