use {
	crate::{
		ProcessPermit, Server,
		context::{Context, PathMap},
		temp::Temp,
	},
	futures::{FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::Path,
		pin::pin,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::TryExt as _, task::Task},
	tokio::io::AsyncReadExt as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::ReaderStream,
};

mod progress;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub value: Option<tg::Value>,
}

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			// Wait for a permit.
			let permit = self
				.process_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = ProcessPermit(tg::Either::Left(permit));

			// Try to dequeue a process locally or from one of the remotes.
			let arg = tg::process::queue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_process(arg)
					.map_ok(|output| tg::Process::new(output.process, None, None, None, None))
					.boxed(),
			)
			.chain(self.config.runner.iter().flat_map(|config| {
				config.remotes.iter().map(|name| {
					let server = self.clone();
					let remote = name.to_owned();
					async move {
						let client = server.get_remote_client(remote).await?;
						let arg = tg::process::queue::Arg::default();
						let output = client.dequeue_process(arg).await?;
						let process =
							tg::Process::new(output.process, None, Some(name.clone()), None, None);
						Ok::<_, tg::Error>(process)
					}
					.boxed()
				})
			}));

			let process = match future::select_ok(futures).await {
				Ok((process, _)) => process,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to dequeue a process");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Spawn the process task.
			self.spawn_process_task(&process, permit);
		}
	}

	pub(crate) fn spawn_process_task(&self, process: &tg::Process, permit: ProcessPermit) {
		self.process_tasks
			.spawn(process.id().clone(), |_| {
				let server = self.clone();
				let process = process.clone();
				async move { server.process_task(&process, permit).await }
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the process task failed");
					})
					.map(|_| ())
			})
			.detach();
	}

	async fn process_task(&self, process: &tg::Process, permit: ProcessPermit) -> tg::Result<()> {
		// Spawn the heartbeat task.
		tokio::spawn({
			let server = self.clone();
			let process = process.clone();
			async move { server.heartbeat_task(&process).await }
				.inspect_err(|error| {
					tracing::error!(error = %error.trace(), "the heartbeat task failed");
				})
				.map(|_| ())
		});

		// Acquire a clean guard.
		let clean_guard = self.acquire_clean_guard().await;

		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.id().clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(process.id());
			drop(clean_guard);
		}

		// Run.
		let wait = match self.run(process).await.map_err(
			|source| tg::error!(!source, process = %process.id(), "failed to run the process"),
		) {
			Ok(output) => output,
			Err(error) => Output {
				error: Some(error),
				checksum: None,
				exit: 1,
				value: None,
			},
		};

		// Store the output.
		let value = if let Some(value) = &wait.value {
			value
				.store(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the output"))?;
			let data = value.to_data();
			Some(data)
		} else {
			None
		};

		// Get the error.
		let error = if let Some(error) = &wait.error {
			match error.to_data_or_id() {
				tg::Either::Left(mut data) => {
					if !self.config.advanced.internal_error_locations {
						data.remove_internal_locations();
					}
					let object = tg::error::Object::try_from_data(data)?;
					let error = tg::Error::with_object(object);
					let id = error.store(self).await?;
					Some(tg::Either::Right(id))
				},
				tg::Either::Right(id) => Some(tg::Either::Right(id)),
			}
		} else {
			None
		};

		// If the process is remote, then push the output.
		if let Some(remote) = process.remote()
			&& let Some(value) = &value
		{
			let mut objects = BTreeSet::new();
			value.children(&mut objects);
			if !objects.is_empty() {
				let arg = tg::push::Arg {
					items: objects.into_iter().map(tg::Either::Left).collect(),
					remote: Some(remote.to_owned()),
					..Default::default()
				};
				let stream = self
					.push(arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to push the output"))?;
				self.write_progress_stream(process, stream)
					.await
					.map_err(|source| tg::error!(!source, "failed to log the progress stream"))?;
			}
		}

		// Finish the process.
		let arg = tg::process::finish::Arg {
			checksum: wait.checksum,
			error,
			exit: wait.exit,
			local: None,
			output: value,
			remotes: process.remote().cloned().map(|r| vec![r]),
		};
		self.finish_process(process.id(), arg).await.map_err(
			|source| tg::error!(!source, process = %process.id(), "failed to finish the process"),
		)?;

		Ok::<_, tg::Error>(())
	}

	async fn heartbeat_task(&self, process: &tg::Process) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::process::heartbeat::Arg {
				local: None,
				remotes: process.remote().cloned().map(|r| vec![r]),
			};
			let result = self.heartbeat_process(process.id(), arg).await;
			if let Ok(output) = result
				&& output.status.is_finished()
			{
				self.process_tasks.abort(process.id());
				break;
			}
			tokio::time::sleep(config.heartbeat_interval).await;
		}
		Ok(())
	}

	async fn run(&self, process: &tg::Process) -> tg::Result<Output> {
		let id = process.id();
		let state = &process
			.load(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to load the process"))?;
		let remote = process.remote();

		let command = process
			.command(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command"))?;
		let command = &command
			.data(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command data"))?;

		// Validate the host.
		let host = command.host.as_str();
		match host {
			#[cfg(target_os = "macos")]
			"builtin" => (),

			#[cfg(target_os = "linux")]
			"builtin" => (),

			#[cfg(all(feature = "js", target_os = "macos"))]
			"js" => (),

			#[cfg(all(feature = "js", target_os = "linux"))]
			"js" => (),

			#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
			"aarch64-darwin" => (),

			#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
			"x86_64-darwin" => (),

			#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
			"aarch64-linux" => (),

			#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
			"x86_64-linux" => (),

			_ => {
				return Err(tg::error!(%host, "cannot run process with host"));
			},
		}

		// Cache the process's children.
		self.cache_children(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache the children"))?;

		// Create the temp.
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		let sandbox_directory = tangram_sandbox::Directory::new(temp.path().to_owned());

		// Determine whether to use a chroot.
		let chroot = cfg!(target_os = "linux");

		let mut path_maps = Vec::new();
		let host_artifacts_path = self.artifacts_path();

		// Get the artifacts path.
		let guest_artifacts_path = if chroot {
			let host = host_artifacts_path.clone();
			let guest = sandbox_directory.guest_artifacts_path();
			path_maps.push(PathMap {
				host: host.clone(),
				guest: guest.clone(),
			});
			guest
		} else {
			host_artifacts_path.clone()
		};

		// Get the output path.
		let (host_output_path, guest_output_path) = if chroot {
			let host = sandbox_directory.host_output_path();
			let guest = sandbox_directory.guest_output_path();
			path_maps.push(PathMap {
				host: host.clone(),
				guest: guest.clone(),
			});
			(host, guest.join(id.to_string()))
		} else {
			(
				sandbox_directory.host_output_path(),
				sandbox_directory.host_output_path_for_process(id),
			)
		};

		// Add the root to the path map.
		if chroot {
			let host = sandbox_directory.host_root_path();
			let guest = sandbox_directory.guest_root_path();
			path_maps.push(PathMap { host, guest });
		}

		// Render the args.
		let mut args = match command.host.as_str() {
			"builtin" | "js" => render_args_dash_a(&command.args),
			_ => render_args_string(&command.args, &guest_artifacts_path, &guest_output_path)?,
		};

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&command.env, &guest_artifacts_path, &guest_output_path)?;

		#[cfg(target_os = "macos")]
		env.entry("TMPDIR".to_owned()).or_insert_with(|| {
			sandbox_directory
				.host_scratch_path()
				.to_string_lossy()
				.into_owned()
		});

		// Render the executable.
		let executable = match command.host.as_str() {
			"builtin" => {
				#[cfg(target_os = "linux")]
				let executable = sandbox_directory.guest_tangram_path();

				#[cfg(target_os = "macos")]
				let executable = self.sandbox_manager.tangram_path().to_owned();

				args.insert(0, "builtin".to_owned());
				args.insert(1, command.executable.to_string());

				executable
			},

			"js" => {
				#[cfg(target_os = "linux")]
				let executable = sandbox_directory.guest_tangram_path();

				#[cfg(target_os = "macos")]
				let executable = self.sandbox_manager.tangram_path().to_owned();

				args.insert(0, "js".to_owned());
				match &self.config.runner.as_ref().unwrap().js.engine {
					crate::config::JsEngine::Auto => {
						args.insert(1, "--engine=auto".into());
					},
					crate::config::JsEngine::QuickJs => {
						args.insert(1, "--engine=quickjs".into());
					},
					crate::config::JsEngine::V8 => {
						args.insert(1, "--engine=v8".into());
					},
				}
				args.insert(1, command.executable.to_string());

				executable
			},

			_ => match &command.executable {
				tg::command::data::Executable::Artifact(executable) => {
					let mut path = guest_artifacts_path.join(executable.artifact.to_string());
					if let Some(executable_path) = &executable.path {
						path.push(executable_path);
					}
					path
				},
				tg::command::data::Executable::Module(_) => {
					return Err(tg::error!("invalid executable"));
				},
				tg::command::data::Executable::Path(executable) => executable.path.clone(),
			},
		};

		// Create mounts.
		let mounts = state.mounts.clone();

		// Set `$TANGRAM_OUTPUT`.
		env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			guest_output_path.to_str().unwrap().to_owned(),
		);

		// Create the guest uri.
		#[cfg(target_os = "linux")]
		let socket_guest_path = sandbox_directory.guest_socket_path();

		#[cfg(target_os = "macos")]
		let (listener, guest_uri) = {
			const MAX_SOCKET_PATH_LEN: usize = 100;
			let socket_path = sandbox_directory.host_socket_path();
			tokio::fs::create_dir_all(socket_path.parent().unwrap())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
			let socket_path_str = socket_path.to_str().unwrap();
			let mut url = if socket_path_str.len() <= MAX_SOCKET_PATH_LEN {
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(socket_path_str)
					.path("")
					.build()
					.unwrap()
			} else {
				"http://localhost:0".parse::<tangram_uri::Uri>().unwrap()
			};
			let listener = Server::listen(&url)
				.await
				.map_err(|source| tg::error!(!source, "failed to listen"))?;
			if let tokio_util::either::Either::Right(ref tcp) = listener {
				let port = tcp
					.local_addr()
					.map_err(|source| tg::error!(!source, "failed to get listener address"))?
					.port();
				url = format!("http://localhost:{port}")
					.parse::<tangram_uri::Uri>()
					.unwrap();
			}
			(listener, url)
		};

		#[cfg(target_os = "linux")]
		let (listener, guest_uri) = {
			let socket_path = sandbox_directory.host_socket_path();
			tokio::fs::create_dir_all(socket_path.parent().unwrap())
				.await
				.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
			let socket_guest_path = socket_guest_path.to_str().unwrap();
			let url = tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(socket_path.to_str().unwrap())
				.path("")
				.build()
				.unwrap();
			let guest_url = tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(socket_guest_path)
				.path("")
				.build()
				.unwrap();
			let listener = Server::listen(&url)
				.await
				.map_err(|source| tg::error!(!source, "failed to listen"))?;
			(listener, guest_url)
		};

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), guest_uri.to_string());

		// Serve.
		let server = self.clone();
		let context = Context {
			process: Some(Arc::new(crate::context::Process {
				id: process.id().clone(),
				path_maps: chroot.then_some(path_maps),
				remote: remote.cloned(),
				retry: *process
					.retry(self)
					.await
					.map_err(|source| tg::error!(!source, "failed to get the process retry"))?,
			})),
			..Default::default()
		};
		let serve_task = Task::spawn({
			let context = context.clone();
			|stop| async move {
				server.serve(listener, context, stop).await;
			}
		});
		let serve_task = Some((serve_task, guest_uri));

		let sandbox_stdin = match state.stdin {
			tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
			tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
			tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
			_ => {
				return Err(tg::error!("invalid stdin"));
			},
		};
		let sandbox_stdout = match state.stdout {
			tg::process::Stdio::Log | tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
			tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
			tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
			_ => {
				return Err(tg::error!("invalid stdout"));
			},
		};
		let sandbox_stderr = match state.stderr {
			tg::process::Stdio::Log | tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
			tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
			tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
			_ => {
				return Err(tg::error!("invalid stderr"));
			},
		};

		// Create the sandbox.
		let sandbox_arg = tangram_sandbox::SpawnArg {
			hostname: None,
			mounts,
			network: state.network,
			path: temp.path().to_owned(),
			user: None,
		};
		let sandbox = self
			.sandbox_manager
			.spawn(sandbox_arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		let sandbox = Arc::new(sandbox);

		// Spawn.
		let sandbox_command = tangram_sandbox::Command {
			args,
			cwd,
			env,
			executable,
			stdin: sandbox_stdin,
			stdout: sandbox_stdout,
			stderr: sandbox_stderr,
		};
		let sandbox_process = sandbox
			.spawn(sandbox_command, id.clone(), state.tty)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to spawn the process in the sandbox"),
			)?;
		let sandbox_process = Arc::new(sandbox_process);
		let stdin = state.stdin.clone();
		let stdout = state.stdout.clone();
		let stderr = state.stderr.clone();

		let _stdin_task = Task::spawn({
			let server = self.clone();
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let id = id.clone();
			let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
			|_| async move {
				server
					.stdin_task(&sandbox, &sandbox_process, &id, stdin, stdin_blob)
					.await
			}
		});

		let stdout_stderr_task = if stdout.is_null() && stderr.is_null() {
			None
		} else {
			Some(Task::spawn({
				let server = self.clone();
				let sandbox = sandbox.clone();
				let sandbox_process = sandbox_process.clone();
				let id = id.clone();
				let remote = remote.cloned();
				|_| async move {
					server
						.stdout_stderr_task(&sandbox, &sandbox_process, &id, remote, stdout, stderr)
						.await
				}
			}))
		};

		// Spawn the tty task.
		let tty_task = if state.tty.is_some() {
			Some(tokio::spawn({
				let server = self.clone();
				let sandbox = sandbox.clone();
				let sandbox_process = sandbox_process.clone();
				let id = id.clone();
				let remote = remote.cloned();
				async move {
					server
						.tty_task(&sandbox, &sandbox_process, &id, remote.as_ref())
						.await
						.inspect_err(|source| tracing::error!(?source, "the tty task failed"))
						.ok();
				}
			}))
		} else {
			None
		};

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.clone();
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let id = id.clone();
			let remote = remote.cloned();
			async move {
				server
					.signal_task(&sandbox, &sandbox_process, &id, remote.as_ref())
					.await
					.inspect_err(|source| tracing::error!(?source, "the signal task failed"))
					.ok();
			}
		});

		// Wait.
		let exit = sandbox
			.wait(&sandbox_process)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?;

		// Abort the tty task.
		if let Some(tty_task) = tty_task {
			tty_task.abort();
		}

		// Abort the signal task.
		signal_task.abort();

		// Await the stdout stderr task.
		if let Some(stdout_stderr_task) = stdout_stderr_task {
			stdout_stderr_task
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "the output task panicked"))
				.and_then(|r| r.map_err(|source| tg::error!(!source, "failed to send output")))?;
		}

		// Stop and await the serve task.
		if let Some((task, _)) = serve_task {
			task.stop();
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the serve task panicked"))?;
		}

		// Create the output.
		let mut output = Output {
			checksum: None,
			error: None,
			exit,
			value: None,
		};

		// Get the output path.
		let path = host_output_path.join(id.to_string());
		let exists = tokio::fs::try_exists(&path).await.map_err(|source| {
			tg::error!(!source, "failed to determine if the output path exists")
		})?;

		// Try to read the user.tangram.checksum xattr.
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.checksum") {
			let checksum = String::from_utf8(bytes)
				.map_err(|source| tg::error!(!source, "failed to parse the checksum xattr"))
				.and_then(|string| string.parse::<tg::Checksum>())
				.map_err(|source| tg::error!(!source, "failed to parse the checksum string"))?;
			output.checksum = Some(checksum);
		}

		// Try to read the user.tangram.output xattr.
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.output") {
			let tgon = String::from_utf8(bytes)
				.map_err(|source| tg::error!(!source, "failed to decode the output xattr"))?;
			output.value = Some(
				tgon.parse::<tg::Value>()
					.map_err(|source| tg::error!(!source, "failed to parse the output xattr"))?,
			);
		}

		// Try to read the user.tangram.error xattr.
		if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.error") {
			let error = serde_json::from_slice::<tg::error::Data>(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error xattr"))?;
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to convert the error data"))?;
			output.error = Some(error);
		}

		// Check in the output.
		if output.value.is_none() && exists {
			let path = if let Some(process) = &context.process {
				process.guest_path_for_host_path(&path).ok_or_else(
					|| tg::error!(path = %path.display(), "no guest path for host path"),
				)?
			} else {
				path.clone()
			};
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: None,
					locked: true,
					root: true,
					..Default::default()
				},
				path,
				updates: Vec::new(),
			};
			let checkin_output = self
				.checkin_with_context(&context, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?
				.try_last()
				.await?
				.and_then(|event| event.try_unwrap_output().ok())
				.ok_or_else(|| tg::error!("stream ended without output"))?;
			let value = tg::Artifact::with_id(checkin_output.artifact.item).into();
			output.value = Some(value);
		}

		// Compute the checksum if necessary.
		if let (Some(checksum), None, Some(value)) =
			(&state.expected_checksum, &output.checksum, &output.value)
		{
			let algorithm = checksum.algorithm();
			let checksum = self
				.compute_checksum(value, algorithm)
				.await
				.map_err(|source| tg::error!(!source, "failed to compute the checksum"))?;
			output.checksum = Some(checksum);
		}

		Ok(output)
	}

	async fn compute_checksum(
		&self,
		value: &tg::Value,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		if let Ok(blob) = value.clone().try_into() {
			self.checksum_blob(&blob, algorithm).await
		} else if let Ok(artifact) = value.clone().try_into() {
			self.checksum_artifact(&artifact, algorithm).await
		} else {
			Err(tg::error!(
				"cannot checksum a value that is not a blob or an artifact"
			))
		}
	}
}

impl Server {
	async fn cache_children(&self, process: &tg::Process) -> tg::Result<()> {
		// Do nothing if the VFS is enabled.
		if self.vfs.lock().unwrap().is_some() {
			return Ok(());
		}

		// Get the process's command's children that are artifacts.
		let artifacts: Vec<tg::artifact::Id> = process
			.command(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command"))?
			.children(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command's children"))?
			.into_iter()
			.filter_map(|object| object.id().try_into().ok())
			.collect::<Vec<_>>();

		// Check out the artifacts.
		let arg = tg::cache::Arg { artifacts };
		let stream = self
			.cache(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache the artifacts"))?;

		// Write progress.
		self.write_progress_stream(process, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to log the progress stream"))?;

		Ok(())
	}

	async fn signal_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		remote: Option<&String>,
	) -> tg::Result<()> {
		// Get the signal stream for the process.
		let arg = tg::process::signal::get::Arg {
			local: None,
			remotes: remote.map(|r| vec![r.clone()]),
		};
		let mut stream = self
			.try_get_process_signal_stream(id, arg)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					process = %id,
					"failed to get the process's signal stream"
				)
			})?
			.ok_or_else(
				|| tg::error!(process = %id, "expected the process's signal stream to exist"),
			)?;

		// Handle the events.
		while let Some(event) = stream.try_next().await.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the next signal event"),
		)? {
			match event {
				tg::process::signal::get::Event::Signal(signal) => {
					sandbox
						.kill(sandbox_process, signal)
						.await
						.map_err(|source| tg::error!(!source, "failed to signal the process"))?;
				},
				tg::process::signal::get::Event::End => {
					break;
				},
			}
		}

		Ok(())
	}

	async fn stdin_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		stdin_mode: tg::process::Stdio,
		stdin_blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		let blob_reader = if let Some(blob) = stdin_blob {
			Some(
				blob.read(self, tg::read::Options::default())
					.await
					.map(|reader| Box::pin(reader) as tangram_futures::BoxAsyncRead<'static>)
					.map_err(|source| tg::error!(!source, "failed to read process stdin blob"))?,
			)
		} else {
			None
		};
		let stdin_reader = if matches!(
			stdin_mode,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			let streams = [tg::process::stdio::Stream::Stdin]
				.into_iter()
				.collect::<BTreeSet<_>>();
			let event_stream = self
				.try_read_process_stdio_messenger_local(id, &streams)
				.await
				.map_err(|source| tg::error!(!source, "failed to read process stdin stream"))?;
			let bytes_stream = event_stream
				.try_filter_map(|event| {
					futures::future::ready(Ok(match event {
						tg::process::stdio::read::Event::Chunk(chunk) => Some(chunk.bytes),
						tg::process::stdio::read::Event::End => None,
					}))
				})
				.map_err(std::io::Error::other)
				.boxed();
			Some(Box::pin(tokio_util::io::StreamReader::new(bytes_stream))
				as tangram_futures::BoxAsyncRead<'static>)
		} else {
			None
		};
		let reader: Option<tangram_futures::BoxAsyncRead<'static>> =
			match (blob_reader, stdin_reader) {
				(Some(blob_reader), Some(stdin_reader)) => {
					Some(blob_reader.chain(stdin_reader).boxed())
				},
				(Some(blob_reader), None) => Some(blob_reader),
				(None, Some(stdin_reader)) => Some(stdin_reader),
				(None, None) => None,
			};
		if let Some(reader) = reader {
			let input = ReaderStream::new(reader)
				.filter_map(|result| {
					futures::future::ready(match result {
						Ok(bytes) if bytes.is_empty() => None,
						Ok(bytes) => Some(Ok(tg::process::stdio::read::Event::Chunk(
							tg::process::stdio::Chunk {
								bytes,
								position: None,
								stream: tg::process::stdio::Stream::Stdin,
							},
						))),
						Err(error) => Some(Err(tg::error!(!error, "failed to read process stdin"))),
					})
				})
				.chain(futures::stream::once(future::ok(
					tg::process::stdio::read::Event::End,
				)))
				.boxed();
			let output = sandbox
				.write_stdio(
					sandbox_process,
					vec![tg::process::stdio::Stream::Stdin],
					input,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to forward process stdin"))?;
			let mut output = pin!(output);
			while let Some(event) = output.try_next().await? {
				match event {
					tg::process::stdio::write::Event::End => {
						break;
					},
					tg::process::stdio::write::Event::Stop => (),
				}
			}
		}
		Ok(())
	}

	async fn stdout_stderr_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		remote: Option<String>,
		stdout: tg::process::Stdio,
		stderr: tg::process::Stdio,
	) -> tg::Result<()> {
		let stream = sandbox
			.read_stdio(
				sandbox_process,
				vec![
					tg::process::stdio::Stream::Stdout,
					tg::process::stdio::Stream::Stderr,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio from sandbox"))?;
		let stdout_forward = if matches!(
			stdout,
			tg::process::Stdio::Log | tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			let (sender, receiver) =
				tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
			let server = self.clone();
			let id = id.clone();
			let arg = tg::process::stdio::write::Arg {
				local: None,
				remotes: remote.clone().map(|remote| vec![remote]),
				streams: vec![tg::process::stdio::Stream::Stdout],
			};
			let task = Task::spawn(move |_| async move {
				let input = ReceiverStream::new(receiver).boxed();
				server
					.write_process_stdio_all(&id, arg, input)
					.await
					.map_err(|source| tg::error!(!source, "failed to write stdout"))
			});
			Some((sender, task))
		} else {
			None
		};
		let stderr_forward = if matches!(
			stderr,
			tg::process::Stdio::Log | tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			let (sender, receiver) =
				tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
			let server = self.clone();
			let id = id.clone();
			let arg = tg::process::stdio::write::Arg {
				local: None,
				remotes: remote.clone().map(|remote| vec![remote]),
				streams: vec![tg::process::stdio::Stream::Stderr],
			};
			let task = Task::spawn(move |_| async move {
				let input = ReceiverStream::new(receiver).boxed();
				server
					.write_process_stdio_all(&id, arg, input)
					.await
					.map_err(|source| tg::error!(!source, "failed to write stderr"))
			});
			Some((sender, task))
		} else {
			None
		};
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio stream"))?
		{
			match event {
				tg::process::stdio::read::Event::Chunk(chunk) => {
					if chunk.bytes.is_empty() {
						continue;
					}
					let sender = match chunk.stream {
						tg::process::stdio::Stream::Stdout => {
							stdout_forward.as_ref().map(|(sender, _)| sender)
						},
						tg::process::stdio::Stream::Stderr => {
							stderr_forward.as_ref().map(|(sender, _)| sender)
						},
						tg::process::stdio::Stream::Stdin => None,
					};
					if let Some(sender) = sender {
						sender
							.send(Ok(tg::process::stdio::read::Event::Chunk(chunk)))
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to forward process stdio")
							})?;
					}
				},
				tg::process::stdio::read::Event::End => {
					break;
				},
			}
		}
		if let Some((sender, task)) = stdout_forward {
			sender
				.send(Ok(tg::process::stdio::read::Event::End))
				.await
				.map_err(|source| tg::error!(!source, "failed to close stdout"))?;
			drop(sender);
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stdout task panicked"))??;
		}
		if let Some((sender, task)) = stderr_forward {
			sender
				.send(Ok(tg::process::stdio::read::Event::End))
				.await
				.map_err(|source| tg::error!(!source, "failed to close stderr"))?;
			drop(sender);
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stderr task panicked"))??;
		}

		Ok(())
	}

	async fn tty_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		remote: Option<&String>,
	) -> tg::Result<()> {
		// Get the signal stream for the process.
		let arg = tg::process::tty::size::get::Arg {
			local: None,
			remotes: remote.map(|r| vec![r.clone()]),
		};
		let mut stream = self
			.try_get_process_tty_size_stream(id, arg)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					process = %id,
					"failed to get the process's tty stream"
				)
			})?
			.ok_or_else(
				|| tg::error!(process = %id, "expected the process's tty stream to exist"),
			)?;

		// Handle the events.
		while let Some(event) = stream.try_next().await.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the next tty event"),
		)? {
			match event {
				tg::process::tty::size::get::Event::Size(size) => {
					sandbox
						.set_tty_size(sandbox_process, size)
						.await
						.map_err(|source| tg::error!(!source, "failed to set the tty size"))?;
				},
				tg::process::tty::size::get::Event::End => {
					break;
				},
			}
		}

		Ok(())
	}
}

fn render_args_string(
	args: &[tg::value::Data],
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<Vec<String>> {
	args.iter()
		.map(|value| render_value_string(value, artifacts_path, output_path))
		.collect::<tg::Result<Vec<_>>>()
}

fn render_args_dash_a(args: &[tg::value::Data]) -> Vec<String> {
	args.iter()
		.flat_map(|value| {
			let value = tg::Value::try_from_data(value.clone()).unwrap().to_string();
			["-A".to_owned(), value]
		})
		.collect::<Vec<_>>()
}

fn render_env(
	env: &tg::value::data::Map,
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<BTreeMap<String, String>> {
	let mut output = BTreeMap::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut output, key)?;
	}
	let output = output
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value_string(value, artifacts_path, output_path)?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<_>>()?;
	Ok(output)
}

fn render_value_string(
	value: &tg::value::Data,
	artifacts_path: &Path,
	output_path: &Path,
) -> tg::Result<String> {
	match value {
		tg::value::Data::String(string) => Ok(string.clone()),
		tg::value::Data::Template(template) => template.try_render(|component| match component {
			tg::template::data::Component::String(string) => Ok(string.clone().into()),
			tg::template::data::Component::Artifact(artifact) => Ok(artifacts_path
				.join(artifact.to_string())
				.to_str()
				.unwrap()
				.to_owned()
				.into()),
			tg::template::data::Component::Placeholder(placeholder) => {
				if placeholder.name == "output" {
					Ok(output_path.to_str().unwrap().to_owned().into())
				} else {
					Err(tg::error!(
						name = %placeholder.name,
						"invalid placeholder"
					))
				}
			},
		}),
		tg::value::Data::Placeholder(placeholder) => {
			if placeholder.name == "output" {
				Ok(output_path.to_str().unwrap().to_owned())
			} else {
				Err(tg::error!(
					name = %placeholder.name,
					"invalid placeholder"
				))
			}
		},
		_ => Ok(tg::Value::try_from_data(value.clone()).unwrap().to_string()),
	}
}
