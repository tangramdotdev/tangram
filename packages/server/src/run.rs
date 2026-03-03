use {
	crate::{
		ProcessPermit, Server,
		context::{Context, PathMap},
		run::util::{
			cache_children, render_args_dash_a, render_args_string, render_env, signal_task,
			stdio_task,
		},
		temp::Temp,
	},
	futures::{FutureExt as _, TryFutureExt as _, future},
	std::{
		collections::BTreeSet,
		os::fd::{AsFd as _, AsRawFd as _},
		path::PathBuf,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::TryExt as _, task::Task, write::Ext as _},
};

mod progress;
mod util;

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

			// Wait for any cleans to finish.
			let clean_guard = self.acquire_clean_guard().await;

			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				local: None,
				remotes: process.remote().cloned().map(|r| vec![r]),
			};
			let result = self.start_process(process.id(), arg.clone()).await;
			if let Err(error) = result {
				tracing::trace!(error = %error.trace(), "failed to start the process");
				continue;
			}

			// Spawn the process task.
			self.spawn_process_task(&process, permit, clean_guard);
		}
	}

	pub(crate) fn spawn_process_task(
		&self,
		process: &tg::Process,
		permit: ProcessPermit,
		clean_guard: crate::CleanGuard,
	) {
		// Spawn the process task.
		self.process_tasks
			.spawn(process.id().clone(), |_| {
				let server = self.clone();
				let process = process.clone();
				async move { server.process_task(&process, permit, clean_guard).await }
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the process task failed");
					})
					.map(|_| ())
			})
			.detach();

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
	}

	async fn process_task(
		&self,
		process: &tg::Process,
		permit: ProcessPermit,
		clean_guard: crate::CleanGuard,
	) -> tg::Result<()> {
		// Guard against concurrent cleans.
		let _clean_guard = self.try_acquire_clean_guard()?;

		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.id().clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(process.id());
			drop(clean_guard);
		}

		// Run.
		let wait = self.run(process).await.map_err(
			|source| tg::error!(!source, process = %process.id(), "failed to run the process"),
		)?;

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
			let arg = tg::push::Arg {
				items: objects.into_iter().map(tg::Either::Left).collect(),
				remote: Some(remote.to_owned()),
				..Default::default()
			};
			let stream = self
				.push(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to push the output"))?;
			self.log_progress_stream(process, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to log the progress stream"))?;
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
		cache_children(self, process)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache the children"))?;

		// Create the temp.
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;

		// Determine whether to use a chroot.
		let chroot = cfg!(target_os = "linux");

		let mut path_maps = Vec::new();

		let output_path = temp.path().join("output");
		let root_path = temp.path().join("root");
		let scratch_path = temp.path().join("scratch");
		let socket_path = temp.path().join("socket");
		let tangram_path = tangram_util::env::current_exe().map_err(|source| {
			tg::error!(!source, "failed to get the path to the tangram executable")
		})?;

		// Get the artifacts path.
		let artifacts_path = if chroot {
			let host = self.artifacts_path();
			let guest = PathBuf::from("/.tangram/artifacts");
			path_maps.push(PathMap {
				host,
				guest: guest.clone(),
			});
			guest
		} else {
			self.artifacts_path()
		};

		// Get the output path.
		let output_path = if chroot {
			let host = output_path.clone();
			let guest = PathBuf::from("/.tangram/output");
			path_maps.push(PathMap {
				host,
				guest: guest.clone(),
			});
			guest
		} else {
			output_path.clone()
		};

		// Add the root to the path map.
		if chroot {
			let host = root_path.clone();
			let guest = PathBuf::from("/");
			path_maps.push(PathMap { host, guest });
		}

		// Render the args.
		let mut args = match command.host.as_str() {
			"builtin" | "js" => render_args_dash_a(&command.args),
			_ => render_args_string(&command.args, &artifacts_path, &output_path)?,
		};

		// Get the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else {
			"/".into()
		};

		// Render the env.
		let mut env = render_env(&command.env, &artifacts_path, &output_path)?;

		// Render the executable.
		let executable = match command.host.as_str() {
			"builtin" => {
				let tg = PathBuf::from("/.tangram/bin/tg");
				args.insert(0, "builtin".to_owned());
				args.insert(1, command.executable.to_string());
				tg
			},

			"js" => {
				let tg = PathBuf::from("/.tangram/bin/tg");
				args.insert(0, "js".to_owned());
				args.insert(1, command.executable.to_string());
				tg
			},

			_ => match &command.executable {
				tg::command::data::Executable::Artifact(executable) => {
					let mut path = artifacts_path.join(executable.artifact.to_string());
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
		let mounts = std::iter::empty()
			.chain(command.mounts.iter().cloned().map(tg::Either::Left))
			.chain(state.mounts.iter().cloned().map(tg::Either::Right))
			.collect::<Vec<_>>();

		// Set `$ThANGRAM_OUTPUT`.
		env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			output_path
				.join(id.to_string())
				.to_str()
				.unwrap()
				.to_owned(),
		);

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

		// Create the guest uri.
		let socket_guest_path = PathBuf::from("/.tangram/socket");
		let authority = socket_guest_path.to_str().unwrap();
		let guest_uri = tangram_uri::Uri::builder()
			.scheme("http+unix")
			.authority(authority)
			.path("")
			.build()
			.unwrap();

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), guest_uri.to_string());

		// Create the host uri.
		tokio::fs::create_dir_all(socket_path.parent().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the host path"))?;
		let authority = socket_path.to_str().unwrap();
		let host_uri = tangram_uri::Uri::builder()
			.scheme("http+unix")
			.authority(authority)
			.path("")
			.build()
			.unwrap();

		// Listen.
		let listener = Server::listen(&host_uri)
			.await
			.map_err(|source| tg::error!(!source, "failed to listen"))?;

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

		let pty = None
			.or_else(|| {
				state.stdin.as_ref().and_then(|stdio| match stdio {
					tg::process::Stdio::Pty(id) => Some(id),
					tg::process::Stdio::Pipe(_) => None,
				})
			})
			.or_else(|| {
				state.stdout.as_ref().and_then(|stdio| match stdio {
					tg::process::Stdio::Pty(id) => Some(id),
					tg::process::Stdio::Pipe(_) => None,
				})
			})
			.or_else(|| {
				state.stderr.as_ref().and_then(|stdio| match stdio {
					tg::process::Stdio::Pty(id) => Some(id),
					tg::process::Stdio::Pipe(_) => None,
				})
			});

		let mut fds = Vec::new();

		// Handle stdin.
		let (stdin, stdin_writer) = if command.stdin.is_some() {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stdin"))?;
			let sender = sender.boxed();
			let fd = receiver
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let stdin = Some(fd.as_raw_fd());
			fds.push(fd);
			(stdin, Some(sender))
		} else {
			match state.stdin.as_ref() {
				Some(tg::process::Stdio::Pipe(pipe)) => {
					let pipe = self
						.pipes
						.get(pipe)
						.ok_or_else(|| tg::error!("failed to find the pipe"))?;
					let fd = pipe
						.receiver
						.as_fd()
						.try_clone_to_owned()
						.map_err(|source| tg::error!(!source, "failed to clone the receiver"))?;
					let receiver = tokio::net::unix::pipe::Receiver::from_owned_fd_unchecked(fd)
						.map_err(|source| tg::error!(!source, "io error"))?;
					let fd = receiver.into_blocking_fd().map_err(|source| {
						tg::error!(!source, "failed to get the fd from the pipe")
					})?;
					let stdin = Some(fd.as_raw_fd());
					fds.push(fd);
					(stdin, None)
				},
				Some(tg::process::Stdio::Pty(pty)) => {
					todo!()
				},
				None => (None, None),
			}
		};

		// Handle stdout.
		let (stdout, stdout_reader) = match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let pipe = self
					.pipes
					.get(pipe)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.sender
					.as_ref()
					.ok_or_else(|| tg::error!("the pipe is closed"))?
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
				let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stdout = fd.as_raw_fd();
				fds.push(fd);
				(stdout, None)
			},
			Some(tg::process::Stdio::Pty(_)) => {
				todo!()
			},
			None => {
				let (sender, receiver) = tokio::net::unix::pipe::pipe()
					.map_err(|source| tg::error!(!source, "failed to create a pipe for stdout"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stdout = fd.as_raw_fd();
				fds.push(fd);
				let receiver = receiver.boxed();
				(stdout, Some(receiver))
			},
		};

		// Handle stderr.
		let (stderr, stderr_reader) = match state.stderr.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let pipe = self
					.pipes
					.get(pipe)
					.ok_or_else(|| tg::error!("failed to find the pipe"))?;
				let fd = pipe
					.sender
					.as_ref()
					.ok_or_else(|| tg::error!("the pipe is closed"))?
					.as_fd()
					.try_clone_to_owned()
					.map_err(|source| tg::error!(!source, "failed to clone the sender"))?;
				let sender = tokio::net::unix::pipe::Sender::from_owned_fd_unchecked(fd)
					.map_err(|source| tg::error!(!source, "io error"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stderr = fd.as_raw_fd();
				fds.push(fd);
				(stderr, None)
			},
			Some(tg::process::Stdio::Pty(_)) => {
				todo!()
			},
			None => {
				let (sender, receiver) = tokio::net::unix::pipe::pipe()
					.map_err(|source| tg::error!(!source, "failed to create a pipe for stderr"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let stderr = fd.as_raw_fd();
				fds.push(fd);
				let receiver = receiver.boxed();
				(stderr, Some(receiver))
			},
		};

		// Create the sandbox.
		let sandbox_config = tangram_sandbox::Config {
			artifacts_path,
			hostname: None,
			mounts,
			network: state.network,
			output_path: output_path.clone(),
			root_path,
			scratch_path,
			socket_path,
			tangram_path,
			user: None,
		};
		let sandbox = tangram_sandbox::Sandbox::new(sandbox_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox"))?;
		let sandbox = Arc::new(sandbox);

		// Spawn.
		let sandbox_command = tangram_sandbox::Command {
			args,
			cwd,
			env,
			executable,
			stdin: if stdin.is_some() {
				tangram_sandbox::Stdio::Pipe
			} else {
				tangram_sandbox::Stdio::Null
			},
			stdout: tangram_sandbox::Stdio::Pipe,
			stderr: tangram_sandbox::Stdio::Pipe,
		};
		let sandbox_process = sandbox.spawn(sandbox_command).await.map_err(
			|source| tg::error!(!source, %id, "failed to spawn the process in the sandbox"),
		)?;
		let sandbox_process = Arc::new(sandbox_process);

		// Drop the FDs.
		drop(fds);

		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let remote = remote.cloned();
			let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
			async move {
				stdio_task(
					&server,
					&id,
					remote.as_ref(),
					stdin_blob,
					stdin_writer,
					stdout_reader,
					stderr_reader,
				)
				.await?;
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.clone();
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let id = id.clone();
			let remote = remote.cloned();
			async move {
				signal_task(&server, &sandbox, &sandbox_process, &id, remote.as_ref())
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

		// Abort the signal task.
		signal_task.abort();

		// Await the stdio task.
		stdio_task
			.await
			.map_err(|source| tg::error!(!source, "the stdio task panicked"))??;

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
		let path = output_path.join(id.to_string());
		let exists = tokio::fs::try_exists(&path).await.map_err(|source| {
			tg::error!(!source, "failed to determine if the output path exists")
		})?;

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
