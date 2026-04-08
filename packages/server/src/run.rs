use {
	crate::{SandboxPermit, Server, context::Context, temp::Temp},
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream,
	},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::Path,
		pin::pin,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::TryExt as _,
		task::{Stopper, Task},
	},
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

type ProcessTaskMap =
	tangram_futures::task::Map<tg::process::Id, tg::Result<()>, (), tg::id::BuildHasher>;

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			let permit = self
				.sandbox_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = SandboxPermit(tg::Either::Left(permit));

			let arg = tg::sandbox::queue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_sandbox(arg)
					.map_ok(|output| (output, None))
					.boxed(),
			)
			.chain(self.config.runner.iter().flat_map(|config| {
				config.remotes.iter().map(|name| {
					let server = self.clone();
					let remote = name.to_owned();
					async move {
						let client = server.get_remote_client(remote.clone()).await?;
						let arg = tg::sandbox::queue::Arg::default();
						let output = client.dequeue_sandbox(arg).await?;
						Ok::<_, tg::Error>((output, Some(remote)))
					}
					.boxed()
				})
			}));

			let (output, remote) = match future::select_ok(futures).await {
				Ok((output, _)) => output,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to dequeue a sandbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			self.spawn_sandbox_task(&output.sandbox, remote, permit, output.process);
		}
	}

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

		// Create the sandbox.
		let sandbox_arg = tangram_sandbox::SpawnArg {
			artifacts_path: self.artifacts_path(),
			hostname: state.hostname.clone(),
			mounts: state.mounts.clone(),
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

		// Get the sandbox status stream.
		let status = self
			.try_get_sandbox_status_stream(
				id,
				tg::sandbox::status::Arg {
					local: None,
					remotes: remotes.clone(),
				},
			)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the sandbox status stream"))?
			.map(|stream| {
				stream
					.try_filter_map(|event| async move {
						match event {
							tg::sandbox::status::Event::Status(status) => Ok(Some(status)),
							tg::sandbox::status::Event::End => Ok(None),
						}
					})
					.boxed()
			})
			.ok_or_else(|| tg::error!("failed to get the sandbox status stream"))?;
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
					.process_task(&process, sandbox, guest_uri, stopper)
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), process = %process.id(), "the process task failed");
					})
			})
			.detach();
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

	async fn process_task(
		&self,
		process: &tg::Process,
		sandbox: tangram_sandbox::Sandbox,
		guest_uri: tangram_uri::Uri,
		stopper: Stopper,
	) -> tg::Result<()> {
		let _clean_guard = self.acquire_clean_guard().await;

		let wait = match self
			.run(process, sandbox, &guest_uri, stopper)
			.await
			.map_err(
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

	async fn run(
		&self,
		process: &tg::Process,
		sandbox: tangram_sandbox::Sandbox,
		guest_uri: &tangram_uri::Uri,
		stopper: Stopper,
	) -> tg::Result<Output> {
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

		let guest_artifacts_path = sandbox.guest_artifacts_path();
		let guest_output_path = sandbox.guest_output_path_for_process(id);
		let host_output_path = sandbox.host_output_path_for_process(id);

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
		env.entry("TMPDIR".to_owned())
			.or_insert_with(|| sandbox.host_scratch_path().to_string_lossy().into_owned());

		// Render the executable.
		let executable = match command.host.as_str() {
			"builtin" => {
				args.insert(0, "builtin".to_owned());
				args.insert(1, command.executable.to_string());

				sandbox.guest_tangram_path()
			},

			"js" => {
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

				sandbox.guest_tangram_path()
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

		// Set `$TANGRAM_OUTPUT`.
		env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			guest_output_path.to_str().unwrap().to_owned(),
		);

		// Set `$TANGRAM_URL`.
		env.insert("TANGRAM_URL".to_owned(), guest_uri.to_string());
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

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
			.spawn(
				sandbox_command,
				id.clone(),
				state.tty,
				remote.cloned(),
				state.retry,
			)
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
			let remote = remote.cloned();
			let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
			|_| async move {
				server
					.stdin_task(&sandbox, &sandbox_process, &id, remote, stdin, stdin_blob)
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

		let wait = sandbox.wait(&sandbox_process).await.map_err(
			|source| tg::error!(!source, %id, "failed to start waiting for the process"),
		)?;
		let mut wait = std::pin::pin!(wait);
		let (exit, stopped) = tokio::select! {
			result = &mut wait => {
				let exit = result
					.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?;
				(exit, false)
			},
			() = stopper.wait() => {
				sandbox.kill(&sandbox_process, tg::process::Signal::SIGKILL).await.ok();
				let exit = wait
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?;
				(exit, true)
			},
		};

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

		if stopped {
			return Err(tg::error!(
				code = tg::error::Code::Cancellation,
				"the process was canceled"
			));
		}
		let context = Context {
			process: Some(Arc::new(crate::context::Process {
				id: id.clone(),
				remote: remote.cloned(),
				retry: state.retry,
			})),
			sandbox: state.sandbox.clone(),
			..Default::default()
		};

		// Create the output.
		let mut output = Output {
			checksum: None,
			error: None,
			exit,
			value: None,
		};

		// Get the output path.
		let path = host_output_path;
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
			let path = self.guest_path_for_host_path(&context, &path)?;
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
		remote: Option<String>,
		mode: tg::process::Stdio,
		blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		// Create a blob stream if necessary.
		let blob_stream = if let Some(blob) = blob {
			let reader = blob
				.read(self, tg::read::Options::default())
				.await
				.map_err(|source| tg::error!(!source, "failed to read process stdin blob"))?;
			let stream = ReaderStream::new(reader)
				.map_ok(|bytes| {
					tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
						bytes,
						position: None,
						stream: tg::process::stdio::Stream::Stdin,
					})
				})
				.map_err(|error| tg::error!(!error, "failed to read from the blob"))
				.boxed();
			Some(stream)
		} else {
			None
		};

		// Create the stdio stream if necessary.
		let stream = if matches!(mode, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			let arg = tg::process::stdio::read::Arg {
				local: None,
				remotes: remote.map(|remote| vec![remote]),
				streams: vec![tg::process::stdio::Stream::Stdin],
				..Default::default()
			};
			let stream = self
				.try_read_process_stdio_all(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to read process stdin stream"))?
				.ok_or_else(
					|| tg::error!(process = %id, "expected the process stdin stream to exist"),
				)?
				.boxed();
			Some(stream)
		} else {
			None
		};

		// Chain the blob stream and stdio stream if necessary.
		let input = match (blob_stream, stream) {
			(Some(blob_stream), Some(stream)) => Some(blob_stream.chain(stream).boxed()),
			(Some(blob_stream), None) => Some(
				blob_stream
					.chain(stream::once(future::ok(
						tg::process::stdio::read::Event::End,
					)))
					.boxed(),
			),
			(None, Some(stream)) => Some(stream),
			(None, None) => None,
		};

		// Write.
		if let Some(input) = input {
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
		// Create the read stream.
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

		// Create the stdout task.
		let stdout = if matches!(
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

		// Create the stderr task.
		let stderr = if matches!(
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

		// Consume the stream and send the events.
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
						tg::process::stdio::Stream::Stdin => {
							return Err(tg::error!("unexpected stdin chunk"));
						},
						tg::process::stdio::Stream::Stdout => {
							stdout.as_ref().map(|(sender, _)| sender)
						},
						tg::process::stdio::Stream::Stderr => {
							stderr.as_ref().map(|(sender, _)| sender)
						},
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

		// Send the end events and await the tasks.
		if let Some((sender, task)) = stdout {
			sender
				.send(Ok(tg::process::stdio::read::Event::End))
				.await
				.map_err(|source| tg::error!(!source, "failed to close stdout"))?;
			drop(sender);
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stdout task panicked"))??;
		}
		if let Some((sender, task)) = stderr {
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
	for key in env.keys() {
		if key.starts_with(tg::process::env::PREFIX) {
			return Err(tg::error!(
				key = %key,
				"env vars prefixed with TANGRAM_ENV_ are reserved"
			));
		}
	}
	let mut resolved = tg::value::data::Map::new();
	for (key, value) in env {
		let mutation = match value {
			tg::value::Data::Mutation(value) => value.clone(),
			value => tg::mutation::Data::Set {
				value: Box::new(value.clone()),
			},
		};
		mutation.apply(&mut resolved, key)?;
	}
	let mut output = resolved
		.iter()
		.map(|(key, value)| {
			let key = key.clone();
			let value = render_value_string(value, artifacts_path, output_path)?;
			Ok::<_, tg::Error>((key, value))
		})
		.collect::<tg::Result<BTreeMap<_, _>>>()?;
	for (key, value) in &resolved {
		if matches!(value, tg::value::Data::String(_)) {
			continue;
		}
		let value = tg::Value::try_from_data(value.clone()).unwrap().to_string();
		output.insert(format!("{}{key}", tg::process::env::PREFIX), value);
	}
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
