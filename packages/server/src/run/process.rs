use {
	super::Output,
	crate::Session,
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::Path,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::TryExt as _,
		task::{Stopper, Task},
	},
	tokio::task::JoinSet,
	tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream},
};

mod control;

use control::RunControlTaskArg;

pub(super) struct SpawnProcessTaskArg<'a> {
	pub guest_url: &'a tangram_uri::Uri,
	pub process: tg::Process,
	pub process_token: String,
	pub process_stopper: &'a Stopper,
	pub process_tasks: &'a mut JoinSet<tg::Result<()>>,
	pub sandbox: &'a tangram_sandbox::Sandbox,
}

struct RunProcessArg<'a> {
	guest_url: &'a tangram_uri::Uri,
	process: &'a tg::Process,
	progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	progress_sender: tokio::sync::mpsc::UnboundedSender<Bytes>,
	sandbox: tangram_sandbox::Sandbox,
	sandbox_process_sender: tokio::sync::watch::Sender<Option<Arc<tangram_sandbox::Process>>>,
	stopper: Stopper,
	token: String,
}

impl Session {
	pub(super) fn spawn_process_task(&self, arg: SpawnProcessTaskArg<'_>) {
		let session = self.clone();
		let process = arg.process;
		let process_token = arg.process_token;
		let sandbox = arg.sandbox.clone();
		let guest_url = arg.guest_url.clone();
		let stopper = arg.process_stopper.clone();
		arg.process_tasks.spawn(async move {
			Box::pin(session.process_task(&process, process_token, sandbox, guest_url, stopper))
				.await
		});
	}

	pub(crate) async fn process_task(
		&self,
		process: &tg::Process,
		process_token: String,
		sandbox: tangram_sandbox::Sandbox,
		guest_url: tangram_uri::Uri,
		stopper: Stopper,
	) -> tg::Result<()> {
		let id = process.id().unwrap_right();
		let location = process
			.location()
			.and_then(|location| location.to_location());
		let state = process
			.load_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to load the process"))?;
		let command = process
			.command_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command"))?;
		let command = command
			.data_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command data"))?;

		// Create the control stream.
		let (control_sender, control_responses) = tokio::sync::mpsc::channel(512);
		let control_responses = ReceiverStream::new(control_responses).boxed();
		let arg = tg::process::control::Arg {
			location: location.as_ref().map(|location| location.clone().into()),
		};
		let requests = self
			.try_get_process_control_stream_all(id, arg, control_responses)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
			.ok_or_else(|| tg::error!("expected a control stream"))?
			.boxed();

		// Create the progress stream. It is the buffer between progress reporting and the readers of the process's stderr. It is served to the readers by the control task if the stderr is a pipe, drained to the log by the log task if the stderr is logged, or dropped if the stderr is null.
		let (progress_sender, progress_receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
		let progress = UnboundedReceiverStream::new(progress_receiver)
			.filter(|bytes| future::ready(!bytes.is_empty()))
			.map(Ok::<_, tg::Error>)
			.boxed();
		let mut progress = Some(progress);
		let stderr_progress = if matches!(
			state.stderr,
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			progress.take()
		} else {
			None
		};

		// Spawn the control task. The sandbox process is provided to it once it has been spawned. If the sender is dropped before the sandbox process is spawned, then the i/o tasks respond to all requests with EOF.
		let (sandbox_process_sender, sandbox_process_receiver) = tokio::sync::watch::channel(None);
		let (exited_sender, exited_receiver) = tokio::sync::oneshot::channel();
		let control_task = Task::spawn({
			let session = self.clone();
			let sandbox = sandbox.clone();
			let stdin = state.stdin.clone();
			let stdout = state.stdout.clone();
			let stderr = state.stderr.clone();
			let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
			|_| async move {
				session
					.run_control_task(RunControlTaskArg {
						sandbox,
						sandbox_process: sandbox_process_receiver,
						exited: exited_receiver,
						requests,
						sender: control_sender,
						stdin,
						stdout,
						stderr,
						stdin_blob,
						stderr_progress,
					})
					.await
					.inspect_err(|error| {
						tracing::error!(error = %error.trace(), "the control task failed");
					})
			}
		});

		let result = self
			.run_process(RunProcessArg {
				guest_url: &guest_url,
				process,
				progress,
				progress_sender: progress_sender.clone(),
				sandbox,
				sandbox_process_sender,
				stopper,
				token: process_token,
			})
			.boxed()
			.await;

		// Notify the control task that the sandbox process has exited.
		exited_sender.send(()).ok();

		let output = match result {
			Ok(output) => output,
			Err(error) => {
				let error = tg::error!(
					!error,
					code = tg::error::Code::Internal,
					process = %process.id(),
					"failed to run the process"
				);
				Output {
					error: Some(error),
					checksum: None,
					exit: 1,
					value: None,
				}
			},
		};

		// Store the output.
		let value = if let Some(value) = &output.value {
			value
				.store_with_handle(self)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the output"))?;
			let data = value.to_data();
			Some(data)
		} else {
			None
		};

		// Store the error.
		let error = if let Some(error) = &output.error {
			let error = error.to_data_or_id();
			let error = self.store_process_error(error).await;
			Some(error)
		} else {
			None
		};

		// If the process is remote, then push the output and error.
		if let Some(tg::Location::Remote(remote)) = process
			.location()
			.and_then(|location| location.to_location())
		{
			let mut objects = BTreeSet::new();
			if let Some(value) = &value {
				value.children(&mut objects);
			}
			if let Some(tg::Either::Right(id)) = &error {
				objects.insert(tg::object::Id::Error(id.clone()));
			}
			if !objects.is_empty() {
				let arg = tg::push::Arg {
					destination: Some(tg::Location::Remote(tg::location::Remote {
						name: remote.name.clone(),
						region: remote.region.clone(),
					})),
					items: objects.into_iter().map(tg::Either::Left).collect(),
					..Default::default()
				};
				let stream = self
					.push(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to push the output"))?;
				self.write_progress_stream(process, progress_sender.clone(), stream)
					.await
					.map_err(|error| tg::error!(!error, "failed to log the progress stream"))?;
			}
		}

		// Get the location.
		let arg = tg::process::finish::Arg {
			checksum: output.checksum,
			error,
			exit: output.exit,
			location: process.location(),
			output: value,
		};

		// Finish the process.
		let finished = self
			.try_finish_process(process.id().unwrap_right(), arg)
			.await
			.map_err(
				|error| tg::error!(!error, process = %process.id(), "failed to finish the process"),
			)?;
		if finished.is_none() {
			return Err(tg::error!(process = %process.id(), "failed to find the process"));
		}

		// Join the control task, which completes when the i/o tasks have read and written to EOF. The control task is aborted if it does not complete within the grace period, after which piped stdio is lost.
		let timeout = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(std::time::Duration::from_secs(10), |runner| {
				runner.control_timeout
			});
		tokio::time::timeout(timeout, control_task.wait())
			.await
			.ok();

		Ok::<_, tg::Error>(())
	}

	async fn run_process(&self, arg: RunProcessArg<'_>) -> tg::Result<Output> {
		let RunProcessArg {
			guest_url,
			process,
			mut progress,
			progress_sender,
			sandbox,
			sandbox_process_sender,
			stopper,
			token,
		} = arg;
		let id = process.id().unwrap_right();
		let location = process
			.location()
			.and_then(|location| location.to_location());
		let state = &process
			.load_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to load the process"))?;

		// Register the process so it can authenticate to its location while its command and artifacts are pulled, before it is added to the sandbox.
		self.server.process_auth.insert(
			id.clone(),
			crate::authentication::Process {
				debug: state.debug.clone(),
				location: location.clone(),
				retry: state.retry,
				sandbox: state.sandbox.clone(),
				token: Some(token.clone()),
			},
		);
		let process_auth_id = id.clone();
		scopeguard::defer! {
			self.server.process_auth.remove(&process_auth_id);
		}

		let command = process
			.command_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command"))?;
		let command = &command
			.data_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command data"))?;

		// Run the process.
		let result = async {
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
			self.checkout_process_artifacts(process, progress_sender.clone())
				.await
				.map_err(|error| tg::error!(!error, "failed to cache the children"))?;

			let guest_artifacts_path = sandbox.guest_artifacts_path();
			let guest_output_path = sandbox.guest_output_path_for_process(id);
			let host_output_path = sandbox.host_output_path_for_process(id);

			// Render the args.
			let mut args = match (&command.executable, command.host.as_str()) {
				(tg::command::data::Executable::Module(_), _) | (_, "builtin" | "js") => {
					render_args_dash_a(&command.args)
				},
				_ => render_args_string(&command.args, &guest_artifacts_path, &guest_output_path)?,
			};

			// Get the working directory. On macOS there is no chroot, so "/" is the host root and not writable. Default to the scratch directory instead.
			let cwd = if let Some(cwd) = &command.cwd {
				cwd.clone()
			} else if cfg!(target_os = "macos") {
				sandbox.host_scratch_path()
			} else {
				"/".into()
			};

			// Render the env.
			let env = render_env(&command.env, &guest_artifacts_path, &guest_output_path)?;

			#[cfg(target_os = "macos")]
			let env = {
				let mut env = env;
				env.entry("TMPDIR".to_owned())
					.or_insert_with(|| sandbox.host_scratch_path().to_string_lossy().into_owned());
				env
			};

			// Render the executable.
			let executable = match (&command.executable, command.host.as_str()) {
				(tg::command::data::Executable::Module(_), _) | (_, "js") => {
					let mut js_args = Vec::new();
					js_args.push("js".to_owned());
					js_args.push("--host".to_owned());
					js_args.push(command.host.clone());
					match &self.server.config.runner.as_ref().unwrap().js.engine {
						crate::config::JsEngine::Auto => {
							js_args.push("--engine=auto".into());
						},
						crate::config::JsEngine::QuickJs => {
							js_args.push("--engine=quickjs".into());
						},
						crate::config::JsEngine::V8 => {
							js_args.push("--engine=v8".into());
						},
					}
					render_js_debug_args(&mut js_args, state.debug.as_ref());
					js_args.push(command.executable.to_string());
					js_args.extend(args);
					args = js_args;

					sandbox.guest_tangram_path()
				},

				(_, "builtin") => {
					args.insert(0, "builtin".to_owned());
					args.insert(1, command.executable.to_string());

					sandbox.guest_tangram_path()
				},

				(tg::command::data::Executable::Artifact(executable), _) => {
					let mut path = guest_artifacts_path.join(executable.artifact.to_string());
					if let Some(executable_path) = &executable.path {
						path.push(executable_path);
					}
					path
				},

				(tg::command::data::Executable::Path(executable), _) => executable.path.clone(),
			};

			let stdin = match state.stdin {
				tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
				tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
				tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
				_ => {
					return Err(tg::error!("invalid stdin"));
				},
			};
			let stdout = match state.stdout {
				tg::process::Stdio::Log | tg::process::Stdio::Pipe => tangram_sandbox::Stdio::Pipe,
				tg::process::Stdio::Null => tangram_sandbox::Stdio::Null,
				tg::process::Stdio::Tty => tangram_sandbox::Stdio::Tty,
				_ => {
					return Err(tg::error!("invalid stdout"));
				},
			};
			let stderr = match state.stderr {
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
				stdin,
				stdout,
				stderr,
			};
			let sandbox_process = sandbox
				.spawn(tangram_sandbox::SpawnArg {
					command: sandbox_command,
					creator: sandbox.creator().cloned(),
					debug: state.debug.clone(),
					id: id.clone(),
					location: location.clone(),
					retry: state.retry,
					token: token.clone(),
					tty: state.tty,
					url: guest_url.clone(),
				})
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to spawn the process in the sandbox"),
				)?;
			let token_for_cleanup = token.clone();
			scopeguard::defer! {
				sandbox.remove_process(&token_for_cleanup);
			}
			let sandbox_process = Arc::new(sandbox_process);

			// Provide the sandbox process to the control task.
			sandbox_process_sender.send_replace(Some(sandbox_process.clone()));

			// Spawn a task to drain the logged stdout and stderr if necessary.
			let mut log_streams = Vec::new();
			if matches!(state.stdout, tg::process::Stdio::Log) {
				log_streams.push(tg::process::stdio::Stream::Stdout);
			}
			if matches!(state.stderr, tg::process::Stdio::Log) {
				log_streams.push(tg::process::stdio::Stream::Stderr);
			}
			let log_progress = if matches!(state.stderr, tg::process::Stdio::Log) {
				progress.take()
			} else {
				None
			};
			let log_task = if log_streams.is_empty() {
				None
			} else {
				Some(Task::spawn({
					let session = self.clone();
					let sandbox = sandbox.clone();
					let id = id.clone();
					let location = location.clone();
					let sandbox_process = sandbox_process.clone();
					|_| async move {
						let input = sandbox
							.read_stdio(&sandbox_process, log_streams.clone())
							.await
							.map_err(|error| tg::error!(!error, "failed to read process stdio"))?
							.boxed();

						// Drain the progress stream to the log along with the output of the sandbox process.
						let input = match log_progress {
							Some(progress) => {
								let progress = progress
									.map_ok(|bytes| {
										tg::process::stdio::read::Event::Chunk(
											tg::process::stdio::Chunk {
												bytes,
												position: None,
												stream: tg::process::stdio::Stream::Stderr,
											},
										)
									})
									.boxed();
								futures::stream::select(input, progress).boxed()
							},
							None => input,
						};
						let arg = tg::process::stdio::write::Arg {
							location: location.map(Into::into),
							streams: log_streams,
						};
						if let Some(output) =
							session.try_write_process_stdio(&id, arg, input).await?
						{
							let mut output = std::pin::pin!(output);
							while output.try_next().await?.is_some() {}
						}
						Ok::<_, tg::Error>(())
					}
				}))
			};

			// Wait the sandbox process.
			let wait = sandbox.wait(&sandbox_process).await.map_err(
				|error| tg::error!(!error, %id, "failed to start waiting for the process"),
			)?;
			let mut wait = std::pin::pin!(wait);
			let arg = tg::process::status::Arg {
				location: location.clone().map(Into::into),
				timeout: None,
			};
			let status = self.get_process_status(id, arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to get the process status stream"),
			)?;
			let status = async move {
				let mut status = std::pin::pin!(status);
				while let Some(status) = status.try_next().await? {
					if status.is_finished() {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			};
			let mut status = std::pin::pin!(status);
			let (exit, stopped) = tokio::select! {
				result = &mut wait => {
					let exit = result
						.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
					(exit, false)
				},
				result = &mut status => {
					result.map_err(|error| {
						tg::error!(!error, %id, "failed to wait for the process status")
					})?;
					sandbox.kill(&sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait
						.await
						.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
					(exit, true)
				},
				() = stopper.wait() => {
					sandbox.kill(&sandbox_process, tg::process::Signal::SIGKILL).await.ok();
					let exit = wait
						.await
						.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
					(exit, true)
				},
			};
			// Wait for the log task to finish draining the stdio into the log store.
			if let Some(log_task) = log_task {
				log_task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the log task panicked"))?
					.map_err(|error| tg::error!(!error, "failed to drain the process logs"))?;
			}

			// Handle the case where the process was stopped.
			if stopped {
				return Err(tg::error!(
					code = tg::error::Code::Cancellation,
					"the process was canceled"
				));
			}

			// Create the output.
			let mut output = Output {
				checksum: None,
				error: None,
				exit,
				value: None,
			};

			// Get the output path.
			let path = host_output_path;
			let exists = tokio::fs::try_exists(&path).await.map_err(|error| {
				tg::error!(!error, "failed to determine if the output path exists")
			})?;

			// Try to read the user.tangram.checksum xattr.
			if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.checksum") {
				let checksum = String::from_utf8(bytes)
					.map_err(|error| tg::error!(!error, "failed to parse the checksum xattr"))
					.and_then(|string| string.parse::<tg::Checksum>())
					.map_err(|error| tg::error!(!error, "failed to parse the checksum string"))?;
				output.checksum = Some(checksum);
			}

			// Try to read the user.tangram.output xattr.
			if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.output") {
				let tgon = String::from_utf8(bytes)
					.map_err(|error| tg::error!(!error, "failed to decode the output xattr"))?;
				output.value = Some(
					tgon.parse::<tg::Value>()
						.map_err(|error| tg::error!(!error, "failed to parse the output xattr"))?,
				);
			}

			// Try to read the user.tangram.error xattr.
			if let Ok(Some(bytes)) = xattr::get(&path, "user.tangram.error") {
				let error = serde_json::from_slice::<tg::error::Data>(&bytes)
					.map_err(|error| tg::error!(!error, "failed to deserialize the error xattr"))?;
				let error = tg::Error::try_from(error)
					.map_err(|error| tg::error!(!error, "failed to convert the error data"))?;
				output.error = Some(error);
			}

			// Check in the output.
			if output.value.is_none() && exists {
				let path = self.guest_path_for_host_path(&path)?;
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
					.checkin(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to check in the output"))?
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
					.map_err(|error| tg::error!(!error, "failed to compute the checksum"))?;
				output.checksum = Some(checksum);
			}
			Ok(output)
		}
		.boxed()
		.await;

		// Drop the sender so that the i/o tasks observe that the sandbox process will never be spawned if it has not been.
		drop(sandbox_process_sender);

		result
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

	async fn checkout_process_artifacts(
		&self,
		process: &tg::Process,
		progress: tokio::sync::mpsc::UnboundedSender<Bytes>,
	) -> tg::Result<()> {
		// Do nothing if the VFS is enabled.
		if self.server.vfs.lock().unwrap().is_some() {
			return Ok(());
		}

		// Get the process's command's children that are artifacts.
		let artifacts: Vec<tg::artifact::Id> = process
			.command_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command"))?
			.children_with_handle(self)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the command's children"))?
			.into_iter()
			.filter_map(|object| object.id().try_into().ok())
			.collect::<Vec<_>>();

		// Check out the artifacts.
		let arg = tg::cache::Arg { artifacts };
		let stream = self
			.cache(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to cache the artifacts"))?;

		// Write progress.
		self.write_progress_stream(process, progress, stream)
			.await
			.map_err(|error| tg::error!(!error, "failed to log the progress stream"))?;

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

fn render_js_debug_args(args: &mut Vec<String>, debug: Option<&tg::process::Debug>) {
	let Some(debug) = debug else {
		return;
	};
	args.push("--debug".to_owned());
	if let Some(addr) = debug.addr {
		args.push("--debug-addr".to_owned());
		args.push(addr.to_string());
	}
	if debug.mode != tg::process::debug::Mode::Normal {
		args.push("--debug-mode".to_owned());
		args.push(debug.mode.to_string());
	}
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
				.join(
					artifact
						.clone()
						.map_right(|artifact| artifact.id)
						.into_inner()
						.to_string(),
				)
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
