use {
	super::Output,
	crate::{Server, context::Context},
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
};

mod signal;
mod stdio;
mod tty;

impl Server {
	pub(crate) async fn run_process_task(
		&self,
		process: &tg::Process,
		sandbox: tangram_sandbox::Sandbox,
		guest_uri: tangram_uri::Uri,
		stopper: Stopper,
	) -> tg::Result<()> {
		let _clean_guard = self.acquire_clean_guard().await;

		let wait = match self
			.run_process(process, sandbox, &guest_uri, stopper)
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

	async fn run_process(
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
		self.checkout_process_artifacts(process)
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
					.run_stdin_task(&sandbox, &sandbox_process, &id, remote, stdin, stdin_blob)
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
						.run_stdout_stderr_task(
							&sandbox,
							&sandbox_process,
							&id,
							remote,
						)
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
						.run_tty_task(&sandbox, &sandbox_process, &id, remote.as_ref())
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
					.run_signal_task(&sandbox, &sandbox_process, &id, remote.as_ref())
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

	async fn checkout_process_artifacts(&self, process: &tg::Process) -> tg::Result<()> {
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
