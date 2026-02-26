use {
	super::util::{cache_children, render_args_dash_a, render_args_string, render_env},
	crate::{Context, Server, temp::Temp},
	std::{
		os::fd::{AsFd as _, AsRawFd as _},
		path::PathBuf,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::TryExt as _, task::Task, write::Ext as _},
	tangram_sandbox as sandbox,
	tangram_uri::Uri,
};

const MAX_URL_LEN: usize = 100;

impl Server {
	pub(crate) async fn run_darwin(&self, process: &tg::Process) -> tg::Result<super::Output> {
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

		// Cache the process's children.
		cache_children(self, process)
			.await
			.map_err(|source| tg::error!(!source, "failed to cache the children"))?;

		// Get the artifacts path.
		let artifacts_path = self.artifacts_path();

		// Get the output path.
		let temp = Temp::new(self);
		let output_path = if state.sandbox.is_none() {
			tokio::fs::create_dir_all(&temp)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the temp directory"))?;
			tokio::fs::create_dir_all(temp.path().join("output"))
				.await
				.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;
			temp.path().join("output/output")
		} else {
			let sandbox = self
				.sandboxes
				.get(state.sandbox.as_ref().unwrap())
				.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
			let path = sandbox._temp.path().join("output/output");
			drop(sandbox);
			path
		};

		// Render the args.
		let mut args = match command.host.as_str() {
			"builtin" | "js" => render_args_dash_a(&command.args),
			_ => render_args_string(&command.args, &artifacts_path, &output_path)?,
		};

		// Create the working directory.
		let cwd = if let Some(cwd) = &command.cwd {
			cwd.clone()
		} else if state.sandbox.is_none() {
			temp.path().join("work")
		} else {
			"/".into()
		};
		tokio::fs::create_dir_all(&cwd)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

		// Render the env.
		let mut env = render_env(&command.env, &artifacts_path, &output_path)?;

		// Render the executable.
		let executable = match command.host.as_str() {
			"builtin" => {
				let tg = tangram_util::env::current_exe().map_err(|source| {
					tg::error!(!source, "failed to get the current executable")
				})?;
				args.insert(0, "builtin".to_owned());
				args.insert(1, command.executable.to_string());
				tg
			},

			"js" => {
				let tg = tangram_util::env::current_exe().map_err(|source| {
					tg::error!(!source, "failed to get the current executable")
				})?;
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

		// Set `$TANGRAM_OUTPUT`.
		env.insert(
			"TANGRAM_OUTPUT".to_owned(),
			output_path.to_str().unwrap().to_owned(),
		);

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), id.to_string());

		// Set `$TANGRAM_SANDBOX`.
		if let Some(sandbox_id) = &state.sandbox {
			env.insert("TANGRAM_SANDBOX".to_owned(), sandbox_id.to_string());
		}

		// Run the process.
		let output = if let Some(sandbox_id) = &state.sandbox {
			// Set `$TANGRAM_URL` from the sandbox's socket.
			let sandbox = self
				.sandboxes
				.get(sandbox_id)
				.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
			let socket_path = sandbox._temp.path().join(".tangram/socket");
			drop(sandbox);
			let url = tangram_uri::Uri::builder()
				.scheme("http+unix")
				.authority(socket_path.to_str().unwrap())
				.path("")
				.build()
				.unwrap();
			env.insert("TANGRAM_URL".to_owned(), url.to_string());

			drop(temp);
			self.run_darwin_sandboxed(
				state, command, id, remote, sandbox_id, executable, args, env, cwd,
			)
			.await?
		} else {
			// Set up the serve task.
			let path = temp.path().join(".tangram");
			tokio::fs::create_dir_all(&path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to create the directory"),
			)?;

			// Listen.
			let socket_path = path.join("socket").display().to_string();
			let mut url = if socket_path.len() <= MAX_URL_LEN {
				tangram_uri::Uri::builder()
					.scheme("http+unix")
					.authority(&socket_path)
					.path("")
					.build()
					.unwrap()
			} else {
				"http://localhost:0".to_owned().parse::<Uri>().unwrap()
			};
			let listener = Server::listen(&url)
				.await
				.map_err(|source| tg::error!(!source, "failed to listen"))?;
			let listener_addr = listener
				.local_addr()
				.map_err(|source| tg::error!(!source, "failed to get listener address"))?;
			if let tokio_util::either::Either::Right(listener) = listener_addr {
				let port = listener.port();
				url = format!("http://localhost:{port}").parse::<Uri>().unwrap();
			}

			// Serve.
			let server = self.clone();
			let context = Context::default();
			let task = Task::spawn({
				let context = context.clone();
				|stop| async move {
					server.serve(listener, context, stop).await;
				}
			});

			let serve_task = Some((task, url.clone()));

			// Set `$TANGRAM_URL`.
			env.insert("TANGRAM_URL".to_owned(), url.to_string());

			let arg = crate::run::common::Arg {
				args,
				command,
				context: &context,
				cwd,
				env,
				executable,
				id,
				remote,
				serve_task,
				server: self,
				state,
				temp: &temp,
			};
			crate::run::common::run(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to run the process"))?
		};

		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	async fn run_darwin_sandboxed(
		&self,
		state: &tg::process::State,
		command: &tg::command::Data,
		id: &tg::process::Id,
		remote: Option<&String>,
		sandbox_id: &tg::sandbox::Id,
		executable: PathBuf,
		args: Vec<String>,
		env: std::collections::BTreeMap<String, String>,
		cwd: PathBuf,
	) -> tg::Result<super::Output> {
		// Get the sandbox client and the host output path.
		let sandbox = self
			.sandboxes
			.get(sandbox_id)
			.ok_or_else(|| tg::error!("failed to find the sandbox"))?;
		let client = Arc::clone(&sandbox.client);
		let output_path = sandbox._temp.path().join("output/output");
		drop(sandbox);

		// Collect FDs that need to be kept alive until after the spawn call.
		let mut fds = Vec::new();

		// Handle stdin.
		let (stdin, stdin_writer) = if command.stdin.is_some() {
			let (sender, receiver) = tokio::net::unix::pipe::pipe()
				.map_err(|source| tg::error!(!source, "failed to create a pipe for stdin"))?;
			let sender = sender.boxed();
			let fd = receiver
				.into_blocking_fd()
				.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
			let raw_fd = fd.as_raw_fd();
			fds.push(fd);
			(Some(raw_fd), Some(sender))
		} else {
			match state.stdin.as_ref() {
				Some(tg::process::Stdio::Pipe(pipe_id)) => {
					let pipe = self
						.pipes
						.get(pipe_id)
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
					let raw_fd = fd.as_raw_fd();
					fds.push(fd);
					(Some(raw_fd), None)
				},
				Some(tg::process::Stdio::Pty(pty_id)) => {
					let pty = self
						.ptys
						.get(pty_id)
						.ok_or_else(|| tg::error!("failed to find the pty"))?;
					let slave = pty
						.slave
						.as_ref()
						.ok_or_else(|| tg::error!("the pty slave is closed"))?;
					let fd = slave
						.try_clone()
						.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
					let raw_fd = fd.as_raw_fd();
					fds.push(fd);
					(Some(raw_fd), None)
				},
				None => (None, None),
			}
		};

		// Handle stdout.
		let (stdout, stdout_reader) = match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe_id)) => {
				let pipe = self
					.pipes
					.get(pipe_id)
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
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				(Some(raw_fd), None)
			},
			Some(tg::process::Stdio::Pty(pty_id)) => {
				let pty = self
					.ptys
					.get(pty_id)
					.ok_or_else(|| tg::error!("failed to find the pty"))?;
				let slave = pty
					.slave
					.as_ref()
					.ok_or_else(|| tg::error!("the pty slave is closed"))?;
				let fd = slave
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				(Some(raw_fd), None)
			},
			None => {
				let (sender, receiver) = tokio::net::unix::pipe::pipe()
					.map_err(|source| tg::error!(!source, "failed to create a pipe for stdout"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				let receiver = receiver.boxed();
				(Some(raw_fd), Some(receiver))
			},
		};

		// Handle stderr.
		let (stderr, stderr_reader) = match state.stderr.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe_id)) => {
				let pipe = self
					.pipes
					.get(pipe_id)
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
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				(Some(raw_fd), None)
			},
			Some(tg::process::Stdio::Pty(pty_id)) => {
				let pty = self
					.ptys
					.get(pty_id)
					.ok_or_else(|| tg::error!("failed to find the pty"))?;
				let slave = pty
					.slave
					.as_ref()
					.ok_or_else(|| tg::error!("the pty slave is closed"))?;
				let fd = slave
					.try_clone()
					.map_err(|source| tg::error!(!source, "failed to clone the pty slave"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				(Some(raw_fd), None)
			},
			None => {
				let (sender, receiver) = tokio::net::unix::pipe::pipe()
					.map_err(|source| tg::error!(!source, "failed to create a pipe for stderr"))?;
				let fd = sender
					.into_blocking_fd()
					.map_err(|source| tg::error!(!source, "failed to get the fd from the pipe"))?;
				let raw_fd = fd.as_raw_fd();
				fds.push(fd);
				let receiver = receiver.boxed();
				(Some(raw_fd), Some(receiver))
			},
		};

		// Create the sandbox command.
		let sandbox_command = sandbox::Command {
			cwd: Some(cwd),
			env: env.into_iter().collect(),
			executable,
			stdin,
			stdout,
			stderr,
			trailing: args,
		};

		// Spawn the command in the sandbox.
		let pid = client
			.spawn(sandbox_command)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the process in the sandbox"))?;

		// Drop the FDs now that the spawn has completed.
		drop(fds);

		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			let remote = remote.cloned();
			let stdin_blob = command.stdin.clone().map(tg::Blob::with_id);
			async move {
				super::common::stdio_task(
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

		// Wait for the process in the sandbox.
		let status = client.wait(pid).await.map_err(|source| {
			tg::error!(!source, "failed to wait for the process in the sandbox")
		})?;

		// Await the stdio task.
		stdio_task
			.await
			.map_err(|source| tg::error!(!source, "the stdio task panicked"))??;

		// Create the output.
		let exit = u8::try_from(status).unwrap_or(1);
		let mut output = super::Output {
			checksum: None,
			error: None,
			exit,
			output: None,
		};

		// Get the output path on the host.
		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to determine if the output path exists")
			})?;

		// Try to read the user.tangram.output xattr.
		if let Ok(Some(bytes)) = xattr::get(&output_path, "user.tangram.output") {
			let tgon = String::from_utf8(bytes)
				.map_err(|source| tg::error!(!source, "failed to decode the output xattr"))?;
			output.output = Some(
				tgon.parse::<tg::Value>()
					.map_err(|source| tg::error!(!source, "failed to parse the output xattr"))?,
			);
		}

		// Try to read the user.tangram.error xattr.
		if let Ok(Some(bytes)) = xattr::get(&output_path, "user.tangram.error") {
			let error = serde_json::from_slice::<tg::error::Data>(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error xattr"))?;
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to convert the error data"))?;
			output.error = Some(error);
		}

		// Check in the output.
		if output.output.is_none() && exists {
			let context = self
				.sandboxes
				.get(sandbox_id)
				.map(|sandbox| sandbox.context.clone())
				.unwrap_or_default();
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
				path: output_path.clone(),
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
			output.output = Some(value);
		}

		Ok(output)
	}
}
