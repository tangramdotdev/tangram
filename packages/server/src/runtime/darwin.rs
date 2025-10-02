use {
	super::{
		proxy::Proxy,
		util::{render_env, render_value, signal_task, stdio_task, which},
	},
	crate::{Server, temp::Temp},
	num::ToPrimitive as _,
	std::{
		os::{fd::AsRawFd as _, unix::process::ExitStatusExt as _},
		path::Path,
	},
	tangram_client as tg,
	tangram_futures::task::Task,
	url::Url,
};

const MAX_URL_LEN: usize = 100;

#[derive(Clone)]
pub struct Runtime {
	pub(crate) server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		self.run_inner(process)
			.await
			.unwrap_or_else(|error| super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			})
	}

	pub async fn run_inner(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let config = self
			.server
			.config()
			.runtimes
			.get("darwin")
			.ok_or_else(|| tg::error!("server has no runtime configured for darwin"))
			.cloned()?;
		if !matches!(config.kind, crate::config::RuntimeKind::Tangram) {
			return Err(tg::error!("unsupported runtime kind"));
		}

		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let command = command.data(&self.server).await?;
		let remote = process.remote();

		// Checkout the process's children.
		self.server.checkout_children(process).await?;

		// Determine if the host's root is mounted.
		let root_mount = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Get the artifacts path.
		let artifacts_path = self.server.artifacts_path();

		// Create the root.
		let root = Temp::new(&self.server);
		tokio::fs::create_dir_all(&root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the output directory"))?;

		// Create the output path.
		let output_path = root.path().join("output");

		// Render the args.
		let args: Vec<String> = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value))
			.collect();

		// Create the working directory.
		let cwd = command
			.cwd
			.clone()
			.unwrap_or_else(|| root.path().join("work"));
		tokio::fs::create_dir_all(&cwd)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

		// Render the env.
		let mut env = render_env(&artifacts_path, &command.env)?;

		// Render the executable.
		let executable = match command.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = artifacts_path.join(executable.artifact.to_string());
				if let Some(executable_path) = executable.path {
					path.push(executable_path);
				}
				path
			},
			tg::command::data::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tg::command::data::Executable::Path(executable) => {
				which(&executable.path, &env).await?
			},
		};

		// Create the proxy.
		let proxy = if root_mount {
			None
		} else {
			let path = root.path().join(".tangram");
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, %path = path.display(), "failed to create the proxy server directory"))?;
			let proxy = Proxy::new(self.server.clone(), process, remote.cloned(), None);
			let socket_path = path.join("socket").display().to_string();
			let mut url = if socket_path.len() >= MAX_URL_LEN {
				let path = urlencoding::encode(&socket_path);
				format!("http+unix://{path}").parse::<Url>().unwrap()
			} else {
				"http://localhost:0".to_string().parse::<Url>().unwrap()
			};
			let listener = Server::listen(&url).await?;
			let listener_addr = listener
				.local_addr()
				.map_err(|source| tg::error!(!source, "failed to get listener address"))?;
			if let tokio_util::either::Either::Right(listener) = listener_addr {
				let port = listener.port();
				url = format!("http://localhost:{port}").parse::<Url>().unwrap();
			}
			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, url))
		};

		// Set `$OUTPUT`.
		env.insert("OUTPUT".to_owned(), output_path.display().to_string());

		// Set `$TANGRAM_PROCESS`.
		env.insert("TANGRAM_PROCESS".to_owned(), process.id().to_string());

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url);

		// Determine whether to use the sandbox.
		let sandbox = !(root_mount && state.network);

		// Create the command.
		let mut cmd = if sandbox {
			// Create the command.
			let mut cmd = tokio::process::Command::new(config.executable);
			cmd.args(config.args);
			cmd.arg("-C").arg(&cwd);
			for (name, value) in &env {
				cmd.arg("-e").arg(format!("{name}={value}"));
			}
			if !root_mount {
				cmd.arg("--mount")
					.arg(format!("source={}", root.path().display()))
					.arg("--mount")
					.arg(format!("source={},ro", artifacts_path.display()))
					.arg("--mount")
					.arg(format!("source={}", cwd.display()));
			}
			for mount in &state.mounts {
				let mount = if mount.readonly {
					format!("source={},ro", mount.source.display())
				} else {
					format!("source={}", mount.source.display())
				};
				cmd.arg("--mount").arg(mount);
			}
			if state.network {
				cmd.arg("--network");
			}
			cmd.arg(executable)
				.arg("--")
				.args(args)
				.current_dir("/")
				.env_clear();
			cmd
		} else {
			let mut cmd = tokio::process::Command::new(executable);
			cmd.args(args).env_clear().envs(env).current_dir(cwd);
			cmd
		};

		// Create the stdio.
		match state.stdin.as_ref() {
			_ if command.stdin.is_some() => {
				cmd.stdin(std::process::Stdio::piped());
			},
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, true)?;
				cmd.stdin(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let host = self.server.get_pty_fd(pty, true)?;
				let guest = self.server.get_pty_fd(pty, false)?;
				let fd = guest.as_raw_fd();
				cmd.stdin(guest);
				unsafe {
					cmd.pre_exec(move || {
						crate::runtime::util::set_controlling_terminal(fd)?;
						libc::close(host.as_raw_fd());
						Ok(())
					});
				}
			},
			None => {
				cmd.stdin(std::process::Stdio::null());
			},
		}
		match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, false)?;
				cmd.stdout(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let fd = self.server.get_pty_fd(pty, false)?;
				cmd.stdout(fd);
			},
			None => {
				cmd.stdout(std::process::Stdio::piped());
			},
		}
		match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => {
				let fd = self.server.get_pipe_fd(pipe, false)?;
				cmd.stderr(fd);
			},
			Some(tg::process::Stdio::Pty(pty)) => {
				let fd = self.server.get_pty_fd(pty, false)?;
				cmd.stderr(fd);
			},
			None => {
				cmd.stderr(std::process::Stdio::piped());
			},
		}

		// Spawn the process.
		let mut child = cmd
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let stdin = child.stdin.take();
			let stdout = child.stdout.take();
			let stderr = child.stderr.take();
			async move {
				stdio_task(&server, &process, stdin, stdout, stderr).await?;
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let pid = child.id().unwrap().to_i32().unwrap();
			async move {
				signal_task(&server, pid, &process)
					.await
					.inspect_err(|source| tracing::error!(?source, "the signal task failed"))
					.ok();
			}
		});

		// Wait for the process to complete.
		let exit = child.wait().await.map_err(
			|source| tg::error!(!source, %process = process.id(), "failed to wait for the child process"),
		)?;
		let exit = exit
			.code()
			.or(exit.signal().map(|signal| 128 + signal))
			.unwrap()
			.to_u8()
			.unwrap();

		// Stop and await the proxy task.
		if let Some(task) = proxy.as_ref().map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Abort the signal task.
		signal_task.abort();

		// Stop and await the stdio task.
		stdio_task.await.unwrap()?;

		// Create the output.
		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to determine if the output path exists")
			})?;
		let output = if exists {
			let arg = tg::checkin::Arg {
				options: tg::checkin::Options {
					destructive: true,
					deterministic: true,
					ignore: false,
					lock: false,
					locked: true,
					..Default::default()
				},
				path: output_path,
				updates: Vec::new(),
			};
			let artifact = tg::checkin(&self.server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			Some(tg::Value::from(artifact))
		} else {
			None
		};

		// Create the output.
		let output = super::Output {
			checksum: None,
			error: None,
			exit,
			output,
		};

		Ok(output)
	}
}
