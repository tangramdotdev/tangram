use super::{
	proxy::Proxy,
	util::{render_env, render_value, signal_task, stdio_task, which},
};
use crate::{Server, temp::Temp};
use std::path::Path;
use tangram_client as tg;
use tangram_futures::task::Task;
use tangram_sandbox as sandbox;
use url::Url;

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
				error: Some(error),
				..Default::default()
			})
	}

	pub async fn run_inner(&self, process: &tg::Process) -> tg::Result<super::Output> {
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

		// Create the working directory.
		let cwd = command
			.cwd
			.clone()
			.unwrap_or_else(|| root.path().join("work"));
		tokio::fs::create_dir_all(&cwd)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;

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

		// Render the args.
		let args: Vec<String> = command
			.args
			.iter()
			.map(|value| render_value(&artifacts_path, value))
			.collect();

		// Render the env.
		let mut env = render_env(&artifacts_path, &command.env)?;

		// Render the executable.
		let executable = match command.executable {
			tg::command::data::Executable::Artifact(executable) => {
				let mut path = artifacts_path.join(executable.artifact.to_string());
				if let Some(subpath) = executable.path {
					path.push(subpath);
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

		// Set `$OUTPUT`.
		env.insert(
			"OUTPUT".to_owned(),
			output_path.to_str().unwrap().to_owned(),
		);

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

		// Create the mounts.
		let mut mounts = Vec::new();
		if !root_mount {
			mounts.extend([
				(root.path().to_owned(), root.path().to_owned(), false),
				(artifacts_path.clone(), artifacts_path.clone(), true),
				(cwd.clone(), cwd.clone(), false),
			]);
		}
		mounts.extend(
			state
				.mounts
				.iter()
				.map(|mount| (mount.source.clone(), mount.target.clone(), mount.readonly)),
		);

		// Create the stdio.
		let stdin = match state.stdin.as_ref() {
			_ if command.stdin.is_some() => sandbox::Stdio::piped(),
			Some(tg::process::Stdio::Pipe(pipe)) => self.server.get_pipe_fd(pipe, true)?.into(),
			Some(tg::process::Stdio::Pty(pty)) => self.server.get_pty_fd(pty, false)?.into(),
			None => sandbox::Stdio::piped(),
		};
		let stdout = match state.stdout.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => self.server.get_pipe_fd(pipe, true)?.into(),
			Some(tg::process::Stdio::Pty(pty)) => self.server.get_pty_fd(pty, false)?.into(),
			None => sandbox::Stdio::piped(),
		};
		let stderr = match state.stderr.as_ref() {
			Some(tg::process::Stdio::Pipe(pipe)) => self.server.get_pipe_fd(pipe, true)?.into(),
			Some(tg::process::Stdio::Pty(pty)) => self.server.get_pty_fd(pty, false)?.into(),
			None => sandbox::Stdio::piped(),
		};

		// Spawn the process.
		let mut child = sandbox::Command::new(executable)
			.args(args)
			.cwd(cwd)
			.envs(env)
			.mounts(mounts)
			.network(state.network)
			.stderr(stderr)
			.stdin(stdin)
			.stdout(stdout)
			.spawn()
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;
		// Spawn the stdio task.
		let stdio_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();

			// Get the stdio.
			let stdin = child.stdin.take();
			let stdout = child.stdout.take();
			let stderr = child.stderr.take();
			async move {
				stdio_task(&server, &process, stdin, stdout, stderr).await?;
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn
		// Spawn the signal task.
		let signal_task = tokio::spawn({
			let server = self.server.clone();
			let process = process.clone();
			let pid = child.pid();
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
		let exit = match exit {
			sandbox::ExitStatus::Code(code) => code,
			sandbox::ExitStatus::Signal(signal) => 128 + signal,
		};

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
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output_path,
				locked: true,
				lockfile: false,
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
