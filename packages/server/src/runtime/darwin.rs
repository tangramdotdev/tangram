use super::{
	proxy::Proxy,
	util::{render_env, render_value, signal_task, stdio_task},
};
use crate::{Server, temp::Temp};
use futures::{TryStreamExt as _, stream::FuturesUnordered};
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
		let (error, exit, value) = match self.run_inner(process).await {
			Ok((exit, value)) => (None, exit, value),
			Err(error) => (Some(error), None, None),
		};
		super::Output {
			error,
			exit,
			output: value,
		}
	}

	pub async fn run_inner(
		&self,
		process: &tg::Process,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let command = command.data(&self.server).await?;
		let remote = process.remote();

		// If the VFS is disabled, then check out the command's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.children()
				.into_iter()
				.filter_map(|id| id.try_into().ok())
				.map(|id| async move {
					let artifact = tg::Artifact::with_id(id);
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(&self.server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Determine if there is a root mount.
		let root_mount = command
			.mounts
			.iter()
			.any(|mount| mount.target == Path::new("/"));

		// Get the artifacts path.
		let artifacts_path = self.server.artifacts_path();

		// Create the root.
		let root = Temp::new(&self.server);

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
		let Some(executable) = command.executable else {
			return Err(tg::error!("missing executable"));
		};
		let executable = match executable {
			tg::command::data::Executable::Artifact(artifact) => {
				render_value(&artifacts_path, &tg::object::Id::from(artifact).into())
			},
			tg::command::data::Executable::Module(_) => {
				return Err(tg::error!("invalid executable"));
			},
			tg::command::data::Executable::Path(path) => path.to_string_lossy().to_string(),
		};

		// Set `$OUTPUT`.
		env.insert(
			"OUTPUT".to_owned(),
			output_path.to_str().unwrap().to_owned(),
		);

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
		};

		// Create the stdio.
		let stdin = if let Some(tg::process::Stdio::Pty(pty)) = &state.stdin {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: true,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
		};
		let stdout = if let Some(tg::process::Stdio::Pty(pty)) = &state.stdout {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: false,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
		};
		let stderr = if let Some(tg::process::Stdio::Pty(pty)) = &state.stderr {
			let arg = tg::pty::read::Arg {
				remote: process.remote().cloned(),
				master: false,
			};
			let size = self
				.server
				.get_pty_size(pty, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the pty size"))?;
			let tty = sandbox::Tty {
				rows: size.rows,
				cols: size.cols,
			};
			sandbox::Stdio::Tty(tty)
		} else {
			sandbox::Stdio::Pipe
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
		let stdio_task = Task::spawn(|stop| {
			let server = self.server.clone();
			let process = process.clone();
			let stdin = child.stdin.take().unwrap();
			let stdout = child.stdout.take().unwrap();
			let stderr = child.stderr.take().unwrap();
			async move { stdio_task(&server, &process, stop, stdin, stdout, stderr).await }
		});

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
		let result = child.wait().await;
		let exit = match result.map_err(
			|source| tg::error!(!source, %process = process.id(), "failed to wait for the child process"),
		)? {
			sandbox::ExitStatus::Code(code) => tg::process::Exit::Code { code },
			sandbox::ExitStatus::Signal(signal) => tg::process::Exit::Signal { signal },
		};

		// Stop and await the proxy task.
		if let Some(task) = proxy.as_ref().map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Abort the signal task.
		signal_task.abort();

		// Stop and await the stdio task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap()?;

		// Create the output.
		let exists = tokio::fs::try_exists(&output_path)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to determine if the output path exists")
			})?;
		let value = if exists {
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output_path,
				locked: true,
				lockfile: false,
			};
			let artifact = tg::Artifact::check_in(&self.server, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
			Some(tg::Value::from(artifact))
		} else {
			None
		};

		Ok((Some(exit), value))
	}
}
