use super::{
	proxy::Proxy,
	util::{self, render},
};
use crate::{Server, temp::Temp};
use futures::{
	TryStreamExt as _,
	stream::{FuturesOrdered, FuturesUnordered},
};
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
		let remote = process.remote();

		// If the VFS is disabled, then check out the command's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.data(&self.server)
				.await?
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

		// Get the artifacts directory path.
		let artifacts_path = self.server.artifacts_path();

		// Create temps for the output
		let output_parent = Temp::new(&self.server);
		let output = output_parent.path().join("output");
		tokio::fs::create_dir_all(output_parent.path())
			.await
			.map_err(|source| tg::error!(!source, "failed to create output directory"))?;

		// Get or create the home/working directory.
		let root = Temp::new(&self.server);
		let (home, cwd) = if let Some(cwd) = state.cwd.as_ref() {
			(None, cwd.clone())
		} else {
			let home = root.path().join("Users/tangram");
			tokio::fs::create_dir_all(&home)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the home directory"))?;
			let cwd = home.join("work");
			tokio::fs::create_dir_all(&cwd)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the working directory"))?;
			(Some(home), cwd)
		};

		// Create the proxy if running without cwd.
		let proxy = if let Some(home) = &home {
			let path = home.join(".tangram");
			tokio::fs::create_dir_all(&path)
				.await
				.map_err(|source| tg::error!(!source, %path = path.display(), "failed to create the proxy server directory"))?;
			let proxy = Proxy::new(self.server.clone(), process, remote.cloned(), None);

			let socket = path.join("socket").display().to_string();
			let path = urlencoding::encode(&socket);
			let mut url = format!("http+unix://{path}").parse::<Url>().unwrap();
			if url.as_str().len() >= MAX_URL_LEN {
				url = "http://localhost:0".to_string().parse::<Url>().unwrap();
			}
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
		} else {
			None
		};

		// Render the executable.
		let Some(tg::command::Executable::Artifact(executable)) =
			command.executable(&self.server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable = render(&self.server, &executable.into(), &artifacts_path).await?;

		// Create the command
		let mut cmd_ = sandbox::Command::new(executable);

		// Render the env.
		let command_env = command.env(&self.server).await?;
		let process_env = state.env.as_ref();
		cmd_.envs(util::merge_env(&self.server, &artifacts_path, process_env, &command_env).await?);

		// Render the args.
		let args = command.args(&self.server).await?;
		let args: Vec<String> = args
			.iter()
			.map(|value| async {
				let value = render(&self.server, value, &artifacts_path).await?;
				Ok::<_, tg::Error>(value)
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;
		cmd_.args(args);

		// Set `$HOME`.
		if let Some(home) = &home {
			cmd_.env("HOME", &home);
		}

		// Set `$OUTPUT`.
		cmd_.env("OUTUT", &output);

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		cmd_.env("TANGRAM_URL", url.to_string());

		// Set cwd.
		cmd_.cwd(&cwd);

		// Configure the sandbox options.
		if state.cwd.is_some() {
			cmd_.sandbox(false);
		} else {
			cmd_.path(&artifacts_path, true);
			cmd_.path(&cwd, false);
			cmd_.path(&output, false);
			if let Some(home) = &home {
				cmd_.path(&home, false);
			}
		}
		cmd_.network(state.network);

		// Configure stdio
		cmd_.stdin(sandbox::Stdio::Piped);
		if let Some(pipe) = &state.stdin {
			if let Some(ws) = self
				.server
				.try_get_pipe(pipe)
				.await?
				.and_then(|pipe| pipe.window_size)
			{
				let tty = sandbox::Tty {
					rows: ws.rows,
					cols: ws.cols,
					x: ws.xpos,
					y: ws.ypos,
				};
				cmd_.stdin(sandbox::Stdio::Tty(tty));
			}
		}
		cmd_.stdout(sandbox::Stdio::Piped);
		if let Some(pipe) = &state.stdout {
			if let Some(ws) = self
				.server
				.try_get_pipe(pipe)
				.await?
				.and_then(|pipe| pipe.window_size)
			{
				let tty = sandbox::Tty {
					rows: ws.rows,
					cols: ws.cols,
					x: ws.xpos,
					y: ws.ypos,
				};
				cmd_.stdout(sandbox::Stdio::Tty(tty));
			}
		}
		cmd_.stderr(sandbox::Stdio::Piped);
		if let Some(pipe) = &state.stderr {
			if let Some(ws) = self
				.server
				.try_get_pipe(pipe)
				.await?
				.and_then(|pipe| pipe.window_size)
			{
				let tty = sandbox::Tty {
					rows: ws.rows,
					cols: ws.cols,
					x: ws.xpos,
					y: ws.ypos,
				};
				cmd_.stderr(sandbox::Stdio::Tty(tty));
			}
		}
		// Spawn the child process.
		let mut child = cmd_
			.spawn()
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn child process"))?;

		// Spawn the stdio task.
		let stdio_task = Task::spawn(|stop| {
			super::util::stdio_task(
				self.server.clone(),
				process.clone(),
				stop,
				child.stdin.take().unwrap(),
				child.stdout.take().unwrap(),
				child.stderr.take().unwrap(),
			)
		});

		// Wait for the child process to complete.
		let exit = util::wait_or_signal(&self.server, &mut child, process).await?;

		// Stop the proxy task.
		if let Some(task) = proxy.as_ref().map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Join the i/o task.
		stdio_task.stop();
		stdio_task.wait().await.unwrap().ok();

		// Create the output.
		let value = if tokio::fs::try_exists(output_parent.path().join("output"))
			.await
			.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?
		{
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output_parent.path().join("output"),
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
