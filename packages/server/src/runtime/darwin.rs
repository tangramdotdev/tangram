use super::{proxy::Proxy, stdio, util::render};
use crate::{Server, temp::Temp};
use futures::{
	TryStreamExt as _,
	stream::{FuturesOrdered, FuturesUnordered},
};
use indoc::writedoc;
use num::ToPrimitive as _;
use std::{
	ffi::{CStr, CString},
	fmt::Write as _,
	os::fd::AsRawFd,
	os::unix::{ffi::OsStrExt as _, process::ExitStatusExt as _},
	path::Path,
};
use tangram_client as tg;
use tangram_futures::task::Task;
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

		// Render the env.
		let command_env = command.env(&self.server).await?;
		let process_env = state.env.as_ref();
		let mut env =
			super::util::merge_env(&self.server, &artifacts_path, process_env, &command_env)
				.await?;

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

		// Set `$HOME`.
		if let Some(home) = &home {
			env.insert("HOME".to_owned(), home.display().to_string());
		}

		// Set `$OUTPUT`.
		env.insert("OUTPUT".to_owned(), output.display().to_string());

		// Set `$TANGRAM_URL`.
		let url = proxy.as_ref().map_or_else(
			|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			},
			|(_, url)| url.to_string(),
		);
		env.insert("TANGRAM_URL".to_owned(), url.to_string());

		todo!("use tangram_sandbox impl")
	}
}
