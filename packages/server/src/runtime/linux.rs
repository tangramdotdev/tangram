use super::{
	proxy::{self, Proxy},
	util::render,
};
use crate::{Server, temp::Temp};
use futures::{
	TryStreamExt as _, future,
	stream::{FuturesOrdered, FuturesUnordered},
};
use std::path::Path;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;
use url::Url;

mod chroot;
mod process;

/// The home directory guest path.
const HOME_DIRECTORY_GUEST_PATH: &str = "/home/tangram";

/// The output parent directory guest path.
const OUTPUT_PARENT_DIRECTORY_GUEST_PATH: &str = "/output";

/// The server guest path.
const SERVER_DIRECTORY_GUEST_PATH: &str = "/.tangram";

/// The GID for the tangram user.
const TANGRAM_GID: libc::gid_t = 1000;

/// The UID for the tangram user.
const TANGRAM_UID: libc::uid_t = 1000;

/// The working directory guest path.
const WORKING_DIRECTORY_GUEST_PATH: &str = "/home/tangram/work";

#[cfg(target_arch = "aarch64")]
const DASH: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dash_aarch64_linux"));

#[cfg(target_arch = "x86_64")]
const DASH: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dash_x86_64_linux"));

#[cfg(target_arch = "aarch64")]
const ENV: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/env_aarch64_linux"));

#[cfg(target_arch = "x86_64")]
const ENV: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/env_x86_64_linux"));

#[derive(Clone)]
pub struct Runtime {
	pub(super) server: Server,
	env: tg::File,
	sh: tg::File,
}

impl Runtime {
	pub async fn new(server: &Server) -> tg::Result<Self> {
		let env = tg::Blob::with_reader(server, ENV).await?;
		let env = tg::File::builder(env).executable(true).build();
		let arg = tg::tag::put::Arg {
			force: true,
			item: Either::Right(env.id(server).await?.into()),
			remote: None,
		};
		server
			.put_tag(&"internal/runtime/linux/env".parse().unwrap(), arg)
			.await?;

		let sh = tg::Blob::with_reader(server, DASH)
			.await
			.inspect_err(|error| eprintln!("{error}"))?;
		let sh = tg::File::builder(sh).executable(true).build();
		let arg = tg::tag::put::Arg {
			force: true,
			item: Either::Right(sh.id(server).await?.into()),
			remote: None,
		};
		server
			.put_tag(&"internal/runtime/linux/sh".parse().unwrap(), arg)
			.await?;

		let server = server.clone();
		Ok(Self { server, env, sh })
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		let (error, exit, output) = match self.run_inner(process).await {
			Ok((exit, output)) => (None, exit, output),
			Err(error) => (Some(error), None, None),
		};
		super::Output {
			error,
			exit,
			output,
		}
	}

	async fn run_inner(
		&self,
		process: &tg::Process,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
		// Get the process state.
		let state = process.load(&self.server).await?;
		let command = process.command(&self.server).await?;
		let remote = process.remote();

		// If the VFS is disabled, then check out the target's children.
		if self.server.vfs.lock().unwrap().is_none() {
			command
				.data(&self.server)
				.await?
				.children()
				.into_iter()
				.filter_map(|id| id.try_into().ok())
				.map(tg::Artifact::with_id)
				// .chain([self.env.clone().into(), self.sh.clone().into()])
				.map(|artifact| async move {
					let arg = tg::artifact::checkout::Arg::default();
					artifact.check_out(&self.server, arg).await?;
					Ok::<_, tg::Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
		}

		// Create the parent directory for the output.
		let output_parent = Temp::new(&self.server);
		tokio::fs::create_dir_all(output_parent.path())
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to create the output parent directory")
			})?;

		// Setup the chroot if cwd is not set.
		let chroot = if state.cwd.is_some() {
			None
		} else {
			let chroot = chroot::Chroot::new(self, state.network, output_parent.path()).await?;
			Some(chroot)
		};

		// Create the proxy for the chroot.
		let proxy = if let Some(chroot) = &chroot {
			// Create the path map.
			let path_map = proxy::PathMap {
				output_host: output_parent.path().to_owned(),
				output_guest: OUTPUT_PARENT_DIRECTORY_GUEST_PATH.into(),
				root_host: chroot.temp.path().to_owned(),
			};

			// Create the proxy server guest URL.
			let socket = Path::new(SERVER_DIRECTORY_GUEST_PATH).join("socket");
			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?,
			);
			let guest_url = format!("http+unix://{socket}").parse::<Url>().unwrap();

			// Create the proxy server host URL.
			let socket = chroot.temp.path().join(".tangram/socket");
			let socket = urlencoding::encode(
				socket
					.to_str()
					.ok_or_else(|| tg::error!(%path = socket.display(), "invalid path"))?,
			);
			let host_url = format!("http+unix://{socket}").parse::<Url>().unwrap();

			// Start the proxy server.
			let proxy = Proxy::new(
				self.server.clone(),
				process,
				remote.cloned(),
				Some(path_map),
			);
			let listener = Server::listen(&host_url).await?;
			let task = Task::spawn(|stop| Server::serve(proxy, listener, stop));
			Some((task, guest_url))
		} else {
			None
		};

		// Get the artifacts path.
		let artifacts_path = if chroot.is_some() {
			Path::new(SERVER_DIRECTORY_GUEST_PATH).join("artifacts")
		} else {
			self.server.artifacts_path()
		};

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

		// Get the working directory.
		let cwd = if let Some(cwd) = &state.cwd {
			cwd.clone()
		} else {
			WORKING_DIRECTORY_GUEST_PATH.into()
		};

		// Get the command env.
		let command_env = command.env(&self.server).await?;

		// Get the process env.
		let process_env = state.env.as_ref();

		// Merge the environment.
		let mut env =
			super::util::merge_env(&self.server, &artifacts_path, process_env, &*command_env)
				.await?;

		// Render the executable.
		let Some(tg::command::Executable::Artifact(executable)) =
			command.executable(&self.server).await?.as_ref().cloned()
		else {
			return Err(tg::error!("invalid executable"));
		};
		let executable = render(&self.server, &executable.into(), &artifacts_path).await?;

		if chroot.is_some() {
			// Set `$HOME`.
			env.insert("HOME".to_owned(), HOME_DIRECTORY_GUEST_PATH.to_owned());
			// Set `$OUTPUT`.
			let output_guest_path =
				std::path::PathBuf::from(OUTPUT_PARENT_DIRECTORY_GUEST_PATH).join("output");
			env.insert(
				"OUTPUT".to_owned(),
				output_guest_path.to_str().unwrap().to_owned(),
			);
		} else {
			// Set OUTPUT to the host path.
			env.insert(
				"OUTPUT".to_owned(),
				output_parent
					.path()
					.join("output")
					.to_str()
					.unwrap()
					.to_owned(),
			);
		}

		// Set `$TANGRAM_URL`.
		let url = proxy
			.as_ref()
			.map(|(_, url)| url.to_string())
			.unwrap_or_else(|| {
				let path = self.server.path.join("socket").display().to_string();
				let path = urlencoding::encode(&path);
				format!("http+unix://{path}")
			});
		env.insert("TANGRAM_URL".to_owned(), url.to_string());

		// Spawn the child.
		let mut child = process::spawn(args, cwd, env, executable, chroot, state.network)?;

		// Spawn the log task.
		let log_task = super::util::post_log_task(
			&self.server,
			process,
			remote,
			child.stdout.take().unwrap(),
			child.stderr.take().unwrap(),
		);

		// Wait for the child process to complete.
		let exit = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the process to exit"))?;

		// Stop the proxy task.
		if let Some(task) = proxy.map(|(proxy, _)| proxy) {
			task.stop();
			task.wait().await.unwrap();
		}

		// Join the i/o tasks.
		let (input, output) = future::try_join(future::ok(Ok::<_, tg::Error>(())), log_task)
			.await
			.map_err(|source| tg::error!(!source, "failed to join the i/o tasks"))?;
		input.map_err(|source| tg::error!(!source, "the stdin task failed"))?;
		output.map_err(|source| tg::error!(!source, "the log task failed"))?;

		// Create the output.
		let output = output_parent.path().join("output");
		let exists = tokio::fs::try_exists(&output)
			.await
			.map_err(|source| tg::error!(!source, "failed to determine in the path exists"))?;
		let output = if exists {
			let arg = tg::artifact::checkin::Arg {
				cache: true,
				destructive: true,
				deterministic: true,
				ignore: false,
				path: output.clone(),
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

		Ok((Some(exit), output))
	}
}
