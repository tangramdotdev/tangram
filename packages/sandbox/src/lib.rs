use {
	crate::{client::Client, server::Server},
	std::{
		collections::BTreeMap,
		io::{Read as _, Write as _},
		ops::Deref,
		os::fd::{AsRawFd as _, FromRawFd as _, RawFd},
		path::PathBuf,
		sync::Arc,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::BoxAsyncRead,
};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

mod client;
mod server;

pub struct Sandbox(Arc<State>);

#[allow(dead_code)]
pub struct State {
	client: Client,
	config: Config,
	process: tokio::process::Child,
}

#[allow(dead_code)]
pub struct Process {
	command: Command,
	id: tg::process::Id,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
	pub artifacts_path: PathBuf,
	pub hostname: Option<String>,
	pub listen_path: PathBuf,
	pub mounts: Vec<tg::Either<tg::command::data::Mount, tg::process::Mount>>,
	pub network: bool,
	pub output_path: PathBuf,
	pub root_path: PathBuf,
	pub scratch_path: PathBuf,
	pub socket_path: PathBuf,
	pub tangram_path: PathBuf,
	pub user: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub stderr: Stdio,
	pub stdin: Stdio,
	pub stdout: Stdio,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub enum Stdio {
	#[default]
	Null,
	Pipe,
	Pty,
}

impl Sandbox {
	pub async fn new(config: Config) -> tg::Result<Self> {
		// Spawn the process.
		let (mut ready_reader, ready_writer) = std::io::pipe()
			.map_err(|source| tg::error!(!source, "failed to create the sandbox ready pipe"))?;
		let flags = unsafe { libc::fcntl(ready_writer.as_raw_fd(), libc::F_GETFD) };
		if flags < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the ready pipe flags"
			));
		}
		let ret = unsafe {
			libc::fcntl(
				ready_writer.as_raw_fd(),
				libc::F_SETFD,
				flags & !libc::FD_CLOEXEC,
			)
		};
		if ret < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the ready pipe flags"
			));
		}
		let ready_fd = ready_writer.as_raw_fd();
		let mut command = tokio::process::Command::new(&config.tangram_path);
		tokio::fs::create_dir_all(&config.scratch_path).await.ok();
		command.arg("sandbox").arg("run");
		command
			.arg("--artifacts-path")
			.arg(&config.artifacts_path)
			.arg("--listen-path")
			.arg(&config.listen_path)
			.arg("--output-path")
			.arg(&config.output_path)
			.arg("--path")
			.arg(&config.root_path)
			.arg("--ready-fd")
			.arg(ready_fd.to_string())
			.arg("--root-path")
			.arg(&config.root_path)
			.arg("--server-path")
			.arg(&config.tangram_path)
			.arg("--socket-path")
			.arg(&config.socket_path)
			.arg("--tangram-path")
			.arg(&config.tangram_path)
			.arg("--temp-path")
			.arg(&config.scratch_path);
		if let Some(hostname) = &config.hostname {
			command.arg("--hostname").arg(hostname);
		}
		for mount in &config.mounts {
			command.arg("--mount").arg(mount.to_string());
		}
		if config.network {
			command.arg("--network");
		} else {
			command.arg("--no-network");
		}
		if let Some(user) = &config.user {
			command.arg("--user").arg(user);
		}
		command
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::inherit())
			.stderr(std::process::Stdio::inherit())
			.kill_on_drop(true);
		let mut process = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the sandbox process"))?;
		drop(ready_writer);

		// Wait for the sandbox to be ready.
		let task = tokio::task::spawn_blocking(move || -> std::io::Result<u8> {
			let mut byte = [0u8];
			ready_reader.read_exact(&mut byte)?;
			Ok(byte[0])
		});
		let ready = tokio::time::timeout(Duration::from_secs(5), task)
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the sandbox ready signal"))
			.and_then(|output| {
				output.map_err(|source| tg::error!(!source, "the sandbox ready task panicked"))
			})
			.and_then(|output| {
				output.map_err(|source| {
					tg::error!(!source, "failed to read the sandbox ready signal")
				})
			})
			.and_then(|byte| {
				if byte != 0x00 {
					return Err(tg::error!("received an invalid ready byte {byte}"));
				}
				Ok(())
			});
		if let Err(source) = ready {
			process.start_kill().ok();
			process.wait().await.ok();
			return Err(tg::error!(!source, "failed to start the sandbox"));
		}

		// Connect the client.
		let client = Client::new(config.listen_path.clone());
		client
			.connect()
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the sandbox"))?;

		let state = State {
			client,
			config,
			process,
		};

		let sandbox = Self(Arc::new(state));

		Ok(sandbox)
	}

	pub async fn spawn(
		&self,
		command: Command,
		pty: Option<tg::process::Pty>,
	) -> tg::Result<Process> {
		let arg = crate::client::spawn::Arg {
			command: command.clone(),
			pty,
		};
		let output = self.client.spawn(arg).await?;
		let process = Process {
			command,
			id: output.id,
		};
		Ok(process)
	}

	pub async fn set_pty_size(
		&self,
		process: &Process,
		size: tg::process::pty::Size,
	) -> tg::Result<()> {
		let arg = crate::client::pty::SizeArg {
			id: process.id.clone(),
			size,
		};
		self.client.set_pty_size(arg).await?;
		Ok(())
	}

	pub async fn stdin(&self, process: &Process, stdin: BoxAsyncRead<'static>) -> tg::Result<()> {
		let arg = crate::client::stdio::StdinArg {
			id: process.id.clone(),
		};
		self.client.stdin(arg, stdin).await?;
		Ok(())
	}

	pub async fn stdout(&self, process: &Process) -> tg::Result<BoxAsyncRead<'static>> {
		let arg = crate::client::stdio::StdoutArg {
			id: process.id.clone(),
		};
		let stdout = self.client.stdout(arg).await?;
		Ok(stdout)
	}

	pub async fn stderr(&self, process: &Process) -> tg::Result<BoxAsyncRead<'static>> {
		let arg = crate::client::stdio::StderrArg {
			id: process.id.clone(),
		};
		let stderr = self.client.stderr(arg).await?;
		Ok(stderr)
	}

	pub async fn kill(&self, process: &Process, signal: tg::process::Signal) -> tg::Result<()> {
		let arg = crate::client::kill::Arg {
			id: process.id.clone(),
			signal,
		};
		self.client.kill(arg).await?;
		Ok(())
	}

	pub async fn wait(&self, process: &Process) -> tg::Result<u8> {
		let arg = crate::client::wait::Arg {
			id: process.id.clone(),
		};
		let output = self.client.wait(arg).await?;
		Ok(output.status)
	}
}

pub fn run(config: &Config, ready_fd: Option<RawFd>) -> tg::Result<()> {
	// Create a current thread tokio runtime.
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	// Bind the listener.
	let listener = {
		let guard = runtime.enter();
		let listener = Server::listen(&config.listen_path)?;
		drop(guard);
		listener
	};

	// Enter the sandbox.
	#[cfg(target_os = "macos")]
	{
		crate::darwin::enter(config)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;
	}
	#[cfg(target_os = "linux")]
	{
		crate::linux::enter(config)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;
	}

	// Signal the ready f.
	if let Some(fd) = ready_fd {
		let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
		let bytes = &[0u8];
		file.write_all(bytes)
			.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
	}

	// Run the server.
	runtime.block_on(async move {
		let server = Server::new();
		server.serve(listener).await
	});

	Ok(())
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Null => write!(f, "null"),
			Self::Pipe => write!(f, "pipe"),
			Self::Pty => write!(f, "pty"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"null" => Ok(Stdio::Null),
			"pipe" => Ok(Stdio::Pipe),
			"pty" => Ok(Stdio::Pty),
			_ => Err(tg::error!("invalid stdio")),
		}
	}
}

impl Deref for Sandbox {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
