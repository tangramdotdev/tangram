use {
	crate::{client::Client, server::Server},
	std::{io::Write as _, os::fd::FromRawFd as _, path::PathBuf},
	tangram_client::prelude::*,
};

mod common;
#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

mod client;
mod server;

pub struct Sandbox {
	process: tokio::process::Child,
	client: Client,
}

pub struct Process {
	pid: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
	pub hostname: Option<String>,
	pub id: tg::sandbox::Id,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: bool,
	pub ready_fd: Option<i32>,
	pub root_path: PathBuf,
	pub server_path: PathBuf,
	pub socket_path: PathBuf,
	pub temp_path: PathBuf,
	pub user: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Ready {
	pub root_path: Option<PathBuf>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: Option<PathBuf>,
	pub env: Vec<(String, String)>,
	pub executable: PathBuf,
	pub stderr: Option<i32>,
	pub stdin: Option<i32>,
	pub stdout: Option<i32>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct EnterOutput {
	root_path: Option<PathBuf>,
}

impl Sandbox {
	pub async fn start(config: Config) -> tg::Result<Self> {
		todo!()
	}

	pub async fn spawn(&self, command: Command) -> tg::Result<Process> {
		todo!()
	}

	pub async fn wait(&self) -> tg::Result<u8> {
		todo!()
	}
}

pub fn run(config: Config) -> tg::Result<()> {
	// Create a current thread tokio runtime.
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	// Bind the listener.
	let listener = {
		let guard = runtime.enter();
		let listener = Server::bind(&config.socket_path)?;
		drop(guard);
		listener
	};

	// Enter the sandbox.
	let enter_output = {
		#[cfg(target_os = "macos")]
		{
			crate::darwin::enter(&config)?
		}
		#[cfg(target_os = "linux")]
		{
			crate::linux::enter(&config)?
		}
	};

	// Run the server.
	runtime.block_on(async move {
		let server =
			Server::new().map_err(|source| tg::error!(!source, "failed to start the server"))?;

		if let Some(fd) = config.ready_fd {
			let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
			let ready = Ready {
				root_path: enter_output.root_path,
			};
			let bytes = serde_json::to_vec(&ready)
				.map_err(|source| tg::error!(!source, "failed to serialize the ready payload"))?;
			file.write_all(&bytes)
				.map_err(|source| tg::error!(!source, "failed to write the ready signal"))?;
			drop(file);
		}

		// Serve.
		let listener = tokio::net::UnixListener::from_std(listener)
			.inspect_err(|error| eprintln!("failed to convert the listener: {error}"))
			.map_err(|source| tg::error!(!source, "failed to convert the listener"))?;
		server
			.serve(listener)
			.await
			.inspect_err(|error| eprintln!("failed to serve: {error}"))?;

		Ok::<_, tg::Error>(())
	})
}
