use {
	crate::Cli,
	byteorder::ReadBytesExt as _,
	std::{os::fd::AsRawFd as _, path::PathBuf, time::Duration},
	tangram_client::prelude::*,
};

pub mod restart;
pub mod run;
pub mod start;
pub mod status;
pub mod stop;

/// Manage the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Restart(self::restart::Args),
	Run(self::run::Args),
	Start(self::start::Args),
	Status(self::status::Args),
	Stop(self::stop::Args),
}

impl Cli {
	pub async fn command_server(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Restart(args) => {
				self.command_server_restart(args).await?;
			},
			Command::Run(args) => {
				Box::pin(self.command_server_run(args)).await?;
			},
			Command::Start(args) => {
				self.command_server_start(args).await?;
			},
			Command::Status(args) => {
				self.command_server_status(args).await?;
			},
			Command::Stop(args) => {
				self.command_server_stop(args).await?;
			},
		}
		Ok(())
	}

	/// Spawn the server.
	pub(crate) async fn spawn_server(&self, client: &tg::Client) -> tg::Result<()> {
		// Ensure the directory exists.
		let directory = self.directory_path();
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the directory"))?;

		// Acquire an exclusive lock on the start file.
		let start_path = directory.join("start");
		let start_file = tokio::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.truncate(false)
			.open(&start_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the start lock file"))?
			.into_std()
			.await;
		let _start_file = tokio::task::spawn_blocking(move || {
			start_file.lock()?;
			Ok::<_, std::io::Error>(start_file)
		})
		.await
		.map_err(|source| tg::error!(!source, "the start lock task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to lock the start lock file"))?;

		// Attempt to connect.
		if client.connect().await.is_ok() {
			return Ok(());
		}

		// Get the log file path.
		let log_path = directory.join("log");

		// Create files for stdout and stderr.
		let stdout = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.append(true)
			.truncate(false)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;
		let stderr = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.append(true)
			.truncate(false)
			.open(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the log file"))?
			.into_std()
			.await;

		// Get the path to the current executable.
		let executable = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;

		// Spawn the server.
		let mut command = tokio::process::Command::new(executable);

		// Create the ready pipe.
		let (mut ready_reader, ready_writer) = std::io::pipe()
			.map_err(|source| tg::error!(!source, "failed to create the server ready pipe"))?;

		// Unset FD_CLOEXEC on the ready writer.
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

		let mut args = vec![];
		if let Some(config) = &self.args.config {
			args.push("-c".to_owned());
			args.push(config.to_string_lossy().into_owned());
		}
		if let Some(directory) = &self.args.directory {
			args.push("-d".to_owned());
			args.push(directory.to_string_lossy().into_owned());
		}
		if let Some(remotes) = self.args.remotes.get() {
			if remotes.is_empty() {
				args.push("--no-remotes".to_owned());
			} else {
				args.push("-r".to_owned());
				args.push(remotes.join(","));
			}
		}
		if let Some(url) = &self.args.url {
			args.push("-u".to_owned());
			args.push(url.to_string());
		}
		if let Some(tracing) = &self.args.tracing {
			args.push("--tracing".to_owned());
			args.push(tracing.clone());
		}
		args.push("serve".to_owned());
		args.push("--ready-fd".to_owned());
		args.push(ready_fd.to_string());
		command
			.args(args)
			.current_dir(PathBuf::from(std::env::var("HOME").unwrap()))
			.stdin(std::process::Stdio::null())
			.stdout(stdout)
			.stderr(stderr);
		unsafe {
			command.pre_exec(|| {
				let id = libc::setsid();
				if id < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			});
		}

		let mut child = command
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the server"))?;
		drop(ready_writer);

		// Wait for the server to be ready.
		let task = tokio::task::spawn_blocking(move || ready_reader.read_u8());
		let ready = tokio::time::timeout(Duration::from_secs(5), task)
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the server ready signal"))
			.and_then(|output| {
				output.map_err(|source| tg::error!(!source, "the server ready task panicked"))
			})
			.and_then(|output| {
				output
					.map_err(|source| tg::error!(!source, "failed to read the server ready signal"))
			})
			.and_then(|byte| {
				if byte != 0x00 {
					return Err(tg::error!("received an invalid ready byte {byte}"));
				}
				Ok(())
			});
		if let Err(source) = ready {
			child.start_kill().ok();
			child.wait().await.ok();
			if client.connect().await.is_ok() {
				return Ok(());
			}
			return Err(tg::error!(!source, "failed to start the server"));
		}
		if client.connect().await.is_err() {
			return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
		}

		Ok(())
	}

	/// Stop the server.
	pub(crate) async fn stop_server(&self) -> tg::Result<()> {
		// Read the PID from the lock file.
		let lock_path = self.directory_path().join("lock");
		let pid = tokio::fs::read_to_string(&lock_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the pid from the lock file"))?
			.parse::<i32>()
			.map_err(|source| tg::error!(!source, "invalid lock file"))?;

		// Open a wait handle for the process.
		let handle = match waitpid_any::WaitHandle::open(pid) {
			Ok(handle) => handle,
			Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
				return Ok(());
			},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to open a handle to the server process"
				));
			},
		};

		// Send SIGINT to the server.
		let ret = unsafe { libc::kill(pid, libc::SIGINT) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(());
			}
			return Err(tg::error!(!error, "failed to send SIGINT to the server"));
		}

		// Wait up to one second for the server to exit.
		let (mut handle, wait) = tokio::task::spawn_blocking(move || {
			let mut handle = handle;
			let wait = handle.wait_timeout(Duration::from_secs(1))?;
			Ok::<_, std::io::Error>((handle, wait))
		})
		.await
		.map_err(|source| tg::error!(!source, "the server wait task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to wait for the server process"))?;
		if wait.is_some() {
			return Ok(());
		}

		// If the server has still not exited, then send SIGTERM to the server.
		let ret = unsafe { libc::kill(pid, libc::SIGTERM) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(());
			}
			return Err(tg::error!(!error, "failed to send SIGTERM to the server"));
		}

		// Wait up to one second for the server to exit.
		let wait = tokio::task::spawn_blocking(move || handle.wait_timeout(Duration::from_secs(1)))
			.await
			.map_err(|source| tg::error!(!source, "the server wait task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to wait for the server process"))?;
		if wait.is_some() {
			return Ok(());
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}
}
