use {
	crate::Cli,
	std::path::Path,
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, Interest, unix::AsyncFd},
};

/// The maximum number of bytes of the server's log to include in a startup error.
const LOG_TAIL_LENGTH: u64 = 8192;

/// The outcome of an attempt to stop the server.
#[derive(Clone, Copy, Debug)]
pub enum Stop {
	/// The server was left running, because `tangram server start` did not start it.
	Refused {
		manager: Option<tg::server::Manager>,
	},

	/// The server stopped, or no server was running.
	Stopped,
}

/// A command that only the server's manager may run.
#[derive(Clone, Copy, Debug)]
enum Action {
	Restart,
	Stop,
}

impl Cli {
	/// Spawn a server.
	pub async fn spawn_server(&self, client: &tg::Client) -> tg::Result<()> {
		// Ensure the directory exists.
		let directory = self.directory_path()?;
		let directory = std::path::absolute(&directory)
			.map_err(|error| tg::error!(!error, "failed to get the absolute directory path"))?;
		tokio::fs::create_dir_all(&directory)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the directory"))?;

		// Acquire an exclusive lock on the start file.
		let start_path = directory.join("start");
		let start_file = tokio::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.truncate(false)
			.open(&start_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to open the start lock file"))?
			.into_std()
			.await;
		let _start_file = tokio::task::spawn_blocking(move || {
			start_file.lock()?;
			Ok::<_, std::io::Error>(start_file)
		})
		.await
		.map_err(|error| tg::error!(!error, "the start lock task panicked"))?
		.map_err(|error| tg::error!(!error, "failed to lock the start lock file"))?;

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
			.map_err(|error| tg::error!(!error, "failed to open the log file"))?
			.into_std()
			.await;
		let stderr = tokio::fs::OpenOptions::new()
			.create(true)
			.write(true)
			.append(true)
			.truncate(false)
			.open(&log_path)
			.await
			.map_err(|error| tg::error!(!error, "failed to open the log file"))?
			.into_std()
			.await;

		// Get the path to the current executable.
		let executable = tangram_util::env::current_exe()
			.map_err(|error| tg::error!(!error, "failed to get the current executable path"))?;

		// Get the length of the log, so that a startup failure reports only this server's output.
		let log_offset = stdout
			.metadata()
			.map_err(|error| tg::error!(!error, "failed to get the log file metadata"))?
			.len();

		// Create the notify socket, on which the server sends `READY=1` when it is ready.
		let listener = tangram_util::notify::Listener::new()
			.map_err(|error| tg::error!(!error, "failed to create the server notify socket"))?;
		// SAFETY: The listener owns the socket, which stays open and unchanged for the lifetime of the `AsyncFd`.
		let listener = unsafe { AsyncFd::register_with_interest(listener, Interest::READABLE) }
			.map_err(|error| tg::error!(!error, "failed to register the server notify socket"))?;

		// Spawn the server.
		let mut command = tokio::process::Command::new(executable);
		let mut args = vec![];
		if let Some(config) = &self.args.config {
			let config = std::path::absolute(config)
				.map_err(|error| tg::error!(!error, "failed to get the absolute config path"))?;
			args.push("-c".to_owned());
			args.push(config.to_string_lossy().into_owned());
		}
		args.push("-d".to_owned());
		args.push(directory.to_string_lossy().into_owned());
		if let Some(remotes) = self.args.remotes.values() {
			if remotes.is_empty() {
				args.push("--no-remotes".to_owned());
			} else {
				args.push("-r".to_owned());
				args.push(remotes.join(","));
			}
		}
		if let Some(remotes) = self.args.remotes.trusted() {
			args.push("--trusted-remotes".to_owned());
			args.push(remotes.join(","));
		}
		if let Some(url) = &self.args.url {
			let url = Self::absolute_url(url)?;
			args.push("-u".to_owned());
			args.push(url.to_string());
		}
		if let Some(tracing) = &self.args.tracing {
			args.push("--tracing".to_owned());
			args.push(tracing.clone());
		}
		args.push("serve".to_owned());
		args.push("--manager".to_owned());
		args.push(tg::server::Manager::Cli.to_string());
		command
			.args(args)
			.current_dir("/")
			.env("NOTIFY_SOCKET", listener.get_ref().path())
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

		let mut server = command
			.spawn()
			.map_err(|error| tg::error!(!error, "failed to spawn the server"))?;

		// Wait for the server to be ready, to exit, or to time out.
		let start_timeout = self.server_config().start_timeout;
		let ready = tokio::time::timeout(start_timeout, async {
			tokio::select! {
				result = wait_for_ready(&listener) => {
					result.map_err(|error| tg::error!(!error, "failed to receive the server ready notification"))
				},
				result = server.wait() => {
					let status = result.map_err(|error| tg::error!(!error, "failed to wait for the server"))?;
					Err(tg::error!(%status, "the server exited before it was ready"))
				},
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "timed out waiting for the server to be ready"))
		.and_then(|result| result);
		drop(listener);

		// Stop the server if it did not become ready, and report its log unless a concurrent server is running.
		if let Err(error) = ready {
			server.start_kill().ok();
			server.wait().await.ok();
			if client.connect().await.is_ok() {
				return Ok(());
			}
			let log = read_log_tail(&log_path, log_offset).await;
			let error = if log.is_empty() {
				tg::error!(!error, "failed to start the server")
			} else {
				tg::error!(!error, path = %log_path.display(), "failed to start the server\nlog:\n{log}")
			};
			return Err(error);
		}
		if client.connect().await.is_err() {
			return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
		}

		Ok(())
	}

	/// Make a unix socket URL absolute.
	pub fn absolute_url(url: &tangram_uri::Uri) -> tg::Result<tangram_uri::Uri> {
		if url.scheme() != Some("http+unix") {
			return Ok(url.clone());
		}
		let path = url.host().ok_or_else(|| tg::error!(%url, "invalid url"))?;
		let path = std::path::absolute(path)
			.map_err(|error| tg::error!(!error, "failed to get the absolute socket path"))?;
		let path = path
			.to_str()
			.ok_or_else(|| tg::error!("the socket path is not valid UTF-8"))?;
		let url = url
			.to_builder()
			.authority(path)
			.build()
			.map_err(|error| tg::error!(!error, "failed to build the URL"))?;

		Ok(url)
	}

	/// Stop the server if `tangram server start` started it, and refuse otherwise.
	pub async fn stop_server(&self) -> tg::Result<()> {
		match self.try_stop_server().await? {
			Stop::Refused { manager } => Err(server_manager_error(manager, Action::Stop)),
			Stop::Stopped => Ok(()),
		}
	}

	/// Restart the server if `tangram server start` started it, and refuse otherwise.
	pub async fn restart_server(&self, client: &tg::Client) -> tg::Result<()> {
		match self.try_stop_server().await {
			Ok(Stop::Refused { manager }) => {
				return Err(server_manager_error(manager, Action::Restart));
			},
			Ok(Stop::Stopped) => (),
			Err(error) => {
				Self::print_warning_message(&format!("failed to stop existing server. {error}"));
			},
		}
		self.spawn_server(client).await?;
		Ok(())
	}

	/// Stop the server if `tangram server start` started it, and leave it running otherwise.
	pub async fn try_stop_server(&self) -> tg::Result<Stop> {
		// Open a handle to the process that holds the lock, then read the lock again to confirm that the process still holds it, so that the manager belongs to the process that the handle refers to.
		let (lock, pid, handle) = loop {
			let Some(lock) = self.try_read_server_lock().await? else {
				return Ok(Stop::Stopped);
			};
			let pid = i32::try_from(lock.pid)
				.map_err(|error| tg::error!(!error, pid = %lock.pid, "invalid lock file"))?;
			let handle = match waitpid_any::WaitHandle::open(pid) {
				Ok(handle) => handle,
				Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
					return Ok(Stop::Stopped);
				},
				Err(error) => {
					return Err(tg::error!(
						!error,
						"failed to open a handle to the server process"
					));
				},
			};
			let Some(confirmation) = self.try_read_server_lock().await? else {
				return Ok(Stop::Stopped);
			};
			if confirmation.pid == lock.pid {
				break (confirmation, pid, handle);
			}
		};

		// Refuse to stop a server that `tangram server start` did not start.
		if lock.manager != Some(tg::server::Manager::Cli) {
			let stop = Stop::Refused {
				manager: lock.manager,
			};
			return Ok(stop);
		}

		// Send SIGINT to the server.
		let ret = unsafe { libc::kill(pid, libc::SIGINT) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(Stop::Stopped);
			}
			return Err(tg::error!(!error, "failed to send SIGINT to the server"));
		}

		// Wait for the server to exit.
		let server_config = self.server_config();
		let stop_timeout = server_config.stop_timeout;
		let (mut handle, status) = tokio::task::spawn_blocking(move || {
			let mut handle = handle;
			let status = handle.wait_timeout(stop_timeout)?;
			Ok::<_, std::io::Error>((handle, status))
		})
		.await
		.map_err(|error| tg::error!(!error, "the server wait task panicked"))?
		.map_err(|error| tg::error!(!error, "failed to wait for the server process"))?;
		if status.is_some() {
			return Ok(Stop::Stopped);
		}

		// If the server has still not exited, then send SIGTERM to the server.
		let ret = unsafe { libc::kill(pid, libc::SIGTERM) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(Stop::Stopped);
			}
			return Err(tg::error!(!error, "failed to send SIGTERM to the server"));
		}

		// Wait for the server to exit.
		let kill_timeout = server_config.kill_timeout;
		let status = tokio::task::spawn_blocking(move || handle.wait_timeout(kill_timeout))
			.await
			.map_err(|error| tg::error!(!error, "the server wait task panicked"))?
			.map_err(|error| tg::error!(!error, "failed to wait for the server process"))?;
		if status.is_some() {
			return Ok(Stop::Stopped);
		}

		// If the server has still not exited, then return an error.
		Err(tg::error!("failed to terminate the server"))
	}

	/// Read the lock file in the server's directory, returning `None` if no server holds the lock.
	pub async fn try_read_server_lock(&self) -> tg::Result<Option<tg::server::Lock>> {
		let path = self.directory_path()?.join("lock");
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) => {
				return Err(
					tg::error!(!error, path = %path.display(), "failed to read the lock file"),
				);
			},
		};
		let lock = tg::server::Lock::try_parse(&bytes);
		Ok(lock)
	}
}

/// Wait for the server to send `READY=1` to the notify socket.
async fn wait_for_ready(listener: &AsyncFd<tangram_util::notify::Listener>) -> std::io::Result<()> {
	loop {
		let mut guard = listener.readable().await?;
		if guard.get_inner().try_receive_ready()? {
			return Ok(());
		}
		guard.clear_ready();
	}
}

/// Read the end of the log that follows the offset.
async fn read_log_tail(path: &Path, offset: u64) -> String {
	let Ok(mut file) = tokio::fs::File::open(path).await else {
		return String::new();
	};
	let Ok(metadata) = file.metadata().await else {
		return String::new();
	};
	let start = offset.max(metadata.len().saturating_sub(LOG_TAIL_LENGTH));
	if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
		return String::new();
	}
	let mut bytes = Vec::new();
	if file.read_to_end(&mut bytes).await.is_err() {
		return String::new();
	}
	String::from_utf8_lossy(&bytes).trim().to_owned()
}

/// Create the error for a command that only the server's manager may run, with a hint for how to run it.
fn server_manager_error(manager: Option<tg::server::Manager>, action: Action) -> tg::Error {
	let verb = match action {
		Action::Restart => "restart",
		Action::Stop => "stop",
	};
	let (message, hint) = match manager {
		Some(tg::server::Manager::App) => (
			"the server is managed by the Tangram app".to_owned(),
			format!("use the Tangram app to {verb} the server"),
		),
		Some(tg::server::Manager::Cli) => (
			"the server is managed by `tangram server start`".to_owned(),
			format!("run `tangram server {verb}`"),
		),
		Some(tg::server::Manager::Openrc) => (
			"the server is managed by OpenRC".to_owned(),
			format!("run `rc-service tangram {verb}`"),
		),
		Some(tg::server::Manager::Systemd) => (
			"the server is managed by systemd".to_owned(),
			format!("run `systemctl {verb} tangram`"),
		),
		None => {
			let hint = match action {
				Action::Restart => {
					"stop the process that is running `tangram server run`, then run `tangram server start`"
				},
				Action::Stop => "stop the process that is running `tangram server run`",
			};
			(
				"the server was not started by `tangram server start`".to_owned(),
				hint.to_owned(),
			)
		},
	};
	tg::error!(%hint, "{message}")
}
