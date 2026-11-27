use {
	crate::Cli,
	std::{
		os::fd::AsRawFd as _,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, Interest, unix::AsyncFd},
};

/// The maximum number of bytes of the server's output and of its log to include in a startup error.
const LOG_TAIL_LENGTH: usize = 8192;

/// The time to wait for the rest of the server's output after it exits during startup.
const OUTPUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

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
pub(crate) enum Action {
	Restart,
	Start,
	Stop,
}

/// The outcome of an attempt to terminate the server with SIGINT and then SIGTERM.
enum Terminate {
	/// The server was left running, because `tangram server start` did not start it.
	Refused {
		manager: Option<tg::server::Manager>,
	},

	/// The server is still running after SIGTERM.
	Running(Process),

	/// The server stopped, or no server was running.
	Stopped,
}

/// A handle to a process that is not a child of this process. On Linux, it is a pidfd, which signals and waits for the process that it was opened for even if that process exits and its pid is reused.
struct Process {
	#[cfg(not(target_os = "linux"))]
	handle: std::sync::Arc<std::sync::Mutex<waitpid_any::WaitHandle>>,

	#[cfg(not(target_os = "linux"))]
	pid: libc::pid_t,

	#[cfg(target_os = "linux")]
	pidfd: AsyncFd<std::os::fd::OwnedFd>,
}

impl Cli {
	/// Spawn a server, and refuse if the config declares a manager other than `tangram server start`.
	pub async fn spawn_server(&self, client: &tg::Client) -> tg::Result<()> {
		// Refuse to start a server that another manager starts.
		if let Some(manager) = self.server_manager()
			&& manager != tg::server::Manager::Cli
		{
			return Err(server_manager_error(Some(manager), Action::Start));
		}

		// Ensure the directory exists, and when the access is `group`, create it so that only the owner may list it and only the group may enter it, as the packaged service does.
		let directory = self.server_directory_path()?;
		let directory = std::path::absolute(&directory)
			.map_err(|error| tg::error!(!error, "failed to get the absolute directory path"))?;
		let access = self.server_config().access;
		let mut builder = tokio::fs::DirBuilder::new();
		builder.recursive(true);
		if access == tg::server::Access::Group {
			builder.mode(0o710);
		}
		builder
			.create(&directory)
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

		// Get the path to the current executable.
		let executable = tangram_util::env::current_exe()
			.map_err(|error| tg::error!(!error, "failed to get the current executable path"))?;

		// Get the names of the existing log files, so that a startup failure reports only the log of this server.
		let log_directory = directory.join("log");
		let log_names = crate::log::list(&log_directory).unwrap_or_default();

		// Create the pipe that receives the server's output until the server redirects its output to its log, and read it as it arrives so that the server never blocks writing to it.
		let (reader, writer) = std::io::pipe()
			.map_err(|error| tg::error!(!error, "failed to create the output pipe"))?;
		let stdout = writer
			.try_clone()
			.map_err(|error| tg::error!(!error, "failed to duplicate the output pipe"))?;
		let stderr = writer;
		let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader.into())
			.map_err(|error| tg::error!(!error, "failed to register the output pipe"))?;
		let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
		let output_task = tokio::spawn({
			let output = output.clone();
			async move { read_output(reader, &output).await }
		});

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
		args.push("--access".to_owned());
		args.push(access.to_string());
		args.push("--log".to_owned());
		args.push("--manager".to_owned());
		args.push(tg::server::Manager::Cli.to_string());
		command
			.args(args)
			.current_dir("/")
			.env("NOTIFY_SOCKET", listener.get_ref().path())
			.stdin(std::process::Stdio::null())
			.stdout(stdout)
			.stderr(stderr);
		// SAFETY: The closure calls only `setsid` and `umask`, which are async-signal-safe.
		unsafe {
			command.pre_exec(move || {
				let id = libc::setsid();
				if id < 0 {
					return Err(std::io::Error::last_os_error());
				}

				// Restrict the permissions of the server's files when the access is `group`, so that the group may use only what that access opens, as the packaged service's umask does.
				if access == tg::server::Access::Group {
					libc::umask(0o077);
				}

				Ok(())
			});
		}

		let mut server = command
			.spawn()
			.map_err(|error| tg::error!(!error, "failed to spawn the server"))?;

		// Close this process's ends of the output pipe, so that the pipe ends when the server redirects its output or exits.
		drop(command);

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

		// Stop the server if it did not become ready, and report its output and its log unless a concurrent server is running.
		if let Err(error) = ready {
			server.start_kill().ok();
			server.wait().await.ok();
			if client.connect().await.is_ok() {
				return Ok(());
			}
			tokio::time::timeout(OUTPUT_TIMEOUT, output_task).await.ok();
			let output = output.lock().unwrap().clone();
			let output = String::from_utf8_lossy(&output).trim().to_owned();
			let log = read_new_log_tail(&log_directory, &log_names).await;
			let mut message = "failed to start the server".to_owned();
			if !output.is_empty() {
				message.push_str("\noutput:\n");
				message.push_str(&output);
			}
			let Some((path, log)) = log else {
				return Err(tg::error!(!error, "{message}"));
			};
			if !log.is_empty() {
				message.push_str("\nlog:\n");
				message.push_str(&log);
			}
			return Err(tg::error!(!error, path = %path.display(), "{message}"));
		}
		output_task.abort();
		if client.connect().await.is_err() {
			return Err(tg::error!(url = %client.url(), "failed to connect to the server"));
		}

		Ok(())
	}

	/// Get the manager that starts and stops the server, which the config declares if it is running, or on macOS, the Tangram app if this executable is in an app bundle.
	#[must_use]
	pub fn server_manager(&self) -> Option<tg::server::Manager> {
		if let Some(manager) = self.declared_server_manager() {
			return Some(manager);
		}
		#[cfg(target_os = "macos")]
		if tangram_util::env::current_exe().is_ok_and(|path| is_app_helper_path(&path)) {
			return Some(tg::server::Manager::App);
		}
		None
	}

	/// Get the manager that the config declares, if it is running, so that a server whose declared manager is not running is managed as if the config declared no manager.
	#[must_use]
	pub fn declared_server_manager(&self) -> Option<tg::server::Manager> {
		self.config
			.as_ref()
			.and_then(|config| config.manager)
			.filter(|manager| is_manager_running(*manager))
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

	/// Stop the server if `tangram server start` started it, sending SIGKILL if it does not exit after SIGTERM, and refuse otherwise.
	pub async fn force_stop_server(&self) -> tg::Result<()> {
		match self.try_force_stop_server().await? {
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
		match self.try_stop_server_inner().await? {
			Terminate::Refused { manager } => Ok(Stop::Refused { manager }),
			Terminate::Running(_) => Err(tg::error!("failed to terminate the server")),
			Terminate::Stopped => Ok(Stop::Stopped),
		}
	}

	/// Stop the server if `tangram server start` started it, sending SIGKILL if it does not exit after SIGTERM, and leave it running otherwise.
	pub async fn try_force_stop_server(&self) -> tg::Result<Stop> {
		// Terminate the server.
		let process = match self.try_stop_server_inner().await? {
			Terminate::Refused { manager } => {
				return Ok(Stop::Refused { manager });
			},
			Terminate::Running(process) => process,
			Terminate::Stopped => {
				return Ok(Stop::Stopped);
			},
		};

		// Send SIGKILL to the server.
		if let Err(error) = process.signal(libc::SIGKILL) {
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(Stop::Stopped);
			}
			return Err(tg::error!(!error, "failed to send SIGKILL to the server"));
		}

		// Wait for the server to exit, which it cannot prevent after SIGKILL.
		process
			.wait_for_exit()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the server process"))?;

		Ok(Stop::Stopped)
	}

	/// Send SIGINT and then SIGTERM to the server if `tangram server start` started it, and return the server's process if it is still running.
	async fn try_stop_server_inner(&self) -> tg::Result<Terminate> {
		// Refuse to stop a server that the config declares another manager for, whose lock file may not be in this directory or readable by this user.
		if let Some(manager) = self.server_manager()
			&& manager != tg::server::Manager::Cli
		{
			let terminate = Terminate::Refused {
				manager: Some(manager),
			};
			return Ok(terminate);
		}

		// Open a handle to the process whose pid the lock file contains, then read the lock again to confirm that a process still holds the lock and that the pid is unchanged, so that the handle refers to the server and the manager belongs to it.
		let (lock, pid, process) = loop {
			let Some(lock) = self.try_read_server_lock().await? else {
				return Ok(Terminate::Stopped);
			};
			let pid = libc::pid_t::try_from(lock.pid)
				.map_err(|error| tg::error!(!error, pid = %lock.pid, "invalid lock file"))?;
			let process = Process::try_open(pid).map_err(
				|error| tg::error!(!error, %pid, "failed to open a handle to the server process"),
			)?;
			let Some(process) = process else {
				return Ok(Terminate::Stopped);
			};
			let Some(confirmation) = self.try_read_server_lock().await? else {
				return Ok(Terminate::Stopped);
			};
			if confirmation.pid == lock.pid {
				break (confirmation, pid, process);
			}
		};

		// Refuse to stop a server that `tangram server start` did not start.
		if lock.manager != Some(tg::server::Manager::Cli) {
			let terminate = Terminate::Refused {
				manager: lock.manager,
			};
			return Ok(terminate);
		}

		// Send SIGINT to the server.
		if let Err(error) = process.signal(libc::SIGINT) {
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(Terminate::Stopped);
			}
			if error.raw_os_error() == Some(libc::EPERM) {
				let hint = "run the command as the user that started the server";
				return Err(tg::error!(
					!error,
					%hint,
					%pid,
					"permission denied to stop the server"
				));
			}
			return Err(tg::error!(!error, "failed to send SIGINT to the server"));
		}

		// Wait for the server to exit.
		let server_config = self.server_config();
		let exited = process
			.wait(server_config.stop_timeout)
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the server process"))?;
		if exited {
			return Ok(Terminate::Stopped);
		}

		// If the server has still not exited, then send SIGTERM to the server.
		if let Err(error) = process.signal(libc::SIGTERM) {
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(Terminate::Stopped);
			}
			return Err(tg::error!(!error, "failed to send SIGTERM to the server"));
		}

		// Wait for the server to exit.
		let exited = process
			.wait(server_config.kill_timeout)
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the server process"))?;
		if exited {
			return Ok(Terminate::Stopped);
		}

		// If the server has still not exited, then return its process.
		Ok(Terminate::Running(process))
	}

	/// Read the lock file in the server's directory, returning `None` if no server holds the lock.
	pub async fn try_read_server_lock(&self) -> tg::Result<Option<tg::server::Lock>> {
		let path = self.server_directory_path()?.join("lock");
		let lock = match try_read_lock(&path).await {
			Ok(lock) => lock,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
				let hint = "run the command as the user that runs the server, or as a member of its group if the server's access is `group`";
				return Err(tg::error!(
					!error,
					%hint,
					path = %path.display(),
					"permission denied to read the server's lock file"
				));
			},
			Err(error) => {
				return Err(
					tg::error!(!error, path = %path.display(), "failed to read the lock file"),
				);
			},
		};
		Ok(lock)
	}

	/// Read the lock file in the server's directory, returning `None` if no server holds the lock or if this user may not read it.
	pub async fn try_read_server_lock_if_readable(&self) -> tg::Result<Option<tg::server::Lock>> {
		let path = self.server_directory_path()?.join("lock");
		let lock = match try_read_lock(&path).await {
			Ok(lock) => lock,
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
				) =>
			{
				return Ok(None);
			},
			Err(error) => {
				return Err(
					tg::error!(!error, path = %path.display(), "failed to read the lock file"),
				);
			},
		};
		Ok(lock)
	}
}

impl Process {
	/// Open a handle to the process, returning `None` if it does not exist.
	#[cfg(target_os = "linux")]
	fn try_open(pid: libc::pid_t) -> std::io::Result<Option<Self>> {
		// SAFETY: The system call takes only scalar arguments and returns a new descriptor.
		let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
		if fd < 0 {
			let error = std::io::Error::last_os_error();
			if error.raw_os_error() == Some(libc::ESRCH) {
				return Ok(None);
			}
			return Err(error);
		}
		let fd = std::os::fd::RawFd::try_from(fd).map_err(std::io::Error::other)?;
		// SAFETY: The system call succeeded, so the descriptor is open and nothing else owns it.
		let pidfd = unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) };
		// SAFETY: The handle owns the pidfd, which stays open and unchanged for the lifetime of the `AsyncFd`.
		let pidfd = unsafe { AsyncFd::register_with_interest(pidfd, Interest::READABLE) }?;
		let process = Self { pidfd };
		Ok(Some(process))
	}

	/// Open a handle to the process, returning `None` if it does not exist.
	#[cfg(not(target_os = "linux"))]
	fn try_open(pid: libc::pid_t) -> std::io::Result<Option<Self>> {
		let handle = match waitpid_any::WaitHandle::open(pid) {
			Ok(handle) => handle,
			Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
				return Ok(None);
			},
			Err(error) => {
				return Err(error);
			},
		};
		let handle = std::sync::Arc::new(std::sync::Mutex::new(handle));
		let process = Self { handle, pid };
		Ok(Some(process))
	}

	/// Send a signal to the process through its pidfd.
	#[cfg(target_os = "linux")]
	fn signal(&self, signal: libc::c_int) -> std::io::Result<()> {
		// SAFETY: The descriptor is an open pidfd, and a null info pointer asks the kernel to fill in the signal's info.
		let ret = unsafe {
			libc::syscall(
				libc::SYS_pidfd_send_signal,
				self.pidfd.as_raw_fd(),
				signal,
				std::ptr::null::<libc::siginfo_t>(),
				0,
			)
		};
		if ret != 0 {
			return Err(std::io::Error::last_os_error());
		}
		Ok(())
	}

	/// Send a signal to the process by its pid.
	#[cfg(not(target_os = "linux"))]
	fn signal(&self, signal: libc::c_int) -> std::io::Result<()> {
		// SAFETY: The call takes only scalar arguments.
		let ret = unsafe { libc::kill(self.pid, signal) };
		if ret != 0 {
			return Err(std::io::Error::last_os_error());
		}
		Ok(())
	}

	/// Wait for the process to exit.
	#[cfg(target_os = "linux")]
	async fn wait_for_exit(&self) -> std::io::Result<()> {
		let _guard = self.pidfd.readable().await?;
		Ok(())
	}

	/// Wait for the process to exit.
	#[cfg(not(target_os = "linux"))]
	async fn wait_for_exit(&self) -> std::io::Result<()> {
		let handle = self.handle.clone();
		tokio::task::spawn_blocking(move || handle.lock().unwrap().wait())
			.await
			.map_err(std::io::Error::other)??;
		Ok(())
	}

	/// Wait for the process to exit, returning whether it exited before the timeout.
	#[cfg(target_os = "linux")]
	async fn wait(&self, timeout: std::time::Duration) -> std::io::Result<bool> {
		match tokio::time::timeout(timeout, self.pidfd.readable()).await {
			Ok(Ok(_)) => Ok(true),
			Ok(Err(error)) => Err(error),
			Err(_) => Ok(false),
		}
	}

	/// Wait for the process to exit, returning whether it exited before the timeout.
	#[cfg(not(target_os = "linux"))]
	async fn wait(&self, timeout: std::time::Duration) -> std::io::Result<bool> {
		let handle = self.handle.clone();
		let status =
			tokio::task::spawn_blocking(move || handle.lock().unwrap().wait_timeout(timeout))
				.await
				.map_err(std::io::Error::other)??;
		let exited = status.is_some();
		Ok(exited)
	}
}

/// Read and parse the lock file at the path, returning `None` if it is empty or invalid, or if no process holds the lock on it, because the server that wrote it exited without truncating it.
async fn try_read_lock(path: &Path) -> std::io::Result<Option<tg::server::Lock>> {
	// Read the lock file.
	let mut file = tokio::fs::File::open(path).await?;
	let mut bytes = Vec::new();
	file.read_to_end(&mut bytes).await?;
	let Some(lock) = tg::server::Lock::try_parse(&bytes) else {
		return Ok(None);
	};

	// Return `None` if no process holds the lock on the file.
	let file = file.into_std().await;
	if !is_lock_held(&file, lock.pid)? {
		return Ok(None);
	}

	Ok(Some(lock))
}

/// Check whether the process holds the server's `flock` lock on the file, without taking the lock, by finding the lock in `/proc/locks`. The lock is matched by the inode of the file and by the pid, because `/proc/locks` reports the device of the superblock, which differs from the device that `stat` reports on some file systems.
#[cfg(target_os = "linux")]
fn is_lock_held(file: &std::fs::File, pid: u32) -> std::io::Result<bool> {
	let inode = std::os::unix::fs::MetadataExt::ino(&file.metadata()?);
	let locks = std::fs::read_to_string("/proc/locks")?;
	let held = locks.lines().any(|line| is_flock_line(line, inode, pid));
	Ok(held)
}

/// Check whether a process holds a `flock` lock on the file, without taking the lock, with `F_GETLK`, which reports `flock` locks on macOS. The pid is not checked, because `F_GETLK` does not report the pid of a `flock` lock.
#[cfg(not(target_os = "linux"))]
fn is_lock_held(file: &std::fs::File, _pid: u32) -> std::io::Result<bool> {
	#[expect(
		clippy::cast_possible_truncation,
		reason = "`SEEK_SET` is zero, which fits in a short"
	)]
	let mut lock = libc::flock {
		l_len: 0,
		l_pid: 0,
		l_start: 0,
		l_type: libc::F_WRLCK,
		l_whence: libc::SEEK_SET as libc::c_short,
	};
	// SAFETY: The descriptor is open, and the pointer refers to a valid `flock` for the duration of the call.
	let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &raw mut lock) };
	if ret != 0 {
		return Err(std::io::Error::last_os_error());
	}
	let held = lock.l_type != libc::F_UNLCK;
	Ok(held)
}

/// Check whether a line of `/proc/locks` is a `flock` lock that the process holds on the inode. A line whose type follows `->` is a request that waits for the lock, which it does not hold.
#[cfg_attr(
	not(target_os = "linux"),
	allow(dead_code, reason = "`/proc/locks` exists only on Linux")
)]
fn is_flock_line(line: &str, inode: u64, pid: u32) -> bool {
	let mut fields = line.split_ascii_whitespace().skip(1);
	if fields.next() != Some("FLOCK") {
		return false;
	}
	let mut fields = fields.skip(2);
	let holder = fields.next().and_then(|field| field.parse::<u32>().ok());
	let file_inode = fields
		.next()
		.and_then(|field| field.rsplit(':').next())
		.and_then(|field| field.parse::<u64>().ok());
	holder == Some(pid) && file_inode == Some(inode)
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

/// Read the server's output from the pipe until it ends, keeping only its end.
async fn read_output(
	mut reader: tokio::net::unix::pipe::Receiver,
	output: &std::sync::Mutex<Vec<u8>>,
) {
	let mut buffer = vec![0; 4096];
	while let Ok(n) = reader.read(&mut buffer).await {
		if n == 0 {
			break;
		}
		let mut output = output.lock().unwrap();
		output.extend_from_slice(&buffer[..n]);
		let excess = output.len().saturating_sub(LOG_TAIL_LENGTH);
		output.drain(..excess);
	}
}

/// Read the end of the newest log file that is not one of the existing files, returning its path and its end.
async fn read_new_log_tail(directory: &Path, existing: &[String]) -> Option<(PathBuf, String)> {
	let names = crate::log::list(directory).ok()?;
	let name = names.iter().rev().find(|name| !existing.contains(name))?;
	let path = directory.join(name);
	let Ok(mut file) = tokio::fs::File::open(&path).await else {
		return Some((path, String::new()));
	};
	let Ok(metadata) = file.metadata().await else {
		return Some((path, String::new()));
	};
	let start = metadata.len().saturating_sub(LOG_TAIL_LENGTH as u64);
	if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
		return Some((path, String::new()));
	}
	let mut bytes = Vec::new();
	if file.read_to_end(&mut bytes).await.is_err() {
		return Some((path, String::new()));
	}
	let log = String::from_utf8_lossy(&bytes).trim().to_owned();
	Some((path, log))
}

/// Create the error for a command that only the server's manager may run, with a hint for how to run it.
fn server_manager_error(manager: Option<tg::server::Manager>, action: Action) -> tg::Error {
	let message = match manager {
		Some(tg::server::Manager::App) => "the server is managed by the Tangram app",
		Some(tg::server::Manager::Cli) => "the server is managed by `tangram server start`",
		Some(tg::server::Manager::Openrc) => "the server is managed by OpenRC",
		Some(tg::server::Manager::Systemd) => "the server is managed by systemd",
		None => "the server was not started by `tangram server start`",
	};
	let hint = server_manager_hint(manager, action);
	tg::error!(%hint, "{message}")
}

/// Create the hint for how to run a command that only the server's manager may run.
#[must_use]
pub(crate) fn server_manager_hint(manager: Option<tg::server::Manager>, action: Action) -> String {
	let verb = match action {
		Action::Restart => "restart",
		Action::Start => "start",
		Action::Stop => "stop",
	};
	match (manager, action) {
		(Some(tg::server::Manager::App), Action::Start) => "open the Tangram app".to_owned(),
		(Some(tg::server::Manager::App), Action::Restart | Action::Stop) => {
			format!("use the Tangram app to {verb} the server")
		},
		(Some(tg::server::Manager::Cli) | None, Action::Start) => {
			"run `tangram server start` or pass `--start`".to_owned()
		},
		(Some(tg::server::Manager::Cli), Action::Restart | Action::Stop) => {
			format!("run `tangram server {verb}`")
		},
		(Some(tg::server::Manager::Openrc), _) => format!("run `rc-service tangram {verb}`"),
		(Some(tg::server::Manager::Systemd), _) => format!("run `systemctl {verb} tangram`"),
		(None, Action::Restart) => {
			"stop the process that is running `tangram server run`, then run `tangram server start`"
				.to_owned()
		},
		(None, Action::Stop) => "stop the process that is running `tangram server run`".to_owned(),
	}
}

/// Check whether a manager is running. The `app` and `cli` managers are always running, and the `openrc` and `systemd` managers run only on Linux.
fn is_manager_running(manager: tg::server::Manager) -> bool {
	#[cfg(target_os = "linux")]
	{
		is_manager_running_with_root(manager, Path::new("/"))
	}
	#[cfg(not(target_os = "linux"))]
	{
		matches!(manager, tg::server::Manager::App | tg::server::Manager::Cli)
	}
}

/// Check whether a manager is running on the system whose root is the path. The `systemd` manager is running if `/run/systemd/system` is a directory, as `sd_booted` checks, and the `openrc` manager is running if `/run/openrc/softlevel` exists.
#[cfg_attr(
	not(target_os = "linux"),
	allow(dead_code, reason = "systemd and OpenRC run only on Linux")
)]
fn is_manager_running_with_root(manager: tg::server::Manager, root: &Path) -> bool {
	match manager {
		tg::server::Manager::App | tg::server::Manager::Cli => true,
		tg::server::Manager::Openrc => root.join("run/openrc/softlevel").exists(),
		tg::server::Manager::Systemd => root.join("run/systemd/system").is_dir(),
	}
}

/// Check whether a path is that of an executable in the `Contents/Helpers` directory of an app bundle.
#[cfg_attr(
	not(target_os = "macos"),
	allow(dead_code, reason = "the app bundle is only inferred on macOS")
)]
fn is_app_helper_path(path: &Path) -> bool {
	path.file_name().is_some()
		&& path
			.parent()
			.filter(|helpers| helpers.file_name().is_some_and(|name| name == "Helpers"))
			.and_then(Path::parent)
			.filter(|contents| contents.file_name().is_some_and(|name| name == "Contents"))
			.and_then(Path::parent)
			.is_some_and(|bundle| {
				bundle
					.extension()
					.is_some_and(|extension| extension == "app")
			})
}

#[cfg(test)]
mod tests {
	use {super::*, tangram_util::fs::Temp};

	// The Tangram app and the CLI are running on a system with no runtime directories.
	#[test]
	fn manager_running_app_and_cli() {
		let temp = Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		assert!(is_manager_running_with_root(
			tg::server::Manager::App,
			temp.path()
		));
		assert!(is_manager_running_with_root(
			tg::server::Manager::Cli,
			temp.path()
		));
	}

	// systemd and OpenRC are not running on a system with no runtime directories.
	#[test]
	fn manager_running_none() {
		let temp = Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		assert!(!is_manager_running_with_root(
			tg::server::Manager::Openrc,
			temp.path()
		));
		assert!(!is_manager_running_with_root(
			tg::server::Manager::Systemd,
			temp.path()
		));
	}

	// systemd is running when its runtime directory is a directory, and not when it is a file.
	#[test]
	fn manager_running_systemd() {
		let temp = Temp::new().unwrap();
		std::fs::create_dir_all(temp.path().join("run/systemd")).unwrap();
		std::fs::write(temp.path().join("run/systemd/system"), "").unwrap();
		assert!(!is_manager_running_with_root(
			tg::server::Manager::Systemd,
			temp.path()
		));
		std::fs::remove_file(temp.path().join("run/systemd/system")).unwrap();
		std::fs::create_dir(temp.path().join("run/systemd/system")).unwrap();
		assert!(is_manager_running_with_root(
			tg::server::Manager::Systemd,
			temp.path()
		));
		assert!(!is_manager_running_with_root(
			tg::server::Manager::Openrc,
			temp.path()
		));
	}

	// OpenRC is running when its softlevel file exists.
	#[test]
	fn manager_running_openrc() {
		let temp = Temp::new().unwrap();
		std::fs::create_dir_all(temp.path().join("run/openrc")).unwrap();
		std::fs::write(temp.path().join("run/openrc/softlevel"), "default").unwrap();
		assert!(is_manager_running_with_root(
			tg::server::Manager::Openrc,
			temp.path()
		));
		assert!(!is_manager_running_with_root(
			tg::server::Manager::Systemd,
			temp.path()
		));
	}

	// A `flock` lock in `/proc/locks` matches only the inode and the pid of its holder, and a request that waits for the lock does not match.
	#[test]
	fn flock_line() {
		assert!(is_flock_line(
			"1: FLOCK  ADVISORY  WRITE 1234 00:1a:5678 0 EOF",
			5678,
			1234
		));
		assert!(!is_flock_line(
			"1: FLOCK  ADVISORY  WRITE 1234 00:1a:5678 0 EOF",
			5679,
			1234
		));
		assert!(!is_flock_line(
			"1: FLOCK  ADVISORY  WRITE 1234 00:1a:5678 0 EOF",
			5678,
			1235
		));
		assert!(!is_flock_line(
			"1: POSIX  ADVISORY  WRITE 1234 00:1a:5678 0 EOF",
			5678,
			1234
		));
		assert!(!is_flock_line(
			"1: -> FLOCK  ADVISORY  WRITE 1234 00:1a:5678 0 EOF",
			5678,
			1234
		));
		assert!(!is_flock_line("", 5678, 1234));
	}

	// An executable in the helpers directory of an app bundle is an app helper.
	#[test]
	fn app_helper_path() {
		assert!(is_app_helper_path(Path::new(
			"/Applications/Tangram.app/Contents/Helpers/tangram"
		)));
		assert!(is_app_helper_path(Path::new(
			"/Users/me/Applications/Tangram Dev.app/Contents/Helpers/tangram"
		)));
	}

	// An executable outside the helpers directory of an app bundle is not an app helper.
	#[test]
	fn app_helper_path_invalid() {
		assert!(!is_app_helper_path(Path::new("/usr/local/bin/tangram")));
		assert!(!is_app_helper_path(Path::new(
			"/Applications/Tangram.app/Contents/Resources/tg"
		)));
		assert!(!is_app_helper_path(Path::new(
			"/Applications/Tangram.app/Contents/MacOS/Tangram"
		)));
		assert!(!is_app_helper_path(Path::new(
			"/Applications/Tangram/Contents/Helpers/tangram"
		)));
		assert!(!is_app_helper_path(Path::new(
			"/Applications/Tangram.app/Helpers/tangram"
		)));
		assert!(!is_app_helper_path(Path::new("Contents/Helpers/tangram")));
		assert!(!is_app_helper_path(Path::new(
			"/Applications/Tangram.app/Contents/Helpers"
		)));
	}
}
