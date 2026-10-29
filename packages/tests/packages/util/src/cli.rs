use {
	crate::{Config, Temp},
	std::{
		fs::File,
		io::Read as _,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::process::CommandExt as _,
		},
		path::{Path, PathBuf},
		process::{Child, Command, Stdio},
		time::Duration,
	},
	tangram_client::prelude::*,
};

/// A server that runs as a `tangram serve` subprocess.
pub struct Server {
	child: Option<Child>,
	config: Config,
	tangram: PathBuf,
	temp: Temp,
}

impl Server {
	/// Start a server with the `tangram` binary at the path, such as `env!("CARGO_BIN_EXE_tangram")`, in a new temporary directory.
	pub async fn start(tangram: impl Into<PathBuf>, config: Config) -> tg::Result<Self> {
		let tangram = tangram.into();
		crate::semaphore::record(&config.semaphore_names());
		let temp = Temp::new()?;

		// Write the configuration.
		let bytes = serde_json::to_vec_pretty(config.value())
			.map_err(|error| tg::error!(!error, "failed to serialize the configuration"))?;
		let path = temp.path().join("config.json");
		std::fs::write(&path, bytes)
			.map_err(|error| tg::error!(!error, "failed to write the configuration"))?;

		// Start the server.
		let child = spawn(&tangram, temp.path()).await?;
		let server = Self {
			child: Some(child),
			config,
			tangram,
			temp,
		};

		Ok(server)
	}

	pub fn client(&self) -> tg::Result<tg::Client> {
		crate::client::client(&self.path())
	}

	/// Kill the server and the rest of its process group with `SIGKILL`, as a crash would, and wait for the server to exit.
	pub async fn kill(&mut self) -> tg::Result<()> {
		let Some(child) = self.child.take() else {
			return Ok(());
		};
		signal_group(&child, libc::SIGKILL);
		wait(child).await?;

		Ok(())
	}

	/// Get the server's directory.
	#[must_use]
	pub fn path(&self) -> PathBuf {
		self.temp.path().join("server")
	}

	/// Stop the server gracefully if it is running, and start it again with the same directory.
	pub async fn restart(&mut self) -> tg::Result<()> {
		self.shutdown().await?;
		let child = spawn(&self.tangram, self.temp.path()).await?;
		self.child = Some(child);

		Ok(())
	}

	/// Stop the server gracefully and remove its directory.
	pub async fn stop(mut self) -> tg::Result<()> {
		self.shutdown().await?;

		Ok(())
	}

	/// Interrupt the server and wait for it to exit, killing its process group if it does not exit in time.
	async fn shutdown(&mut self) -> tg::Result<()> {
		let Some(child) = self.child.take() else {
			return Ok(());
		};
		let pid = pid(&child);
		// SAFETY: The child has not been reaped, so the ID still refers to it.
		unsafe { libc::kill(pid, libc::SIGINT) };
		let wait = wait(child);
		tokio::pin!(wait);
		if tokio::time::timeout(Duration::from_secs(30), &mut wait)
			.await
			.is_err()
		{
			// SAFETY: The child has not been reaped, because the wait has not returned, so its process group still exists.
			unsafe { libc::kill(-pid, libc::SIGKILL) };
			wait.await?;
		}

		Ok(())
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		// Kill the server, and print its log if the test is failing.
		if let Some(mut child) = self.child.take() {
			signal_group(&child, libc::SIGKILL);
			child.wait().ok();
		}
		if std::thread::panicking()
			&& let Ok(log) = std::fs::read_to_string(self.temp.path().join("log"))
		{
			eprintln!("{log}");
		}
		crate::semaphore::unlink(&self.config.semaphore_names());
	}
}

/// Spawn `tangram serve` and wait for it to signal that it is ready.
async fn spawn(tangram: &Path, path: &Path) -> tg::Result<Child> {
	// Create the ready pipe. Both ends close on exec, and the child duplicates the write end to descriptor 3.
	let mut fds = [0; 2];
	// SAFETY: The array has room for the two descriptors.
	if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(!error, "failed to create the ready pipe"));
	}
	for fd in fds {
		// SAFETY: The descriptor was just created.
		unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
	}
	// SAFETY: The descriptors were just created and are owned by nothing else.
	let (reader, writer) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };

	// Create the command, sending its output to the log.
	let log = File::create(path.join("log"))
		.map_err(|error| tg::error!(!error, "failed to create the log"))?;
	let stderr = log
		.try_clone()
		.map_err(|error| tg::error!(!error, "failed to clone the log"))?;
	let mut command = Command::new(tangram);
	command
		.arg("--config")
		.arg(path.join("config.json"))
		.arg("--directory")
		.arg(path.join("server"))
		.arg("serve")
		.arg("--ready-fd")
		.arg("3")
		.process_group(0)
		.stdin(Stdio::null())
		.stdout(log)
		.stderr(stderr);

	// Keep Tangram environment variables from the test's environment out of the server's.
	for (key, _) in std::env::vars_os() {
		if key.to_str().is_some_and(|key| key.starts_with("TANGRAM_")) {
			command.env_remove(key);
		}
	}
	let writer_fd = writer.as_raw_fd();
	// SAFETY: The closure calls only async-signal-safe functions.
	unsafe {
		command.pre_exec(move || {
			if libc::dup2(writer_fd, 3) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
	let mut child = command.spawn().map_err(
		|error| tg::error!(!error, path = %tangram.display(), "failed to spawn the server"),
	)?;
	drop(writer);

	// Wait for the ready signal. The pipe closes without one if the server exits first.
	let ready = tokio::task::spawn_blocking(move || {
		let mut file = File::from(reader);
		let mut byte = [0u8; 1];
		file.read(&mut byte)
	})
	.await
	.map_err(|error| tg::error!(!error, "the ready task panicked"))?
	.map_err(|error| tg::error!(!error, "failed to read the ready signal"))?;
	if ready == 0 {
		child.wait().ok();
		let log = std::fs::read_to_string(path.join("log")).unwrap_or_default();
		return Err(tg::error!(%log, "the server exited before it was ready"));
	}

	Ok(child)
}

async fn wait(mut child: Child) -> tg::Result<()> {
	tokio::task::spawn_blocking(move || child.wait())
		.await
		.map_err(|error| tg::error!(!error, "the wait task panicked"))?
		.map_err(|error| tg::error!(!error, "failed to wait for the server"))?;

	Ok(())
}

/// Signal the child's process group, which the child leads.
fn signal_group(child: &Child, signal: libc::c_int) {
	// SAFETY: The child has not been reaped, so its process group still exists.
	unsafe { libc::kill(-pid(child), signal) };
}

fn pid(child: &Child) -> libc::pid_t {
	libc::pid_t::try_from(child.id()).unwrap()
}
