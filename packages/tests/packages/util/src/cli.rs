use {
	crate::{Config, Instance, Temp},
	std::{
		ffi::OsStr,
		fs::File,
		os::unix::process::CommandExt as _,
		path::{Path, PathBuf},
		process::{Child, Command, Stdio},
		time::Duration,
	},
	tangram_client::prelude::*,
	tokio::io::{Interest, unix::AsyncFd},
};

/// A server that runs as a `tangram serve` subprocess.
pub struct Server {
	child: Option<Child>,
	path: PathBuf,
	semaphores: Vec<String>,
	tangram: PathBuf,
	temp: Temp,
	url: tangram_uri::Uri,
}

impl Server {
	/// Start a server with the `tangram` binary at the path, such as `env!("CARGO_BIN_EXE_tangram")`, in a new temporary directory.
	pub async fn start(tangram: impl Into<PathBuf>, mut config: Config) -> tg::Result<Self> {
		crate::instance::assign_ids(&mut config);
		let semaphores = config.semaphore_names();
		crate::semaphore::record(&semaphores);
		Self::start_inner(tangram.into(), config, None, semaphores).await
	}

	/// Start a server whose directory is at the path, such as a directory on a file system with project quotas. The caller removes the directory.
	pub async fn start_with_path(
		tangram: impl Into<PathBuf>,
		mut config: Config,
		path: impl Into<PathBuf>,
	) -> tg::Result<Self> {
		crate::instance::assign_ids(&mut config);
		let semaphores = config.semaphore_names();
		crate::semaphore::record(&semaphores);
		Self::start_inner(tangram.into(), config, Some(path.into()), semaphores).await
	}

	/// Start a server in an instance with a configuration from `Instance::config`. A server in a local instance uses the instance's directory.
	pub async fn start_with_instance(
		tangram: impl Into<PathBuf>,
		instance: &Instance,
		config: Config,
	) -> tg::Result<Self> {
		let config = instance.server_config(config)?;
		let path = instance.directory().map(ToOwned::to_owned);
		Self::start_inner(tangram.into(), config, path, Vec::new()).await
	}

	async fn start_inner(
		tangram: PathBuf,
		config: Config,
		path: Option<PathBuf>,
		semaphores: Vec<String>,
	) -> tg::Result<Self> {
		let temp = Temp::new()
			.map_err(|error| tg::error!(!error, "failed to create the temporary directory"))?;
		let path = path.unwrap_or_else(|| temp.path().join("server"));
		let url = crate::client::server_url(&config, &path)?;

		// Write the configuration.
		let bytes = serde_json::to_vec_pretty(config.value())
			.map_err(|error| tg::error!(!error, "failed to serialize the configuration"))?;
		std::fs::write(temp.path().join("config.json"), bytes)
			.map_err(|error| tg::error!(!error, "failed to write the configuration"))?;

		// Start the server.
		let child = spawn(&tangram, temp.path(), &path, &url).await?;
		let server = Self {
			child: Some(child),
			path,
			semaphores,
			tangram,
			temp,
			url,
		};

		Ok(server)
	}

	pub fn client(&self) -> tg::Result<tg::Client> {
		crate::client::client(&self.url)
	}

	/// Create a command that runs the `tangram` binary as a client of the server in an empty working directory, with its own configuration file and without the Tangram environment variables from the test's environment.
	pub fn command(&self) -> tg::Result<tokio::process::Command> {
		let work_path = self.temp.path().join("work");
		std::fs::create_dir_all(&work_path)
			.map_err(|error| tg::error!(!error, "failed to create the working directory"))?;
		let mut command = tokio::process::Command::new(&self.tangram);
		for key in tangram_env_keys() {
			command.env_remove(key);
		}
		command
			.env("TANGRAM_QUIET", "true")
			.arg("--config")
			.arg(self.temp.path().join("client.json"))
			.arg("--url")
			.arg(self.url.to_string())
			.current_dir(work_path)
			.stdin(Stdio::null());

		Ok(command)
	}

	/// Run the CLI against the server with a token if one is given, and return its trimmed standard output, or an error with its standard error if it fails.
	pub async fn tg<I, S>(&self, token: Option<&str>, args: I) -> tg::Result<String>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let args = args
			.into_iter()
			.map(|arg| arg.as_ref().to_owned())
			.collect::<Vec<_>>();
		let output = self.output(token, &args).await?;
		if !output.status.success() {
			let stderr = String::from_utf8_lossy(&output.stderr);
			return Err(tg::error!(%stderr, ?args, "the command failed"));
		}
		let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();

		Ok(stdout)
	}

	/// Run the CLI against the server with a token if one is given, and return its output.
	pub async fn output<I, S>(
		&self,
		token: Option<&str>,
		args: I,
	) -> tg::Result<std::process::Output>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let mut command = self.command()?;
		if let Some(token) = token {
			command.arg("--token").arg(token);
		}
		let output = command
			.args(args)
			.output()
			.await
			.map_err(|error| tg::error!(!error, "failed to run the command"))?;

		Ok(output)
	}

	/// Log in a user with the insecure provider.
	pub async fn login(&self, name: &str) -> tg::Result<tg::user::login::wait::Output> {
		let output = self
			.tg(None, ["login", "--verbose", "--name", name])
			.await?;
		deserialize(&output)
	}

	/// Create a runner, owned by the owner if one is given.
	pub async fn create_runner(
		&self,
		token: &str,
		owner: Option<&tg::principal::Selector>,
	) -> tg::Result<tg::runner::create::Output> {
		let mut args = vec!["runner".to_owned(), "create".to_owned()];
		if let Some(owner) = owner {
			args.push("--owner".to_owned());
			args.push(owner.to_string());
		}
		let output = self.tg(Some(token), args).await?;
		deserialize(&output)
	}

	/// Spawn a process with the arguments to `tg spawn`.
	pub async fn spawn<I, S>(&self, token: &str, args: I) -> tg::Result<tg::process::spawn::Output>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<OsStr>,
	{
		let args = ["spawn", "--no-tokens", "--verbose"]
			.into_iter()
			.map(|arg| OsStr::new(arg).to_owned())
			.chain(args.into_iter().map(|arg| arg.as_ref().to_owned()));
		let output = self.tg(Some(token), args).await?;
		deserialize(&output)
	}

	/// Get an object's or a process's data without its tokens.
	pub async fn get<T>(&self, token: Option<&str>, id: &str) -> tg::Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		let output = self.tg(token, ["get", "--no-tokens", id]).await?;
		deserialize(&output)
	}

	/// Watch a checkpoint with the parameters, and return the watch's ID.
	pub async fn watch_checkpoint(
		&self,
		token: Option<&str>,
		name: &str,
		params: serde_json::Value,
	) -> tg::Result<u64> {
		let params = params.to_string();
		let output = self
			.tg(token, ["checkpoint", "watch", name, "--params", &params])
			.await?;
		let output = deserialize::<serde_json::Value>(&output)?;
		let watch = output["watch"]
			.as_u64()
			.ok_or_else(|| tg::error!(%output, "expected the watch"))?;
		Ok(watch)
	}

	/// Wait up to 30 seconds for a hit of a watched checkpoint.
	pub async fn wait_checkpoint(
		&self,
		token: Option<&str>,
		name: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<()> {
		let watch = watch.to_string();
		let hit = hit.to_string();
		self.tg(
			token,
			["checkpoint", "wait", "--timeout", "30s", name, &watch, &hit],
		)
		.await?;
		Ok(())
	}

	/// Let a held hit of a watched checkpoint continue.
	pub async fn continue_checkpoint(
		&self,
		token: Option<&str>,
		name: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<()> {
		let watch = watch.to_string();
		let hit = hit.to_string();
		self.tg(token, ["checkpoint", "continue", name, &watch, &hit])
			.await?;
		Ok(())
	}

	/// Stop watching a checkpoint, which lets every held hit continue.
	pub async fn unwatch_checkpoint(
		&self,
		token: Option<&str>,
		name: &str,
		watch: u64,
	) -> tg::Result<()> {
		let watch = watch.to_string();
		self.tg(token, ["checkpoint", "unwatch", name, &watch])
			.await?;
		Ok(())
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
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Stop the server gracefully if it is running, and start it again with the same directory.
	pub async fn restart(&mut self) -> tg::Result<()> {
		self.shutdown().await?;
		let child = spawn(&self.tangram, self.temp.path(), &self.path, &self.url).await?;
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
		crate::semaphore::unlink(&self.semaphores);
	}
}

fn deserialize<T>(output: &str) -> tg::Result<T>
where
	T: serde::de::DeserializeOwned,
{
	serde_json::from_str(output)
		.map_err(|error| tg::error!(!error, %output, "failed to deserialize the output"))
}

/// Spawn `tangram serve` with the configuration and log in the temporary directory, and wait for it to signal that it is ready.
async fn spawn(
	tangram: &Path,
	temp: &Path,
	path: &Path,
	url: &tangram_uri::Uri,
) -> tg::Result<Child> {
	// Create the notify socket, on which the server sends `READY=1` when it is ready.
	let listener = tangram_util::notify::Listener::new()
		.map_err(|error| tg::error!(!error, "failed to create the notify socket"))?;
	// SAFETY: The listener owns the socket, which stays open and unchanged for the lifetime of the `AsyncFd`.
	let listener = unsafe { AsyncFd::register_with_interest(listener, Interest::READABLE) }
		.map_err(|error| tg::error!(!error, "failed to register the notify socket"))?;

	// Listen for SIGCHLD before spawning, so that the server's exit is not missed.
	let mut sigchld = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
		.map_err(|error| tg::error!(!error, "failed to listen for SIGCHLD"))?;

	// Create the command, sending its output to the log.
	let log = File::options()
		.append(true)
		.create(true)
		.open(temp.join("log"))
		.map_err(|error| tg::error!(!error, "failed to create the log"))?;
	let stderr = log
		.try_clone()
		.map_err(|error| tg::error!(!error, "failed to clone the log"))?;
	let mut command = Command::new(tangram);
	command
		.arg("--config")
		.arg(temp.join("config.json"))
		.arg("--directory")
		.arg(path)
		.arg("--url")
		.arg(url.to_string())
		.arg("serve")
		.env("NOTIFY_SOCKET", listener.get_ref().path())
		.process_group(0)
		.stdin(Stdio::null())
		.stdout(log)
		.stderr(stderr);

	// Keep Tangram environment variables from the test's environment out of the server's.
	for key in tangram_env_keys() {
		command.env_remove(key);
	}
	let mut child = command.spawn().map_err(
		|error| tg::error!(!error, path = %tangram.display(), "failed to spawn the server"),
	)?;

	// Wait for the ready notification, or for the server to exit first.
	loop {
		tokio::select! {
			result = listener.readable() => {
				let mut guard = result
					.map_err(|error| tg::error!(!error, "failed to wait for the notify socket"))?;
				let ready = guard
					.get_inner()
					.try_receive_ready()
					.map_err(|error| tg::error!(!error, "failed to receive the ready notification"))?;
				if ready {
					break;
				}
				guard.clear_ready();
			},
			_ = sigchld.recv() => {
				let status = child
					.try_wait()
					.map_err(|error| tg::error!(!error, "failed to wait for the server"))?;
				if let Some(status) = status {
					let log = std::fs::read_to_string(temp.join("log")).unwrap_or_default();
					return Err(tg::error!(%log, %status, "the server exited before it was ready"));
				}
			},
		}
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

/// Get the names of the Tangram environment variables in the test's environment.
fn tangram_env_keys() -> Vec<std::ffi::OsString> {
	std::env::vars_os()
		.map(|(key, _)| key)
		.filter(|key| key.to_str().is_some_and(|key| key.starts_with("TANGRAM_")))
		.collect()
}
