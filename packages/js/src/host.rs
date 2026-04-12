use {
	bytes::Bytes,
	dashmap::{DashMap, mapref::entry::Entry},
	std::{
		collections::BTreeMap,
		fs::OpenOptions,
		future::Future,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd},
			unix::process::ExitStatusExt as _,
		},
		path::PathBuf,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tokio::io::{Interest, unix::AsyncFd},
};

#[derive(Clone, Default)]
pub struct Host(Arc<State>);

#[derive(Default)]
pub struct State {
	fds: FileDescriptors,
	processes: Processes,
	signals: Signals,
	stoppers: Stoppers,
	termios: Termios,
	next_token: AtomicUsize,
}

type FileDescriptors = DashMap<i32, Arc<AsyncFd<OwnedFd>>, fnv::FnvBuildHasher>;

type Processes = DashMap<u32, tokio::sync::Mutex<tokio::process::Child>, fnv::FnvBuildHasher>;

type Signals = DashMap<usize, Arc<Signal>, fnv::FnvBuildHasher>;

type Stoppers = DashMap<usize, Stopper, fnv::FnvBuildHasher>;

type Termios = DashMap<i32, libc::termios, fnv::FnvBuildHasher>;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SpawnArg {
	pub args: Vec<String>,
	pub cwd: Option<String>,
	pub env: BTreeMap<String, String>,
	pub executable: String,
	pub stderr: Stdio,
	pub stdin: Stdio,
	pub stdout: Stdio,
}

#[derive(Clone, Copy, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stdio {
	Inherit,
	#[default]
	Null,
	Pipe,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpawnOutput {
	pub pid: u32,
	pub stdin: Option<i32>,
	pub stdout: Option<i32>,
	pub stderr: Option<i32>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct WaitOutput {
	pub exit: u8,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
	Sigwinch,
}

struct Signal {
	signal: tokio::sync::Mutex<tokio::signal::unix::Signal>,
	stop: tokio::sync::watch::Sender<bool>,
}

impl Host {
	pub async fn close(&self, fd: i32) -> tg::Result<()> {
		let removed = self.0.fds.remove(&fd);
		if removed.is_none() {
			return Err(tg::error!(%fd, "failed to find the file descriptor"));
		}
		Ok(())
	}

	pub async fn disable_raw_mode(&self, fd: i32) -> tg::Result<()> {
		let original = self
			.0
			.termios
			.remove(&fd)
			.map(|(_, original)| original)
			.ok_or_else(|| tg::error!(%fd, "failed to find the file descriptor raw mode"))?;
		if let Err(error) = set_termios(fd, &original) {
			self.0.termios.insert(fd, original);
			return Err(error);
		}
		Ok(())
	}

	pub async fn enable_raw_mode(&self, fd: i32) -> tg::Result<()> {
		match self.0.termios.entry(fd) {
			Entry::Occupied(_) => Ok(()),
			Entry::Vacant(entry) => {
				let original = get_termios(fd)?;
				let raw = raw_termios(&original);
				set_termios(fd, &raw)?;
				entry.insert(original);
				Ok(())
			},
		}
	}

	pub async fn exists(&self, path: String) -> tg::Result<bool> {
		let path = PathBuf::from(path);
		let exists = tokio::fs::try_exists(&path).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to determine if the path exists"),
		)?;
		Ok(exists)
	}

	pub fn get_tty_size() -> Option<tg::process::tty::Size> {
		let tty = OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.ok();
		let fd = tty.as_ref().map(std::os::fd::AsRawFd::as_raw_fd)?;
		let mut size = unsafe { std::mem::zeroed::<libc::winsize>() };
		if unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } < 0
			|| size.ws_col == 0
			|| size.ws_row == 0
		{
			return None;
		}
		Some(tg::process::tty::Size {
			cols: size.ws_col,
			rows: size.ws_row,
		})
	}

	pub async fn getxattr(&self, path: String, name: String) -> tg::Result<Option<Bytes>> {
		let path_ = PathBuf::from(path);
		let path_display = path_.display().to_string();
		let path_for_task = path_.clone();
		let name_for_task = name.clone();
		let bytes = tokio::task::spawn_blocking(move || xattr::get(&path_for_task, &name_for_task))
			.await
			.map_err(|source| tg::error!(!source, "the xattr task panicked"))?
			.map_err(
				|source| tg::error!(!source, path = %path_display, %name, "failed to read the xattr"),
			)?;
		Ok(bytes.map(Bytes::from))
	}

	pub fn is_tty(fd: i32) -> bool {
		unsafe { libc::isatty(fd) == 1 }
	}

	pub async fn listen_signal_close(&self, token: usize) {
		let signal = self.0.signals.remove(&token).map(|(_, signal)| signal);
		if let Some(signal) = signal {
			let _ = signal.stop.send(true);
		}
	}

	pub async fn listen_signal_open(&self, kind: SignalKind) -> tg::Result<usize> {
		let signal_kind = match kind {
			SignalKind::Sigwinch => tokio::signal::unix::SignalKind::window_change(),
		};
		let signal = tokio::signal::unix::signal(signal_kind)
			.map_err(|source| tg::error!(!source, "failed to create the signal handler"))?;
		let (stop, _) = tokio::sync::watch::channel(false);
		let token = self.0.next_token.fetch_add(1, Ordering::Relaxed) + 1;
		let signal = Arc::new(Signal {
			signal: tokio::sync::Mutex::new(signal),
			stop,
		});
		self.0.signals.insert(token, signal);
		Ok(token)
	}

	pub async fn listen_signal_read(&self, token: usize) -> tg::Result<Option<()>> {
		let signal = self
			.0
			.signals
			.get(&token)
			.map(|signal| signal.value().clone());
		let Some(signal) = signal else {
			return Ok(None);
		};
		let mut stop = signal.stop.subscribe();
		let mut signal_ = signal.signal.lock().await;
		tokio::select! {
			value = signal_.recv() => Ok(value),
			_ = stop.changed() => Ok(None),
		}
	}

	pub async fn mkdtemp(&self) -> tg::Result<String> {
		let path = tempfile::tempdir()
			.map_err(|source| tg::error!(!source, "failed to create a temp directory"))?
			.keep();
		let path = path
			.into_os_string()
			.into_string()
			.map_err(|path| tg::error!(?path, "failed to convert the temp directory path"))?;
		Ok(path)
	}

	pub async fn read(
		&self,
		fd: i32,
		length: Option<usize>,
		stopper: Option<usize>,
	) -> tg::Result<Option<Bytes>> {
		let stopper = self.get_stopper(stopper).await?;
		let fd_ = self.0.fds.get(&fd).map(|fd_| fd_.value().clone());
		let Some(fd_) = fd_ else {
			let length = length.unwrap_or(64 * 1024);
			return match stopper {
				Some(stopper) => read_with_stopper(fd, length, stopper).await,
				None => tokio::task::spawn_blocking(move || read(fd, length))
					.await
					.map_err(|source| tg::error!(!source, "the task panicked"))?,
			};
		};
		let mut buffer = vec![0; length.unwrap_or(64 * 1024)];
		let bytes_read = with_stopper(stopper, async {
			fd_.async_io(Interest::READABLE, |fd_| {
				let bytes_read = unsafe {
					libc::read(
						fd_.as_raw_fd(),
						buffer.as_mut_ptr().cast::<libc::c_void>(),
						buffer.len(),
					)
				};
				if bytes_read < 0 {
					Err(std::io::Error::last_os_error())
				} else {
					Ok(bytes_read.cast_unsigned())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, %fd, "failed to read the file descriptor"))
		})
		.await?;
		if bytes_read == 0 {
			return Ok(None);
		}
		buffer.truncate(bytes_read);
		Ok(Some(buffer.into()))
	}

	pub async fn remove(&self, path: String) -> tg::Result<()> {
		let path = PathBuf::from(path);
		let metadata = match tokio::fs::symlink_metadata(&path).await {
			Ok(metadata) => metadata,
			Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
			Err(source) => {
				return Err(
					tg::error!(!source, path = %path.display(), "failed to read the path metadata"),
				);
			},
		};
		if metadata.is_dir() {
			tokio::fs::remove_dir_all(&path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to remove the directory"),
			)?;
		} else {
			tokio::fs::remove_file(&path).await.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to remove the file"),
			)?;
		}
		Ok(())
	}

	pub async fn signal(&self, pid: u32, signal: tg::process::Signal) -> tg::Result<()> {
		let pid = i32::try_from(pid)
			.map_err(|source| tg::error!(!source, "failed to convert the process id"))?;
		let signal = i32::from(signal as u8);
		let ret = unsafe { libc::kill(pid, signal) };
		if ret < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to signal the process"
			));
		}
		Ok(())
	}

	pub async fn sleep(&self, duration: f64, stopper: Option<usize>) -> tg::Result<()> {
		let stopper = self.get_stopper(stopper).await?;
		with_stopper(stopper, async move {
			tokio::time::sleep(Duration::from_secs_f64(duration)).await;
			Ok(())
		})
		.await
	}

	pub async fn spawn(&self, arg: SpawnArg) -> tg::Result<SpawnOutput> {
		let mut command = tokio::process::Command::new(&arg.executable);
		command.args(&arg.args);
		command.env_clear();
		command.envs(&arg.env);
		if let Some(cwd) = &arg.cwd {
			command.current_dir(cwd);
		}
		command.stdin(arg.stdin.to_std());
		command.stdout(arg.stdout.to_std());
		command.stderr(arg.stderr.to_std());
		let mut child = command.spawn().map_err(|source| {
			tg::error!(
				!source,
				executable = %arg.executable,
				"failed to spawn the process"
			)
		})?;
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("failed to get the process id"))?;
		let stdin_ = child
			.stdin
			.take()
			.map(|stdin| {
				stdin
					.into_owned_fd()
					.map_err(|source| {
						tg::error!(!source, "failed to get the stdin file descriptor")
					})
					.and_then(|value| {
						let fd = value.as_raw_fd();
						let value = AsyncFd::new(value).map_err(
							|source| tg::error!(!source, %fd, "failed to register the file descriptor"),
						)?;
						Ok((fd, Arc::new(value)))
					})
			})
			.transpose()?;
		let stdout_ = child
			.stdout
			.take()
			.map(|stdout| {
				stdout
					.into_owned_fd()
					.map_err(|source| {
						tg::error!(!source, "failed to get the stdout file descriptor")
					})
					.and_then(|value| {
						let fd = value.as_raw_fd();
						let value = AsyncFd::new(value).map_err(
							|source| tg::error!(!source, %fd, "failed to register the file descriptor"),
						)?;
						Ok((fd, Arc::new(value)))
					})
			})
			.transpose()?;
		let stderr_ = child
			.stderr
			.take()
			.map(|stderr| {
				stderr
					.into_owned_fd()
					.map_err(|source| {
						tg::error!(!source, "failed to get the stderr file descriptor")
					})
					.and_then(|value| {
						let fd = value.as_raw_fd();
						let value = AsyncFd::new(value).map_err(
							|source| tg::error!(!source, %fd, "failed to register the file descriptor"),
						)?;
						Ok((fd, Arc::new(value)))
					})
			})
			.transpose()?;
		let stdin = stdin_.as_ref().map(|(fd, _)| *fd);
		let stdout = stdout_.as_ref().map(|(fd, _)| *fd);
		let stderr = stderr_.as_ref().map(|(fd, _)| *fd);

		if let Some((fd, stdin)) = stdin_ {
			self.0.fds.insert(fd, stdin);
		}
		if let Some((fd, stdout)) = stdout_ {
			self.0.fds.insert(fd, stdout);
		}
		if let Some((fd, stderr)) = stderr_ {
			self.0.fds.insert(fd, stderr);
		}
		self.0.processes.insert(pid, tokio::sync::Mutex::new(child));

		Ok(SpawnOutput {
			pid,
			stdin,
			stdout,
			stderr,
		})
	}

	pub async fn stopper_close(&self, token: usize) -> tg::Result<()> {
		let stopper = self
			.stoppers
			.remove(&token)
			.map(|(_, stopper)| stopper)
			.ok_or_else(|| tg::error!(%token, "failed to find the stopper"))?;
		stopper.stop();
		Ok(())
	}

	pub async fn stopper_open(&self) -> tg::Result<usize> {
		let token = self.0.next_token.fetch_add(1, Ordering::Relaxed) + 1;
		self.0.stoppers.insert(token, Stopper::new());
		Ok(token)
	}

	pub async fn stopper_stop(&self, token: usize) -> tg::Result<()> {
		let stopper = self
			.stoppers
			.get(&token)
			.map(|stopper| stopper.value().clone())
			.ok_or_else(|| tg::error!(%token, "failed to find the stopper"))?;
		stopper.stop();
		Ok(())
	}

	pub async fn wait(&self, pid: u32, stopper: Option<usize>) -> tg::Result<WaitOutput> {
		let stopper = self.get_stopper(stopper).await?;
		let child = self
			.0
			.processes
			.remove(&pid)
			.map(|(_, child)| child)
			.ok_or_else(|| tg::error!(%pid, "failed to find the process"))?;
		let mut child = child.into_inner();
		let status = match stopper {
			Some(stopper) => {
				if stopper.stopped() {
					self.0.processes.insert(pid, tokio::sync::Mutex::new(child));
					return Err(stopped_error());
				}
				let result = tokio::select! {
					result = child.wait() => Ok(result),
					() = stopper.wait() => Err(stopped_error()),
				};
				match result {
					Ok(result) => result,
					Err(error) => {
						self.0.processes.insert(pid, tokio::sync::Mutex::new(child));
						return Err(error);
					},
				}
			},
			None => child.wait().await,
		}
		.map_err(|source| tg::error!(!source, %pid, "failed to wait for the process"))?;
		let exit = exit_status_to_code(status)?;
		Ok(WaitOutput { exit })
	}

	pub async fn write(&self, fd: i32, bytes: Bytes) -> tg::Result<()> {
		let fd_ = self.0.fds.get(&fd).map(|fd_| fd_.value().clone());
		let Some(fd_) = fd_ else {
			let bytes = bytes.clone();
			return tokio::task::spawn_blocking(move || Self::write_sync(fd, bytes.as_ref()))
				.await
				.map_err(|source| tg::error!(!source, "the task panicked"))?;
		};
		let mut bytes = bytes.as_ref();
		while !bytes.is_empty() {
			let bytes_written = fd_
				.async_io(Interest::WRITABLE, |fd_| {
					let bytes_written = unsafe {
						libc::write(
							fd_.as_raw_fd(),
							bytes.as_ptr().cast::<libc::c_void>(),
							bytes.len(),
						)
					};
					if bytes_written < 0 {
						Err(std::io::Error::last_os_error())
					} else {
						Ok(bytes_written.cast_unsigned())
					}
				})
				.await
				.map_err(
					|source| tg::error!(!source, %fd, "failed to write the file descriptor"),
				)?;
			bytes = &bytes[bytes_written..];
		}
		Ok(())
	}

	pub fn write_sync(fd: i32, bytes: &[u8]) -> tg::Result<()> {
		let mut bytes = bytes;
		while !bytes.is_empty() {
			let bytes_written =
				unsafe { libc::write(fd, bytes.as_ptr().cast::<libc::c_void>(), bytes.len()) };
			if bytes_written < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					%fd,
					"failed to write the file descriptor"
				));
			}
			bytes = &bytes[bytes_written.cast_unsigned()..];
		}
		Ok(())
	}

	async fn get_stopper(&self, token: Option<usize>) -> tg::Result<Option<Stopper>> {
		let Some(token) = token else {
			return Ok(None);
		};
		let stopper = self
			.0
			.stoppers
			.get(&token)
			.map(|stopper| stopper.value().clone())
			.ok_or_else(|| tg::error!(%token, "failed to find the stopper"))?;
		Ok(Some(stopper))
	}
}

impl Stdio {
	fn to_std(self) -> std::process::Stdio {
		match self {
			Self::Inherit => std::process::Stdio::inherit(),
			Self::Null => std::process::Stdio::null(),
			Self::Pipe => std::process::Stdio::piped(),
		}
	}
}

fn exit_status_to_code(status: std::process::ExitStatus) -> tg::Result<u8> {
	if let Some(code) = status.code() {
		return u8::try_from(code)
			.map_err(|source| tg::error!(!source, "failed to convert the exit code"));
	}
	if let Some(signal) = status.signal() {
		let code = signal
			.checked_add(128)
			.ok_or_else(|| tg::error!("failed to convert the signal"))?;
		return u8::try_from(code)
			.map_err(|source| tg::error!(!source, "failed to convert the signal"));
	}
	Err(tg::error!("failed to determine the exit status"))
}

fn get_termios(fd: i32) -> tg::Result<libc::termios> {
	let mut termios = std::mem::MaybeUninit::<libc::termios>::uninit();
	if unsafe { libc::tcgetattr(fd, termios.as_mut_ptr()) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			%fd,
			"failed to get the file descriptor termios"
		));
	}
	Ok(unsafe { termios.assume_init() })
}

async fn read_with_stopper(fd: i32, length: usize, stopper: Stopper) -> tg::Result<Option<Bytes>> {
	if stopper.stopped() {
		return Err(stopped_error());
	}
	let (wake_read, wake_write) = pipe()?;
	let stop_task = tokio::spawn(async move {
		stopper.wait().await;
		let _ = write_wakeup(wake_write.as_raw_fd());
	});
	let result = tokio::task::spawn_blocking(move || {
		let wake_fd = wake_read.as_raw_fd();
		read_with_wakeup(fd, length, wake_fd)
	})
	.await
	.map_err(|source| tg::error!(!source, "the task panicked"))?;
	stop_task.abort();
	result
}

fn read(fd: i32, length: usize) -> tg::Result<Option<Bytes>> {
	let mut buffer = vec![0; length];
	let bytes_read = loop {
		let bytes_read =
			unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
		if bytes_read >= 0 {
			break bytes_read.cast_unsigned();
		}
		let source = std::io::Error::last_os_error();
		if source.kind() == std::io::ErrorKind::Interrupted {
			continue;
		}
		return Err(tg::error!(
			!source,
			%fd,
			"failed to read the file descriptor"
		));
	};
	if bytes_read == 0 {
		return Ok(None);
	}
	buffer.truncate(bytes_read);
	Ok(Some(buffer.into()))
}

fn read_with_wakeup(fd: i32, length: usize, wake_fd: i32) -> tg::Result<Option<Bytes>> {
	loop {
		let mut readfds = unsafe { std::mem::zeroed::<libc::fd_set>() };
		unsafe {
			libc::FD_ZERO(&raw mut readfds);
			libc::FD_SET(fd, &raw mut readfds);
			libc::FD_SET(wake_fd, &raw mut readfds);
		}
		let nfds = std::cmp::max(fd, wake_fd) + 1;
		let result = unsafe {
			libc::select(
				nfds,
				&raw mut readfds,
				std::ptr::null_mut(),
				std::ptr::null_mut(),
				std::ptr::null_mut(),
			)
		};
		if result < 0 {
			let source = std::io::Error::last_os_error();
			if source.kind() == std::io::ErrorKind::Interrupted {
				continue;
			}
			return Err(tg::error!(!source, %fd, "failed to poll the file descriptor"));
		}
		let wake_ready = unsafe { libc::FD_ISSET(wake_fd, &raw const readfds) };
		if wake_ready {
			return Err(stopped_error());
		}
		let fd_ready = unsafe { libc::FD_ISSET(fd, &raw const readfds) };
		if !fd_ready {
			continue;
		}
		return read(fd, length);
	}
}

fn stopped_error() -> tg::Error {
	tg::error!(
		code = tg::error::Code::Cancellation,
		"the operation was stopped"
	)
}

fn pipe() -> tg::Result<(OwnedFd, OwnedFd)> {
	let mut fds = [0; 2];
	if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to create a pipe"
		));
	}
	let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
	let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
	Ok((read, write))
}

fn raw_termios(termios: &libc::termios) -> libc::termios {
	let mut termios = unsafe { std::ptr::read(termios) };
	unsafe {
		libc::cfmakeraw(std::ptr::addr_of_mut!(termios));
	}
	termios
}

fn set_termios(fd: i32, termios: &libc::termios) -> tg::Result<()> {
	if unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, std::ptr::from_ref(termios)) } != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			%fd,
			"failed to set the file descriptor termios"
		));
	}
	Ok(())
}

async fn with_stopper<T, F>(stopper: Option<Stopper>, future: F) -> tg::Result<T>
where
	F: Future<Output = tg::Result<T>>,
{
	let Some(stopper) = stopper else {
		return future.await;
	};
	if stopper.stopped() {
		return Err(stopped_error());
	}
	tokio::select! {
		result = future => result,
		() = stopper.wait() => Err(stopped_error()),
	}
}

fn write_wakeup(fd: i32) -> tg::Result<()> {
	let byte = [0_u8; 1];
	loop {
		let bytes_written =
			unsafe { libc::write(fd, byte.as_ptr().cast::<libc::c_void>(), byte.len()) };
		if bytes_written >= 0 {
			return Ok(());
		}
		let source = std::io::Error::last_os_error();
		match source.kind() {
			std::io::ErrorKind::BrokenPipe => return Ok(()),
			std::io::ErrorKind::Interrupted => {},
			_ => {
				return Err(tg::error!(!source, "failed to write the wakeup pipe"));
			},
		}
	}
}

impl std::ops::Deref for Host {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
