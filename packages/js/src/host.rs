use {
	bytes::Bytes,
	std::{
		collections::BTreeMap,
		fs::OpenOptions,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd},
			unix::process::ExitStatusExt as _,
		},
		path::PathBuf,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	},
	tangram_client as tg,
	tokio::io::{Interest, unix::AsyncFd},
};

#[derive(Clone, Default)]
pub struct Host {
	inner: Arc<tokio::sync::Mutex<Inner>>,
	next_token: Arc<AtomicUsize>,
}

#[derive(Default)]
struct Inner {
	fds: BTreeMap<i32, Arc<AsyncFd<OwnedFd>>>,
	processes: BTreeMap<u32, tokio::process::Child>,
	signals: BTreeMap<usize, Arc<Signal>>,
	stdins: BTreeMap<usize, Arc<Stdin>>,
}

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

enum StdinFd {
	Async(Arc<AsyncFd<OwnedFd>>),
	Blocking(Arc<OwnedFd>),
}

struct Stdin {
	fd: StdinFd,
	stop: tokio::sync::watch::Sender<bool>,
}

impl Host {
	pub async fn close(&self, fd: i32) -> tg::Result<()> {
		let removed = self.inner.lock().await.fds.remove(&fd);
		if removed.is_none() {
			return Err(tg::error!(%fd, "failed to find the file descriptor"));
		}
		Ok(())
	}

	pub async fn exists(&self, path: String) -> tg::Result<bool> {
		let path = PathBuf::from(path);
		let exists = tokio::fs::try_exists(&path).await.map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to determine if the path exists"),
		)?;
		Ok(exists)
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

	pub async fn read(&self, fd: i32, length: Option<usize>) -> tg::Result<Option<Bytes>> {
		let fd_ = self.inner.lock().await.fds.get(&fd).cloned();
		let Some(fd_) = fd_ else {
			let length = length.unwrap_or(64 * 1024);
			return tokio::task::spawn_blocking(move || read(fd, length))
				.await
				.map_err(|source| tg::error!(!source, "the task panicked"))?;
		};
		let mut buffer = vec![0; length.unwrap_or(64 * 1024)];
		let bytes_read = fd_
			.async_io(Interest::READABLE, |fd_| {
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
			.map_err(|source| tg::error!(!source, %fd, "failed to read the file descriptor"))?;
		if bytes_read == 0 {
			return Ok(None);
		}
		buffer.truncate(bytes_read);
		Ok(Some(buffer.into()))
	}

	pub fn is_tty(fd: i32) -> bool {
		unsafe { libc::isatty(fd) == 1 }
	}

	pub async fn listen_signal_close(&self, token: usize) {
		let signal = self.inner.lock().await.signals.remove(&token);
		if let Some(signal) = signal {
			let _ = signal.stop.send(true);
		}
	}

	pub async fn listen_signal_read(&self, token: usize) -> tg::Result<Option<()>> {
		let signal = self.inner.lock().await.signals.get(&token).cloned();
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

	pub async fn listen_signal_open(&self, kind: SignalKind) -> tg::Result<usize> {
		let signal_kind = match kind {
			SignalKind::Sigwinch => tokio::signal::unix::SignalKind::window_change(),
		};
		let signal = tokio::signal::unix::signal(signal_kind)
			.map_err(|source| tg::error!(!source, "failed to create the signal handler"))?;
		let (stop, _) = tokio::sync::watch::channel(false);
		let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
		let signal = Arc::new(Signal {
			signal: tokio::sync::Mutex::new(signal),
			stop,
		});
		self.inner.lock().await.signals.insert(token, signal);
		Ok(token)
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

		let mut inner = self.inner.lock().await;
		if let Some((fd, stdin)) = stdin_ {
			inner.fds.insert(fd, stdin);
		}
		if let Some((fd, stdout)) = stdout_ {
			inner.fds.insert(fd, stdout);
		}
		if let Some((fd, stderr)) = stderr_ {
			inner.fds.insert(fd, stderr);
		}
		inner.processes.insert(pid, child);

		Ok(SpawnOutput {
			pid,
			stdin,
			stdout,
			stderr,
		})
	}

	pub async fn stdin_close(&self, token: usize) {
		let stdin = self.inner.lock().await.stdins.remove(&token);
		if let Some(stdin) = stdin {
			let _ = stdin.stop.send(true);
		}
	}

	pub async fn stdin_open(&self) -> tg::Result<usize> {
		let fd = unsafe { libc::dup(0) };
		if fd < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to duplicate stdin"
			));
		}
		let fd = unsafe { OwnedFd::from_raw_fd(fd) };
		let fd_number = fd.as_raw_fd();
		unsafe {
			let flags = libc::fcntl(fd_number, libc::F_GETFL);
			if flags < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to get the stdin flags"
				));
			}
			if libc::fcntl(fd_number, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set stdin to non-blocking"
				));
			}
		}
		let async_fd = unsafe { libc::dup(fd_number) };
		if async_fd < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to duplicate stdin for async registration"
			));
		}
		let async_fd = unsafe { OwnedFd::from_raw_fd(async_fd) };
		let fd = match AsyncFd::new(async_fd) {
			Ok(async_fd) => {
				drop(fd);
				StdinFd::Async(Arc::new(async_fd))
			},
			Err(source) if source.raw_os_error() == Some(libc::EPERM) => {
				StdinFd::Blocking(Arc::new(fd))
			},
			Err(source) => {
				return Err(tg::error!(!source, %fd_number, "failed to register stdin"));
			},
		};
		let (stop, _) = tokio::sync::watch::channel(false);
		let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
		let stdin = Arc::new(Stdin { fd, stop });
		self.inner.lock().await.stdins.insert(token, stdin);
		Ok(token)
	}

	pub async fn stdin_read(
		&self,
		token: usize,
		length: Option<usize>,
	) -> tg::Result<Option<Bytes>> {
		let stdin = self.inner.lock().await.stdins.get(&token).cloned();
		let Some(stdin) = stdin else {
			return Ok(None);
		};
		let mut stop = stdin.stop.subscribe();
		let mut buffer = vec![0; length.unwrap_or(64 * 1024)];
		match &stdin.fd {
			StdinFd::Async(fd) => {
				tokio::select! {
					result = fd.async_io(Interest::READABLE, |fd| {
						let bytes_read = unsafe {
							libc::read(
								fd.as_raw_fd(),
								buffer.as_mut_ptr().cast::<libc::c_void>(),
								buffer.len(),
							)
						};
						if bytes_read < 0 {
							Err(std::io::Error::last_os_error())
						} else {
							Ok(bytes_read.cast_unsigned())
						}
					}) => {
						let bytes_read = result.map_err(|source| {
							tg::error!(!source, %token, "failed to read stdin")
						})?;
						if bytes_read == 0 {
							Ok(None)
						} else {
							buffer.truncate(bytes_read);
							Ok(Some(buffer.into()))
						}
					},
					_ = stop.changed() => Ok(None),
				}
			},
			StdinFd::Blocking(fd) => {
				let fd = fd.clone();
				let length = buffer.len();
				let result = tokio::task::spawn_blocking(move || read(fd.as_raw_fd(), length))
					.await
					.map_err(|source| tg::error!(!source, "the task panicked"))?;
				if *stop.borrow() { Ok(None) } else { result }
			},
		}
	}

	pub async fn wait(&self, pid: u32) -> tg::Result<WaitOutput> {
		let mut child = self
			.inner
			.lock()
			.await
			.processes
			.remove(&pid)
			.ok_or_else(|| tg::error!(%pid, "failed to find the process"))?;
		let status = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, %pid, "failed to wait for the process"))?;
		let exit = exit_status_to_code(status)?;
		Ok(WaitOutput { exit })
	}

	pub async fn write(&self, fd: i32, bytes: Bytes) -> tg::Result<()> {
		let fd_ = self.inner.lock().await.fds.get(&fd).cloned();
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

fn read(fd: i32, length: usize) -> tg::Result<Option<Bytes>> {
	let mut buffer = vec![0; length];
	let bytes_read =
		unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
	if bytes_read < 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			%fd,
			"failed to read the file descriptor"
		));
	}
	let bytes_read = bytes_read.cast_unsigned();
	if bytes_read == 0 {
		return Ok(None);
	}
	buffer.truncate(bytes_read);
	Ok(Some(buffer.into()))
}
