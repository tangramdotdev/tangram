use {
	super::Options,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{
		io::IsTerminal as _,
		mem::MaybeUninit,
		os::fd::{AsRawFd as _, RawFd},
		pin::pin,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Stop, Task},
	tokio::io::{AsyncWrite, AsyncWriteExt as _},
};

#[derive(Clone, Debug)]
pub struct Stdio {
	pub tty: Option<Arc<Tty>>,
	pub remote: Option<String>,
	pub stdin: Option<tg::process::Stdio>,
	pub stdout: Option<tg::process::Stdio>,
	pub stderr: Option<tg::process::Stdio>,
}

#[derive(Clone, Debug)]
pub struct Tty {
	pub fd: RawFd,
	pub termios: libc::termios,
}

pub(super) async fn task<H>(handle: &H, stop: Stop, stdio: Stdio) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create the stdin future.
	let stdin_future =
		{
			let handle = handle.clone();
			let remote = stdio.remote.clone();
			let stdin = stdio.stdin.clone();
			let tty = stdio.tty.clone();
			
			async move {
				let Some(stdin) = stdin else {
					return Ok(());
				};
				let stream = crate::util::stdio::stdin_stream().take_until(stop.wait());

				// Write to stdin until it is finished or the task is stopped.
				let future = match &stdin {
					tg::process::Stdio::Pipe(id) => {		
						async {
							let mut stream = pin!(stream);
							while let Some(bytes) = stream.try_next().await? {
								let arg = tg::pipe::write::Arg {
									bytes,
									local: None,
									remotes: remote.clone().map(|r| vec![r]),
								};
								handle.write_pipe(id, arg).await.map_err(
									|source| tg::error!(!source, %id, "failed to write to the pipe"),
								)?;
							}
							Ok::<_, tg::Error>(())
						}
						.boxed()
					},
					tg::process::Stdio::Pty(id) => {
						// Get the tty.
						let Some(tty) = tty else {
							return Err(tg::error!("expected a tty"));
						};

						// Spawn a task to handle sigwinch, which is canceled after stdin is finished.
						let sigwinch_task = Task::spawn({
							let handle = handle.clone();
							let pty = id.clone();
							let tty = Arc::clone(&tty);
							let remote = remote.clone();
							|_stop| async move {
								sigwinch_task(&handle, tty, pty, remote)
									.await
									.inspect_err(|error| eprintln!("sigwinch task failed: {error}"))
									.ok();
							}
						});

						// Spawn the stdio task.
						let handle = handle.clone();
						let remote = remote.clone();
						async move {
							let mut stream = pin!(stream);
							while let Some(bytes) = stream.try_next().await? {
								let arg = tg::pty::write::Arg {
									bytes,
									local: None,
									master: true,
									remotes: remote.clone().map(|r| vec![r]),
								};
								handle.write_pty(id, arg).await.map_err(
									|source| tg::error!(!source, %id, "failed to write to the pty"),
								)?;
							}
							sigwinch_task.abort();
							Ok::<_, tg::Error>(())
						}
						.boxed()
					},
				};
				let stop = stop.wait();
				let future = pin!(future);
				let stop = pin!(stop);
				let either = future::select(future, stop).await;
				if let future::Either::Left((Err(error), _)) = either {
					return Err(error);
				}

				// Close stdin.
				match &stdin {
					tg::process::Stdio::Pipe(id) => {
						let arg = tg::pipe::close::Arg {
							local: None,
							remotes: remote.clone().map(|r| vec![r]),
						};
						handle.close_pipe(id, arg).await.map_err(
							|source| tg::error!(!source, %id, "failed to close the pipe"),
						)?;
					},
					tg::process::Stdio::Pty(id) => {
						let arg = tg::pty::close::Arg {
							local: None,
							master: true,
							remotes: remote.clone().map(|r| vec![r]),
						};
						handle.close_pty(id, arg).await.map_err(
							|source| tg::error!(!source, %id, "failed to close the pty"),
						)?;
					},
				}

				Ok(())
			}
		};

	// Create the stdout future.
	let stdout_future = {
		let handle = handle.clone();
		let remote = stdio.remote.clone();
		let stdout = stdio.stdout.clone();
		async move {
			let Some(stdout) = stdout else {
				return Ok(());
			};
			let writer = tokio::io::BufWriter::new(tokio::io::stdout());
			task_inner(&handle, &stdout, remote, writer).await?;
			Ok::<_, tg::Error>(())
		}
	};

	// Create the stderr future.
	let stderr_future = {
		let handle = handle.clone();
		let remote = stdio.remote.clone();
		let stdout = stdio.stdout.clone();
		let stderr = stdio.stderr.clone();
		async move {
			if stderr == stdout {
				return Ok(());
			}
			let Some(stderr) = stderr else {
				return Ok(());
			};
			let writer = tokio::io::BufWriter::new(tokio::io::stderr());
			task_inner(&handle, &stderr, remote, writer).await?;
			Ok::<_, tg::Error>(())
		}
	};

	// Join the futures.
	future::try_join3(stdin_future, stdout_future, stderr_future).await?;

	Ok(())
}

async fn task_inner<H>(
	handle: &H,
	stdio: &tg::process::Stdio,
	remote: Option<String>,
	mut writer: impl AsyncWrite + Unpin,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	let stream = match stdio {
		tg::process::Stdio::Pipe(id) => {
			let arg = tg::pipe::read::Arg {
				local: None,
				remotes: remote.map(|r| vec![r]),
			};
			handle
				.try_read_pipe(id, arg)
				.await?
				.ok_or_else(|| tg::error!("pipe not found"))?
				.left_stream()
		},
		tg::process::Stdio::Pty(id) => {
			let arg = tg::pty::read::Arg {
				local: None,
				master: true,
				remotes: remote.map(|r| vec![r]),
			};
			handle
				.try_read_pty(id, arg)
				.await?
				.ok_or_else(|| tg::error!("pty not found"))?
				.right_stream()
		},
	};
	let mut stream = pin!(stream);
	while let Some(chunk) = stream.try_next().await? {
		writer
			.write_all(&chunk)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the chunk"))?;
		writer
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush the writer"))?;
	}
	Ok(())
}

/// Create a stream of tty size change events.
pub async fn sigwinch_task<H>(
	handle: &H,
	tty: Arc<Tty>,
	pty: tg::pty::Id,
	remote: Option<String>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create the signal handler.
	let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())
		.map_err(|source| tg::error!(!source, "failed to create signal handler"))?;

	// Wait for the next signal.
	while let Some(()) = signal.recv().await {
		let size = tty.get_size()?;
		let arg = tg::pty::size::put::Arg {
			local: None,
			master: true,
			size,
			remotes: remote.clone().map(|r| vec![r]),
		};
		handle
			.put_pty_size(&pty, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the pty size"))?;
	}
	Ok(())
}

impl Stdio {
	pub(crate) async fn new<H>(
		handle: &H,
		remote: Option<String>,
		options: &Options,
	) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// If the process is detached, then do not create stdio.
		if options.detach {
			return Ok(Self {
				tty: None,
				remote,
				stdin: None,
				stdout: None,
				stderr: None,
			});
		}

		// Create a PTY for stdin if it is a terminal.
		let (tty, stdin) = if options.spawn.tty.get() && std::io::stdin().is_terminal() {
			let tty = Tty::new()?;
			let size = tty.get_size()?;
			let arg = tg::pty::create::Arg {
				local: None,
				remotes: remote.clone().map(|r| vec![r]),
				size,
			};
			let output = handle
				.create_pty(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to open pty"))?;
			let stdin = tg::process::Stdio::Pty(output.id);
			(Some(Arc::new(tty)), Some(stdin))
		} else {
			let arg = tg::pipe::create::Arg {
				local: None,
				remotes: remote.clone().map(|r| vec![r]),
			};
			let output = handle
				.create_pipe(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
			let stdin = tg::process::Stdio::Pipe(output.id);
			(None, Some(stdin))
		};

		// Open stdout.
		let stdout = if tty.is_some() && std::io::stdout().is_terminal() {
			stdin.clone()
		} else {
			let arg = tg::pipe::create::Arg {
				local: None,
				remotes: remote.clone().map(|r| vec![r]),
			};
			let output = handle
				.create_pipe(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
			let stdout = tg::process::Stdio::Pipe(output.id);
			Some(stdout)
		};

		// Open stderr.
		let stderr = if tty.is_some() && std::io::stderr().is_terminal() {
			stdin.clone()
		} else {
			let arg = tg::pipe::create::Arg {
				local: None,
				remotes: remote.clone().map(|r| vec![r]),
			};
			let output = handle
				.create_pipe(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
			let stderr = tg::process::Stdio::Pipe(output.id);
			Some(stderr)
		};

		Ok(Self {
			tty,
			remote,
			stdin,
			stdout,
			stderr,
		})
	}

	pub(crate) async fn close<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(stdout) = &self.stdout {
			match stdout {
				tg::process::Stdio::Pipe(pipe) => {
					let arg = tg::pipe::close::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.close_pipe(pipe, arg).await.map_err(
						|source| tg::error!(!source, id = %pipe, "failed to close the pipe"),
					)?;
				},
				tg::process::Stdio::Pty(pty) => {
					let arg = tg::pty::close::Arg {
						local: None,
						master: false,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.close_pty(pty, arg).await.map_err(
						|source| tg::error!(!source, id = %pty, "failed to close the pty"),
					)?;
				},
			}
		}
		if let Some(stderr) = &self.stderr {
			match stderr {
				tg::process::Stdio::Pipe(pipe) => {
					let arg = tg::pipe::close::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.close_pipe(pipe, arg).await.map_err(
						|source| tg::error!(!source, id = %pipe, "failed to close the pipe"),
					)?;
				},
				tg::process::Stdio::Pty(pty) => {
					let arg = tg::pty::close::Arg {
						local: None,
						master: false,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.close_pty(pty, arg).await.map_err(
						|source| tg::error!(!source, id = %pty, "failed to close the pty"),
					)?;
				},
			}
		}
		Ok(())
	}

	pub(crate) async fn delete<H>(self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		// Delete stdout and stderr.
		if let Some(stdout) = &self.stdout {
			match stdout {
				tg::process::Stdio::Pipe(pipe) => {
					let arg = tg::pipe::delete::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.delete_pipe(pipe, arg).await.map_err(
						|source| tg::error!(!source, id = %pipe, "failed to delete the pipe"),
					)?;
				},
				tg::process::Stdio::Pty(pty) => {
					let arg = tg::pty::delete::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.delete_pty(pty, arg).await.map_err(
						|source| tg::error!(!source, id = %pty, "failed to delete the pty"),
					)?;
				},
			}
		}
		if let Some(stderr) = &self.stderr {
			match stderr {
				tg::process::Stdio::Pipe(pipe) => {
					let arg = tg::pipe::delete::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.delete_pipe(pipe, arg).await.map_err(
						|source| tg::error!(!source, id = %pipe, "failed to delete the pipe"),
					)?;
				},
				tg::process::Stdio::Pty(pty) => {
					let arg = tg::pty::delete::Arg {
						local: None,
						remotes: self.remote.clone().map(|r| vec![r]),
					};
					handle.delete_pty(pty, arg).await.map_err(
						|source| tg::error!(!source, id = %pty, "failed to delete the pty"),
					)?;
				},
			}
		}
		Ok(())
	}
}

impl Tty {
	pub fn new() -> tg::Result<Self> {
		unsafe {
			// Get the fd.
			let fd = std::io::stdin().as_raw_fd();

			// Get the termios.
			let mut termios = MaybeUninit::zeroed();
			if libc::tcgetattr(fd, termios.as_mut_ptr()) != 0 {
				let error = std::io::Error::last_os_error();
				let error = tg::error!(!error, "failed to get the termios");
				return Err(error);
			}
			let termios = termios.assume_init();

			let tty = Tty { fd, termios };

			Ok(tty)
		}
	}

	pub fn get_size(&self) -> tg::Result<tg::pty::Size> {
		unsafe {
			let mut winsize = MaybeUninit::<libc::winsize>::zeroed();
			let ret = libc::ioctl(self.fd, libc::TIOCGWINSZ, winsize.as_mut_ptr());
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				let error = tg::error!(!error, "failed to get the size");
				return Err(error);
			}
			let winsize = winsize.assume_init();
			let size = tg::pty::Size {
				cols: winsize.ws_col,
				rows: winsize.ws_row,
			};
			Ok(size)
		}
	}

	pub fn enable_raw_mode(&self) -> tg::Result<()> {
		unsafe {
			let mut termios = self.termios;
			termios.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
			termios.c_iflag &=
				!(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
			termios.c_oflag &= !(libc::OPOST);
			let ret = libc::tcsetattr(self.fd, libc::TCSADRAIN, std::ptr::addr_of!(termios));
			if ret != 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(!source, "failed to set the tty to raw mode"));
			}
			Ok(())
		}
	}
}

impl Drop for Tty {
	fn drop(&mut self) {
		unsafe {
			libc::tcsetattr(self.fd, libc::TCSANOW, std::ptr::addr_of!(self.termios));
		}
	}
}
