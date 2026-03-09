use {
	super::Options,
	futures::{FutureExt as _, StreamExt as _, future},
	std::{
		io::IsTerminal as _,
		mem::MaybeUninit,
		os::fd::{AsRawFd as _, RawFd},
		pin::pin,
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Stop, Task},
	tokio_util::io::StreamReader,
};

#[derive(Clone, Debug)]
pub struct Stdio {
	pub remote: Option<String>,
	pub stderr: tg::process::Stdio,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub tty: Option<Arc<Tty>>,
}

#[derive(Clone, Debug)]
pub struct Tty {
	pub fd: RawFd,
	pub termios: libc::termios,
}

pub(super) async fn task<H>(
	handle: &H,
	stop: Stop,
	process: tg::process::Id,
	stdio: Stdio,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	// Create the sigwinch task.
	let sigwinch_task = if let Some(tty) = &stdio.tty {
		Some(Task::spawn({
			let handle = handle.clone();
			let process = process.clone();
			let remote = stdio.remote.clone();
			let tty = Arc::clone(tty);
			|_stop| async move {
				sigwinch_task(&handle, tty, process, remote)
					.await
					.inspect_err(|error| eprintln!("the sigwinch task failed: {error}"))
					.ok();
			}
		}))
	} else {
		None
	};

	// Create the stdin future.
	let stdin_future = {
		let handle = handle.clone();
		let process = process.clone();
		let remote = stdio.remote.clone();
		let stdin = stdio.stdin;
		async move {
			if !matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Pty) {
				return Ok(());
			}
			let remote_ = remote.clone();
			let stream = crate::util::stdio::stdin_stream().take_until(stop.wait());
			let future = async {
				let arg = tg::process::stdio::Arg {
					remotes: remote_.map(|remote| vec![remote]),
					..tg::process::stdio::Arg::default()
				};
				handle
					.write_process_stdin(&process, arg, StreamReader::new(stream))
					.await?;
				Ok::<_, tg::Error>(())
			}
			.boxed();
			let stop = stop.wait();
			let future = pin!(future);
			let stop = pin!(stop);
			let either = future::select(future, stop).await;
			if let future::Either::Left((Err(error), _)) = either {
				return Err(error);
			}

			// Close stdin.
			let arg = tg::process::stdio::Arg {
				remotes: remote.map(|remote| vec![remote]),
				..tg::process::stdio::Arg::default()
			};
			handle.close_process_stdin(&process, arg).await.ok();

			Ok(())
		}
	};

	// Create the stdout future.
	let stdout_future = {
		let process = process.clone();
		let handle = handle.clone();
		let remote = stdio.remote.clone();
		let stdout = stdio.stdout;
		async move {
			if !matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Pty) {
				return Ok(());
			}
			let arg = tg::process::stdio::Arg {
				remotes: remote.map(|remote| vec![remote]),
				..tg::process::stdio::Arg::default()
			};
			let mut writer = tokio::io::BufWriter::new(tokio::io::stdout());
			let Some(reader) = handle.try_read_process_stdout(&process, arg).await? else {
				return Ok(());
			};
			let mut reader = pin!(reader);
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|source| tg::error!(!source, "failed to copy stdout"))?;
			Ok::<_, tg::Error>(())
		}
	};

	// Create the stderr future.
	let stderr_future = {
		let process = process.clone();
		let handle = handle.clone();
		let remote = stdio.remote.clone();
		let stderr = stdio.stderr;
		async move {
			if !matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Pty) {
				return Ok(());
			}
			let arg = tg::process::stdio::Arg {
				remotes: remote.map(|remote| vec![remote]),
				..tg::process::stdio::Arg::default()
			};
			let mut writer = tokio::io::BufWriter::new(tokio::io::stderr());
			let Some(reader) = handle.try_read_process_stderr(&process, arg).await? else {
				return Ok(());
			};
			let mut reader = pin!(reader);
			tokio::io::copy(&mut reader, &mut writer)
				.await
				.map_err(|source| tg::error!(!source, "failed to copy stdout"))?;

			Ok::<_, tg::Error>(())
		}
	};

	// Join the futures.
	future::try_join3(stdin_future, stdout_future, stderr_future).await?;

	if let Some(task) = sigwinch_task {
		task.abort();
	}

	Ok(())
}

/// Create a stream of tty size change events.
pub async fn sigwinch_task<H>(
	handle: &H,
	tty: Arc<Tty>,
	process: tg::process::Id,
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
		let arg = tg::process::pty::size::put::Arg {
			local: None,
			remotes: remote.clone().map(|remote| vec![remote]),
			size,
		};
		handle
			.set_process_pty_size(&process, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the pty"))?;
	}
	Ok(())
}

impl Stdio {
	pub(crate) async fn new(remote: Option<String>, options: &Options) -> tg::Result<Self> {
		// If the process is detached, then do not create stdio.
		if options.detach {
			return Ok(Self {
				tty: None,
				remote,
				stdin: tg::process::Stdio::Null,
				stdout: tg::process::Stdio::Null,
				stderr: tg::process::Stdio::Null,
			});
		}

		// Create a PTY for stdin if it is a terminal.
		let (tty, stdin) = if options.spawn.tty.get() && std::io::stdin().is_terminal() {
			let tty = Tty::new()?;
			let stdin = tg::process::Stdio::Pty;
			(Some(Arc::new(tty)), stdin)
		} else {
			let stdin = tg::process::Stdio::Pipe;
			(None, stdin)
		};

		let stdout = if tty.is_some() && std::io::stdout().is_terminal() {
			tg::process::Stdio::Pty
		} else {
			tg::process::Stdio::Pipe
		};

		let stderr = if tty.is_some() && std::io::stderr().is_terminal() {
			tg::process::Stdio::Pty
		} else {
			tg::process::Stdio::Pipe
		};

		let stdio = Self {
			remote,
			stderr,
			stdin,
			stdout,
			tty,
		};

		Ok(stdio)
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

	pub fn get_size(&self) -> tg::Result<tg::process::pty::Size> {
		unsafe {
			let mut winsize = MaybeUninit::<libc::winsize>::zeroed();
			let ret = libc::ioctl(self.fd, libc::TIOCGWINSZ, winsize.as_mut_ptr());
			if ret != 0 {
				let error = std::io::Error::last_os_error();
				let error = tg::error!(!error, "failed to get the size");
				return Err(error);
			}
			let winsize = winsize.assume_init();
			let size = tg::process::pty::Size {
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
