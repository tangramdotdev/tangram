use super::signal::handle_sigwinch;
use crate::Cli;
use std::{
	io::IsTerminal as _,
	mem::MaybeUninit,
	os::fd::{AsRawFd as _, RawFd},
};
use tangram_client as tg;

pub struct Stdio {
	pub termios: Option<(RawFd, libc::termios)>,
	pub remote: Option<String>,
	pub stdin: tg::process::Io,
	pub stdout: tg::process::Io,
	pub stderr: tg::process::Io,
}

impl Stdio {
	pub async fn delete_io(&self, handle: &impl tg::Handle) {
		let io = [self.stdin.clone(), self.stdout.clone(), self.stderr.clone()];
		let handle = handle.clone();
		let remote = self.remote.clone();
		// tokio::spawn(async move {
			for io in &io {
				match io {
					tg::process::Io::Pipe(id) => {
						let arg = tg::pipe::delete::Arg {
							remote: remote.clone(),
						};
						handle.delete_pipe(id, arg).await.ok();
					},
					tg::process::Io::Pty(id) => {
						let arg = tg::pty::delete::Arg {
							remote: remote.clone(),
						};
						handle.delete_pty(id, arg).await.ok();
					},
				}
			}
		// });
	}
}

impl Drop for Stdio {
	fn drop(&mut self) {
		let Some((fd, termios)) = self.termios.take() else {
			return;
		};
		unsafe {
			libc::tcsetattr(fd, libc::TCSANOW, std::ptr::addr_of!(termios));
		}
	}
}

impl Cli {
	pub(crate) async fn create_stdio(
		&self,
		remote: Option<String>,
		detached: bool,
	) -> tg::Result<Stdio> {
		let handle = self.handle().await?;

		// If the process is detached, don't create any interactive i/o.
		if detached {
			let stdin = create(&handle, remote.clone(), None).await?;
			let stdout = create(&handle, remote.clone(), None).await?;
			let stderr = create(&handle, remote.clone(), None).await?;
			let stdio = Stdio {
				termios: None,
				remote,
				stdin,
				stdout,
				stderr,
			};
			return Ok(stdio);
		}

		// Open stdin.
		let (termios, stdin) = if std::io::stdin().is_terminal() {
			let fd = std::io::stdin().as_raw_fd();
			let (termios, window_size) = get_termios_and_window_size(fd)
				.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;
			let stdin = create(&handle, remote.clone(), Some(window_size)).await?;
			tokio::spawn({
				let pipe = stdin.clone();
				let handle = handle.clone();
				let remote = remote.clone();
				async move {
					handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
				}
			});
			(Some((fd, termios)), stdin)
		} else {
			let stdin = create(&handle, remote.clone(), None).await?;
			(None, stdin)
		};

		// Open stdout.
		let (termios, stdout) = match (termios, std::io::stdout().is_terminal()) {
			(Some(termios), true) => {
				let stdout = stdin.clone();
				(Some(termios), stdout)
			},
			(None, true) => {
				let fd = std::io::stdout().as_raw_fd();
				let (termios, window_size) = get_termios_and_window_size(fd)
					.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;
				let stdout = create(&handle, remote.clone(), Some(window_size)).await?;
				tokio::spawn({
					let pipe = stdout.clone();
					let handle = handle.clone();
					let remote = remote.clone();
					async move {
						handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
					}
				});
				(Some((fd, termios)), stdout)
			},
			(termios, false) => {
				let stdout = create(&handle, remote.clone(), None).await?;
				(termios, stdout)
			},
		};

		// Open stderr.
		let (termios, stderr) = match (termios, std::io::stdout().is_terminal()) {
			(Some(termios), true) => {
				let stderr = stdout.clone();
				(Some(termios), stderr)
			},
			(None, true) => {
				let fd = std::io::stderr().as_raw_fd();
				let (termios, window_size) = get_termios_and_window_size(fd)
					.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;
				let stderr = create(&handle, remote.clone(), Some(window_size)).await?;
				tokio::spawn({
					let pipe = stderr.clone();
					let handle = handle.clone();
					let remote = remote.clone();
					async move {
						handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
					}
				});
				(Some((fd, termios)), stderr)
			},
			(termios, false) => {
				let stderr = create(&handle, remote.clone(), None).await?;
				(termios, stderr)
			},
		};

		// Set raw mode.
		if let Some((fd, mut termios)) = termios {
			unsafe {
				termios.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
				termios.c_iflag &=
					!(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
				termios.c_oflag &= !(libc::OPOST);
				if libc::tcsetattr(fd, libc::TCSADRAIN, std::ptr::addr_of!(termios)) != 0 {
					let source = std::io::Error::last_os_error();
					return Err(tg::error!(!source, "failed to set terminal to raw mode"));
				}
			}
		}

		Ok(Stdio {
			termios,
			remote,
			stdin,
			stdout,
			stderr,
		})
	}
}

async fn create<H>(
	handle: &H,
	remote: Option<String>,
	window_size: Option<tg::pty::WindowSize>,
) -> tg::Result<tg::process::Io>
where
	H: tg::Handle,
{
	if let Some(window_size) = window_size {
		let arg = tg::pty::create::Arg {
			remote,
			window_size,
		};
		let output = handle
			.create_pty(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pty"))?;
		Ok(tg::process::Io::Pty(output.id))
	} else {
		let arg = tg::pipe::create::Arg { remote };
		let output = handle
			.create_pipe(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
		Ok(tg::process::Io::Pipe(output.id))
	}
}

fn get_termios_and_window_size(fd: RawFd) -> std::io::Result<(libc::termios, tg::pty::WindowSize)> {
	// Get the window size.
	unsafe {
		// Get the termio modes.
		let mut termio = MaybeUninit::zeroed();
		if libc::tcgetattr(fd, termio.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let termios = termio.assume_init();

		// Get the window size.
		let mut window_size: MaybeUninit<libc::winsize> = MaybeUninit::zeroed();
		if libc::ioctl(fd, libc::TIOCGWINSZ, window_size.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let window_size = window_size.assume_init();

		// Create the window size.
		let window_size = tg::pty::WindowSize {
			cols: window_size.ws_col,
			rows: window_size.ws_row,
			xpos: window_size.ws_xpixel,
			ypos: window_size.ws_ypixel,
		};

		Ok((termios, window_size))
	}
}
