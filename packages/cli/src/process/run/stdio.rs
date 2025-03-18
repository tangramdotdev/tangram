use itertools::Itertools;
use std::{
	io::IsTerminal,
	mem::MaybeUninit,
	os::fd::{AsRawFd, RawFd},
};
use tangram_client as tg;

use crate::Cli;

use super::signal::handle_sigwinch;

pub struct Stdio {
	pub termios: Option<(RawFd, libc::termios)>,
	pub remote: Option<String>,
	pub stdin: Pipe,
	pub stdout: Pipe,
	pub stderr: Pipe,
}

#[derive(Clone, Debug)]
pub struct Pipe {
	pub client: tg::pty::Id,
	pub server: tg::pty::Id,
}

impl Stdio {
	pub async fn close_server_half(&self, handle: &impl tg::Handle) {
		let arg = tg::pty::close::Arg {
			remote: self.remote.clone(),
		};
		let pipes = [&self.stdin.server, &self.stdout.server, &self.stderr.server]
			.into_iter()
			.unique();
		for pipe in pipes {
			handle.close_pipe(pipe, arg.clone()).await.ok();
		}
	}

	pub async fn close_client_half(&self, handle: &impl tg::Handle) {
		let arg = tg::pty::close::Arg {
			remote: self.remote.clone(),
		};
		let pipes = [&self.stdin.client, &self.stdout.client, &self.stderr.client]
			.into_iter()
			.unique();
		for pipe in pipes {
			handle.close_pipe(pipe, arg.clone()).await.ok();
		}
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
	pub(crate) async fn init_stdio(
		&self,
		remote: Option<String>,
		detached: bool,
	) -> tg::Result<Stdio> {
		let handle = self.handle().await?;

		// If the process is detached, don't create any interactive i/o.
		if detached {
			let stdin = pair(&handle, remote.clone(), None).await?;
			let stdout = pair(&handle, remote.clone(), None).await?;
			let stderr = pair(&handle, remote.clone(), None).await?;
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
			let stdin = pair(&handle, remote.clone(), Some(window_size)).await?;
			tokio::spawn({
				let pipe = stdin.client.clone();
				let handle = handle.clone();
				let remote = remote.clone();
				async move {
					handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
				}
			});
			(Some((fd, termios)), stdin)
		} else {
			let stdin = pair(&handle, remote.clone(), None).await?;
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
				let stdout = pair(&handle, remote.clone(), Some(window_size)).await?;
				tokio::spawn({
					let pipe = stdout.client.clone();
					let handle = handle.clone();
					let remote = remote.clone();
					async move {
						handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
					}
				});
				(Some((fd, termios)), stdout)
			},
			(termios, false) => {
				let stdout = pair(&handle, remote.clone(), None).await?;
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
				let stderr = pair(&handle, remote.clone(), Some(window_size)).await?;
				tokio::spawn({
					let pipe = stderr.client.clone();
					let handle = handle.clone();
					let remote = remote.clone();
					async move {
						handle_sigwinch(&handle, fd, &pipe, remote).await.ok();
					}
				});
				(Some((fd, termios)), stderr)
			},
			(termios, false) => {
				let stderr = pair(&handle, remote.clone(), None).await?;
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

async fn pair<H>(
	handle: &H,
	remote: Option<String>,
	window_size: Option<tg::pty::WindowSize>,
) -> tg::Result<Pipe>
where
	H: tg::Handle,
{
	let arg = tg::pty::open::Arg {
		remote,
		window_size,
	};
	let output = handle
		.open_pipe(arg)
		.await
		.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
	Ok(Pipe {
		client: output.reader,
		server: output.writer,
	})
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

		// Return.
		let window_size = tg::pty::WindowSize {
			cols: window_size.ws_col,
			rows: window_size.ws_row,
			xpos: window_size.ws_xpixel,
			ypos: window_size.ws_ypixel,
		};
		Ok((termios, window_size))
	}
}
