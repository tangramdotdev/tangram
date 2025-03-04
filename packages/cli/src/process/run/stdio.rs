use itertools::Itertools;
use std::{
	io::IsTerminal,
	mem::MaybeUninit,
	os::fd::{AsRawFd, RawFd},
};
use tangram_client::{self as tg, Handle};

use crate::Cli;

pub struct Stdio {
	pub termios: Option<(RawFd, libc::termios)>,
	pub remote: Option<String>,
	pub stdin: tg::pipe::open::Output,
	pub stdout: tg::pipe::open::Output,
	pub stderr: tg::pipe::open::Output,
}

impl Stdio {
	pub async fn close_server_half(&self, handle: &impl tg::Handle) {
		let arg = tg::pipe::close::Arg {
			remote: self.remote.clone(),
		};
		let pipes = [&self.stdin.reader, &self.stdout.writer, &self.stderr.writer]
			.into_iter()
			.unique();
		for pipe in pipes {
			handle.close_pipe(pipe, arg.clone()).await.ok();
		}
	}

	pub async fn close_client_half(&self, handle: &impl tg::Handle) {
		let arg = tg::pipe::close::Arg {
			remote: self.remote.clone(),
		};
		let pipes = [&self.stdin.writer, &self.stdout.reader, &self.stderr.reader]
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

		let (termios, stdin, stdout, stderr) = if std::io::stdin().is_terminal() && !detached {
			// If stdin is a tty, create a pipe with a window size attached.
			let (termios, window_size) = get_termios_and_window_size(&std::io::stdin())
				.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: Some(window_size),
			};
			let stdin = handle.open_pipe(arg).await?;

			// Open a pipe for stdout.
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: None,
			};
			let stdout = handle.open_pipe(arg.clone()).await?;

			// Dup stdout to stderr if necessarsy.
			let stderr = if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
				stdout.clone()
			} else {
				handle.open_pipe(arg.clone()).await?
			};
			(
				Some((std::io::stdin().as_raw_fd(), termios)),
				stdin,
				stdout,
				stderr,
			)
		} else if std::io::stdout().is_terminal() && !detached {
			// If stdout is a tty but stdin is not, open the pipe and dup to stdout while creating a unique pipe for stdin.
			let (termios, window_size) = get_termios_and_window_size(&std::io::stdout())
				.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: Some(window_size),
			};
			let stdout = handle.open_pipe(arg).await?;

			// Dup stderr to stdout if necessarsy.
			let stderr = if std::io::stderr().is_terminal() {
				stdout.clone()
			} else {
				let arg = tg::pipe::open::Arg {
					remote: remote.clone(),
					window_size: None,
				};
				handle.open_pipe(arg.clone()).await?
			};

			// Create a pipe for stdin.
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: None,
			};
			let stdin = handle.open_pipe(arg.clone()).await?;
			(
				Some((std::io::stdout().as_raw_fd(), termios)),
				stdin,
				stdout,
				stderr,
			)
		} else if std::io::stderr().is_terminal() && !detached {
			// If stderr is a tty but stdin and stdout are not, open the pipe and create unique pipes for stdout and stdin.
			let (termios, window_size) = get_termios_and_window_size(&std::io::stderr())
				.map_err(|source| tg::error!(!source, "failed to get terminal size"))?;

			// Open stderr.
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: Some(window_size),
			};
			let stderr = handle.open_pipe(arg).await?;

			// Open stdin and stdout.
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: None,
			};
			let stdin = handle.open_pipe(arg.clone()).await?;
			let stdout = handle.open_pipe(arg.clone()).await?;
			(
				Some((std::io::stderr().as_raw_fd(), termios)),
				stdin,
				stdout,
				stderr,
			)
		} else {
			let arg = tg::pipe::open::Arg {
				remote: remote.clone(),
				window_size: None,
			};
			let stdin = handle.open_pipe(arg.clone()).await?;
			let stdout = handle.open_pipe(arg.clone()).await?;
			let stderr = handle.open_pipe(arg.clone()).await?;
			(None, stdin, stdout, stderr)
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

fn get_termios_and_window_size(
	fd: &impl AsRawFd,
) -> std::io::Result<(libc::termios, tg::pipe::WindowSize)> {
	// Get the window size.
	unsafe {
		let fd = fd.as_raw_fd();

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
		let window_size = tg::pipe::WindowSize {
			cols: window_size.ws_col,
			rows: window_size.ws_row,
			xpos: window_size.ws_xpixel,
			ypos: window_size.ws_ypixel,
		};
		Ok((termios, window_size))
	}
}
