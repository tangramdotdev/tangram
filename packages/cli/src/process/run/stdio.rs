use std::{io::IsTerminal, mem::MaybeUninit, os::fd::RawFd};
use tangram_client::{self as tg, Handle};

use crate::Cli;

pub struct Stdio {
	pub termio: Option<(RawFd, libc::termios)>,
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
		handle
			.close_pipe(&self.stdin.reader, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&self.stdout.writer, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&self.stderr.writer, arg.clone())
			.await
			.ok();
	}

	pub async fn close_client_half(&self, handle: &impl tg::Handle) {
		let arg = tg::pipe::close::Arg {
			remote: self.remote.clone(),
		};
		handle
			.close_pipe(&self.stdin.writer, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&self.stdout.reader, arg.clone())
			.await
			.ok();
		handle
			.close_pipe(&self.stderr.reader, arg.clone())
			.await
			.ok();
	}
}

impl Drop for Stdio {
	fn drop(&mut self) {
		let Some((fd, termio)) = self.termio.take() else {
			return;
		};
		unsafe {
			libc::tcsetattr(fd, libc::TCSANOW, std::ptr::addr_of!(termio));
		}
	}
}

impl Cli {
	pub(crate) async fn init_stdio(&self, remote: Option<String>) -> tg::Result<Stdio> {
		let handle = self.handle().await?;

		let (window_size, termio) = 'a: {
			for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
				unsafe {
					if libc::isatty(fd) != 0 {
						// Get the window size.
						let mut window_size: MaybeUninit<libc::winsize> = MaybeUninit::zeroed();
						libc::ioctl(fd, libc::TIOCGWINSZ, window_size.as_mut_ptr());
						let window_size = window_size.assume_init();

						// Get the termio modes.
						let mut termio = MaybeUninit::zeroed();
						libc::tcgetattr(fd, termio.as_mut_ptr());
						let termio = termio.assume_init();

						// Return the window size and original state.
						break 'a (Some(window_size), Some((fd, termio)));
					}
				}
			}
			(None, None)
		};

		// Create pipes.
		let window_size = window_size.map(|w| tg::pipe::WindowSize {
			rows: w.ws_row,
			cols: w.ws_col,
			xpos: w.ws_xpixel,
			ypos: w.ws_ypixel,
		});
		let (stdin, stdout, stderr) = if let Some(window_size) = window_size {
			// Create a pipe for the terminal that will be shared as necessary.
			let tty_pipe = handle
				.open_pipe(tg::pipe::open::Arg {
					remote: remote.clone(),
					window_size: Some(window_size),
				})
				.await?;

			// Create in/out/err pipes.
			let stdin = if std::io::stdin().is_terminal() {
				tty_pipe.clone()
			} else {
				handle
					.open_pipe(tg::pipe::open::Arg {
						remote: remote.clone(),
						window_size: None,
					})
					.await?
			};
			let stdout = if std::io::stdout().is_terminal() {
				tty_pipe.clone()
			} else {
				handle
					.open_pipe(tg::pipe::open::Arg {
						remote: remote.clone(),
						window_size: None,
					})
					.await?
			};
			let stderr = if std::io::stdout().is_terminal() {
				tty_pipe.clone()
			} else {
				handle
					.open_pipe(tg::pipe::open::Arg {
						remote: remote.clone(),
						window_size: None,
					})
					.await?
			};
			(stdin, stdout, stderr)
		} else {
			let stdin = handle
				.open_pipe(tg::pipe::open::Arg {
					remote: remote.clone(),
					window_size: None,
				})
				.await?;
			let stdout = handle
				.open_pipe(tg::pipe::open::Arg {
					remote: remote.clone(),
					window_size: None,
				})
				.await?;
			let stderr = handle
				.open_pipe(tg::pipe::open::Arg {
					remote: remote.clone(),
					window_size: None,
				})
				.await?;
			(stdin, stdout, stderr)
		};

		// Set raw mode.
		if let Some((fd, mut termio)) = termio {
			unsafe {
				termio.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
				termio.c_iflag &=
					!(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
				termio.c_oflag &= !(libc::OPOST);
				if libc::tcsetattr(fd, libc::TCSANOW, std::ptr::addr_of!(termio)) != 0 {
					let source = std::io::Error::last_os_error();
					return Err(tg::error!(!source, "failed to set terminal to raw mode"));
				}
			}
		}

		Ok(Stdio {
			termio,
			remote,
			stdin,
			stdout,
			stderr,
		})
	}
}
