use super::{Options, signal::handle_sigwinch};
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
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub stderr: tg::process::Stdio,
}

impl Cli {
	pub(crate) async fn create_stdio(
		&mut self,
		remote: Option<String>,
		options: &Options,
	) -> tg::Result<Stdio> {
		let handle = self.handle().await?;

		// If the process is detached, don't create any interactive stdio.
		if options.detach || options.spawn.no_tty {
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
			let (termios, size) = get_termios_and_size(fd)
				.map_err(|source| tg::error!(!source, "failed to get tty size"))?;
			let stdin = create(&handle, remote.clone(), Some(size)).await?;
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
				let (termios, size) = get_termios_and_size(fd)
					.map_err(|source| tg::error!(!source, "failed to get tty size"))?;
				let stdout = create(&handle, remote.clone(), Some(size)).await?;
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
				let (termios, size) = get_termios_and_size(fd)
					.map_err(|source| tg::error!(!source, "failed to get tty size"))?;
				let stderr = create(&handle, remote.clone(), Some(size)).await?;
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
	size: Option<tg::pty::Size>,
) -> tg::Result<tg::process::Stdio>
where
	H: tg::Handle,
{
	if let Some(size) = size {
		let arg = tg::pty::create::Arg { remote, size };
		let output = handle
			.create_pty(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pty"))?;
		Ok(tg::process::Stdio::Pty(output.id))
	} else {
		let arg = tg::pipe::create::Arg { remote };
		let output = handle
			.create_pipe(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to open pipe"))?;
		Ok(tg::process::Stdio::Pipe(output.id))
	}
}

fn get_termios_and_size(fd: RawFd) -> std::io::Result<(libc::termios, tg::pty::Size)> {
	unsafe {
		// Get the termios.
		let mut termios = MaybeUninit::zeroed();
		if libc::tcgetattr(fd, termios.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let termios = termios.assume_init();

		// Get the size.
		let mut winsize: MaybeUninit<libc::winsize> = MaybeUninit::zeroed();
		if libc::ioctl(fd, libc::TIOCGWINSZ, winsize.as_mut_ptr()) != 0 {
			return Err(std::io::Error::last_os_error());
		}
		let winsize = winsize.assume_init();
		let size = tg::pty::Size {
			cols: winsize.ws_col,
			rows: winsize.ws_row,
		};

		Ok((termios, size))
	}
}

impl Stdio {
	pub fn set_raw_mode(&self) -> tg::Result<()> {
		if let Some((fd, mut termios)) = self.termios {
			unsafe {
				termios.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
				termios.c_iflag &=
					!(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
				termios.c_oflag &= !(libc::OPOST);
				if libc::tcsetattr(fd, libc::TCSADRAIN, std::ptr::addr_of!(termios)) != 0 {
					let source = std::io::Error::last_os_error();
					return Err(tg::error!(!source, "failed to set the tty to raw mode"));
				}
			}
		}
		Ok(())
	}

	pub(super) fn close(
		&self,
		handle: &impl tg::Handle,
	) -> impl Future<Output = tg::Result<()>> + Send + 'static {
		let handle = handle.clone();
		let remote = self.remote.clone();
		let io = [self.stdin.clone(), self.stdout.clone(), self.stderr.clone()];
		async move {
			for io in io {
				match io {
					tg::process::Stdio::Pipe(pipe) => {
						let arg = tg::pipe::delete::Arg {
							remote: remote.clone(),
						};
						handle.delete_pipe(&pipe, arg).await.ok();
					},
					tg::process::Stdio::Pty(pty) => {
						let arg = tg::pty::delete::Arg {
							remote: remote.clone(),
						};
						handle.delete_pty(&pty, arg).await.ok();
					},
				}
			}
			Ok(())
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
