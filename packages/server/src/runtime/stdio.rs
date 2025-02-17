use super::pty::{Master, Slave};
use futures::future::Either;
use std::os::fd::{AsRawFd, IntoRawFd};
use std::pin::pin;
use std::ptr::addr_of_mut;
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncWrite};

pub enum Stdio {
	Pty(Pty),
	Piped(Piped),
}

pub struct Pty {
	pub master: Master,
	pub slave: Slave,
}

pub struct Piped {
	pub stdin: Pipe,
	pub stdout: Pipe,
	pub stderr: Pipe,
}

pub struct Pipe {
	pub host: tokio::net::UnixStream,
	pub guest: std::os::unix::net::UnixStream,
}

pub struct HostIo {
	inner: Either<super::pty::Master, tokio::net::UnixStream>,
	child: libc::pid_t,
}

impl Stdio {
	pub fn new(pty: Option<tg::process::pty::Pty>) -> std::io::Result<Self> {
		if let Some(_pty) = pty {
			let (master, slave) = super::pty::open()?;
			let pty = Pty { master, slave };
			Ok(Self::Pty(pty))
		} else {
			let pipes = Piped {
				stdin: Pipe::open()?,
				stdout: Pipe::open()?,
				stderr: Pipe::open()?,
			};
			Ok(Self::Piped(pipes))
		}
	}

	/// Consume the struct, returning streams for stdin/stdout/stderr of the child.
	pub fn host(
		self,
	) -> std::io::Result<(
		Box<dyn AsyncWrite + Unpin + Send + 'static>,
		Box<dyn AsyncRead + Unpin + Send + 'static>,
		Box<dyn AsyncRead + Unpin + Send + 'static>,
	)> {
		match self {
			Self::Pty(pty) => {
				let stdin = pty.master;
				let stdout = stdin.try_clone()?;
				let stderr = stdin.try_clone()?;
				Ok((Box::new(stdin), Box::new(stdout), Box::new(stderr)))
			},
			Self::Piped(piped) => {
				let Piped {
					stdin,
					stdout,
					stderr,
				} = piped;
				Ok((
					Box::new(stdin.host),
					Box::new(stdout.host),
					Box::new(stderr.host),
				))
			},
		}
	}

	/// Consume the struct, setting stdin/stdout/stderr for the child process.
	pub fn guest(self) -> std::io::Result<()> {
		let (stdin, stdout, stderr) = match self {
			Self::Pty(pty) => {
				drop(pty.master);
				let slave = pty.slave.fd.into_raw_fd();
				unsafe {
					if libc::setsid() < 0 {
						return Err(std::io::Error::last_os_error());
					}
				}

				(slave, slave, slave)
			},
			Self::Piped(piped) => {
				let Piped {
					stdin,
					stdout,
					stderr,
				} = piped;
				let stdin = stdin.guest.into_raw_fd();
				let stdout = stdout.guest.into_raw_fd();
				let stderr: i32 = stderr.guest.into_raw_fd();
				(stdin, stdout, stderr)
			},
		};
		unsafe {
			libc::dup2(stdin, libc::STDIN_FILENO);
			libc::dup2(stdout, libc::STDOUT_FILENO);
			libc::dup2(stderr, libc::STDOUT_FILENO);
		}
		Ok(())
	}
}

impl Pipe {
	fn open() -> std::io::Result<Self> {
		let (host, guest) = tokio::net::UnixStream::pair()?;
		let guest = guest.into_std()?;
		guest.set_nonblocking(false)?;
		Ok(Self { host, guest })
	}
}

impl HostIo {
	pub fn signal(&self, signal: i32) -> tg::Result<()> {
		unsafe {
			if libc::kill(self.child, signal) != 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(!source, "failed to signal process"));
			}
		}
		Ok(())
	}

	pub fn change_window_size(&self, rows: u16, cols: u16) -> tg::Result<()> {
		let Either::Left(pty) = &self.inner else {
			return Err(tg::error!("stream is not a pty"));
		};
		unsafe {
			let mut winsize = libc::winsize {
				ws_col: cols,
				ws_row: rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let fd = pty.fd.as_raw_fd();
			if libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize)) < 0 {
				let source = std::io::Error::last_os_error();
				return Err(tg::error!(!source, "failed to set child window size"));
			}
		}
		Ok(())
	}
}

impl AsyncRead for HostIo {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match &mut self.get_mut().inner {
			Either::Left(pty) => pin!(pty).poll_read(cx, buf),
			Either::Right(sock) => pin!(sock).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for HostIo {
	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(pty) => pin!(pty).poll_flush(cx),
			Either::Right(sock) => pin!(sock).poll_flush(cx),
		}
	}

	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(pty) => pin!(pty).poll_write(cx, buf),
			Either::Right(sock) => pin!(sock).poll_write(cx, buf),
		}
	}

	fn poll_write_vectored(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		bufs: &[std::io::IoSlice<'_>],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(pty) => pin!(pty).poll_write_vectored(cx, bufs),
			Either::Right(sock) => pin!(sock).poll_write_vectored(cx, bufs),
		}
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().inner {
			Either::Left(pty) => pin!(pty).poll_shutdown(cx),
			Either::Right(sock) => pin!(sock).poll_shutdown(cx),
		}
	}

	fn is_write_vectored(&self) -> bool {
		match &self.inner {
			Either::Left(pty) => pty.is_write_vectored(),
			Either::Right(sock) => sock.is_write_vectored(),
		}
	}
}
