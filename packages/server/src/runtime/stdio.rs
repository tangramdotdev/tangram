use std::{
	os::fd::{AsRawFd, FromRawFd, OwnedFd},
	pin::pin,
};
use tangram_client as tg;
use tangram_either::Either;
use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite};

pub struct Host(Either<host::Pty, host::Socket>);

pub struct Guest(Either<guest::Pty, guest::Socket>);

pub fn pair(pty: bool) -> std::io::Result<(Host, Guest)> {
	if pty {
		unsafe {
			// Open the master terminal.
			let host = libc::posix_openpt(0);
			if host < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let host = OwnedFd::from_raw_fd(host);

			// Grant and unlock access to the slave terminal.
			if libc::grantpt(host.as_raw_fd()) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if libc::unlockpt(host.as_raw_fd()) < 0 {
				return Err(std::io::Error::last_os_error());
			}

			// Open the slave terminal.
			let mut buf = [0; 256];
			if libc::ptsname_r(host.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = libc::open(buf.as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
			if fd < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let guest = OwnedFd::from_raw_fd(fd);

			// Set the host as nonblocking.
			let flags = libc::fcntl(fd, libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
				return Err(std::io::Error::last_os_error());
			}

			// Return.
			let host = host::Pty(AsyncFd::new(host)?);
			let guest = guest::Pty(guest);
			Ok((Host(Either::Left(host)), Guest(Either::Left(guest))))
		}
	} else {
		let (host, guest) = tokio::net::UnixStream::pair()?;
		let guest = guest.into_std()?;
		guest.set_nonblocking(false)?;
		Ok((Host(Either::Right(host)), Guest(Either::Right(guest))))
	}
}

mod host {
	use num::ToPrimitive;
	use std::{
		os::fd::{AsRawFd, OwnedFd},
		task::Poll,
	};
	use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite};
	pub struct Pty(pub AsyncFd<OwnedFd>);
	pub type Socket = tokio::net::UnixStream;
	impl AsRawFd for Pty {
		fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
			self.0.as_raw_fd()
		}
	}

	impl AsyncRead for Pty {
		fn poll_read(
			self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
			buf: &mut tokio::io::ReadBuf<'_>,
		) -> std::task::Poll<std::io::Result<()>> {
			let fd = &mut self.get_mut().0;
			let mut guard = match fd.poll_read_ready(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Ok(guard)) => guard,
				Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			};
			let filled = buf.initialize_unfilled();
			let n = unsafe { libc::read(fd.as_raw_fd(), filled.as_mut_ptr().cast(), filled.len()) };
			if n < 0 {
				match std::io::Error::last_os_error() {
					error if error.kind() == std::io::ErrorKind::WouldBlock => {
						guard.clear_ready();
						return Poll::Pending;
					},
					error => return Poll::Ready(Err(error)),
				}
			}
			buf.advance(n.to_usize().unwrap());
			Poll::Ready(Ok(()))
		}
	}

	impl AsyncWrite for Pty {
		fn poll_shutdown(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
		) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}

		fn poll_flush(
			self: std::pin::Pin<&mut Self>,
			_cx: &mut std::task::Context<'_>,
		) -> Poll<std::io::Result<()>> {
			Poll::Ready(Ok(()))
		}

		fn poll_write(
			self: std::pin::Pin<&mut Self>,
			cx: &mut std::task::Context<'_>,
			buf: &[u8],
		) -> Poll<std::io::Result<usize>> {
			let fd = &mut self.get_mut().0;
			let mut guard = match fd.poll_write_ready(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Ok(guard)) => guard,
				Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			};
			let n = unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
			if n < 0 {
				match std::io::Error::last_os_error() {
					error if error.kind() == std::io::ErrorKind::WouldBlock => {
						guard.clear_ready();
						return Poll::Pending;
					},
					error => return Poll::Ready(Err(error)),
				}
			}
			Poll::Ready(Ok(n.to_usize().unwrap()))
		}
	}
}

mod guest {
	use std::os::fd::OwnedFd;
	pub struct Pty(pub OwnedFd);
	pub type Socket = std::os::unix::net::UnixStream;
}

impl Host {
	pub fn set_window_size(&mut self, window_size: tg::pipe::WindowSize) -> std::io::Result<()> {
		let Either::Left(pty) = &self.0 else {
			return Err(std::io::Error::other("not a pty"));
		};
		unsafe {
			let mut winsize = libc::winsize {
				ws_col: window_size.cols,
				ws_row: window_size.rows,
				ws_xpixel: window_size.xpos,
				ws_ypixel: window_size.ypos,
			};
			if libc::ioctl(
				pty.as_raw_fd(),
				libc::TIOCSWINSZ,
				std::ptr::addr_of_mut!(winsize),
			) < 0
			{
				return Err(std::io::Error::last_os_error());
			};
			Ok(())
		}
	}
}

impl AsyncRead for Host {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		match &mut self.get_mut().0 {
			Either::Left(io) => pin!(io).poll_read(cx, buf),
			Either::Right(io) => pin!(io).poll_read(cx, buf),
		}
	}
}

impl AsyncWrite for Host {
	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().0 {
			Either::Left(io) => pin!(io).poll_flush(cx),
			Either::Right(io) => pin!(io).poll_flush(cx),
		}
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		match &mut self.get_mut().0 {
			Either::Left(io) => pin!(io).poll_shutdown(cx),
			Either::Right(io) => pin!(io).poll_shutdown(cx),
		}
	}

	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		match &mut self.get_mut().0 {
			Either::Left(io) => pin!(io).poll_write(cx, buf),
			Either::Right(io) => pin!(io).poll_write(cx, buf),
		}
	}
}

impl AsRawFd for Guest {
	fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
		match &self.0 {
			Either::Left(io) => io.0.as_raw_fd(),
			Either::Right(io) => io.as_raw_fd(),
		}
	}
}
