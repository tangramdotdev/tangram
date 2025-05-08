use std::{
	os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
	task::Poll,
};

use num::ToPrimitive;
use tokio::io::unix::AsyncFd;

pub struct Stdio {
	inner: Inner,
}

enum Inner {
	Null,
	Inherit,
	MakePipe,
	FileDescriptor(RawFd),
}

impl<T> From<T> for Stdio
where
	T: IntoRawFd,
{
	fn from(value: T) -> Self {
		Self {
			inner: Inner::FileDescriptor(value.into_raw_fd()),
		}
	}
}

impl Stdio {
	#[must_use]
	pub fn inherit() -> Self {
		Self {
			inner: Inner::Inherit,
		}
	}

	#[must_use]
	pub fn piped() -> Self {
		Self {
			inner: Inner::MakePipe,
		}
	}

	#[must_use]
	pub fn null() -> Self {
		Self { inner: Inner::Null }
	}

	pub(crate) fn split_stdin(&self) -> std::io::Result<(Option<ChildStdin>, RawFd)> {
		match self.inner {
			Inner::Null => {
				let host = None;
				let guest = std::fs::File::open("/dev/null")?.into_raw_fd();
				Ok((host, guest))
			},
			Inner::Inherit => {
				let host = None;
				let guest = libc::STDIN_FILENO;
				Ok((host, guest))
			},
			Inner::MakePipe => unsafe {
				let mut fds = [0; 2];
				if libc::pipe(fds.as_mut_ptr()) < 0 {
					return Err(std::io::Error::last_os_error());
				}
				let [guest, host] = fds;
				set_cloexec(host)?;
				set_cloexec(guest)?;
				let host = make_async_fd(host, tokio::io::Interest::WRITABLE)?;
				let host = ChildStdin { fd: host };
				Ok((Some(host), guest))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd)),
		}
	}

	pub(crate) fn split_stdout(&self) -> std::io::Result<(Option<ChildStdout>, RawFd)> {
		match self.inner {
			Inner::Null => {
				let host = None;
				let guest = std::fs::OpenOptions::new()
					.write(true)
					.open("/dev/null")?
					.into_raw_fd();
				Ok((host, guest))
			},
			Inner::Inherit => {
				let host = None;
				let guest = libc::STDOUT_FILENO;
				Ok((host, guest))
			},
			Inner::MakePipe => unsafe {
				let mut fds = [0; 2];
				if libc::pipe(fds.as_mut_ptr()) < 0 {
					return Err(std::io::Error::last_os_error());
				}
				let [host, guest] = fds;
				set_cloexec(host)?;
				set_cloexec(guest)?;
				let host = make_async_fd(host, tokio::io::Interest::READABLE)?;
				let host = ChildStdout { fd: host };
				Ok((Some(host), guest))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd)),
		}
	}

	pub(crate) fn split_stderr(&self) -> std::io::Result<(Option<ChildStderr>, RawFd)> {
		match self.inner {
			Inner::Null => {
				let host = None;
				let guest = std::fs::OpenOptions::new()
					.write(true)
					.open("/dev/null")?
					.into_raw_fd();
				Ok((host, guest))
			},
			Inner::Inherit => {
				let host = None;
				let guest = libc::STDERR_FILENO;
				Ok((host, guest))
			},
			Inner::MakePipe => unsafe {
				let mut fds = [0; 2];
				if libc::pipe(fds.as_mut_ptr()) < 0 {
					return Err(std::io::Error::last_os_error());
				}
				let [host, guest] = fds;
				let host = make_async_fd(host, tokio::io::Interest::READABLE)?;
				let host = ChildStderr { fd: host };
				Ok((Some(host), guest))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd)),
		}
	}
}

pub struct ChildStdin {
	fd: AsyncFd<OwnedFd>,
}

pub struct ChildStdout {
	fd: AsyncFd<OwnedFd>,
}

pub struct ChildStderr {
	fd: AsyncFd<OwnedFd>,
}

impl ChildStdin {
	pub fn new(fd: OwnedFd) -> std::io::Result<Self> {
		unsafe {
			let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = AsyncFd::with_interest(fd, tokio::io::Interest::WRITABLE)?;
			Ok(Self { fd })
		}
	}
}

impl ChildStdout {
	pub fn new(fd: OwnedFd) -> std::io::Result<Self> {
		unsafe {
			let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)?;
			Ok(Self { fd })
		}
	}
}

impl ChildStderr {
	pub fn new(fd: OwnedFd) -> std::io::Result<Self> {
		unsafe {
			let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = AsyncFd::with_interest(fd, tokio::io::Interest::READABLE)?;
			Ok(Self { fd })
		}
	}
}

impl tokio::io::AsyncWrite for ChildStdin {
	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &[u8],
	) -> std::task::Poll<Result<usize, std::io::Error>> {
		let this = self.get_mut();
		let mut guard = match this.fd.poll_write_ready(cx) {
			Poll::Ready(Ok(guard)) => guard,
			Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			Poll::Pending => return Poll::Pending,
		};
		let ret = guard.try_io(|fd| {
			let n = unsafe { libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };
			if n < 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(n.to_usize().unwrap())
		});
		match ret {
			Ok(result) => Poll::Ready(result),
			Err(_would_block) => Poll::Pending,
		}
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		std::task::Poll::Ready(Ok(()))
	}

	fn is_write_vectored(&self) -> bool {
		false
	}
}

impl tokio::io::AsyncRead for ChildStdout {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		let mut guard = match this.fd.poll_read_ready(cx) {
			Poll::Ready(Ok(guard)) => guard,
			Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			Poll::Pending => {
				return Poll::Pending;
			},
		};
		let ret = guard.try_io(|fd| {
			let n = unsafe {
				let len = buf.remaining();
				let buf = buf.unfilled_mut().as_mut_ptr().cast();
				libc::read(fd.as_raw_fd(), buf, len)
			};
			if n < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if n == 0 {
				return Ok(0);
			}
			let n = n.to_usize().unwrap();
			unsafe {
				buf.assume_init(n);
			}
			buf.advance(n);
			Ok(n)
		});
		match ret {
			Ok(result) => Poll::Ready(result.map(|_| ())),
			Err(_would_block) => Poll::Pending,
		}
	}
}

impl tokio::io::AsyncRead for ChildStderr {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		let mut guard = match this.fd.poll_read_ready(cx) {
			Poll::Ready(Ok(guard)) => guard,
			Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			Poll::Pending => return Poll::Pending,
		};
		let ret = guard.try_io(|fd| {
			let n = unsafe {
				let len = buf.remaining();
				let buf = buf.unfilled_mut().as_mut_ptr().cast();
				libc::read(fd.as_raw_fd(), buf, len)
			};
			if n < 0 {
				return Err(std::io::Error::last_os_error());
			}
			if n == 0 {
				return Ok(0);
			}
			let n = n.to_usize().unwrap();
			unsafe {
				buf.assume_init(n);
			}
			buf.advance(n);
			Ok(n)
		});

		match ret {
			Ok(result) => Poll::Ready(result.map(|_| ())),
			Err(_would_block) => Poll::Pending,
		}
	}
}

fn make_async_fd(fd: RawFd, interest: tokio::io::Interest) -> std::io::Result<AsyncFd<OwnedFd>> {
	unsafe {
		let flags = libc::fcntl(fd, libc::F_GETFL);
		if flags < 0 {
			return Err(std::io::Error::last_os_error());
		}
		let flags = flags | libc::O_NONBLOCK;
		let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags);
		if ret < 0 {
			return Err(std::io::Error::last_os_error());
		}
		AsyncFd::with_interest(OwnedFd::from_raw_fd(fd), interest)
	}
}

fn set_cloexec(fd: RawFd) -> std::io::Result<()> {
	unsafe {
		let flags = libc::fcntl(fd, libc::F_GETFD);
		if flags < 0 {
			return Err(std::io::Error::last_os_error());
		}
		let flags = flags | libc::O_CLOEXEC;
		let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, flags);
		if ret < 0 {
			return Err(std::io::Error::last_os_error());
		}
		Ok(())
	}
}
