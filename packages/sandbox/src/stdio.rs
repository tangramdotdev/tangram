use crate::common::socket_pair;
use std::{
	os::fd::{IntoRawFd, OwnedFd, RawFd},
	pin::pin,
	task::Poll,
};

pub struct Stdio {
	inner: Inner,
}

pub struct ChildStdin {
	host: tokio::net::UnixStream,
}

pub struct ChildStdout {
	host: tokio::net::UnixStream,
}

pub struct ChildStderr {
	host: tokio::net::UnixStream,
}

enum Inner {
	Null,
	Inherit,
	MakePipe,
	FileDescriptor(OwnedFd),
}

impl From<OwnedFd> for Stdio {
	fn from(value: OwnedFd) -> Self {
		Self {
			inner: Inner::FileDescriptor(value),
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

	pub(crate) fn split_stdin(self) -> std::io::Result<(Option<ChildStdin>, RawFd)> {
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
			Inner::MakePipe => {
				let (host, guest) = socket_pair()?;
				let host = ChildStdin { host };
				Ok((Some(host), guest.into_raw_fd()))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd.into_raw_fd())),
		}
	}

	pub(crate) fn split_stdout(self) -> std::io::Result<(Option<ChildStdout>, RawFd)> {
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
			Inner::MakePipe => {
				let (host, guest) = socket_pair()?;
				let host = ChildStdout { host };
				Ok((Some(host), guest.into_raw_fd()))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd.into_raw_fd())),
		}
	}

	pub(crate) fn split_stderr(self) -> std::io::Result<(Option<ChildStderr>, RawFd)> {
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
			Inner::MakePipe => {
				let (host, guest) = socket_pair()?;
				let host = ChildStderr { host };
				Ok((Some(host), guest.into_raw_fd()))
			},
			Inner::FileDescriptor(fd) => Ok((None, fd.into_raw_fd())),
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
		pin!(&mut this.host).poll_write(cx, buf)
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		let this = self.get_mut();
		pin!(&mut this.host).poll_flush(cx)
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), std::io::Error>> {
		let this = self.get_mut();
		pin!(&mut this.host).poll_shutdown(cx)
	}

	fn is_write_vectored(&self) -> bool {
		self.host.is_write_vectored()
	}

	fn poll_write_vectored(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		bufs: &[std::io::IoSlice<'_>],
	) -> Poll<Result<usize, std::io::Error>> {
		let this = self.get_mut();
		pin!(&mut this.host).poll_write_vectored(cx, bufs)
	}
}

impl tokio::io::AsyncRead for ChildStdout {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		pin!(&mut this.host).poll_read(cx, buf)
	}
}

impl tokio::io::AsyncRead for ChildStderr {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = self.get_mut();
		pin!(&mut this.host).poll_read(cx, buf)
	}
}
