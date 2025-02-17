use num::ToPrimitive;
use std::{
	os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
	task::{Context, Poll},
};
use tokio::io::{unix::AsyncFd, AsyncRead, AsyncWrite};
pub struct Master {
	pub fd: AsyncFd<OwnedFd>,
}

pub struct Slave {
	pub fd: OwnedFd,
}

pub fn open() -> std::io::Result<(Master, Slave)> {
	unsafe {
		// Open the master.
		let fd = libc::posix_openpt(libc::O_RDWR);
		if fd < 0 {
			return Err(std::io::Error::last_os_error());
		}

		// Create a slave.
		let master = OwnedFd::from_raw_fd(fd);
		if libc::grantpt(master.as_raw_fd()) < 0 {
			return Err(std::io::Error::last_os_error());
		}

		// Unlock the slave.
		let master = OwnedFd::from_raw_fd(fd);
		if libc::unlockpt(master.as_raw_fd()) < 0 {
			return Err(std::io::Error::last_os_error());
		}

		// Get the slave name.
		let mut slave_name = [0i8; 256];
		if libc::ptsname_r(
			master.as_raw_fd(),
			slave_name.as_mut_ptr(),
			slave_name.len(),
		) < 0
		{
			return Err(std::io::Error::last_os_error());
		}

		// Open the save.
		let fd = libc::open(slave_name.as_ptr(), libc::O_RDWR);
		if fd < 0 {
			return Err(std::io::Error::last_os_error());
		}
		let slave = OwnedFd::from_raw_fd(fd);

		// Set the master/slave as nonblocking
		set_nonblocking(master.as_raw_fd())?;
		set_nonblocking(slave.as_raw_fd())?;

		let master = AsyncFd::new(master).unwrap();
		Ok((Master { fd: master }, Slave { fd: slave }))
	}
}

impl Master {
	pub fn try_clone(&self) -> std::io::Result<Self> {
		let fd = self.fd.as_fd().try_clone_to_owned()?;
		let fd = AsyncFd::new(fd)?;
		Ok(Self { fd })
	}
}

unsafe fn set_nonblocking(fd: impl AsRawFd) -> std::io::Result<()> {
	let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
	if flags < 0 {
		return Err(std::io::Error::last_os_error());
	}
	let ret = libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
	if ret < 0 {
		return Err(std::io::Error::last_os_error());
	}

	Ok(())
}

fn poll_read(
	fd: &mut AsyncFd<OwnedFd>,
	buf: &mut [u8],
	cx: &mut Context,
) -> Poll<std::io::Result<usize>> {
	match fd.poll_read_ready_mut(cx) {
		Poll::Pending => return Poll::Pending,
		Poll::Ready(Ok(_)) => (),
		Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
	};
	unsafe {
		match libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) {
			n if n >= 0 => Poll::Ready(Ok(n.to_usize().unwrap())),
			_ => {
				let error = std::io::Error::last_os_error();
				if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
					return Poll::Pending;
				}
				Poll::Ready(Err(error))
			},
		}
	}
}

fn poll_write(
	fd: &mut AsyncFd<OwnedFd>,
	buf: &[u8],
	cx: &mut Context,
) -> Poll<std::io::Result<usize>> {
	match fd.poll_write_ready_mut(cx) {
		Poll::Pending => return Poll::Pending,
		Poll::Ready(Ok(_)) => (),
		Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
	};
	unsafe {
		match libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), buf.len()) {
			n if n >= 0 => Poll::Ready(Ok(n.to_usize().unwrap())),
			_ => {
				let error = std::io::Error::last_os_error();
				if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) {
					return Poll::Pending;
				}
				Poll::Ready(Err(error))
			},
		}
	}
}

impl AsyncRead for Master {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let fd = &mut self.get_mut().fd;
		match poll_read(fd, buf.initialize_unfilled(), cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Ok(n)) => {
				buf.advance(n);
				if buf.remaining() == 0 || n == 0 {
					Poll::Ready(Ok(()))
				} else {
					Poll::Pending
				}
			},
			Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
		}
	}
}

impl AsyncWrite for Master {
	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut Context<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		Poll::Ready(Ok(()))
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		_cx: &mut Context<'_>,
	) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let fd = &mut self.get_mut().fd;
		poll_write(fd, buf, cx)
	}
}
