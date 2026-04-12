use {
	num::ToPrimitive,
	std::{
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		pin::Pin,
		task::{Context, Poll, ready},
	},
	tangram_client::prelude::*,
	tokio::io::{AsyncRead, AsyncWrite, ReadBuf, unix::AsyncFd},
};

pub struct Pty {
	input: AsyncFd<OwnedFd>,
	master: OwnedFd,
	output: AsyncFd<OwnedFd>,
	slave: Option<OwnedFd>,
}

impl Pty {
	pub fn new(tty: tg::process::Tty) -> tg::Result<Self> {
		unsafe {
			let mut win_size = libc::winsize {
				ws_col: tty.size.cols,
				ws_row: tty.size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut master = 0;
			let mut slave = 0;
			let ret = libc::openpty(
				std::ptr::addr_of_mut!(master),
				std::ptr::addr_of_mut!(slave),
				std::ptr::null_mut(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			);
			if ret < 0 {
				let error = std::io::Error::last_os_error();
				return Err(tg::error!(!error, "failed to open the pty"));
			}
			let master = OwnedFd::from_raw_fd(master);
			let slave = Some(OwnedFd::from_raw_fd(slave));
			let input = master
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone the pty input fd"))
				.and_then(|fd| {
					async_fd(fd)
						.map_err(|source| tg::error!(!source, "failed to create the pty input"))
				})?;
			let output = master
				.try_clone()
				.map_err(|source| tg::error!(!source, "failed to clone the pty output fd"))
				.and_then(|fd| {
					async_fd(fd)
						.map_err(|source| tg::error!(!source, "failed to create the pty output"))
				})?;
			Ok(Self {
				input,
				master,
				output,
				slave,
			})
		}
	}

	pub fn master(&self) -> &OwnedFd {
		&self.master
	}

	pub fn slave(&self) -> Option<&OwnedFd> {
		self.slave.as_ref()
	}

	pub fn take_slave(&mut self) -> Option<OwnedFd> {
		self.slave.take()
	}
}

impl AsyncRead for &Pty {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		let this = *self.get_mut();
		loop {
			let mut guard = ready!(this.output.poll_read_ready(cx))?;
			let fd = this.output.as_raw_fd();
			let unfilled = unsafe { buf.unfilled_mut() };
			let result = unsafe { libc::read(fd, unfilled.as_mut_ptr().cast(), unfilled.len()) };
			if result < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::WouldBlock {
					guard.clear_ready();
					continue;
				}
				return Poll::Ready(Err(error));
			}
			let n = result.to_usize().unwrap();
			unsafe {
				buf.assume_init(n);
			}
			buf.advance(n);
			return Poll::Ready(Ok(()));
		}
	}
}

impl AsyncWrite for &Pty {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		let this = *self.get_mut();
		loop {
			let mut guard = ready!(this.input.poll_write_ready(cx))?;
			let fd = this.input.as_raw_fd();
			let result = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
			if result < 0 {
				let error = std::io::Error::last_os_error();
				if error.kind() == std::io::ErrorKind::WouldBlock {
					guard.clear_ready();
					continue;
				}
				return Poll::Ready(Err(error));
			}
			return Poll::Ready(Ok(result.to_usize().unwrap()));
		}
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Poll::Ready(Ok(()))
	}
}

fn async_fd(fd: OwnedFd) -> std::io::Result<AsyncFd<OwnedFd>> {
	unsafe {
		let flags = libc::fcntl(fd.as_raw_fd(), libc::F_GETFL);
		if flags < 0 {
			return Err(std::io::Error::last_os_error());
		}
		if libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
			return Err(std::io::Error::last_os_error());
		}
	}
	AsyncFd::new(fd)
}
