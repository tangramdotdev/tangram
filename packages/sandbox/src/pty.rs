use crate::Tty;
use num::ToPrimitive;
use std::{
	ffi::{CStr, CString},
	os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
	task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};

#[allow(clippy::struct_field_names)]
#[derive(Clone)]
pub(crate) struct Pty {
	pub(crate) pty_fd: Option<RawFd>,
	pub(crate) tty_fd: Option<RawFd>,
	pub(crate) tty_path: CString,
}

pub struct Writer {
	pty_fd: Option<OwnedFd>,
	writer: DuplexStream,
}

pub struct Reader {
	pty_fd: Option<OwnedFd>,
	reader: DuplexStream,
}

impl Drop for Writer {
	fn drop(&mut self) {
		let Some(fd) = self.pty_fd.take() else {
			return;
		};
		unsafe { libc::close(fd.into_raw_fd()) };
	}
}

impl Drop for Reader {
	fn drop(&mut self) {
		let Some(fd) = self.pty_fd.take() else {
			return;
		};
		unsafe { libc::close(fd.into_raw_fd()) };
	}
}

impl Writer {
	pub fn get_reader(&self) -> std::io::Result<Reader> {
		let pty_fd = self.pty_fd.as_ref().unwrap().try_clone()?;
		let (mut send, recv) = tokio::io::duplex(256);
		tokio::task::spawn_blocking({
			let fd = pty_fd.as_raw_fd();
			move || {
				let mut buf = vec![0u8; 256];
				unsafe {
					loop {
						let n = libc::read(fd, buf.as_mut_ptr().cast(), buf.len());
						if n == 0 {
							break;
						}
						if n < 0 {
							return Err(std::io::Error::last_os_error());
						}
						let n = n.to_usize().unwrap();
						tokio::runtime::Handle::current().block_on(async {
							send.write_all(&buf[0..n]).await?;
							send.flush().await?;
							Ok::<_, std::io::Error>(())
						})?;
					}
					Ok(())
				}
			}
		});
		Ok(Reader {
			pty_fd: Some(pty_fd),
			reader: recv,
		})
	}
}

impl Pty {
	pub(crate) fn into_writer(mut self) -> Writer {
		self.close_tty();
		let (send, mut recv) = tokio::io::duplex(256);
		let fd = unsafe { OwnedFd::from_raw_fd(self.pty_fd.take().unwrap()) };
		tokio::task::spawn_blocking({
			let fd = fd.as_raw_fd();
			move || {
				'outer: loop {
					let mut buf = vec![0u8; 256];
					let mut n = tokio::runtime::Handle::current().block_on(recv.read(&mut buf))?;
					if n == 0 {
						break;
					}
					unsafe {
						while n > 0 {
							let m = libc::write(fd.as_raw_fd(), buf.as_ptr().cast(), n);
							if m <= 0 {
								break 'outer;
							}
							n -= m.to_usize().unwrap();
						}
					}
				}
				Ok::<_, std::io::Error>(())
			}
		});
		Writer {
			pty_fd: Some(fd),
			writer: send,
		}
	}

	// Identical to the darwin implementation, with different mutability of the argument pointers.
	#[cfg(target_os = "linux")]
	pub(crate) async fn open(tty: Tty) -> std::io::Result<Self> {
		tokio::task::spawn_blocking(move || unsafe {
			let win_size = libc::winsize {
				ws_col: tty.cols,
				ws_row: tty.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut pty_fd = 0;
			let mut tty_fd = 0;
			let mut tty_name = [0; 256];
			if libc::openpty(
				std::ptr::addr_of_mut!(pty_fd),
				std::ptr::addr_of_mut!(tty_fd),
				tty_name.as_mut_ptr(),
				std::ptr::null(),
				std::ptr::addr_of!(win_size),
			) < 0
			{
				return Err(std::io::Error::last_os_error());
			}
			let tty_path = CStr::from_ptr(tty_name.as_ptr()).to_owned();
			let pty = Self {
				pty_fd: Some(pty_fd),
				tty_fd: Some(tty_fd),
				tty_path: tty_path.clone(),
			};
			Ok(pty)
		})
		.await
		.unwrap()
	}

	// Identical to the linux implementation, with different mutability for the argument pointers.
	#[cfg(target_os = "macos")]
	pub(crate) async fn open(tty: Tty) -> std::io::Result<Self> {
		tokio::task::spawn_blocking(move || unsafe {
			let mut win_size = libc::winsize {
				ws_col: tty.cols,
				ws_row: tty.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut pty_fd = 0;
			let mut tty_fd = 0;
			let mut tty_name = [0; 256];
			if libc::openpty(
				std::ptr::addr_of_mut!(pty_fd),
				std::ptr::addr_of_mut!(tty_fd),
				tty_name.as_mut_ptr(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			) < 0
			{
				return Err(std::io::Error::last_os_error());
			}
			let tty_path = CStr::from_ptr(tty_name.as_ptr()).to_owned();
			let pty = Self {
				pty_fd: Some(pty_fd),
				tty_fd: Some(tty_fd),
				tty_path: tty_path.clone(),
			};
			Ok(pty)
		})
		.await
		.unwrap()
	}

	pub(crate) fn close_pty(&mut self) {
		let Some(fd) = self.pty_fd.take() else {
			return;
		};
		unsafe { libc::close(fd) };
	}

	pub(crate) fn close_tty(&mut self) {
		let Some(fd) = self.tty_fd.take() else {
			return;
		};
		unsafe { libc::close(fd) };
	}

	pub(crate) fn set_controlling_terminal(&self) -> std::io::Result<()> {
		unsafe {
			// Disconnect from the old controlling terminal.
			let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
			#[allow(clippy::useless_conversion)]
			if fd > 0 {
				libc::ioctl(fd, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
				libc::close(fd);
			}

			// Set the current process as session leader.
			if libc::setsid() == -1 {
				return Err(std::io::Error::last_os_error());
			}

			// Verify that we disconnected from the controlling terminal.
			let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
			if fd >= 0 {
				libc::close(fd);
				return Err(std::io::Error::other("failed to remove controlling tty"));
			}

			// Set the slave as the controlling tty.
			#[allow(clippy::useless_conversion)]
			if libc::ioctl(
				self.tty_fd.as_ref().unwrap().as_raw_fd(),
				libc::TIOCSCTTY.into(),
				0,
			) < 0
			{
				return Err(std::io::Error::last_os_error());
			}

			Ok(())
		}
	}
}

impl Drop for Pty {
	fn drop(&mut self) {
		let tty_path = self.tty_path.clone();
		tokio::task::spawn_blocking(move || unsafe {
			libc::chown(tty_path.as_ptr(), 0, 0);
			libc::chmod(tty_path.as_ptr(), 0o666);
		});
	}
}

impl AsyncWrite for Writer {
	fn is_write_vectored(&self) -> bool {
		self.writer.is_write_vectored()
	}

	fn poll_flush(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		std::pin::pin!(&mut self.get_mut().writer).poll_flush(cx)
	}

	fn poll_shutdown(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Result<(), std::io::Error>> {
		let this = self.get_mut();
		this.pty_fd.take();
		std::pin::pin!(&mut this.writer).poll_shutdown(cx)
	}

	fn poll_write(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<Result<usize, std::io::Error>> {
		std::pin::pin!(&mut self.get_mut().writer).poll_write(cx, buf)
	}

	fn poll_write_vectored(
		self: std::pin::Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[std::io::IoSlice<'_>],
	) -> Poll<Result<usize, std::io::Error>> {
		std::pin::pin!(&mut self.get_mut().writer).poll_write_vectored(cx, bufs)
	}
}

impl AsyncRead for Reader {
	fn poll_read(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
		buf: &mut tokio::io::ReadBuf<'_>,
	) -> std::task::Poll<std::io::Result<()>> {
		std::pin::pin!(&mut self.get_mut().reader).poll_read(cx, buf)
	}
}

impl Writer {
	pub(crate) async fn change_window_size(&self, tty: Tty) -> std::io::Result<()> {
		let fd = self.pty_fd.as_ref().unwrap().as_raw_fd();
		tokio::task::spawn_blocking(move || unsafe {
			let mut winsize = libc::winsize {
				ws_col: tty.cols,
				ws_row: tty.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			if libc::ioctl(fd, libc::TIOCSWINSZ, std::ptr::addr_of_mut!(winsize)) != 0 {
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		})
		.await
		.unwrap()
	}
}
