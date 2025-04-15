use crate::{
	Tty,
	stdio::{Reader, Writer},
};
use std::{
	collections::VecDeque,
	ffi::{CStr, CString},
	os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};

#[allow(clippy::struct_field_names)]
#[derive(Clone)]
pub(crate) struct Pty {
	pub(crate) pty_fd: Option<RawFd>,
	pub(crate) tty_fd: Option<RawFd>,
	pub(crate) tty_path: CString,
}

impl Pty {
	pub fn into_reader_writer(&mut self) -> std::io::Result<(Reader, Writer)> {
		unsafe {
			let read = self.pty_fd.take().unwrap();
			let write = libc::dup(read);
			if write < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let reader = Reader {
				file: OwnedFd::from_raw_fd(read),
				buffer: VecDeque::new(),
				future: None,
			};
			let writer = Writer {
				file: OwnedFd::from_raw_fd(write),
				isatty: true,
				future: None,
			};
			Ok((reader, writer))
		}
	}
}

impl Pty {
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
