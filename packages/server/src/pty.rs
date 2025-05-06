use std::os::fd::{FromRawFd, OwnedFd};
use tangram_client as tg;

mod close;
mod create;
mod read;
mod size;
mod write;

pub(crate) struct Pty {
	pub pty_fd: OwnedFd,
	pub tty_fd: OwnedFd,
}

impl Pty {
	async fn open(size: tg::pty::Size) -> tg::Result<Self> {
		tokio::task::spawn_blocking(move || unsafe {
			let mut win_size = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
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

			// Mark pty as non blocking.
			let flags = libc::fcntl(pty_fd, libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(pty_fd, libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}

			// Take ownership of the FDs.
			let pty_fd = OwnedFd::from_raw_fd(pty_fd);
			let tty_fd = OwnedFd::from_raw_fd(tty_fd);

			let pty = Self { pty_fd, tty_fd };
			Ok(pty)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to open pty"))
	}
}
