use std::os::fd::RawFd;
use tangram_client as tg;

mod close;
mod create;
mod read;
mod size;
mod write;

pub(crate) struct Pty {
	pub host: RawFd,
	pub guest: RawFd,
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
			let mut host = 0;
			let mut guest = 0;
			let mut tty_name = [0; 256];
			if libc::openpty(
				std::ptr::addr_of_mut!(host),
				std::ptr::addr_of_mut!(guest),
				tty_name.as_mut_ptr(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			) < 0
			{
				return Err(std::io::Error::last_os_error());
			}

			// Mark pty as non blocking.
			let flags = libc::fcntl(host, libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(host, libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}

			let pty = Self { host, guest };
			Ok(pty)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to open pty"))
	}
}
