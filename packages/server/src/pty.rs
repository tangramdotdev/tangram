use {
	crate::Server,
	std::{
		ffi::{CStr, CString},
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
	},
	tangram_client as tg,
};

mod close;
mod create;
mod read;
mod size;
mod write;

pub(crate) struct Pty {
	host: OwnedFd,
	guest: OwnedFd,
	name: CString,
}

impl Pty {
	async fn new(size: tg::pty::Size) -> tg::Result<Self> {
		tokio::task::spawn_blocking(move || unsafe {
			// Create the pty.
			let mut win_size = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut host = 0;
			let mut guest = 0;
			let mut name = [0; 256];
			let ret = libc::openpty(
				std::ptr::addr_of_mut!(host),
				std::ptr::addr_of_mut!(guest),
				name.as_mut_ptr(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let host = OwnedFd::from_raw_fd(host);
			let guest = OwnedFd::from_raw_fd(guest);
			let name = CStr::from_ptr(name.as_ptr().cast()).to_owned();

			// Make it non-blocking.
			let flags = libc::fcntl(host.as_raw_fd(), libc::F_GETFL);
			if flags < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let flags = flags | libc::O_NONBLOCK;
			let ret = libc::fcntl(host.as_raw_fd(), libc::F_SETFL, flags);
			if ret < 0 {
				return Err(std::io::Error::last_os_error());
			}

			let pty = Self { host, guest, name };

			Ok(pty)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create a pty"))
	}
}

impl Server {
	pub(crate) fn get_pty_fd(&self, pty: &tg::pty::Id, host: bool) -> tg::Result<OwnedFd> {
		let pty = self
			.ptys
			.get(pty)
			.ok_or_else(|| tg::error!("failed to find the pty"))?;
		let fd = if host { &pty.host } else { &pty.guest };
		let fd = fd
			.try_clone()
			.map_err(|source| tg::error!(!source, "failed to clone the fd"))?;
		Ok(fd)
	}

	#[allow(dead_code)]
	pub(crate) fn get_pty_name(&self, pty: &tg::pty::Id) -> tg::Result<CString> {
		let pty = self
			.ptys
			.get(pty)
			.ok_or_else(|| tg::error!("failed to find the pty"))?;
		let name = pty.name.clone();
		Ok(name)
	}
}
