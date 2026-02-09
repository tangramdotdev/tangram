use {
	crate::{Server, temp::Temp},
	std::{
		ffi::{CStr, CString},
		os::fd::{FromRawFd as _, OwnedFd},
	},
	tangram_client::prelude::*,
};

mod close;
mod create;
mod delete;
mod read;
mod size;
mod write;

pub(crate) struct Pty {
	master: Option<OwnedFd>,
	slave: Option<OwnedFd>,
	#[expect(dead_code)]
	name: CString,
	session: Option<tokio::process::Child>,
	pub(crate) temp: Temp,
}

impl Pty {
	async fn new(server: &Server, size: tg::pty::Size) -> tg::Result<Self> {
		let (master, slave, name) = unsafe {
			// Create the pty.
			let mut win_size = libc::winsize {
				ws_col: size.cols,
				ws_row: size.rows,
				ws_xpixel: 0,
				ws_ypixel: 0,
			};
			let mut master = 0;
			let mut slave = 0;
			let mut name = [0; 256];
			let ret = libc::openpty(
				std::ptr::addr_of_mut!(master),
				std::ptr::addr_of_mut!(slave),
				name.as_mut_ptr(),
				std::ptr::null_mut(),
				std::ptr::addr_of_mut!(win_size),
			);
			if ret < 0 {
				let error = std::io::Error::last_os_error();
				let error = tg::error!(!error, "failed to open the pty");
				return Err(error);
			}

			let master = OwnedFd::from_raw_fd(master);
			let master = Some(master);

			let slave = OwnedFd::from_raw_fd(slave);
			let slave = Some(slave);

			let name = CStr::from_ptr(name.as_ptr().cast()).to_owned();

			(master, slave, name)
		};

		// Create a temp for the session socket.
		let temp = Temp::new(server);

		// Spawn the session process.
		let executable: std::path::PathBuf = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;
		let pty = name
			.to_str()
			.map_err(|source| tg::error!(!source, "failed to convert the pty name to a string"))?;
		let session = tokio::process::Command::new(executable)
			.kill_on_drop(true)
			.arg("session")
			.arg("--pty")
			.arg(pty)
			.arg("--path")
			.arg(temp.path())
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the session process"))?;
		let session = Some(session);

		let pty = Self {
			master,
			slave,
			name,
			session,
			temp,
		};

		Ok(pty)
	}
}
