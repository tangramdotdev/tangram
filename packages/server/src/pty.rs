use {
	crate::{Server, temp::Temp},
	byteorder::ReadBytesExt as _,
	std::{
		ffi::{CStr, CString},
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
			unix::process::ExitStatusExt as _,
		},
		process::Stdio,
		time::Duration,
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

		// Create a pipe to wait for the session readiness signal.
		let (mut ready_reader, ready_writer) = std::io::pipe()
			.map_err(|source| tg::error!(!source, "failed to create the session ready pipe"))?;

		// Unset FD_CLOEXEC on the pipe writer.
		let flags = unsafe { libc::fcntl(ready_writer.as_raw_fd(), libc::F_GETFD) };
		if flags < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to get the ready pipe flags"
			));
		}
		let ret = unsafe {
			libc::fcntl(
				ready_writer.as_raw_fd(),
				libc::F_SETFD,
				flags & !libc::FD_CLOEXEC,
			)
		};
		if ret < 0 {
			return Err(tg::error!(
				source = std::io::Error::last_os_error(),
				"failed to set the ready pipe flags"
			));
		}
		let ready_fd = ready_writer.as_raw_fd();

		// Spawn the session process.
		let executable = tangram_util::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get the current executable path"))?;
		let pty = name
			.to_str()
			.map_err(|source| tg::error!(!source, "failed to convert the pty name to a string"))?;
		let mut session = tokio::process::Command::new(executable)
			.kill_on_drop(true)
			.arg("session")
			.arg("--pty")
			.arg(pty)
			.arg("--path")
			.arg(temp.path())
			.arg("--ready-fd")
			.arg(ready_fd.to_string())
			.stderr(Stdio::piped())
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the session process"))?;
		drop(ready_writer);

		// Wait for the session process to signal readiness.
		let task = tokio::task::spawn_blocking(move || ready_reader.read_u8());
		let result = tokio::time::timeout(Duration::from_secs(5), task)
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the session ready signal"))
			.and_then(|output| {
				output.map_err(|source| tg::error!(!source, "the session ready task panicked"))
			})
			.and_then(|output| {
				output.map_err(|source| {
					tg::error!(!source, "failed to read the session ready signal")
				})
			})
			.and_then(|byte| {
				if byte != 0x00 {
					return Err(tg::error!("received an invalid ready byte {byte}"));
				}
				Ok(())
			});
		if let Err(source) = result {
			session.start_kill().ok();
			let output = tokio::time::timeout(Duration::from_secs(1), session.wait_with_output())
				.await
				.ok()
				.and_then(Result::ok);
			let stderr = output
				.as_ref()
				.map(|output| String::from_utf8_lossy(&output.stderr).trim().to_owned())
				.filter(|stderr| !stderr.is_empty());
			let exit_code = output.as_ref().and_then(|output| output.status.code());
			let signal = output.as_ref().and_then(|output| output.status.signal());
			let error = if let Some(stderr) = stderr {
				tg::error!(
					!source,
					stderr = %stderr,
					exit_code = ?exit_code,
					signal = ?signal,
					"failed to start the session listener"
				)
			} else {
				tg::error!(
					!source,
					exit_code = ?exit_code,
					signal = ?signal,
					"failed to start the session listener"
				)
			};
			return Err(error);
		}
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
