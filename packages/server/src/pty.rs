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
pub(crate) mod session;
mod size;
mod write;

pub(crate) struct Pty {
	host: OwnedFd,
	guest: OwnedFd,
	name: CString,
	session_task: tokio::task::JoinHandle<()>,
	spawn_tx: tokio::sync::mpsc::UnboundedSender<session::PtySpawnRequest>,
}

impl Pty {
	async fn new(size: tg::pty::Size) -> tg::Result<Self> {
		// Create the pty.
		let (host, guest, name) = tokio::task::spawn_blocking(move || unsafe {
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

			Ok::<_, std::io::Error>((host, guest, name))
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create a pty"))?;

		// Create socketpair for communication with session leader.
		let (server_sock, session_sock) = tokio::net::UnixStream::pair()
			.map_err(|source| tg::error!(!source, "failed to create socketpair"))?;

		// Spawn session leader process.
		let executable = std::env::current_exe()
			.map_err(|source| tg::error!(!source, "failed to get current executable"))?;
		let name_str = name
			.to_str()
			.map_err(|source| tg::error!(!source, "invalid pty name"))?;

		let mut cmd = tokio::process::Command::new(executable);
		cmd.arg("session")
			.arg("--name")
			.arg(name_str)
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::inherit()); // Inherit stderr for debugging

		// Pass socket as FD 3.
		let session_sock_fd = session_sock.as_raw_fd();
		unsafe {
			cmd.pre_exec(move || {
				// Write a marker to stderr to confirm pre_exec is running
				let msg = b"PRE_EXEC RUNNING\n";
				libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());

				// Dup session_sock to FD 3.
				if libc::dup2(session_sock_fd, 3) < 0 {
					let msg = b"PRE_EXEC: dup2 failed\n";
					libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
					return Err(std::io::Error::last_os_error());
				}

				// Close all other FDs except 0, 1, 2, 3.
				// We need to be careful not to close session_sock_fd before dup2
				let max_fd = libc::sysconf(libc::_SC_OPEN_MAX);
				for fd in 4..max_fd {
					libc::close(fd as i32);
				}

				let msg = b"PRE_EXEC COMPLETE\n";
				libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
				Ok(())
			});
		}

		tracing::debug!("spawning session leader process");
	let child = cmd
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn session leader"))?;
	tracing::debug!(pid = child.id(), "spawned session leader process");
		// Note: we don't wait for this child, it lives as long as the PTY.

		// Drop the session socket since it's been duped to the child.
		drop(session_sock);

		// Create channel for spawn requests.
		let (spawn_tx, spawn_rx) = tokio::sync::mpsc::unbounded_channel();

		// Spawn tokio task to manage communication with session leader.
		let session_task = tokio::spawn(session::pty_session_task(server_sock, spawn_rx));

		Ok(Self {
			host,
			guest,
			name,
			session_task,
			spawn_tx,
		})
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

	pub(crate) fn send_pty_spawn_request(
		&self,
		pty_id: &tg::pty::Id,
		request: session::PtySpawnRequest,
	) -> tg::Result<()> {
		let pty = self
			.ptys
			.get(pty_id)
			.ok_or_else(|| tg::error!("PTY not found"))?;
		pty.spawn_tx
			.send(request)
			.map_err(|_| tg::error!("PTY session task died"))?;
		Ok(())
	}
}
