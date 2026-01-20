use {
	crate::{Server, temp::Temp},
	bytes::Bytes,
	futures::TryFutureExt as _,
	num::ToPrimitive,
	std::{
		ffi::{CStr, CString},
		os::fd::{AsRawFd, FromRawFd as _, OwnedFd},
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
	pub(crate) master: Option<OwnedFd>,
	pub(crate) slave: Option<OwnedFd>,
	pub(crate) name: CString,
	session: Option<tokio::process::Child>,
	sender: tokio::sync::mpsc::UnboundedSender<WriteMessage>,
	pub(crate) task: tokio::task::JoinHandle<tg::Result<()>>,
	pub(crate) temp: Temp,
}

enum WriteMessage {
	Size(tg::pty::Size),
	Chunk { master: bool, bytes: Bytes },
}

impl Pty {
	async fn new(server: &Server, size: tg::pty::Size, id: tg::pty::Id) -> tg::Result<Self> {
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
		let executable = tangram_util::env::current_exe()
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

		// Create a sender/receiver pair.
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

		// Spawn a task to handle writes.
		let task = tokio::task::spawn(
			{
				let server = server.clone();
				async move {
					while let Some(message) = receiver.recv().await {
						// Break out of the loop if we can't get the PTY or any of its fds.
						let Some(pty) = server.ptys.get(&id) else {
							break;
						};
						let Some(master_fd) = pty.master.as_ref().map(AsRawFd::as_raw_fd) else {
							break;
						};
						let Some(slave_fd) = pty.slave.as_ref().map(AsRawFd::as_raw_fd) else {
							break;
						};

						// Drop the handle to the PTY to avoid a deadlock when read requests come in.
						drop(pty);

						// Handle the message.
						match message {
							WriteMessage::Chunk { master, bytes } => {
								tokio::task::spawn_blocking({
									let fd = if master {
										master_fd.as_raw_fd()
									} else {
										slave_fd.as_raw_fd()
									};
									move || unsafe {
										let mut bytes = bytes.as_ref();
										while !bytes.is_empty() {
											let n =
												libc::write(fd, bytes.as_ptr().cast(), bytes.len());
											if n < 0 {
												let error = std::io::Error::last_os_error();
												if error.raw_os_error() == Some(libc::EBADF) {
													break;
												}
												return Err(error);
											}
											let n = n.to_usize().unwrap();
											if n == 0 {
												break;
											}
											bytes = &bytes[n..];
										}
										Ok(())
									}
								})
								.await
								.map_err(|source| {
									tg::error!(!source, "the pty write task panicked")
								})?
								.map_err(|source| tg::error!(!source, "failed to write to the"))?;
							},
							WriteMessage::Size(size) => {
								Server::pty_write_set_size(master_fd.as_raw_fd(), size)
									.await
									.map_err(|source| {
										tg::error!(!source, "failed to change the size")
									})?;
							},
						}
					}
					Ok::<_, tg::Error>(())
				}
			}
			.inspect_err(|error| tracing::error!(?error, "failed to write th")),
		);

		let pty = Self {
			master,
			slave,
			name,
			session,
			sender,
			task,
			temp,
		};

		Ok(pty)
	}
}
