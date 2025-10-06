use {
	std::{
		collections::HashMap,
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
		path::PathBuf,
		sync::Arc,
	},
	tangram_client as tg,
	tokio::{
		net::UnixStream,
		sync::{oneshot, Mutex},
	},
};

#[derive(Debug)]
pub struct PtySpawnRequest {
	pub request_id: u64,
	pub executable: PathBuf,
	pub args: Vec<String>,
	pub env: std::collections::BTreeMap<String, String>,
	pub cwd: PathBuf,
	pub stdin_fd: Option<OwnedFd>,
	pub stdout_fd: Option<OwnedFd>,
	pub stderr_fd: Option<OwnedFd>,
	pub response_tx: oneshot::Sender<tg::Result<u32>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SpawnRequest {
	pub request_id: u64,
	pub executable: PathBuf,
	pub args: Vec<String>,
	pub env: Vec<(String, String)>,
	pub cwd: PathBuf,
	pub has_stdin_fd: bool,
	pub has_stdout_fd: bool,
	pub has_stderr_fd: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum SessionResponse {
	SpawnSuccess { request_id: u64, pid: u32 },
	SpawnFailure { request_id: u64, error: String },
	ProcessExited { pid: u32, exit_code: u8 },
}

/// Send a message with file descriptors over a Unix socket using sendmsg with SCM_RIGHTS.
async fn send_message_with_fds(
	socket: &Arc<Mutex<UnixStream>>,
	message: &[u8],
	fds: &[RawFd],
) -> tg::Result<()> {
	let socket = socket.lock().await;

	// First, send the length
	let len_bytes = (message.len() as u32).to_le_bytes();

	// Get the raw FD for sendmsg
	let socket_fd = socket.as_raw_fd();

	// Clone message and fds for the blocking task
	let message = message.to_vec();
	let fds = fds.to_vec();

	tokio::task::spawn_blocking(move || unsafe {
		// Send length first (without FDs)
		let mut iov = libc::iovec {
			iov_base: len_bytes.as_ptr() as *mut libc::c_void,
			iov_len: 4,
		};
		let mut msg: libc::msghdr = std::mem::zeroed();
		msg.msg_iov = &raw mut iov;
		msg.msg_iovlen = 1;

		let ret = libc::sendmsg(socket_fd, &msg, 0);
		if ret != 4 {
			return Err(std::io::Error::last_os_error())
				.map_err(|source| tg::error!(!source, "failed to send length"));
		}

		// Now send the message with FDs
		let mut iov = libc::iovec {
			iov_base: message.as_ptr() as *mut libc::c_void,
			iov_len: message.len(),
		};

		if fds.is_empty() {
			// No FDs, simple send
			let mut msg: libc::msghdr = std::mem::zeroed();
			msg.msg_iov = &raw mut iov;
			msg.msg_iovlen = 1;

			let ret = libc::sendmsg(socket_fd, &msg, 0);
			if ret as usize != message.len() {
				return Err(std::io::Error::last_os_error())
					.map_err(|source| tg::error!(!source, "failed to send message"));
			}
		} else {
			// Send with FDs
			let cmsg_space = libc::CMSG_SPACE((fds.len() * std::mem::size_of::<RawFd>()) as u32);
			let mut cmsg_buf = vec![0u8; cmsg_space as usize];

			let mut msg: libc::msghdr = std::mem::zeroed();
			msg.msg_iov = &raw mut iov;
			msg.msg_iovlen = 1;
			msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
			msg.msg_controllen = cmsg_space as _;

			let cmsg = libc::CMSG_FIRSTHDR(&msg);
			if cmsg.is_null() {
				return Err(tg::error!("failed to get control message header"));
			}
			(*cmsg).cmsg_level = libc::SOL_SOCKET;
			(*cmsg).cmsg_type = libc::SCM_RIGHTS;
			(*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<RawFd>()) as u32) as _;

			let data_ptr = libc::CMSG_DATA(cmsg) as *mut RawFd;
			std::ptr::copy_nonoverlapping(fds.as_ptr(), data_ptr, fds.len());

			let ret = libc::sendmsg(socket_fd, &msg, 0);
			if ret as usize != message.len() {
				return Err(std::io::Error::last_os_error())
					.map_err(|source| tg::error!(!source, "failed to send message with fds"));
			}
		}

		Ok::<_, tg::Error>(())
	})
	.await
	.map_err(|source| tg::error!(!source, "spawn_blocking failed"))??;

	Ok(())
}

/// Receive a message with file descriptors from a Unix socket using recvmsg with SCM_RIGHTS.
pub async fn recv_message_with_fds(
	socket: &Arc<Mutex<UnixStream>>,
) -> tg::Result<(Vec<u8>, Vec<OwnedFd>)> {
	use tokio::io::AsyncReadExt as _;

	let mut socket = socket.lock().await;

	// First, read the length using tokio's async read
	let len = socket
		.read_u32_le()
		.await
		.map_err(|source| tg::error!(!source, "failed to read message length"))?
		as usize;

	// Receive message with FDs using recvmsg with try_io
	loop {
		socket
			.readable()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for readable"))?;

		match socket.try_io(tokio::io::Interest::READABLE, || {
			let socket_fd = socket.as_raw_fd();

			unsafe {
				let mut msg_bytes = vec![0u8; len];
				let mut iov = libc::iovec {
					iov_base: msg_bytes.as_mut_ptr() as *mut libc::c_void,
					iov_len: len,
				};

				// Prepare control message buffer (space for up to 3 FDs)
				let cmsg_space = libc::CMSG_SPACE((3 * std::mem::size_of::<RawFd>()) as u32);
				let mut cmsg_buf = vec![0u8; cmsg_space as usize];

				let mut msg: libc::msghdr = std::mem::zeroed();
				msg.msg_iov = &raw mut iov;
				msg.msg_iovlen = 1;
				msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
				msg.msg_controllen = cmsg_space as _;

				// Receive the message
				let ret = libc::recvmsg(socket_fd, &raw mut msg, 0);
				if ret < 0 {
					return Err(std::io::Error::last_os_error());
				}

				if ret as usize != len {
					return Err(std::io::Error::other(format!(
						"recvmsg received incomplete message: {} of {} bytes",
						ret,
						len
					)));
				}

				// Extract file descriptors from control message
				let mut fds = Vec::new();
				let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
				while !cmsg.is_null() {
					if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
						let data_ptr = libc::CMSG_DATA(cmsg) as *const RawFd;
						let fd_count = ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize)
							/ std::mem::size_of::<RawFd>();
						for i in 0..fd_count {
							let fd = *data_ptr.add(i);
							fds.push(OwnedFd::from_raw_fd(fd));
						}
					}
					cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
				}

				Ok((msg_bytes, fds))
			}
		}) {
			Ok(result) => return Ok(result),
			Err(_would_block) => continue,
		}
	}
}

pub async fn pty_session_task(
	socket: tokio::net::UnixStream,
	mut spawn_rx: tokio::sync::mpsc::UnboundedReceiver<PtySpawnRequest>,
) {
	if let Err(error) = pty_session_task_inner(socket, &mut spawn_rx).await {
		tracing::error!(?error, "pty session task failed");
	}
}

async fn pty_session_task_inner(
	socket: tokio::net::UnixStream,
	spawn_rx: &mut tokio::sync::mpsc::UnboundedReceiver<PtySpawnRequest>,
) -> tg::Result<()> {
	let socket = Arc::new(Mutex::new(socket));

	// Track pending requests
	let mut pending: HashMap<u64, oneshot::Sender<tg::Result<u32>>> = HashMap::new();
	let mut next_request_id = 0u64;

	loop {
		tokio::select! {
			// Handle spawn requests from server
			Some(request) = spawn_rx.recv() => {
				let request_id = next_request_id;
				next_request_id += 1;

				pending.insert(request_id, request.response_tx);

				// Send request to session leader
				let msg = SpawnRequest {
					request_id,
					executable: request.executable,
					args: request.args,
					env: request.env.into_iter().collect(),
					cwd: request.cwd,
					has_stdin_fd: request.stdin_fd.is_some(),
					has_stdout_fd: request.stdout_fd.is_some(),
					has_stderr_fd: request.stderr_fd.is_some(),
				};

				let fds: Vec<RawFd> = [
					request.stdin_fd.as_ref().map(|f| f.as_raw_fd()),
					request.stdout_fd.as_ref().map(|f| f.as_raw_fd()),
					request.stderr_fd.as_ref().map(|f| f.as_raw_fd()),
				]
				.into_iter()
				.flatten()
				.collect();

				let msg_bytes = serde_json::to_vec(&msg)
					.map_err(|source| tg::error!(!source, "failed to serialize spawn request"))?;
				tracing::debug!(request_id, "sending spawn request to session leader");
				send_message_with_fds(&socket, &msg_bytes, &fds).await?;
				tracing::debug!(request_id, "sent spawn request to session leader");
			}

			// Handle responses from session leader
			result = recv_message_with_fds(&socket) => {
				tracing::debug!("received response from session leader");
				let (msg_bytes, _fds) = result?;
				let response: SessionResponse = serde_json::from_slice(&msg_bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize response"))?;

				tracing::debug!(?response, "parsed response from session leader");

				match response {
					SessionResponse::SpawnSuccess { request_id, pid } => {
						if let Some(tx) = pending.remove(&request_id) {
							tx.send(Ok(pid)).ok();
						}
					}
					SessionResponse::SpawnFailure { request_id, error } => {
						if let Some(tx) = pending.remove(&request_id) {
							tx.send(Err(tg::error!("{error}"))).ok();
						}
					}
					SessionResponse::ProcessExited { pid, exit_code } => {
						// TODO: Propagate to process tracking
						tracing::debug!(pid, exit_code, "process exited");
					}
				}
			}
		}
	}
}
