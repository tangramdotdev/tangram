use {
	std::{
		ffi::CString,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, RawFd},
			unix::process::ExitStatusExt as _,
		},
		path::PathBuf,
		sync::Arc,
	},
	tangram_client as tg,
	tokio::{
		io::{AsyncWriteExt as _, BufReader, BufWriter},
		net::UnixStream,
		process::Command,
		sync::Mutex,
	},
};

#[derive(Clone, Debug, clap::Args)]
pub struct Args {
	/// PTY slave device name
	#[arg(long)]
	pub name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct SpawnRequest {
	request_id: u64,
	executable: PathBuf,
	args: Vec<String>,
	env: Vec<(String, String)>,
	cwd: PathBuf,
	has_stdin_fd: bool,
	has_stdout_fd: bool,
	has_stderr_fd: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SessionResponse {
	SpawnSuccess { request_id: u64, pid: u32 },
	SpawnFailure { request_id: u64, error: String },
	ProcessExited { pid: u32, exit_code: u8 },
}

pub async fn main(args: Args) -> tg::Result<()> {
	// Initialize tracing for debugging
	tracing_subscriber::fmt()
		.with_writer(std::io::stderr)
		.init();

	eprintln!("SESSION PROCESS STARTED - args: {:?}", args);

	// Get the socket FD passed from parent (FD 3)
	let socket_fd = 3;
	let socket = unsafe { std::os::unix::net::UnixStream::from_raw_fd(socket_fd) };
	socket.set_nonblocking(true).map_err(|source| {
		tg::error!(!source, "failed to set socket to non-blocking")
	})?;
	let socket = UnixStream::from_std(socket)
		.map_err(|source| tg::error!(!source, "failed to create tokio UnixStream"))?;

	// Open the PTY slave
	let pty_fd = unsafe {
		let name = CString::new(args.name.clone())
			.map_err(|source| tg::error!(!source, "invalid pty name"))?;
		let fd = libc::open(name.as_ptr(), libc::O_RDWR);
		if fd < 0 {
			return Err(std::io::Error::last_os_error())
				.map_err(|source| tg::error!(!source, "failed to open pty slave"));
		}
		fd
	};

	// Become session leader and set controlling terminal (blocking operations)
	set_controlling_tty(pty_fd)?;

	// Close the PTY fd since we don't need it anymore in the session process
	unsafe {
		libc::close(pty_fd);
	}

	// Run async session loop
	session_loop(socket).await
}

fn set_controlling_tty(tty_fd: RawFd) -> tg::Result<()> {
	unsafe {
		// Disconnect from the old controlling terminal.
		let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if fd > 0 {
			#[allow(clippy::useless_conversion)]
			libc::ioctl(fd, libc::TIOCNOTTY.into(), std::ptr::null_mut::<()>());
			libc::close(fd);
		}

		// Set the current process as session leader.
		let ret = libc::setsid();
		if ret == -1 {
			return Err(std::io::Error::last_os_error())
				.map_err(|source| tg::error!(!source, "failed to call setsid"));
		}

		// Verify that we disconnected from the controlling terminal.
		let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if fd >= 0 {
			libc::close(fd);
			return Err(tg::error!("failed to remove controlling tty"));
		}

		// Set the slave as the controlling tty.
		#[allow(clippy::useless_conversion)]
		let ret = libc::ioctl(tty_fd, libc::TIOCSCTTY.into(), 0);
		if ret < 0 {
			return Err(std::io::Error::last_os_error())
				.map_err(|source| tg::error!(!source, "failed to set controlling tty"));
		}

		// Verify that we have a controlling terminal again.
		let fd = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
		if fd <= 0 {
			return Err(tg::error!("failed to verify controlling tty"));
		}
		libc::close(fd);

		Ok(())
	}
}

async fn session_loop(socket: UnixStream) -> tg::Result<()> {
	let (read, write) = socket.into_split();
	let mut reader = BufReader::new(read);
	let writer = Arc::new(Mutex::new(BufWriter::new(write)));

	tracing::debug!("session loop starting");

	loop {
		tracing::debug!("waiting for spawn request");
		// Read spawn request (message + FDs via SCM_RIGHTS)
		let (msg_bytes, fds) = match recv_message_with_fds(&mut reader).await {
			Ok(result) => {
				tracing::debug!("received spawn request message");
				result
			},
			Err(error) => {
				tracing::error!(?error, "failed to read message, exiting session loop");
				break;
			},
		};

		let request: SpawnRequest = serde_json::from_slice(&msg_bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize spawn request"))?;

		tracing::debug!(?request, "deserialized spawn request");

		// Handle spawn in separate task for concurrency
		tokio::spawn({
			let writer = writer.clone();
			async move {
				handle_spawn_request(request, fds, writer)
					.await
					.inspect_err(|error| tracing::error!(?error, "spawn handling failed"))
					.ok();
			}
		});
	}

	Ok(())
}

async fn handle_spawn_request(
	request: SpawnRequest,
	mut fds: Vec<std::os::fd::OwnedFd>,
	writer: Arc<Mutex<BufWriter<tokio::net::unix::OwnedWriteHalf>>>,
) -> tg::Result<()> {
	tracing::debug!(?request, "handling spawn request");

	// Build command
	let mut cmd = Command::new(&request.executable);
	cmd.args(&request.args)
		.env_clear()
		.envs(request.env.iter().map(|(k, v)| (k, v)))
		.current_dir(&request.cwd);

	// Set up stdio from passed FDs
	let mut fd_idx = 0;
	if request.has_stdin_fd {
		let fd = fds.remove(fd_idx);
		unsafe {
			cmd.stdin(std::fs::File::from(fd));
		}
	}
	if request.has_stdout_fd {
		let fd = fds.remove(fd_idx);
		unsafe {
			cmd.stdout(std::fs::File::from(fd));
		}
	}
	if request.has_stderr_fd {
		let fd = fds.remove(fd_idx);
		unsafe {
			cmd.stderr(std::fs::File::from(fd));
		}
	}

	// Spawn the child
	let mut child = match cmd.spawn() {
		Ok(child) => {
			let pid = child.id().unwrap();
			tracing::debug!(pid, "spawned child process");

			// Send success response immediately
			send_response(
				&writer,
				SessionResponse::SpawnSuccess {
					request_id: request.request_id,
					pid,
				},
			)
			.await?;

			child
		},
		Err(error) => {
			tracing::error!(?error, "failed to spawn child");
			// Send failure response
			send_response(
				&writer,
				SessionResponse::SpawnFailure {
					request_id: request.request_id,
					error: error.to_string(),
				},
			)
			.await?;
			return Ok(());
		},
	};

	// Wait for child and send exit notification immediately
	let pid = child.id().unwrap();
	match child.wait().await {
		Ok(status) => {
			let exit_code = status
				.code()
				.or_else(|| status.signal().map(|s| 128 + s))
				.unwrap_or(1) as u8;

			tracing::debug!(pid, exit_code, "child process exited");

			send_response(
				&writer,
				SessionResponse::ProcessExited { pid, exit_code },
			)
			.await?;
		},
		Err(error) => {
			tracing::error!(?error, pid, "failed to wait for child");
		},
	}

	Ok(())
}

async fn send_response(
	writer: &Arc<Mutex<BufWriter<tokio::net::unix::OwnedWriteHalf>>>,
	response: SessionResponse,
) -> tg::Result<()> {
	let bytes = serde_json::to_vec(&response)
		.map_err(|source| tg::error!(!source, "failed to serialize response"))?;
	let mut writer = writer.lock().await;
	writer
		.write_u32_le(bytes.len() as u32)
		.await
		.map_err(|source| tg::error!(!source, "failed to write message length"))?;
	writer
		.write_all(&bytes)
		.await
		.map_err(|source| tg::error!(!source, "failed to write message"))?;
	writer
		.flush()
		.await
		.map_err(|source| tg::error!(!source, "failed to flush"))?;
	Ok(())
}

async fn recv_message_with_fds(
	reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> tg::Result<(Vec<u8>, Vec<std::os::fd::OwnedFd>)> {
	use tokio::io::AsyncReadExt as _;

	// First, read the length.
	let len = reader
		.read_u32_le()
		.await
		.map_err(|source| tg::error!(!source, "failed to read message length"))?
		as usize;

	let socket_fd = reader.get_ref().as_ref().as_raw_fd();

	// Receive message with FDs using recvmsg in blocking task.
	let (msg_bytes, fds) = tokio::task::spawn_blocking(move || unsafe {
		let mut msg_bytes = vec![0u8; len];
		let mut iov = libc::iovec {
			iov_base: msg_bytes.as_mut_ptr() as *mut libc::c_void,
			iov_len: len,
		};

		// Prepare control message buffer (space for up to 3 FDs).
		let cmsg_space = libc::CMSG_SPACE((3 * std::mem::size_of::<RawFd>()) as u32);
		let mut cmsg_buf = vec![0u8; cmsg_space as usize];

		let mut msg: libc::msghdr = std::mem::zeroed();
		msg.msg_iov = &raw mut iov;
		msg.msg_iovlen = 1;
		msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
		msg.msg_controllen = cmsg_space as _;

		// Receive the message.
		let ret = libc::recvmsg(socket_fd, &raw mut msg, 0);
		if ret < 0 {
			return Err(std::io::Error::last_os_error())
				.map_err(|source| tg::error!(!source, "failed to recvmsg"));
		}

		if ret as usize != len {
			return Err(tg::error!(
				"recvmsg received incomplete message: {} of {} bytes",
				ret,
				len
			));
		}

		// Extract file descriptors from control message.
		let mut fds = Vec::new();
		let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
		while !cmsg.is_null() {
			if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS
			{
				let data_ptr = libc::CMSG_DATA(cmsg) as *const RawFd;
				let fd_count = ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize)
					/ std::mem::size_of::<RawFd>();
				for i in 0..fd_count {
					let fd = *data_ptr.add(i);
					fds.push(std::os::fd::OwnedFd::from_raw_fd(fd));
				}
			}
			cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
		}

		Ok::<_, tg::Error>((msg_bytes, fds))
	})
	.await
	.map_err(|source| tg::error!(!source, "spawn_blocking failed"))??;

	Ok((msg_bytes, fds))
}
