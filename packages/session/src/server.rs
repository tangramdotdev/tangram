use {
	crate::{Message, PidMessage, SpawnMessage, Stdio, WaitMessage},
	num::ToPrimitive as _,
	std::{
		os::{
			fd::{FromRawFd, RawFd},
			unix::{io::AsRawFd, process::ExitStatusExt as _},
		},
		path::PathBuf,
	},
	tangram_client as tg,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt},
};

#[derive(Clone)]
pub struct Server;

impl Server {
	pub async fn run(path: PathBuf) -> tg::Result<()> {
		let server = Self;

		// Bind the Unix listener to the specified path.
		let listener = tokio::net::UnixListener::bind(&path)
			.map_err(|source| tg::error!(!source, "failed to bind the listener"))?;

		// Accept connections in a loop.
		loop {
			let (stream, _addr) = listener
				.accept()
				.await
				.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
			let server = server.clone();
			tokio::spawn(async move {
				let result = server.handle_connection(stream).await;
				if let Err(error) = result {
					tracing::error!(?error, "connection handler failed");
				}
			});
		}
	}

	async fn handle_connection(&self, mut stream: tokio::net::UnixStream) -> tg::Result<()> {
		// Read the message length.
		let length = stream
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the message length"))?;

		// Read the message.
		let mut bytes = vec![0u8; length.to_usize().unwrap()];
		stream
			.read_exact(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the message"))?;

		// Deserialize the message.
		let message = serde_json::from_slice::<Message>(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

		// Extract the spawn message.
		let Message::Spawn(SpawnMessage { command }) = message else {
			return Err(tg::error!("expected a spawn message"));
		};

		// Receive the file descriptors using recvmsg with SCM_RIGHTS.
		let fd = stream.as_raw_fd();
		#[allow(clippy::cast_possible_truncation)]
		let fds = stream
			.async_io(tokio::io::Interest::READABLE, move || unsafe {
				// Receive the message.
				let mut buffer = [0u8; 1];
				let iov = libc::iovec {
					iov_base: buffer.as_mut_ptr().cast(),
					iov_len: 1,
				};
				let length = libc::CMSG_SPACE((3 * std::mem::size_of::<RawFd>()) as _);
				let mut cmsg_buffer = vec![0u8; length as _];
				let mut msg: libc::msghdr = std::mem::zeroed();
				msg.msg_iov = (&raw const iov).cast_mut();
				msg.msg_iovlen = 1;
				msg.msg_control = cmsg_buffer.as_mut_ptr().cast();
				msg.msg_controllen = cmsg_buffer.len() as _;
				let ret = libc::recvmsg(fd, &raw mut msg, 0);
				if ret < 0 {
					let error = std::io::Error::last_os_error();
					return Err(error);
				}

				// Read the fds.
				let mut fds = Vec::new();
				let cmsg = libc::CMSG_FIRSTHDR(&raw const msg);
				if !cmsg.is_null()
					&& (*cmsg).cmsg_level == libc::SOL_SOCKET
					&& (*cmsg).cmsg_type == libc::SCM_RIGHTS
				{
					let data = libc::CMSG_DATA(cmsg);
					let n = ((*cmsg).cmsg_len.to_usize().unwrap()
						- libc::CMSG_LEN(0).to_usize().unwrap())
						/ std::mem::size_of::<RawFd>();
					for i in 0..n {
						fds.push(
							data.add(i * std::mem::size_of::<RawFd>())
								.cast::<RawFd>()
								.read_unaligned(),
						);
					}
				}

				Ok(fds)
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to receive the fds"))?;

		// Spawn the process.
		let mut cmd = tokio::process::Command::new(&command.executable);
		cmd.args(&command.args);
		cmd.current_dir(&command.cwd);
		cmd.env_clear();
		cmd.envs(&command.env);

		// Set up stdin.
		let stdin = match command.stdin {
			Stdio::Null => std::process::Stdio::null(),
			Stdio::Inherit => std::process::Stdio::inherit(),
			Stdio::Fd(index) => {
				let index = index.to_usize().unwrap();
				let fd = fds
					.get(index)
					.copied()
					.ok_or_else(|| tg::error!("stdin index {index} out of bounds"))?;
				unsafe { std::process::Stdio::from_raw_fd(fd) }
			},
		};
		cmd.stdin(stdin);

		// Set up stdout.
		let stdout = match command.stdout {
			Stdio::Null => std::process::Stdio::null(),
			Stdio::Inherit => std::process::Stdio::inherit(),
			Stdio::Fd(index) => {
				let index = index.to_usize().unwrap();
				let fd = fds
					.get(index)
					.copied()
					.ok_or_else(|| tg::error!("stdout index {index} out of bounds"))?;
				unsafe { std::process::Stdio::from_raw_fd(fd) }
			},
		};
		cmd.stdout(stdout);

		// Set up stderr.
		let stderr = match command.stderr {
			Stdio::Null => std::process::Stdio::null(),
			Stdio::Inherit => std::process::Stdio::inherit(),
			Stdio::Fd(index) => {
				let index = index.to_usize().unwrap();
				let fd = fds
					.get(index)
					.copied()
					.ok_or_else(|| tg::error!("stderr index {index} out of bounds"))?;
				unsafe { std::process::Stdio::from_raw_fd(fd) }
			},
		};
		cmd.stderr(stderr);

		// Spawn the process.
		let mut child = cmd
			.spawn()
			.map_err(|source| tg::error!(!source, "failed to spawn the process"))?;

		// Send the pid message.
		let pid = child
			.id()
			.ok_or_else(|| tg::error!("failed to get process id"))?;
		let message = Message::Pid(PidMessage { pid });
		let bytes = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize message"))?;
		let length = bytes.len().to_u64().unwrap();
		stream
			.write_uvarint(length)
			.await
			.map_err(|source| tg::error!(!source, "failed to write length"))?;
		stream
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write message"))?;

		// Wait for the process to exit.
		let status = child
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for process"))?;
		let code = None
			.or(status.code())
			.or(status.signal().map(|signal| 128 + signal))
			.unwrap()
			.to_u8()
			.unwrap();

		// Send the wait message.
		let message = Message::Wait(WaitMessage { code });
		let bytes = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize message"))?;
		let length = bytes.len().to_u64().unwrap();
		stream
			.write_uvarint(length)
			.await
			.map_err(|source| tg::error!(!source, "failed to write length"))?;
		stream
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write message"))?;

		Ok(())
	}
}
