use {
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		os::{fd::RawFd, unix::io::AsRawFd},
		path::PathBuf,
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

pub struct Client {
	stream: tokio::net::UnixStream,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Message {
	Spawn(SpawnMessage),
	Pid(PidMessage),
	Wait(WaitMessage),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpawnMessage {
	pub command: Command,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
	pub executable: PathBuf,
	pub stdin: Stdio,
	pub stdout: Stdio,
	pub stderr: Stdio,
}

#[derive(Debug)]
pub enum Stdio {
	Null,
	Inherit,
	Fd(RawFd),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PidMessage {
	pub pid: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WaitMessage {
	pub code: u8,
}

impl Client {
	pub async fn connect(path: PathBuf) -> tg::Result<Self> {
		let stream = tokio::net::UnixStream::connect(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;

		let client = Self { stream };

		Ok(client)
	}

	pub async fn spawn(&mut self, command: Command) -> tg::Result<u32> {
		// Collect the fds from the command and map to indices.
		let mut fds = Vec::new();
		let stdin = match command.stdin {
			Stdio::Fd(fd) => {
				let index = fds.len();
				fds.push(fd);
				Stdio::Fd(index.try_into().unwrap())
			},
			other => other,
		};
		let stdout = match command.stdout {
			Stdio::Fd(fd) => {
				let index = fds.len();
				fds.push(fd);
				Stdio::Fd(index.try_into().unwrap())
			},
			other => other,
		};
		let stderr = match command.stderr {
			Stdio::Fd(fd) => {
				let index = fds.len();
				fds.push(fd);
				Stdio::Fd(index.try_into().unwrap())
			},
			other => other,
		};

		// Create command with indices instead of raw FDs.
		let command = Command {
			stdin,
			stdout,
			stderr,
			..command
		};

		// Serialize the message.
		let message = Message::Spawn(SpawnMessage { command });
		let bytes = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize message"))?;

		// Write the length.
		self.stream
			.write_uvarint(bytes.len().to_u64().unwrap())
			.await
			.map_err(|source| tg::error!(!source, "failed to write length"))?;

		// Write the arg.
		self.stream
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write argument"))?;

		// Send the file descriptors using sendmsg with SCM_RIGHTS.
		let fd = self.stream.as_raw_fd();
		#[expect(clippy::cast_possible_truncation)]
		self.stream
			.async_io(tokio::io::Interest::WRITABLE, move || unsafe {
				let buffer = [0u8; 1];
				let iov = libc::iovec {
					iov_base: buffer.as_ptr() as *mut _,
					iov_len: 1,
				};
				let cmsg_space = libc::CMSG_SPACE(std::mem::size_of_val(fds.as_slice()) as _);
				let mut cmsg_buffer = vec![0u8; cmsg_space as _];
				let mut msg: libc::msghdr = std::mem::zeroed();
				msg.msg_iov = (&raw const iov).cast_mut();
				msg.msg_iovlen = 1;
				msg.msg_control = cmsg_buffer.as_mut_ptr().cast();
				msg.msg_controllen = cmsg_space as _;
				let cmsg = libc::CMSG_FIRSTHDR(&raw const msg);
				if cmsg.is_null() {
					let error = std::io::Error::other("failed to get the control message header");
					return Err(error);
				}
				(*cmsg).cmsg_level = libc::SOL_SOCKET;
				(*cmsg).cmsg_type = libc::SCM_RIGHTS;
				(*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(fds.as_slice()) as _) as _;
				let data = libc::CMSG_DATA(cmsg);
				std::ptr::copy_nonoverlapping(fds.as_ptr(), data.cast(), fds.len());
				let ret = libc::sendmsg(fd, &raw const msg, 0);
				if ret < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to send the file descriptors"))?;

		// Read the length.
		let length = self
			.stream
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the length"))?;

		// Read the message.
		let mut bytes = vec![0u8; length.to_usize().unwrap()];
		self.stream
			.read_exact(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the message"))?;

		// Deserialize the message.
		let message = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

		// Extract the pid from the message.
		let Message::Pid(PidMessage { pid }) = message else {
			return Err(tg::error!("expected pid message"));
		};

		Ok(pid)
	}

	pub async fn wait(&mut self) -> tg::Result<u8> {
		// Read the length.
		let length = self
			.stream
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the length"))?;

		// Read the message.
		let mut bytes = vec![0u8; length.to_usize().unwrap()];
		self.stream
			.read_exact(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the message"))?;

		// Deserialize the message.
		let message = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize message"))?;

		// Extract the exit code from the message.
		let Message::Wait(WaitMessage { code }) = message else {
			return Err(tg::error!("expected Wait message"));
		};

		Ok(code)
	}
}

impl serde::Serialize for Stdio {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		match self {
			Self::Null => serializer.serialize_str("null"),
			Self::Inherit => serializer.serialize_str("inherit"),
			Self::Fd(fd) => serializer.serialize_i32(*fd),
		}
	}
}

impl<'de> serde::Deserialize<'de> for Stdio {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		serde_untagged::UntaggedEnumVisitor::new()
			.borrowed_str(|s| match s {
				"null" => Ok(Self::Null),
				"inherit" => Ok(Self::Inherit),
				_ => Err(serde::de::Error::invalid_value(
					serde::de::Unexpected::Str(s),
					&"null or inherit",
				)),
			})
			.i32(|fd| Ok(Self::Fd(fd)))
			.deserialize(deserializer)
	}
}
