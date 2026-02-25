use {
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		os::{fd::RawFd, unix::io::AsRawFd},
		path::PathBuf,
		sync::{Arc, Mutex, atomic::AtomicU32},
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::{
		io::{AsyncReadExt, AsyncWriteExt as _},
		net::UnixStream,
		sync::oneshot,
	},
};

type Requests = Arc<Mutex<BTreeMap<u32, oneshot::Sender<tg::Result<Response>>>>>;

pub struct Client {
	counter: AtomicU32,
	read_task: tokio::task::JoinHandle<()>,
	write_task: tokio::task::JoinHandle<()>,
	sender: tokio::sync::mpsc::Sender<(Request, oneshot::Sender<tg::Result<Response>>)>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
	pub id: u32,
	pub kind: RequestKind,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestKind {
	Spawn(SpawnRequest),
	Wait(WaitRequest),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
	pub id: u32,
	pub kind: ResponseKind,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ResponseKind {
	Spawn(SpawnResponse),
	Wait(WaitResponse),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpawnRequest {
	pub command: crate::Command,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub fds: Vec<RawFd>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpawnResponse {
	pub pid: Option<i32>,
	pub error: Option<tg::error::Data>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WaitRequest {
	pub pid: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WaitResponse {
	pub status: Option<i32>,
}

impl Client {
	pub fn new(socket: UnixStream) -> Self {
		let (sender, mut receiver) =
			tokio::sync::mpsc::channel::<(Request, oneshot::Sender<tg::Result<Response>>)>(128);
		let requests: Requests = Arc::new(Mutex::new(BTreeMap::new()));

		// Get the raw fd for sendmsg before splitting the socket.
		let fd = socket.as_raw_fd();
		let (mut read_half, mut write_half) = socket.into_split();

		// Spawn the write task. This task reads requests from the channel and writes them to the socket.
		let write_task = tokio::spawn({
			let requests = Arc::clone(&requests);
			async move {
				while let Some((request, sender)) = receiver.recv().await {
					match Self::try_send(&mut write_half, fd, &request).await {
						Ok(()) => {
							requests.lock().unwrap().insert(request.id, sender);
						},
						Err(error)
							if matches!(
								error.kind(),
								std::io::ErrorKind::UnexpectedEof
									| std::io::ErrorKind::ConnectionReset
							) =>
						{
							break;
						},
						Err(error) => {
							sender
								.send(Err(tg::error!(!error, "failed to send the request")))
								.ok();
						},
					}
				}
			}
		});

		// Spawn the read task. This task reads responses from the socket and resolves pending requests.
		let read_task = tokio::spawn({
			let requests = Arc::clone(&requests);
			async move {
				loop {
					match Self::try_receive(&mut read_half).await {
						Ok(Some(response)) => {
							let sender = requests.lock().unwrap().remove(&response.id);
							if let Some(sender) = sender {
								sender.send(Ok(response)).ok();
							}
						},
						Ok(None) => (),
						Err(error)
							if matches!(
								error.kind(),
								std::io::ErrorKind::UnexpectedEof
									| std::io::ErrorKind::ConnectionReset
							) =>
						{
							break;
						},
						Err(error) => {
							tracing::error!(?error, "failed to receive the response");
							break;
						},
					}
				}
			}
		});

		Self {
			read_task,
			write_task,
			sender,
			counter: AtomicU32::new(0),
		}
	}

	pub async fn connect(path: PathBuf) -> tg::Result<Self> {
		let socket = tokio::net::UnixStream::connect(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to connect to the socket"))?;
		Ok(Self::new(socket))
	}

	async fn try_send(
		socket: &mut tokio::net::unix::OwnedWriteHalf,
		fd: RawFd,
		request: &Request,
	) -> std::io::Result<()> {
		let bytes = serde_json::to_vec(&request).unwrap();
		let length = bytes.len().to_u64().unwrap();
		socket.write_uvarint(length).await?;
		socket.write_all(&bytes).await?;
		let RequestKind::Spawn(request) = &request.kind else {
			return Ok(());
		};
		let fds = request.fds.clone();
		socket
			.as_ref()
			.async_io(tokio::io::Interest::WRITABLE, move || unsafe {
				let buffer = [0u8; 1];
				let iov = libc::iovec {
					iov_base: buffer.as_ptr() as *mut _,
					iov_len: 1,
				};
				let cmsg_space =
					libc::CMSG_SPACE(std::mem::size_of_val(fds.as_slice()).to_u32().unwrap());
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
				(*cmsg).cmsg_len =
					libc::CMSG_LEN(std::mem::size_of_val(fds.as_slice()).to_u32().unwrap()) as _;
				let data = libc::CMSG_DATA(cmsg);
				std::ptr::copy_nonoverlapping(fds.as_ptr(), data.cast(), fds.len());
				let ret = libc::sendmsg(fd, &raw const msg, 0);
				if ret < 0 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			})
			.await?;
		Ok(())
	}

	async fn try_receive(
		socket: &mut tokio::net::unix::OwnedReadHalf,
	) -> std::io::Result<Option<Response>> {
		let Some(length) = socket.try_read_uvarint().await? else {
			return Err(std::io::ErrorKind::UnexpectedEof.into());
		};
		let mut buf = vec![0; length.to_usize().unwrap()];
		socket.read_exact(&mut buf).await?;
		let response = serde_json::from_slice(&buf)
			.inspect_err(|error| tracing::error!(?error, "failed to deserialize the response"))
			.ok();
		Ok(response)
	}

	pub async fn spawn(&self, mut command: crate::Command) -> tg::Result<i32> {
		// Get the fds as indices.
		let mut fds = Vec::new();
		command.stdin = command.stdin.map(|fd| {
			let index = fds.len().to_i32().unwrap();
			fds.push(fd);
			index
		});
		command.stdout = command.stdout.map(|fd| {
			let index = fds.len().to_i32().unwrap();
			fds.push(fd);
			index
		});
		command.stderr = command.stderr.map(|fd| {
			let index = fds.len().to_i32().unwrap();
			fds.push(fd);
			index
		});
		dbg!(&fds);
		let response = self
			.send_request(RequestKind::Spawn(SpawnRequest { command, fds }))
			.await?;
		let ResponseKind::Spawn(response) = response.kind else {
			return Err(tg::error!("expected a spawn response"));
		};
		if let Some(error) = response.error {
			let error = error.try_into()?;
			return Err(error);
		}
		response.pid.ok_or_else(|| tg::error!("expected a PID"))
	}

	pub async fn wait(&self, pid: i32) -> tg::Result<i32> {
		let response = self
			.send_request(RequestKind::Wait(WaitRequest { pid }))
			.await?;
		let ResponseKind::Wait(wait) = response.kind else {
			return Err(tg::error!("expected a wait response"));
		};
		wait.status.ok_or_else(|| tg::error!("expected a status"))
	}

	async fn send_request(&self, kind: RequestKind) -> tg::Result<Response> {
		let id = self
			.counter
			.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
		let (sender, receiver) = oneshot::channel();
		let request = Request { id, kind };
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "the channel was closed"))?;
		receiver
			.await
			.map_err(|_| tg::error!("expected a response"))?
	}
}

impl Drop for Client {
	fn drop(&mut self) {
		self.read_task.abort();
		self.write_task.abort();
	}
}
