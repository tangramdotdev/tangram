use {
	crate::{
		Options,
		client::{Request, RequestKind, Response, ResponseKind, SpawnResponse, WaitResponse},
	},
	futures::future,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		os::fd::{AsRawFd, RawFd},
		path::{Path, PathBuf},
		pin::pin,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tokio::{
		io::{AsyncReadExt as _, AsyncWriteExt as _},
		sync::{mpsc, oneshot},
	},
};

#[derive(Clone)]
pub struct Server {
	inner: Arc<Mutex<Inner>>,
	sender: tokio::sync::mpsc::Sender<(Request, oneshot::Sender<tg::Result<Response>>)>,
}
struct Inner {
	task: Option<tokio::task::JoinHandle<()>>,
	waits: Waits,
}

type Waits = BTreeMap<i32, tg::Either<i32, Vec<(u32, oneshot::Sender<tg::Result<Response>>)>>>;

impl Drop for Inner {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.abort();
		}
	}
}

impl Server {
	pub fn new() -> tg::Result<Self> {
		let (sender, mut receiver) =
			mpsc::channel::<(Request, oneshot::Sender<tg::Result<Response>>)>(128);
		let inner = Arc::new(Mutex::new(Inner {
			task: None,
			waits: BTreeMap::new(),
		}));
		let task = tokio::spawn({
			let inner = Arc::clone(&inner);
			async move {
				let Ok(mut signal) =
					tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
				else {
					tracing::error!("failed to create a ready signal");
					return;
				};

				loop {
					let signal = signal.recv();
					let request = receiver.recv();
					match future::select(pin!(signal), pin!(request)).await {
						future::Either::Left(_) => {
							let mut inner = inner.lock().unwrap();
							Self::reap_children(&mut inner.waits);
						},
						future::Either::Right((request, _)) => {
							let Some((request, sender)) = request else {
								break;
							};
							Self::handle_request(&inner, request, sender);
						},
					}
				}
			}
		});
		inner.lock().unwrap().task.replace(task);
		Ok(Self { inner, sender })
	}

	fn handle_request(
		inner: &Arc<Mutex<Inner>>,
		request: Request,
		sender: oneshot::Sender<tg::Result<Response>>,
	) {
		match request.kind {
			RequestKind::Spawn(spawn) => {
				let result;
				#[cfg(target_os = "linux")]
				{
					result = crate::linux2::spawn(spawn.command);
				}
				#[cfg(target_os = "macos")]
				{
					result = crate::darwin2::spawn(spawn.command);
				}
				let kind = result
					.map(|pid| {
						ResponseKind::Spawn(SpawnResponse {
							pid: Some(pid),
							error: None,
						})
					})
					.unwrap_or_else(|error| {
						ResponseKind::Spawn(SpawnResponse {
							pid: None,
							error: Some(tg::error::Data {
								message: Some(error.to_string()),
								..tg::error::Data::default()
							}),
						})
					});
				let response = Response {
					id: request.id,
					kind,
				};
				sender.send(Ok(response)).ok();
			},
			RequestKind::Wait(wait) => {
				let mut inner = inner.lock().unwrap();
				if let Some(tg::Either::Left(status)) = inner.waits.get(&wait.pid) {
					let kind = ResponseKind::Wait(WaitResponse {
						status: Some(*status),
					});
					let response = Response {
						id: request.id,
						kind,
					};
					sender.send(Ok(response)).ok();
					return;
				}
				inner
					.waits
					.entry(wait.pid)
					.or_insert(tg::Either::Right(Vec::new()))
					.as_mut()
					.unwrap_right()
					.push((request.id, sender));
			},
		}
	}

	fn reap_children(waits: &mut Waits) {
		unsafe {
			loop {
				// Wait for any child processes without blocking.
				let mut status = 0;
				let pid = libc::waitpid(-1, std::ptr::addr_of_mut!(status), libc::WNOHANG);
				let status = if libc::WIFEXITED(status) {
					libc::WEXITSTATUS(status)
				} else if libc::WIFSIGNALED(status) {
					128 + libc::WTERMSIG(status)
				} else {
					1
				};
				if pid == 0 {
					break;
				}
				if pid < 0 {
					let error = std::io::Error::last_os_error();
					tracing::error!(?error, "error waiting for children");
					break;
				}

				// Deliver the status to any pending waiters.
				if let Some(tg::Either::Right(waiters)) = waits.remove(&pid) {
					for (id, sender) in waiters {
						let kind = ResponseKind::Wait(WaitResponse {
							status: Some(status),
						});
						let response = Response { id, kind };
						sender.send(Ok(response)).ok();
					}
				}

				// Update the shared state.
				waits.insert(pid, tg::Either::Left(status));
			}
		}
	}

	// Enter the sandbox. This is irreversible for the current process.
	pub unsafe fn enter(options: &Options) -> tg::Result<()> {
		#[cfg(target_os = "linux")]
		crate::linux2::enter(&options)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;

		#[cfg(target_os = "macos")]
		crate::darwin2::enter(&options)
			.map_err(|source| tg::error!(!source, "failed to enter the sandbox"))?;

		Ok(())
	}

	pub async fn run(&self, path: PathBuf) -> tg::Result<()> {
		let listener = Self::bind(&path)?;
		self.serve(listener).await
	}

	pub fn bind(path: &Path) -> tg::Result<tokio::net::UnixListener> {
		// Bind the Unix listener to the specified path.
		let listener = tokio::net::UnixListener::bind(path)
			.map_err(|source| tg::error!(!source, "failed to bind the listener"))?;
		Ok(listener)
	}

	pub async fn serve(&self, listener: tokio::net::UnixListener) -> tg::Result<()> {
		// Accept connections in a loop.
		loop {
			let (stream, _addr) = listener
				.accept()
				.await
				.map_err(|source| tg::error!(!source, "failed to accept the connection"))?;
			let server = self.clone();
			tokio::spawn(async move {
				let result = server.handle_connection(stream).await;
				if let Err(error) = result {
					tracing::error!(?error, "connection handler failed");
				}
			});
		}
	}

	async fn handle_connection(&self, mut stream: tokio::net::UnixStream) -> tg::Result<()> {
		loop {
			// Read the message length.
			let length = stream
				.read_uvarint()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the message length"))?;

			// Read the request.
			let mut bytes = vec![0u8; length.to_usize().unwrap()];
			stream
				.read_exact(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the message"))?;

			// Deserialize the request
			let mut request = serde_json::from_slice::<Request>(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;

			// If this is a spawn request, receive the FDs.
			if let RequestKind::Spawn(request) = &mut request.kind {
				let fd = stream.as_raw_fd();
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
				request.fds = fds;
				request.command.stdin = request
					.command
					.stdin
					.and_then(|i| request.fds.get(i.to_usize().unwrap()).copied());
				request.command.stdout = request
					.command
					.stdout
					.and_then(|i| request.fds.get(i.to_usize().unwrap()).copied());
				request.command.stderr = request
					.command
					.stderr
					.and_then(|i| request.fds.get(i.to_usize().unwrap()).copied());
			}

			// Collect the FDs to close after the spawn request is processed.
			let fds_to_close = if let RequestKind::Spawn(spawn) = &request.kind {
				spawn.fds.clone()
			} else {
				Vec::new()
			};

			// Send the request to the shared task.
			let (sender, receiver) = oneshot::channel();
			self.sender
				.send((request, sender))
				.await
				.map_err(|source| tg::error!(!source, "failed to send the request"))?;
			let response = receiver
				.await
				.map_err(|source| tg::error!(!source, "failed to receive the request"))??;

			// Close the received FDs so the pipes can get EOF when the child exits.
			for fd in fds_to_close {
				unsafe {
					libc::close(fd);
				}
			}

			// Write the response.
			let response = serde_json::to_vec(&response).unwrap();
			let length = response.len().to_u64().unwrap();
			stream
				.write_uvarint(length)
				.await
				.map_err(|source| tg::error!(!source, "failed to send the response"))?;
			stream
				.write_all(&response)
				.await
				.map_err(|source| tg::error!(!source, "failed to send the response"))?;
		}
	}

	pub fn shutdown(&self) {
		let mut inner = self.inner.lock().unwrap();
		let Some(task) = inner.task.take() else {
			return;
		};
		task.abort();
	}
}
