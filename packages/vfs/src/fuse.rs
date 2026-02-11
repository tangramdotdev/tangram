use {
	self::sys::{
		fuse_attr, fuse_attr_out, fuse_batch_forget_in, fuse_entry_out, fuse_flush_in,
		fuse_forget_in, fuse_getattr_in, fuse_getxattr_in, fuse_getxattr_out, fuse_in_header,
		fuse_init_in, fuse_init_out, fuse_open_in, fuse_open_out, fuse_out_header, fuse_read_in,
		fuse_release_in,
	},
	crate::{FileType, Provider, Result},
	io_uring::{IoUring, opcode, types},
	num::ToPrimitive as _,
	std::{
		ffi::CString,
		io::Error,
		mem::size_of,
		ops::Deref,
		os::{
			fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd},
			unix::ffi::OsStrExt as _,
		},
		path::Path,
		sync::{Arc, Mutex},
	},
	sys::{FUSE_KERNEL_MINOR_VERSION, FUSE_KERNEL_VERSION, fuse_interrupt_in},
	zerocopy::{FromBytes as _, IntoBytes as _},
};

pub mod sys;

pub struct Server<P>(Arc<Inner<P>>);

pub struct Inner<P> {
	provider: P,
	task: Mutex<Option<tangram_futures::task::Shared<()>>>,
}

/// A request.
#[derive(Clone, Debug)]
struct Request {
	header: sys::fuse_in_header,
	data: RequestData,
}

/// A request's data.
#[derive(Clone, Debug)]
enum RequestData {
	BatchForget(sys::fuse_batch_forget_in),
	Destroy,
	Flush(sys::fuse_flush_in),
	Forget(sys::fuse_forget_in),
	GetAttr(sys::fuse_getattr_in),
	GetXattr(sys::fuse_getxattr_in, CString),
	Init(sys::fuse_init_in),
	ListXattr(sys::fuse_getxattr_in),
	Lookup(CString),
	Open(sys::fuse_open_in),
	OpenDir(sys::fuse_open_in),
	Read(sys::fuse_read_in),
	ReadDir(sys::fuse_read_in),
	ReadDirPlus(sys::fuse_read_in),
	ReadLink,
	Release(sys::fuse_release_in),
	ReleaseDir(sys::fuse_release_in),
	Statfs,
	Statx(sys::fuse_statx_in),
	Interrupt(sys::fuse_interrupt_in),
	Unsupported(u32),
}

/// A response.
#[derive(Clone, Debug)]
enum Response {
	Flush,
	GetAttr(sys::fuse_attr_out),
	GetXattr(Vec<u8>),
	Init(sys::fuse_init_out),
	ListXattr(Vec<u8>),
	Lookup(sys::fuse_entry_out),
	Open(sys::fuse_open_out),
	OpenDir(sys::fuse_open_out),
	Read(Vec<u8>),
	ReadDir(Vec<u8>),
	ReadDirPlus(Vec<u8>),
	ReadLink(CString),
	Release,
	ReleaseDir,
	Statfs(sys::fuse_statfs_out),
	Statx(sys::fuse_statx_out),
}

#[derive(Debug)]
struct AsyncResponse {
	unique: u64,
	opcode: sys::fuse_opcode,
	result: Result<Option<Response>>,
}

enum SyncRequestResult {
	Handled(Option<Response>),
	Defer,
}

const REQUEST_BUFFER_SIZE: usize = 1024 * 1024 + 4096;
const IO_URING_ENTRIES: u32 = 256;
const WORKER_READ_DEPTH: usize = 16;
const WORKER_CQE_BATCH_SIZE: usize = 64;
const WORKER_REQUEST_BATCH_SIZE: usize = 32;
const EVENTFD_USER_DATA: u64 = u64::MAX;
const FUSE_DEV_IOC_CLONE: libc::c_ulong = ior(sys::FUSE_DEV_IOC_MAGIC, 0, 4);

#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseDirentHeader {
	ino: u64,
	off: u64,
	namelen: u32,
	type_: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseDirentPlusHeader {
	entry_out: fuse_entry_out,
	dirent: FuseDirentHeader,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub async fn start(provider: P, path: &Path) -> Result<Self> {
		// Create the server.
		let server = Self(Arc::new(Inner {
			provider,
			task: Mutex::new(None),
		}));

		// Unmount.
		unmount(path).await.ok();

		// Mount.
		let fd = Self::mount(path)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to mount"))?;

		// Create worker threads.
		let worker_count = std::thread::available_parallelism()
			.map(std::num::NonZero::get)
			.unwrap_or(1);
		let runtime = tokio::runtime::Handle::current();
		let mut worker_handles = Vec::with_capacity(worker_count);
		for worker_id in 0..worker_count {
			let server = server.clone();
			let runtime = runtime.clone();
			let worker_fd = Self::clone_worker_fd(fd.as_ref())?;
			let worker = std::thread::spawn(move || {
				server
					.worker_thread(worker_id, &worker_fd, &runtime)
					.inspect_err(
						|error| tracing::error!(%error, %worker_id, "worker thread failed"),
					)
					.ok();
			});
			worker_handles.push(worker);
		}

		let path = path.to_owned();
		let shutdown = async move {
			unmount(&path)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to unmount"))
				.ok();
			drop(fd);
			for worker_handle in worker_handles {
				worker_handle.join().ok();
			}
		};

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn(|stop| async move {
			stop.wait().await;
			shutdown.await;
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait().await.unwrap();
	}

	fn clone_worker_fd(fd: &OwnedFd) -> Result<OwnedFd> {
		let cloned = unsafe { libc::ioctl(fd.as_raw_fd(), FUSE_DEV_IOC_CLONE) };
		if cloned < 0 {
			return Err(Error::last_os_error());
		}
		let ret = unsafe { libc::fcntl(cloned, libc::F_SETFD, libc::FD_CLOEXEC) };
		if ret == -1 {
			let error = Error::last_os_error();
			unsafe { libc::close(cloned) };
			return Err(error);
		}
		let cloned = unsafe { OwnedFd::from_raw_fd(cloned) };
		Ok(cloned)
	}

	fn worker_thread(
		&self,
		worker_id: usize,
		fd: &OwnedFd,
		runtime: &tokio::runtime::Handle,
	) -> Result<()> {
		let eventfd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
		if eventfd < 0 {
			return Err(Error::last_os_error());
		}
		let eventfd = Arc::new(unsafe { OwnedFd::from_raw_fd(eventfd) });
		let (sender, receiver) = async_channel::unbounded::<AsyncResponse>();

		let mut io_uring = IoUring::new(IO_URING_ENTRIES)?;
		let mut request_buffers = (0..WORKER_READ_DEPTH)
			.map(|_| vec![0u8; REQUEST_BUFFER_SIZE])
			.collect::<Vec<_>>();
		let mut free_slots = (0..WORKER_READ_DEPTH).collect::<Vec<_>>();
		let mut inflight_reads = 0usize;
		let mut eventfd_inflight = false;
		let mut eventfd_buffer = [0u8; size_of::<u64>()];
		let mut request_batch = Vec::with_capacity(WORKER_REQUEST_BATCH_SIZE);

		loop {
			{
				let mut submission = io_uring.submission();

				while inflight_reads < WORKER_READ_DEPTH {
					let Some(slot) = free_slots.pop() else {
						break;
					};
					let buffer = request_buffers.get_mut(slot).unwrap();
					let entry = opcode::Read::new(
						types::Fd(fd.as_raw_fd()),
						buffer.as_mut_ptr(),
						buffer.len().to_u32().unwrap(),
					)
					.offset(u64::MAX)
					.build()
					.user_data(slot.to_u64().unwrap());
					if unsafe { submission.push(&entry) }.is_err() {
						free_slots.push(slot);
						break;
					}
					inflight_reads += 1;
				}

				if !eventfd_inflight {
					let entry = opcode::Read::new(
						types::Fd(eventfd.as_raw_fd()),
						eventfd_buffer.as_mut_ptr(),
						eventfd_buffer.len().to_u32().unwrap(),
					)
					.offset(u64::MAX)
					.build()
					.user_data(EVENTFD_USER_DATA);
					if unsafe { submission.push(&entry) }.is_ok() {
						eventfd_inflight = true;
					}
				}
			}

			if let Err(error) = io_uring.submit_and_wait(1) {
				match error.raw_os_error() {
					Some(libc::EINTR) => {
						continue;
					},
					Some(libc::ENODEV) => {
						Self::drain_async_responses(worker_id, fd.as_raw_fd(), &receiver);
						return Ok(());
					},
					_ => return Err(error),
				}
			}

			request_batch.clear();
			let mut saw_eventfd = false;
			{
				let mut completion = io_uring.completion();
				for completion in completion.by_ref().take(WORKER_CQE_BATCH_SIZE) {
					if completion.user_data() == EVENTFD_USER_DATA {
						eventfd_inflight = false;
						saw_eventfd = true;
						continue;
					}

					let slot = completion.user_data().to_usize().unwrap();
					inflight_reads -= 1;
					free_slots.push(slot);

					if completion.result() < 0 {
						let error = -completion.result();
						if error == libc::EINTR || error == libc::EAGAIN || error == libc::ENOENT {
							continue;
						}
						if error == libc::ENODEV {
							Self::drain_async_responses(worker_id, fd.as_raw_fd(), &receiver);
							return Ok(());
						}
						return Err(Error::from_raw_os_error(error));
					}
					if completion.result() == 0 {
						continue;
					}

					let size = completion.result().to_usize().unwrap();
					let buffer = request_buffers.get(slot).unwrap();
					let request = Self::deserialize_request(&buffer[..size])?;
					request_batch.push(request);
				}
			}

			for request in request_batch.iter().cloned() {
				let unique = request.header.unique;
				let opcode = request.header.opcode;
				match self.handle_request_sync(request.clone()) {
					Ok(SyncRequestResult::Handled(Some(response))) => {
						write_response(fd.as_raw_fd(), unique, &response)
							.inspect_err(|error| {
								tracing::error!(?error, %worker_id, "failed to write the response");
							})
							.ok();
					},
					Ok(SyncRequestResult::Handled(None)) => (),
					Ok(SyncRequestResult::Defer) => {
						self.spawn_async_request(
							request,
							runtime,
							sender.clone(),
							eventfd.clone(),
							worker_id,
						);
					},
					Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
						self.spawn_async_request(
							request,
							runtime,
							sender.clone(),
							eventfd.clone(),
							worker_id,
						);
					},
					Err(error) => {
						let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
						if !is_expected_error(opcode, Some(error_code)) {
							tracing::error!(?error, ?opcode, %worker_id, "unexpected error");
						}
						write_error(fd.as_raw_fd(), unique, error_code)
							.inspect_err(|error| {
								tracing::error!(?error, %worker_id, "failed to write the error");
							})
							.ok();
					},
				}
			}

			if saw_eventfd || !receiver.is_empty() {
				Self::drain_async_responses(worker_id, fd.as_raw_fd(), &receiver);
			}
		}
	}

	fn deserialize_request(buffer: &[u8]) -> Result<Request> {
		let (header, _) = sys::fuse_in_header::read_from_prefix(buffer)
			.map_err(|_| Error::other("failed to deserialize the request header"))?;
		let header_len = std::mem::size_of::<sys::fuse_in_header>();
		let request_len = header
			.len
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if request_len < header_len || request_len > buffer.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		let total_extlen = usize::from(header.total_extlen) * 8;
		let data = &buffer[header_len..request_len];
		let Some(data_len) = data.len().checked_sub(total_extlen) else {
			return Err(Error::other("failed to deserialize request data"));
		};
		let data = &data[..data_len];
		let data = match header.opcode {
			sys::fuse_opcode_FUSE_BATCH_FORGET => RequestData::BatchForget(read_data(data)?),
			sys::fuse_opcode_FUSE_DESTROY => RequestData::Destroy,
			sys::fuse_opcode_FUSE_FLUSH => RequestData::Flush(read_data(data)?),
			sys::fuse_opcode_FUSE_FORGET => RequestData::Forget(read_data(data)?),
			sys::fuse_opcode_FUSE_GETATTR => RequestData::GetAttr(read_data(data)?),
			sys::fuse_opcode_FUSE_GETXATTR => {
				if data.len() < std::mem::size_of::<sys::fuse_getxattr_in>() {
					return Err(Error::other("failed to deserialize request data"));
				}
				let (fuse_getxattr_in, name) =
					data.split_at(std::mem::size_of::<sys::fuse_getxattr_in>());
				let fuse_getxattr_in = read_data(fuse_getxattr_in)?;
				let name = CString::from_vec_with_nul(name.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::GetXattr(fuse_getxattr_in, name)
			},
			sys::fuse_opcode_FUSE_INIT => RequestData::Init(read_data(data)?),
			sys::fuse_opcode_FUSE_LISTXATTR => RequestData::ListXattr(read_data(data)?),
			sys::fuse_opcode_FUSE_LOOKUP => {
				let data = CString::from_vec_with_nul(data.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::Lookup(data)
			},
			sys::fuse_opcode_FUSE_OPEN => RequestData::Open(read_data(data)?),
			sys::fuse_opcode_FUSE_OPENDIR => RequestData::OpenDir(read_data(data)?),
			sys::fuse_opcode_FUSE_READ => RequestData::Read(read_data(data)?),
			sys::fuse_opcode_FUSE_READDIR => RequestData::ReadDir(read_data(data)?),
			sys::fuse_opcode_FUSE_READDIRPLUS => RequestData::ReadDirPlus(read_data(data)?),
			sys::fuse_opcode_FUSE_READLINK => RequestData::ReadLink,
			sys::fuse_opcode_FUSE_RELEASE => RequestData::Release(read_data(data)?),
			sys::fuse_opcode_FUSE_RELEASEDIR => RequestData::ReleaseDir(read_data(data)?),
			sys::fuse_opcode_FUSE_STATFS => RequestData::Statfs,
			sys::fuse_opcode_FUSE_STATX => RequestData::Statx(read_data(data)?),
			sys::fuse_opcode_FUSE_INTERRUPT => RequestData::Interrupt(read_data(data)?),
			_ => RequestData::Unsupported(header.opcode),
		};
		let request = Request { header, data };
		Ok(request)
	}

	fn spawn_async_request(
		&self,
		request: Request,
		runtime: &tokio::runtime::Handle,
		sender: async_channel::Sender<AsyncResponse>,
		eventfd: Arc<OwnedFd>,
		worker_id: usize,
	) {
		let server = self.clone();
		runtime.spawn(async move {
			let opcode = request.header.opcode;
			let unique = request.header.unique;
			let result = server.handle_request(request).await.inspect_err(|error| {
				if !is_expected_error(opcode, error.raw_os_error()) {
					tracing::error!(?error, ?opcode, %worker_id, "unexpected error");
				}
			});
			let response = AsyncResponse {
				unique,
				opcode,
				result,
			};
			if sender.send(response).await.is_ok() {
				signal_eventfd(eventfd.as_raw_fd())
					.inspect_err(
						|error| tracing::error!(?error, %worker_id, "failed to signal eventfd"),
					)
					.ok();
			}
		});
	}

	fn drain_async_responses(
		worker_id: usize,
		fd: RawFd,
		receiver: &async_channel::Receiver<AsyncResponse>,
	) {
		while let Ok(response) = receiver.try_recv() {
			match response.result {
				Ok(Some(response_)) => {
					write_response(fd, response.unique, &response_)
						.inspect_err(|error| {
							tracing::error!(?error, %worker_id, "failed to write the async response");
						})
						.ok();
				},
				Ok(None) => (),
				Err(error) => {
					let error = error.raw_os_error().unwrap_or(libc::ENOSYS);
					if !is_expected_error(response.opcode, Some(error)) {
						tracing::error!(?error, opcode = response.opcode, %worker_id, "unexpected error");
					}
					write_error(fd, response.unique, error)
						.inspect_err(|error| {
							tracing::error!(?error, %worker_id, "failed to write the async error");
						})
						.ok();
				},
			}
		}
	}

	fn handle_request_sync(&self, request: Request) -> Result<SyncRequestResult> {
		let result = match request.data {
			RequestData::BatchForget(data) => {
				Ok(Self::handle_batch_forget_request_sync(request.header, data))
			},
			RequestData::Destroy => Ok(None),
			RequestData::Flush(data) => {
				Ok(Some(Self::handle_flush_request_sync(request.header, data)))
			},
			RequestData::Forget(data) => Ok(Self::handle_forget_request_sync(request.header, data)),
			RequestData::GetAttr(data) => self.handle_get_attr_request_sync(request.header, data),
			RequestData::GetXattr(data, name) => {
				self.handle_get_xattr_request_sync(request.header, data, &name)
			},
			RequestData::Init(data) => Self::handle_init_request_sync(request.header, data),
			RequestData::ListXattr(data) => {
				self.handle_list_xattr_request_sync(request.header, data)
			},
			RequestData::Lookup(data) => self.handle_lookup_request_sync(request.header, &data),
			RequestData::Open(data) => self.handle_open_request_sync(request.header, data),
			RequestData::OpenDir(data) => self.handle_open_dir_request_sync(request.header, data),
			RequestData::Read(data) => self.handle_read_request_sync(request.header, data),
			RequestData::ReadDir(data) => {
				self.handle_read_dir_request_sync(request.header, data, false)
			},
			RequestData::ReadDirPlus(data) => {
				self.handle_read_dir_request_sync(request.header, data, true)
			},
			RequestData::ReadLink => self.handle_read_link_request_sync(request.header),
			RequestData::Release(data) => Ok(Some(Self::handle_release_request_sync(
				request.header,
				data,
				&self.provider,
			))),
			RequestData::ReleaseDir(data) => Ok(Some(Self::handle_release_dir_request_sync(
				request.header,
				data,
				&self.provider,
			))),
			RequestData::Statfs => Ok(Some(Self::handle_statfs_request_sync(request.header))),
			RequestData::Statx(data) => self.handle_statx_request_sync(request.header, data),
			RequestData::Interrupt(data) => {
				Ok(Self::handle_interrupt_request_sync(request.header, data))
			},
			RequestData::Unsupported(opcode) => {
				Self::handle_unsupported_request_sync(request.header, opcode)
			},
		};

		match result {
			Ok(response) => Ok(SyncRequestResult::Handled(response)),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				Ok(SyncRequestResult::Defer)
			},
			Err(error) => Err(error),
		}
	}

	fn handle_batch_forget_request_sync(
		_header: fuse_in_header,
		_request: fuse_batch_forget_in,
	) -> Option<Response> {
		None
	}

	fn handle_flush_request_sync(_header: fuse_in_header, _request: fuse_flush_in) -> Response {
		Response::Flush
	}

	fn handle_forget_request_sync(
		_header: fuse_in_header,
		_request: fuse_forget_in,
	) -> Option<Response> {
		None
	}

	fn handle_get_attr_request_sync(
		&self,
		header: fuse_in_header,
		_request: fuse_getattr_in,
	) -> Result<Option<Response>> {
		let attr = self.provider.getattr_sync(header.nodeid)?;
		let out = Self::fuse_attr_out(header.nodeid, attr);
		Ok(Some(Response::GetAttr(out)))
	}

	fn handle_get_xattr_request_sync(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
		name: &CString,
	) -> Result<Option<Response>> {
		let name = name
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?;
		let attr = self
			.provider
			.getxattr_sync(header.nodeid, name)?
			.map(|value| value.to_vec())
			.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;

		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attr.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::GetXattr(response)))
		} else if request.size.to_usize().unwrap() < attr.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::GetXattr(attr)))
		}
	}

	fn handle_init_request_sync(
		_header: fuse_in_header,
		request: fuse_init_in,
	) -> Result<Option<Response>> {
		let response = Self::init_response(request)?;
		Ok(Some(Response::Init(response)))
	}

	fn handle_list_xattr_request_sync(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
	) -> Result<Option<Response>> {
		let attrs = self
			.provider
			.listxattrs_sync(header.nodeid)?
			.into_iter()
			.flat_map(|s| {
				let mut s = s.into_bytes();
				s.push(0);
				s.into_iter()
			})
			.collect::<Vec<_>>();

		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attrs.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::ListXattr(response)))
		} else if request.size.to_usize().unwrap() < attrs.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::ListXattr(attrs)))
		}
	}

	fn handle_lookup_request_sync(
		&self,
		header: fuse_in_header,
		request: &CString,
	) -> Result<Option<Response>> {
		let name = request
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENOENT))?;
		let node = self
			.provider
			.lookup_sync(header.nodeid, name)?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
		let out = self.fuse_entry_out_sync(node)?;
		Ok(Some(Response::Lookup(out)))
	}

	fn handle_open_request_sync(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let (fh, _backing_fd) = self.provider.open_sync(header.nodeid)?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::Open(out)))
	}

	fn handle_open_dir_request_sync(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let fh = self.provider.opendir_sync(header.nodeid)?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::OpenDir(out)))
	}

	fn handle_read_request_sync(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
	) -> Result<Option<Response>> {
		let bytes =
			self.provider
				.read_sync(request.fh, request.offset, request.size.to_u64().unwrap())?;
		Ok(Some(Response::Read(bytes.to_vec())))
	}

	fn handle_read_dir_request_sync(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
		plus: bool,
	) -> Result<Option<Response>> {
		let entries = self.provider.readdir_sync(request.fh)?;
		let struct_size = if plus {
			std::mem::size_of::<FuseDirentPlusHeader>()
		} else {
			std::mem::size_of::<FuseDirentHeader>()
		};

		let entries = entries
			.into_iter()
			.enumerate()
			.skip(request.offset.to_usize().unwrap());
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node)) in entries {
			let attr = self.provider.getattr_sync(node)?;
			let name = name.into_bytes();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let type_ = match attr.typ {
				FileType::Directory => libc::S_IFDIR.to_u32().unwrap(),
				FileType::File { .. } => libc::S_IFREG.to_u32().unwrap(),
				FileType::Symlink => libc::S_IFLNK.to_u32().unwrap(),
			};

			let dirent = FuseDirentHeader {
				ino: node,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			if plus {
				let entry = FuseDirentPlusHeader {
					entry_out: self.fuse_entry_out_sync(node)?,
					dirent,
				};
				response.extend_from_slice(entry.as_bytes());
			} else {
				response.extend_from_slice(dirent.as_bytes());
			}
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		if plus {
			Ok(Some(Response::ReadDirPlus(response)))
		} else {
			Ok(Some(Response::ReadDir(response)))
		}
	}

	fn handle_read_link_request_sync(&self, header: fuse_in_header) -> Result<Option<Response>> {
		let target = self.provider.readlink_sync(header.nodeid)?.to_vec();
		let target = CString::new(target).unwrap();
		Ok(Some(Response::ReadLink(target)))
	}

	fn handle_release_request_sync(
		_header: fuse_in_header,
		request: fuse_release_in,
		provider: &P,
	) -> Response {
		provider.close_sync(request.fh);
		Response::Release
	}

	fn handle_release_dir_request_sync(
		_header: fuse_in_header,
		request: fuse_release_in,
		provider: &P,
	) -> Response {
		provider.close_sync(request.fh);
		Response::ReleaseDir
	}

	fn handle_statfs_request_sync(_header: sys::fuse_in_header) -> Response {
		let out = sys::fuse_statfs_out {
			st: sys::fuse_kstatfs {
				blocks: u64::MAX / 2,
				bfree: u64::MAX / 2,
				bavail: u64::MAX / 2,
				files: u64::MAX / 2,
				ffree: u64::MAX / 2,
				bsize: 65536,
				namelen: u32::MAX,
				frsize: 1024,
				padding: 0,
				spare: [0; 6],
			},
		};
		Response::Statfs(out)
	}

	fn handle_statx_request_sync(
		&self,
		header: sys::fuse_in_header,
		request: sys::fuse_statx_in,
	) -> Result<Option<Response>> {
		let Some(Response::GetAttr(attr)) = self.handle_get_attr_request_sync(header, {
			sys::fuse_getattr_in {
				getattr_flags: request.getattr_flags,
				dummy: 0,
				fh: request.fh,
			}
		})?
		else {
			return Ok(None);
		};
		let out = sys::fuse_statx_out {
			attr_valid: attr.attr_valid,
			attr_valid_nsec: attr.attr_valid_nsec,
			flags: request.getattr_flags,
			spare: [0; 2],
			stat: sys::fuse_statx {
				mask: 0xffff_ffff,
				ino: attr.attr.ino,
				size: attr.attr.size,
				blocks: attr.attr.blocks,
				blksize: attr.attr.blksize,
				attributes: 0,
				nlink: attr.attr.nlink,
				uid: attr.attr.uid,
				gid: attr.attr.gid,
				mode: attr.attr.mode.to_u16().unwrap(),
				__spare0: [0],
				attributes_mask: 0xffff_ffff_ffff_ffff,
				atime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				btime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				mtime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				ctime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				rdev_major: 0,
				rdev_minor: 0,
				dev_major: 0,
				dev_minor: 0,
				__spare2: [0; 14],
			},
		};
		Ok(Some(Response::Statx(out)))
	}

	fn handle_interrupt_request_sync(
		_header: fuse_in_header,
		_request: fuse_interrupt_in,
	) -> Option<Response> {
		None
	}

	fn handle_unsupported_request_sync(
		header: fuse_in_header,
		request: u32,
	) -> Result<Option<Response>> {
		tracing::trace!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn handle_request(&self, request: Request) -> Result<Option<Response>> {
		match request.data {
			RequestData::BatchForget(data) => {
				self.handle_batch_forget_request(request.header, data).await
			},
			RequestData::Destroy => Ok(None),
			RequestData::Flush(data) => self.handle_flush_request(request.header, data).await,
			RequestData::Forget(data) => self.handle_forget_request(request.header, data).await,
			RequestData::GetAttr(data) => self.handle_get_attr_request(request.header, data).await,
			RequestData::GetXattr(data, name) => {
				self.handle_get_xattr_request(request.header, data, name)
					.await
			},
			RequestData::Init(data) => self.handle_init_request(request.header, data).await,
			RequestData::ListXattr(data) => {
				self.handle_list_xattr_request(request.header, data).await
			},
			RequestData::Lookup(data) => self.handle_lookup_request(request.header, data).await,
			RequestData::Open(data) => self.handle_open_request(request.header, data).await,
			RequestData::OpenDir(data) => self.handle_open_dir_request(request.header, data).await,
			RequestData::Read(data) => self.handle_read_request(request.header, data).await,
			RequestData::ReadDir(data) => {
				self.handle_read_dir_request(request.header, data, false)
					.await
			},
			RequestData::ReadDirPlus(data) => {
				self.handle_read_dir_request(request.header, data, true)
					.await
			},
			RequestData::ReadLink => self.handle_read_link_request(request.header).await,
			RequestData::Release(data) => self.handle_release_request(request.header, data).await,
			RequestData::ReleaseDir(data) => {
				self.handle_release_dir_request(request.header, data).await
			},
			RequestData::Statfs => self.handle_statfs_request(request.header).await,
			RequestData::Statx(data) => self.handle_statx_request(request.header, data).await,
			RequestData::Interrupt(data) => {
				self.handle_interrupt_request(request.header, data).await
			},

			RequestData::Unsupported(opcode) => {
				self.handle_unsupported_request(request.header, opcode)
					.await
			},
		}
	}

	fn init_response(request: fuse_init_in) -> Result<fuse_init_out> {
		const REQUIRED_FLAGS: u32 =
			sys::FUSE_INIT_EXT | sys::FUSE_MAX_PAGES | sys::FUSE_MAP_ALIGNMENT;
		const REQUIRED_FLAGS2: u32 = (sys::FUSE_PASSTHROUGH >> 32) as u32;
		const NEGOTIATED_FLAGS: u32 = sys::FUSE_ASYNC_READ
			| sys::FUSE_DO_READDIRPLUS
			| sys::FUSE_PARALLEL_DIROPS
			| sys::FUSE_CACHE_SYMLINKS
			| sys::FUSE_SPLICE_MOVE
			| sys::FUSE_SPLICE_READ
			| sys::FUSE_MAX_PAGES
			| sys::FUSE_MAP_ALIGNMENT
			| sys::FUSE_INIT_EXT;
		const NEGOTIATED_FLAGS2: u32 = (sys::FUSE_PASSTHROUGH >> 32) as u32;
		const MAX_PAGES: u16 = 256;
		const MAP_ALIGNMENT: u16 = 12;

		if request.major != FUSE_KERNEL_VERSION {
			return Err(Error::from_raw_os_error(libc::EPROTO));
		}
		if request.minor < FUSE_KERNEL_MINOR_VERSION {
			return Err(Error::from_raw_os_error(libc::EPROTO));
		}
		if request.flags & REQUIRED_FLAGS != REQUIRED_FLAGS {
			return Err(Error::from_raw_os_error(libc::EPROTO));
		}
		if request.flags2 & REQUIRED_FLAGS2 != REQUIRED_FLAGS2 {
			return Err(Error::from_raw_os_error(libc::EPROTO));
		}

		let response = fuse_init_out {
			major: FUSE_KERNEL_VERSION,
			minor: FUSE_KERNEL_MINOR_VERSION,
			max_readahead: 1024 * 1024,
			flags: NEGOTIATED_FLAGS,
			max_background: 0,
			congestion_threshold: 0,
			max_write: 1024 * 1024,
			time_gran: 0,
			max_pages: MAX_PAGES,
			map_alignment: MAP_ALIGNMENT,
			flags2: NEGOTIATED_FLAGS2,
			max_stack_depth: 0,
			request_timeout: 0,
			unused: [0; 11],
		};

		Ok(response)
	}

	async fn handle_batch_forget_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_batch_forget_in,
	) -> Result<Option<Response>> {
		Ok(None)
	}

	async fn handle_flush_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_flush_in,
	) -> Result<Option<Response>> {
		Ok(Some(Response::Flush))
	}

	async fn handle_forget_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_forget_in,
	) -> Result<Option<Response>> {
		Ok(None)
	}

	async fn handle_get_attr_request(
		&self,
		header: fuse_in_header,
		_request: fuse_getattr_in,
	) -> Result<Option<Response>> {
		let attr = self.provider_getattr(header.nodeid).await?;
		let out = Self::fuse_attr_out(header.nodeid, attr);
		Ok(Some(Response::GetAttr(out)))
	}

	async fn handle_get_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
		name: CString,
	) -> Result<Option<Response>> {
		let name = name
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?;
		let attr = self
			.provider_getxattr(header.nodeid, name)
			.await?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;

		// If the request size is 0, the driver is requesting the size of the xattr.
		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attr.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::GetXattr(response)))
		} else if request.size.to_usize().unwrap() < attr.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::GetXattr(attr)))
		}
	}

	async fn handle_init_request(
		&self,
		_header: fuse_in_header,
		request: fuse_init_in,
	) -> Result<Option<Response>> {
		let response = Self::init_response(request)?;
		Ok(Some(Response::Init(response)))
	}

	async fn handle_list_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
	) -> Result<Option<Response>> {
		let attrs = self
			.provider_listxattrs(header.nodeid)
			.await?
			.into_iter()
			.flat_map(|s| {
				let mut s = s.into_bytes();
				s.push(0);
				s.into_iter()
			})
			.collect::<Vec<_>>();

		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attrs.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::ListXattr(response)))
		} else if request.size.to_usize().unwrap() < attrs.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::ListXattr(attrs)))
		}
	}

	async fn handle_lookup_request(
		&self,
		header: fuse_in_header,
		request: CString,
	) -> Result<Option<Response>> {
		let name = request
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENOENT))?;
		let node = self
			.provider_lookup(header.nodeid, name)
			.await?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
		let out = self.fuse_entry_out(node).await?;
		Ok(Some(Response::Lookup(out)))
	}

	async fn handle_open_request(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let fh = self.provider_open(header.nodeid).await?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::Open(out)))
	}

	async fn handle_open_dir_request(
		&self,
		header: fuse_in_header,
		_request: fuse_open_in,
	) -> Result<Option<Response>> {
		let fh = self.provider_opendir(header.nodeid).await?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::OpenDir(out)))
	}

	async fn handle_read_request(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
	) -> Result<Option<Response>> {
		let bytes = self
			.provider_read(request.fh, request.offset, request.size.to_u64().unwrap())
			.await?;
		Ok(Some(Response::Read(bytes)))
	}

	async fn handle_read_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
		plus: bool,
	) -> Result<Option<Response>> {
		let entries = self.provider_readdir(request.fh).await?;

		let struct_size = if plus {
			std::mem::size_of::<FuseDirentPlusHeader>()
		} else {
			std::mem::size_of::<FuseDirentHeader>()
		};

		let entries = entries
			.into_iter()
			.enumerate()
			.skip(request.offset.to_usize().unwrap());
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node)) in entries {
			let attr = self.provider_getattr(node).await?;
			let name = name.into_bytes();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let type_ = match attr.typ {
				FileType::Directory => libc::S_IFDIR.to_u32().unwrap(),
				FileType::File { .. } => libc::S_IFREG.to_u32().unwrap(),
				FileType::Symlink => libc::S_IFLNK.to_u32().unwrap(),
			};

			let dirent = FuseDirentHeader {
				ino: node,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			if plus {
				let entry = FuseDirentPlusHeader {
					entry_out: self.fuse_entry_out(node).await?,
					dirent,
				};
				response.extend_from_slice(entry.as_bytes());
			} else {
				response.extend_from_slice(dirent.as_bytes());
			}
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		if plus {
			Ok(Some(Response::ReadDirPlus(response)))
		} else {
			Ok(Some(Response::ReadDir(response)))
		}
	}

	async fn handle_read_link_request(&self, header: fuse_in_header) -> Result<Option<Response>> {
		let target = self.provider_readlink(header.nodeid).await?;
		let target = CString::new(target).unwrap();
		Ok(Some(Response::ReadLink(target)))
	}

	async fn handle_release_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		self.provider.close_sync(request.fh);
		self.provider.close(request.fh).await;
		Ok(Some(Response::Release))
	}

	async fn handle_release_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		self.provider.close_sync(request.fh);
		self.provider.close(request.fh).await;
		Ok(Some(Response::ReleaseDir))
	}

	async fn handle_statfs_request(
		&self,
		_header: sys::fuse_in_header,
	) -> Result<Option<Response>> {
		let out = sys::fuse_statfs_out {
			st: sys::fuse_kstatfs {
				blocks: u64::MAX / 2,
				bfree: u64::MAX / 2,
				bavail: u64::MAX / 2,
				files: u64::MAX / 2,
				ffree: u64::MAX / 2,
				bsize: 65536,
				namelen: u32::MAX,
				frsize: 1024,
				padding: 0,
				spare: [0; 6],
			},
		};
		Ok(Some(Response::Statfs(out)))
	}

	async fn handle_statx_request(
		&self,
		header: sys::fuse_in_header,
		request: sys::fuse_statx_in,
	) -> Result<Option<Response>> {
		let Some(Response::GetAttr(attr)) = self
			.handle_get_attr_request(header, {
				sys::fuse_getattr_in {
					getattr_flags: request.getattr_flags,
					dummy: 0,
					fh: request.fh,
				}
			})
			.await?
		else {
			return Ok(None);
		};
		let out = sys::fuse_statx_out {
			attr_valid: attr.attr_valid,
			attr_valid_nsec: attr.attr_valid_nsec,
			flags: request.getattr_flags,
			spare: [0; 2],
			stat: sys::fuse_statx {
				mask: 0xffff_ffff,
				ino: attr.attr.ino,
				size: attr.attr.size,
				blocks: attr.attr.blocks,
				blksize: attr.attr.blksize,
				attributes: 0,
				nlink: attr.attr.nlink,
				uid: attr.attr.uid,
				gid: attr.attr.gid,
				mode: attr.attr.mode.to_u16().unwrap(),
				__spare0: [0],
				attributes_mask: 0xffff_ffff_ffff_ffff,
				atime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				btime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				mtime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				ctime: sys::fuse_sx_time {
					tv_nsec: 0,
					tv_sec: 0,
					__reserved: 0,
				},
				rdev_major: 0,
				rdev_minor: 0,
				dev_major: 0,
				dev_minor: 0,
				__spare2: [0; 14],
			},
		};
		Ok(Some(Response::Statx(out)))
	}

	async fn handle_interrupt_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_interrupt_in,
	) -> Result<Option<Response>> {
		Ok(None)
	}

	async fn handle_unsupported_request(
		&self,
		header: fuse_in_header,
		request: u32,
	) -> Result<Option<Response>> {
		tracing::trace!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	async fn mount(path: &Path) -> Result<Arc<OwnedFd>> {
		unsafe {
			// Create the file socket pair.
			let mut fds = [0, 0];
			let ret = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
			if ret != 0 {
				return Err(std::io::Error::last_os_error());
			}
			let fuse_commfd = CString::new(fds[0].to_string()).unwrap();

			// Create the args.
			let uid = libc::getuid();
			let gid = libc::getgid();
			let options = CString::new(format!(
				"rootmode=40755,user_id={uid},group_id={gid},default_permissions"
			))
			.unwrap();
			let path = CString::new(path.as_os_str().as_bytes()).unwrap();

			// Fork.
			let pid = libc::fork();
			if pid == -1 {
				libc::close(fds[0]);
				libc::close(fds[1]);
				let source = std::io::Error::last_os_error();
				return Err(source);
			}

			// Exec.
			if pid == 0 {
				let argv = [
					c"fusermount3".as_ptr(),
					c"-o".as_ptr(),
					options.as_ptr(),
					c"--".as_ptr(),
					path.as_ptr(),
					std::ptr::null(),
				];
				libc::close(fds[1]);
				libc::fcntl(fds[0], libc::F_SETFD, 0);
				libc::setenv(c"_FUSE_COMMFD".as_ptr(), fuse_commfd.as_ptr(), 1);
				libc::execvp(argv[0], argv.as_ptr());
				libc::close(fds[0]);
				libc::exit(1);
			}
			libc::close(fds[0]);

			// Create the control message.
			let mut control = vec![0u8; libc::CMSG_SPACE(4) as usize];
			let mut buffer = vec![0u8; 8];
			let mut msg = Box::new(libc::msghdr {
				msg_name: std::ptr::null_mut(),
				msg_namelen: 0,
				msg_iov: [libc::iovec {
					iov_base: buffer.as_mut_ptr().cast(),
					iov_len: buffer.len(),
				}]
				.as_mut_ptr(),
				msg_iovlen: 1,
				msg_control: control.as_mut_ptr().cast(),
				#[cfg_attr(target_os = "macos", expect(clippy::cast_possible_truncation))]
				msg_controllen: std::mem::size_of_val(&control) as _,
				msg_flags: 0,
			});
			let msg: *mut libc::msghdr = std::ptr::from_mut(msg.as_mut());

			// Receive the control message.
			let ret = libc::recvmsg(fds[1], msg, 0);
			if ret == -1 {
				return Err(Error::last_os_error());
			}
			if ret == 0 {
				return Err(Error::other("failed to read the control message"));
			}

			// Get the file descriptor.
			let cmsg = libc::CMSG_FIRSTHDR(msg);
			if cmsg.is_null() {
				return Err(Error::other("missing control message"));
			}
			let mut fd: RawFd = 0;
			libc::memcpy(
				std::ptr::addr_of_mut!(fd).cast(),
				libc::CMSG_DATA(cmsg).cast(),
				std::mem::size_of_val(&fd),
			);
			let ret = libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
			if ret == -1 {
				return Err(std::io::Error::last_os_error());
			}
			let fd = Arc::new(OwnedFd::from_raw_fd(fd));

			// Reap the process.
			let ret = libc::waitpid(pid, std::ptr::null_mut(), 0);
			if ret == -1 {
				return Err(std::io::Error::last_os_error());
			}

			Ok(fd)
		}
	}

	async fn provider_getattr(&self, node: u64) -> Result<crate::Attrs> {
		match self.provider.getattr_sync(node) {
			Ok(attr) => Ok(attr),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.getattr(node).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_getxattr(&self, node: u64, name: &str) -> Result<Option<Vec<u8>>> {
		match self.provider.getxattr_sync(node, name) {
			Ok(value) => Ok(value.map(Into::into)),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => self
				.provider
				.getxattr(node, name)
				.await
				.map(|value| value.map(Into::into)),
			Err(error) => Err(error),
		}
	}

	async fn provider_listxattrs(&self, node: u64) -> Result<Vec<String>> {
		match self.provider.listxattrs_sync(node) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.listxattrs(node).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_lookup(&self, node: u64, name: &str) -> Result<Option<u64>> {
		match self.provider.lookup_sync(node, name) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.lookup(node, name).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_open(&self, node: u64) -> Result<u64> {
		match self.provider.open_sync(node) {
			Ok((fh, _backing_fd)) => Ok(fh),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.open(node).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_opendir(&self, node: u64) -> Result<u64> {
		match self.provider.opendir_sync(node) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.opendir(node).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_read(&self, handle: u64, offset: u64, size: u64) -> Result<Vec<u8>> {
		match self.provider.read_sync(handle, offset, size) {
			Ok(value) => Ok(value.to_vec()),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => self
				.provider
				.read(handle, offset, size)
				.await
				.map(|value| value.to_vec()),
			Err(error) => Err(error),
		}
	}

	async fn provider_readdir(&self, handle: u64) -> Result<Vec<(String, u64)>> {
		match self.provider.readdir_sync(handle) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.readdir(handle).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_readlink(&self, node: u64) -> Result<Vec<u8>> {
		match self.provider.readlink_sync(node) {
			Ok(value) => Ok(value.to_vec()),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => self
				.provider
				.readlink(node)
				.await
				.map(|value| value.to_vec()),
			Err(error) => Err(error),
		}
	}

	fn fuse_attr_out(node: u64, attr: crate::Attrs) -> fuse_attr_out {
		let (size, mode) = match attr.typ {
			FileType::Directory => (0, libc::S_IFDIR | 0o555),
			FileType::File { executable, size } => (
				size,
				libc::S_IFREG | 0o444 | (if executable { 0o111 } else { 0o000 }),
			),
			FileType::Symlink => (0, libc::S_IFLNK | 0o444),
		};
		let mode = mode.to_u32().unwrap();
		fuse_attr_out {
			attr_valid: 1024,
			attr_valid_nsec: 0,
			attr: fuse_attr {
				ino: node,
				size,
				blocks: 0,
				atime: attr.atime.secs,
				atimensec: attr.atime.nanos,
				mtime: attr.mtime.secs,
				mtimensec: attr.mtime.nanos,
				ctime: attr.ctime.secs,
				ctimensec: attr.ctime.nanos,
				mode,
				nlink: 1,
				uid: attr.uid,
				gid: attr.gid,
				rdev: 0,
				blksize: 512,
				flags: 0,
			},
			dummy: 0,
		}
	}

	fn fuse_entry_out_sync(&self, node: u64) -> Result<fuse_entry_out> {
		let attr = self.provider.getattr_sync(node)?;
		let attr_out = Self::fuse_attr_out(node, attr);
		let entry_out = fuse_entry_out {
			nodeid: node,
			generation: 0,
			entry_valid: 1024,
			attr_valid: 0,
			entry_valid_nsec: 1024,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		};
		Ok(entry_out)
	}

	async fn fuse_entry_out(&self, node: u64) -> Result<fuse_entry_out> {
		let attr = self.provider_getattr(node).await?;
		let attr_out = Self::fuse_attr_out(node, attr);
		let entry_out = fuse_entry_out {
			nodeid: node,
			generation: 0,
			entry_valid: 1024,
			attr_valid: 0,
			entry_valid_nsec: 1024,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		};
		Ok(entry_out)
	}
}

const fn ior(typ: u32, nr: u32, size: u32) -> libc::c_ulong {
	const IOC_NRBITS: u32 = 8;
	const IOC_TYPEBITS: u32 = 8;
	const IOC_SIZEBITS: u32 = 14;
	const IOC_NRSHIFT: u32 = 0;
	const IOC_TYPESHIFT: u32 = IOC_NRSHIFT + IOC_NRBITS;
	const IOC_SIZESHIFT: u32 = IOC_TYPESHIFT + IOC_TYPEBITS;
	const IOC_DIRSHIFT: u32 = IOC_SIZESHIFT + IOC_SIZEBITS;
	const IOC_READ: u32 = 2;

	((IOC_READ << IOC_DIRSHIFT)
		| (typ << IOC_TYPESHIFT)
		| (nr << IOC_NRSHIFT)
		| (size << IOC_SIZESHIFT)) as libc::c_ulong
}

fn signal_eventfd(fd: RawFd) -> Result<()> {
	let value = 1u64.to_ne_bytes();
	let ret = unsafe { libc::write(fd, value.as_ptr().cast(), value.len()) };
	if ret == -1 {
		return Err(Error::last_os_error());
	}
	Ok(())
}

fn read_data<T>(request_data: &[u8]) -> Result<T>
where
	T: zerocopy::FromBytes,
{
	T::read_from_prefix(request_data)
		.map(|(data, _)| data)
		.map_err(|_| Error::other("failed to deserialize the request data"))
}

fn write_error(fd: RawFd, unique: u64, error: i32) -> std::io::Result<()> {
	let len = std::mem::size_of::<fuse_out_header>();
	let header = fuse_out_header {
		unique,
		len: len.to_u32().unwrap(),
		error: -error,
	};
	let header = header.as_bytes();
	let iov = [libc::iovec {
		iov_base: header.as_ptr() as *mut _,
		iov_len: header.len(),
	}];
	let ret = unsafe { libc::writev(fd.as_raw_fd(), iov.as_ptr(), 1) };
	if ret == -1 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

fn write_response(fd: RawFd, unique: u64, response: &Response) -> std::io::Result<()> {
	let data = match response {
		Response::Flush | Response::Release | Response::ReleaseDir => &[],
		Response::GetAttr(data) => data.as_bytes(),
		Response::Init(data) => data.as_bytes(),
		Response::Lookup(data) => data.as_bytes(),
		Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
		Response::Read(data)
		| Response::ReadDir(data)
		| Response::ReadDirPlus(data)
		| Response::GetXattr(data)
		| Response::ListXattr(data) => data.as_bytes(),
		Response::ReadLink(data) => data.as_bytes(),
		Response::Statfs(data) => data.as_bytes(),
		Response::Statx(data) => data.as_bytes(),
	};
	let len = std::mem::size_of::<fuse_out_header>() + data.len();
	let header = fuse_out_header {
		unique,
		len: len.to_u32().unwrap(),
		error: 0,
	};
	let header = header.as_bytes();
	let iov = [
		libc::iovec {
			iov_base: header.as_ptr() as *mut _,
			iov_len: header.len(),
		},
		libc::iovec {
			iov_base: data.as_ptr() as *mut _,
			iov_len: data.len(),
		},
	];
	let ret = unsafe { libc::writev(fd.as_raw_fd(), iov.as_ptr(), 2) };
	if ret == -1 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

impl<P> Clone for Server<P> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<P> Deref for Server<P> {
	type Target = Inner<P>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub async fn unmount(path: &Path) -> Result<()> {
	tokio::process::Command::new("fusermount3")
		.args(["-u"])
		.arg(path)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::null())
		.status()
		.await?;
	Ok(())
}

// There are a small number of "expected" error conditions, where the request correctly returns an error, but not due to a filesystem error. These are:
// - lookups that return ENOENT (None)
// - getxattrs that return ENODATA (None)
// - getxattr/listxattr that return ERANGE (blame the caller, they provided garbage data)
// - ENOSYS: only returned when the filesystem doesn't support a request.
fn is_expected_error(opcode: sys::fuse_opcode, error: Option<i32>) -> bool {
	(opcode == sys::fuse_opcode_FUSE_LOOKUP && error == Some(libc::ENOENT))
		| (opcode == sys::fuse_opcode_FUSE_GETXATTR && error == Some(libc::ENODATA))
		| (opcode == sys::fuse_opcode_FUSE_GETXATTR && error == Some(libc::ERANGE))
		| (opcode == sys::fuse_opcode_FUSE_LISTXATTR && error == Some(libc::ERANGE))
		| (error == Some(libc::ENOSYS))
}
