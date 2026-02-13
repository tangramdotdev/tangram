use {
	self::sys::{
		fuse_attr, fuse_attr_out, fuse_batch_forget_in, fuse_entry_out, fuse_flush_in,
		fuse_forget_in, fuse_getattr_in, fuse_getxattr_in, fuse_getxattr_out, fuse_in_header,
		fuse_init_in, fuse_init_out, fuse_open_in, fuse_open_out, fuse_out_header, fuse_read_in,
		fuse_release_in,
	},
	crate::{FileType, Provider, Request as ProviderRequest, Response as ProviderResponse, Result},
	bytes::Bytes,
	io_uring::{IoUring, opcode, types},
	num::ToPrimitive as _,
	rustix::{
		event::EventfdFlags,
		fd::BorrowedFd,
		io::{Errno, FdFlags, IoSlice, IoSliceMut},
		ioctl,
		net::{
			AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SocketFlags,
			SocketType,
		},
	},
	std::{
		collections::HashMap,
		ffi::CString,
		io::Error,
		mem::{MaybeUninit, size_of},
		ops::Deref,
		os::fd::{AsRawFd as _, OwnedFd, RawFd},
		os::unix::process::CommandExt as _,
		path::Path,
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
	},
	sys::{FUSE_KERNEL_MINOR_VERSION, FUSE_KERNEL_VERSION, fuse_interrupt_in},
	zerocopy::{FromBytes as _, IntoBytes as _},
};

pub mod sys;

pub struct Server<P>(Arc<Inner<P>>);

pub struct Inner<P> {
	provider: P,
	passthrough_backing_ids: Mutex<HashMap<u64, u32>>,
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
	BatchForget(sys::fuse_batch_forget_in, Vec<sys::fuse_forget_one>),
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
	Read(Bytes),
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
	slot: usize,
	unique: u64,
	opcode: sys::fuse_opcode,
	result: Result<Option<Response>>,
}

enum SyncRequestResult {
	Handled(Option<Response>),
	Defer,
}

struct SyncBatchScratch {
	results: Vec<Option<Result<SyncRequestResult>>>,
	provider_contexts: Vec<(usize, ProviderBatchResponseContext)>,
}

#[derive(Clone, Copy)]
enum ProviderBatchResponseContext {
	GetAttr { nodeid: u64 },
	GetXattr { request: fuse_getxattr_in },
	ListXattr { request: fuse_getxattr_in },
	Lookup,
	Open { fd: RawFd },
	OpenDir,
	Read,
	ReadDir { request: fuse_read_in },
	ReadDirPlus { request: fuse_read_in },
	ReadLink,
}

impl SyncBatchScratch {
	fn new() -> Self {
		Self {
			results: Vec::new(),
			provider_contexts: Vec::new(),
		}
	}

	fn reset(&mut self, request_count: usize) {
		self.results.clear();
		self.results.resize_with(request_count, || None);
		self.provider_contexts.clear();
	}
}

const REQUEST_BUFFER_SIZE: usize = 1024 * 1024 + 4096;
const IO_URING_ENTRIES: u32 = 256;
const WORKER_READ_DEPTH: usize = 16;
const WORKER_CQE_BATCH_SIZE: usize = 64;
const EVENTFD_USER_DATA: u64 = u64::MAX;
const SQPOLL_IDLE_MS: u32 = 2_000;
const EVENTFD_BUFFER_INDEX: u16 = 0;
const WORKER_FIXED_FUSE_FD: u32 = 0;
const WORKER_FIXED_EVENTFD_FD: u32 = 1;
const FUSE_DEV_IOC_MAGIC: u8 = 229;
const FUSE_DEV_IOC_CLONE: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::read::<u32>(FUSE_DEV_IOC_MAGIC, 0);
const FUSE_DEV_IOC_BACKING_OPEN: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::write::<sys::fuse_backing_map>(FUSE_DEV_IOC_MAGIC, 1);
const FUSE_DEV_IOC_BACKING_CLOSE: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::write::<u32>(FUSE_DEV_IOC_MAGIC, 2);
const URING_IOVEC_COUNT: u32 = 2;
const URING_CMD_BYTES: usize = 80;
const S_IFDIR: u32 = 0o040_000;
const S_IFREG: u32 = 0o100_000;
const S_IFLNK: u32 = 0o120_000;

#[repr(C)]
struct IoUringSqePrefix {
	opcode: u8,
	flags: u8,
	ioprio: u16,
	fd: i32,
	off: u64,
	addr: u64,
	len: u32,
	rw_flags: u32,
	user_data: u64,
	buf_index_or_group: u16,
}

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

struct UringSlot {
	header: Box<sys::fuse_uring_req_header>,
	payload: Vec<u8>,
	decode: Vec<u8>,
	iovecs: [libc::iovec; URING_IOVEC_COUNT as usize],
}

struct IoctlPointerInt<'a, const OPCODE: rustix::ioctl::Opcode, T> {
	value: &'a mut T,
}

impl<'a, const OPCODE: rustix::ioctl::Opcode, T> IoctlPointerInt<'a, OPCODE, T> {
	const fn new(value: &'a mut T) -> Self {
		Self { value }
	}
}

unsafe impl<const OPCODE: rustix::ioctl::Opcode, T> ioctl::Ioctl
	for IoctlPointerInt<'_, OPCODE, T>
{
	type Output = i32;

	const IS_MUTATING: bool = true;

	fn opcode(&self) -> ioctl::Opcode {
		OPCODE
	}

	fn as_ptr(&mut self) -> *mut core::ffi::c_void {
		std::ptr::from_mut(self.value).cast()
	}

	unsafe fn output_from_ptr(
		out: ioctl::IoctlOutput,
		_ptr: *mut core::ffi::c_void,
	) -> rustix::io::Result<Self::Output> {
		Ok(out)
	}
}

impl UringSlot {
	fn new() -> Self {
		let mut header = Box::new(unsafe { std::mem::zeroed::<sys::fuse_uring_req_header>() });
		let mut payload = vec![0u8; REQUEST_BUFFER_SIZE];
		let iovecs = [
			libc::iovec {
				iov_base: std::ptr::from_mut(&mut *header).cast(),
				iov_len: size_of::<sys::fuse_uring_req_header>(),
			},
			libc::iovec {
				iov_base: payload.as_mut_ptr().cast(),
				iov_len: payload.len(),
			},
		];
		Self {
			header,
			payload,
			decode: Vec::new(),
			iovecs,
		}
	}
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub async fn start(provider: P, path: &Path) -> Result<Self> {
		// Create the server.
		let server = Self(Arc::new(Inner {
			provider,
			passthrough_backing_ids: Mutex::new(HashMap::default()),
			task: Mutex::new(None),
		}));

		// Unmount.
		unmount(path).await.ok();

		// Mount.
		let fd = Self::mount(path)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to mount"))?;

		// Complete INIT before entering io_uring command mode.
		Self::init_handshake(fd.as_ref())
			.map_err(|error| Error::other(format!("failed to complete init handshake: {error}")))?;

		// Create a primary SQPOLL ring. Worker rings attach to this shared backend.
		let mut sqpoll_builder = IoUring::<io_uring::squeue::Entry128>::builder();
		sqpoll_builder
			.setup_sqpoll(SQPOLL_IDLE_MS)
			.setup_no_sqarray();
		let sqpoll_ring = sqpoll_builder
			.build(IO_URING_ENTRIES)
			.map_err(|error| Error::other(format!("failed to build sqpoll ring: {error}")))?;
		let sqpoll_wq_fd = sqpoll_ring.as_raw_fd();

		// Create worker threads.
		let worker_count = std::thread::available_parallelism()
			.map(std::num::NonZero::get)
			.unwrap_or(1);
		let runtime = tokio::runtime::Handle::current();
		let mut worker_handles = Vec::with_capacity(worker_count);
		for worker_id in 0..worker_count {
			let server = server.clone();
			let runtime = runtime.clone();
			let worker_fd = Self::clone_worker_fd(fd.as_ref()).map_err(|error| {
				Error::other(format!(
					"failed to clone worker fuse fd {worker_id}: {error}"
				))
			})?;
			let worker = std::thread::spawn(move || {
				if let Err(error) =
					server.worker_thread(worker_id, &worker_fd, &runtime, sqpoll_wq_fd)
				{
					if error.raw_os_error() == Some(libc::ENOTCONN) {
						tracing::debug!(%error, %worker_id, "worker thread exited during shutdown");
					} else {
						tracing::error!(%error, %worker_id, "worker thread failed");
					}
				}
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
			drop(sqpoll_ring);
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

	fn init_handshake(fd: &OwnedFd) -> Result<()> {
		let mut buffer = vec![0u8; REQUEST_BUFFER_SIZE];
		loop {
			let ret = match rustix::io::read(fd, &mut buffer) {
				Ok(ret) => ret,
				Err(Errno::INTR) => continue,
				Err(error) => return Err(error.into()),
			};
			if ret == 0 {
				return Err(Error::other("failed to read init request"));
			}
			let size = ret.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
			let RequestData::Init(init) = request.data else {
				return Err(Error::other("expected init request"));
			};
			let response = Self::init_response(init)?;
			write_response(
				fd.as_raw_fd(),
				request.header.unique,
				&Response::Init(response),
			)?;
			return Ok(());
		}
	}

	fn clone_worker_fd(fd: &OwnedFd) -> Result<OwnedFd> {
		let cloned_file = std::fs::File::options()
			.read(true)
			.write(true)
			.open("/dev/fuse")?;
		let cloned: OwnedFd = cloned_file.into();
		rustix::io::fcntl_setfd(&cloned, FdFlags::CLOEXEC).map_err(Error::from)?;
		let mut source_fd = fd
			.as_raw_fd()
			.to_u32()
			.ok_or_else(|| Error::other("invalid source fuse fd"))?;
		unsafe {
			ioctl::ioctl(
				&cloned,
				IoctlPointerInt::<FUSE_DEV_IOC_CLONE, _>::new(&mut source_fd),
			)
		}
		.map_err(Error::from)?;
		Ok(cloned)
	}

	fn worker_thread(
		&self,
		worker_id: usize,
		fd: &OwnedFd,
		runtime: &tokio::runtime::Handle,
		sqpoll_wq_fd: RawFd,
	) -> Result<()> {
		let qid = worker_id
			.to_u16()
			.ok_or_else(|| Error::other("worker id out of range"))?;
		let eventfd = rustix::event::eventfd(0, EventfdFlags::CLOEXEC)
			.map_err(|error| Error::other(format!("failed to create worker eventfd: {error}")))?;
		let eventfd = Arc::new(eventfd);
		let (sender, receiver) = crossbeam_channel::unbounded::<AsyncResponse>();

		let mut builder = IoUring::<io_uring::squeue::Entry128>::builder();
		builder.setup_attach_wq(sqpoll_wq_fd).setup_no_sqarray();
		let mut io_uring = builder
			.build(IO_URING_ENTRIES)
			.map_err(|error| Error::other(format!("failed to build worker ring: {error}")))?;
		io_uring
			.submitter()
			.register_files(&[fd.as_raw_fd(), eventfd.as_raw_fd()])
			.map_err(|error| Error::other(format!("failed to register worker files: {error}")))?;
		let mut slots = (0..WORKER_READ_DEPTH)
			.map(|_| UringSlot::new())
			.collect::<Vec<_>>();
		let mut pending_async = vec![None::<(u64, sys::fuse_opcode)>; WORKER_READ_DEPTH];
		let async_notification_pending = Arc::new(AtomicBool::new(false));
		let mut eventfd_inflight = false;
		let mut eventfd_buffer = [0u8; size_of::<u64>()];
		let mut batch_requests =
			Vec::<(usize, Request, u64, sys::fuse_opcode)>::with_capacity(WORKER_CQE_BATCH_SIZE);
		let mut batch_results =
			Vec::<Result<SyncRequestResult>>::with_capacity(WORKER_CQE_BATCH_SIZE);
		let mut commits =
			Vec::<(usize, u64, Result<Option<Response>>)>::with_capacity(WORKER_CQE_BATCH_SIZE);
		let mut deferred =
			Vec::<(usize, Request, u64, sys::fuse_opcode)>::with_capacity(WORKER_CQE_BATCH_SIZE);
		let mut sync_batch_scratch = SyncBatchScratch::new();
		let registered_buffers = [libc::iovec {
			iov_base: eventfd_buffer.as_mut_ptr().cast(),
			iov_len: eventfd_buffer.len(),
		}];
		unsafe {
			io_uring
				.submitter()
				.register_buffers(&registered_buffers)
				.map_err(|error| {
					Error::other(format!("failed to register worker buffers: {error}"))
				})?;
		}

		{
			let mut submission = io_uring.submission();
			for (slot, slot_data) in slots.iter().enumerate() {
				let entry = Self::build_fuse_uring_cmd_entry(
					types::Fixed(WORKER_FIXED_FUSE_FD),
					sys::fuse_uring_cmd_FUSE_IO_URING_CMD_REGISTER,
					0,
					qid,
					slot_data.iovecs.as_ptr(),
					slot.to_u64().unwrap(),
					0,
				);
				if unsafe { submission.push(&entry) }.is_err() {
					return Err(Error::other("failed to submit uring register command"));
				}
			}
		}

		loop {
			{
				let mut submission = io_uring.submission();
				if !eventfd_inflight {
					let entry: io_uring::squeue::Entry128 = opcode::ReadFixed::new(
						types::Fixed(WORKER_FIXED_EVENTFD_FD),
						eventfd_buffer.as_mut_ptr(),
						eventfd_buffer.len().to_u32().unwrap(),
						EVENTFD_BUFFER_INDEX,
					)
					.offset(u64::MAX)
					.build()
					.user_data(EVENTFD_USER_DATA)
					.into();
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
						return Ok(());
					},
					_ => {
						return Err(Error::other(format!(
							"failed to submit worker ring and wait: {error}",
						)));
					},
				}
			}

			let mut saw_eventfd = false;
			batch_requests.clear();
			{
				let mut completion = io_uring.completion();
				for completion in completion.by_ref().take(WORKER_CQE_BATCH_SIZE) {
					if completion.user_data() == EVENTFD_USER_DATA {
						eventfd_inflight = false;
						saw_eventfd = true;
						async_notification_pending.store(false, Ordering::Release);
						continue;
					}

					let slot = completion.user_data().to_usize().unwrap();

					if completion.result() < 0 {
						let error = -completion.result();
						if error == libc::EINTR || error == libc::EAGAIN || error == libc::ENOENT {
							continue;
						}
						if error == libc::ENODEV {
							return Ok(());
						}
						return Err(Error::from_raw_os_error(error));
					}

					if pending_async.get(slot).and_then(Option::as_ref).is_some() {
						return Err(Error::other(
							"received a completion for an async pending slot",
						));
					}

					let request = Self::deserialize_uring_request(slots.get_mut(slot).unwrap())?;
					let unique = request.header.unique;
					let opcode = request.header.opcode;
					batch_requests.push((slot, request, unique, opcode));
				}
			}
			self.handle_request_sync_batch(
				fd.as_raw_fd(),
				&batch_requests,
				&mut sync_batch_scratch,
				&mut batch_results,
			)?;
			commits.clear();
			deferred.clear();
			for ((slot, request, unique, opcode), result) in
				std::iter::zip(batch_requests.drain(..), batch_results.drain(..))
			{
				match result {
					Ok(SyncRequestResult::Handled(response)) => {
						commits.push((slot, unique, Ok(response)));
					},
					Ok(SyncRequestResult::Defer) => {
						deferred.push((slot, request, unique, opcode));
					},
					Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
						deferred.push((slot, request, unique, opcode));
					},
					Err(error) => {
						let error_code = error.raw_os_error().unwrap_or(libc::ENOSYS);
						if !is_expected_error(opcode, Some(error_code)) {
							tracing::error!(?error, ?opcode, %worker_id, "unexpected error");
						}
						commits.push((slot, unique, Err(error)));
					},
				}
			}
			for (slot, request, unique, opcode) in deferred.drain(..) {
				if pending_async.get(slot).and_then(Option::as_ref).is_some() {
					return Err(Error::other("slot already has a pending async request"));
				}
				pending_async[slot] = Some((unique, opcode));
				self.spawn_async_request(
					slot,
					request,
					runtime,
					sender.clone(),
					eventfd.clone(),
					async_notification_pending.clone(),
				);
			}
			for (slot, unique, result) in commits.drain(..) {
				Self::submit_commit_and_fetch(
					&mut io_uring,
					types::Fixed(WORKER_FIXED_FUSE_FD),
					qid,
					slot,
					slots.get_mut(slot).unwrap(),
					unique,
					result,
				)?;
			}

			if saw_eventfd || !receiver.is_empty() {
				Self::drain_async_responses(
					worker_id,
					types::Fixed(WORKER_FIXED_FUSE_FD),
					qid,
					&mut io_uring,
					&mut slots,
					&mut pending_async,
					&receiver,
				)?;
			}
		}
	}

	fn build_fuse_uring_cmd_entry(
		fd: types::Fixed,
		cmd_op: u32,
		commit_id: u64,
		qid: u16,
		iovecs: *const libc::iovec,
		user_data: u64,
		command_flags: u64,
	) -> io_uring::squeue::Entry128 {
		let request = sys::fuse_uring_cmd_req {
			flags: command_flags,
			commit_id,
			qid,
			padding: [0; 6],
		};
		let mut cmd = [0u8; URING_CMD_BYTES];
		cmd[..size_of::<sys::fuse_uring_cmd_req>()].copy_from_slice(request.as_bytes());
		let mut entry = opcode::UringCmd80::new(fd, cmd_op)
			.cmd(cmd)
			.build()
			.user_data(user_data);
		Self::configure_fuse_uring_entry_iovec(&mut entry, iovecs);
		entry
	}

	fn configure_fuse_uring_entry_iovec(
		entry: &mut io_uring::squeue::Entry128,
		iovecs: *const libc::iovec,
	) {
		let sqe = std::ptr::from_mut(entry).cast::<IoUringSqePrefix>();
		unsafe {
			(*sqe).addr = iovecs as u64;
			(*sqe).len = URING_IOVEC_COUNT;
		}
	}

	fn submit_commit_and_fetch(
		io_uring: &mut IoUring<io_uring::squeue::Entry128>,
		fixed_fuse_fd: types::Fixed,
		qid: u16,
		slot: usize,
		slot_data: &mut UringSlot,
		unique: u64,
		result: Result<Option<Response>>,
	) -> Result<()> {
		Self::prepare_uring_response(slot_data, unique, result)?;
		let entry = Self::build_fuse_uring_cmd_entry(
			fixed_fuse_fd,
			sys::fuse_uring_cmd_FUSE_IO_URING_CMD_COMMIT_AND_FETCH,
			unique,
			qid,
			slot_data.iovecs.as_ptr(),
			slot.to_u64().unwrap(),
			0,
		);
		let mut submission = io_uring.submission();
		if unsafe { submission.push(&entry) }.is_err() {
			return Err(Error::other(
				"failed to submit uring commit and fetch command",
			));
		}
		Ok(())
	}

	fn prepare_uring_response(
		slot_data: &mut UringSlot,
		unique: u64,
		result: Result<Option<Response>>,
	) -> Result<()> {
		let (error, payload_len) = match result {
			Ok(Some(response)) => {
				let payload = Self::response_bytes(&response);
				if payload.len() > slot_data.payload.len() {
					return Err(Error::other("response payload too large"));
				}
				slot_data.payload[..payload.len()].copy_from_slice(payload);
				(0, payload.len())
			},
			Ok(None) => (0, 0),
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), 0),
		};
		let len = size_of::<fuse_out_header>() + payload_len;
		let out = fuse_out_header {
			unique,
			len: len.to_u32().unwrap(),
			error: -error,
		};
		let in_out = unsafe {
			std::slice::from_raw_parts_mut(
				slot_data.header.in_out.as_mut_ptr().cast::<u8>(),
				slot_data.header.in_out.len(),
			)
		};
		in_out.fill(0);
		in_out[..size_of::<fuse_out_header>()].copy_from_slice(out.as_bytes());
		slot_data.header.ring_ent_in_out = sys::fuse_uring_ent_in_out {
			flags: 0,
			commit_id: unique,
			payload_sz: payload_len.to_u32().unwrap(),
			padding: 0,
			reserved: 0,
		};
		Ok(())
	}

	fn response_bytes(response: &Response) -> &[u8] {
		match response {
			Response::Flush | Response::Release | Response::ReleaseDir => &[],
			Response::GetAttr(data) => data.as_bytes(),
			Response::Init(data) => data.as_bytes(),
			Response::Lookup(data) => data.as_bytes(),
			Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
			Response::Read(data) => data.as_ref(),
			Response::ReadDir(data)
			| Response::ReadDirPlus(data)
			| Response::GetXattr(data)
			| Response::ListXattr(data) => data.as_bytes(),
			Response::ReadLink(data) => data.as_bytes(),
			Response::Statfs(data) => data.as_bytes(),
			Response::Statx(data) => data.as_bytes(),
		}
	}

	fn deserialize_uring_request(slot: &mut UringSlot) -> Result<Request> {
		let in_out = unsafe {
			std::slice::from_raw_parts(
				slot.header.in_out.as_ptr().cast::<u8>(),
				slot.header.in_out.len(),
			)
		};
		let (header, _) = sys::fuse_in_header::read_from_prefix(in_out)
			.map_err(|_| Error::other("failed to deserialize the request header"))?;
		let header_len = size_of::<sys::fuse_in_header>();
		let request_len = header
			.len
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if request_len < header_len {
			return Err(Error::other("failed to deserialize request data"));
		}
		let total_extlen = usize::from(header.total_extlen) * 8;
		let Some(request_data_len) = request_len.checked_sub(header_len + total_extlen) else {
			return Err(Error::other("failed to deserialize request data"));
		};
		let payload_size = slot
			.header
			.ring_ent_in_out
			.payload_sz
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if payload_size > request_data_len || payload_size > slot.payload.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		let op_data_len = request_data_len - payload_size;
		let op_in = unsafe {
			std::slice::from_raw_parts(
				slot.header.op_in.as_ptr().cast::<u8>(),
				slot.header.op_in.len(),
			)
		};
		if op_data_len > op_in.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		slot.decode.clear();
		slot.decode.extend_from_slice(&op_in[..op_data_len]);
		slot.decode
			.extend_from_slice(&slot.payload.as_slice()[..payload_size]);
		let data = Self::parse_request_data(&header, &slot.decode)?;
		let request = Request { header, data };
		Ok(request)
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
		let data = Self::parse_request_data(&header, data)?;
		let request = Request { header, data };
		Ok(request)
	}

	fn parse_request_data(header: &fuse_in_header, data: &[u8]) -> Result<RequestData> {
		let data = match header.opcode {
			sys::fuse_opcode_FUSE_BATCH_FORGET => {
				if data.len() < size_of::<sys::fuse_batch_forget_in>() {
					return Err(Error::other("failed to deserialize request data"));
				}
				let (batch, entries) = data.split_at(size_of::<sys::fuse_batch_forget_in>());
				let batch: sys::fuse_batch_forget_in = read_data(batch)?;
				let count = batch
					.count
					.to_usize()
					.ok_or_else(|| Error::other("failed to deserialize request data"))?;
				let one_len = size_of::<sys::fuse_forget_one>();
				let len = count
					.checked_mul(one_len)
					.ok_or_else(|| Error::other("failed to deserialize request data"))?;
				if entries.len() < len {
					return Err(Error::other("failed to deserialize request data"));
				}
				let entries = entries[..len]
					.chunks_exact(one_len)
					.map(read_data::<sys::fuse_forget_one>)
					.collect::<Result<Vec<_>>>()?;
				RequestData::BatchForget(batch, entries)
			},
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
		Ok(data)
	}

	fn spawn_async_request(
		&self,
		slot: usize,
		request: Request,
		runtime: &tokio::runtime::Handle,
		sender: crossbeam_channel::Sender<AsyncResponse>,
		eventfd: Arc<OwnedFd>,
		async_notification_pending: Arc<AtomicBool>,
	) {
		let server = self.clone();
		runtime.spawn(async move {
			let opcode = request.header.opcode;
			let unique = request.header.unique;
			let result = server.handle_request(request).await.inspect_err(|error| {
				if !is_expected_error(opcode, error.raw_os_error()) {
					tracing::error!(?error, ?opcode, "unexpected error");
				}
			});
			let response = AsyncResponse {
				slot,
				unique,
				opcode,
				result,
			};
			if sender.send(response).is_ok()
				&& !async_notification_pending.swap(true, Ordering::AcqRel)
				&& let Err(error) = signal_eventfd(eventfd.as_raw_fd())
			{
				async_notification_pending.store(false, Ordering::Release);
				tracing::error!(?error, "failed to signal eventfd");
			}
		});
	}

	fn drain_async_responses(
		worker_id: usize,
		fixed_fuse_fd: types::Fixed,
		qid: u16,
		io_uring: &mut IoUring<io_uring::squeue::Entry128>,
		slots: &mut [UringSlot],
		pending_async: &mut [Option<(u64, sys::fuse_opcode)>],
		receiver: &crossbeam_channel::Receiver<AsyncResponse>,
	) -> Result<()> {
		while let Ok(response) = receiver.try_recv() {
			let Some(slot_data) = slots.get_mut(response.slot) else {
				tracing::error!(slot = response.slot, "invalid async slot");
				continue;
			};
			let Some((unique, opcode)) = pending_async[response.slot].take() else {
				tracing::error!(slot = response.slot, "received unexpected async response");
				continue;
			};
			if unique != response.unique || opcode != response.opcode {
				tracing::error!(
					slot = response.slot,
					expected_unique = unique,
					expected_opcode = opcode,
					received_unique = response.unique,
					received_opcode = response.opcode,
					"received an async response for a different request",
				);
				continue;
			}
			if let Err(error) = &response.result {
				let code = error.raw_os_error().unwrap_or(libc::ENOSYS);
				if !is_expected_error(response.opcode, Some(code)) {
					tracing::error!(?error, opcode = response.opcode, %worker_id, "unexpected error");
				}
			}
			Self::submit_commit_and_fetch(
				io_uring,
				fixed_fuse_fd,
				qid,
				response.slot,
				slot_data,
				response.unique,
				response.result,
			)?;
		}
		Ok(())
	}

	fn handle_request_sync_batch(
		&self,
		fd: RawFd,
		requests: &[(usize, Request, u64, sys::fuse_opcode)],
		scratch: &mut SyncBatchScratch,
		batch_results: &mut Vec<Result<SyncRequestResult>>,
	) -> Result<()> {
		scratch.reset(requests.len());
		let mut provider_requests = Vec::<ProviderRequest>::with_capacity(requests.len());

		for (index, (_, request, _, _)) in requests.iter().enumerate() {
			match &request.data {
				RequestData::BatchForget(data, entries) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(
						self.handle_batch_forget_request_sync(request.header, *data, entries),
					)));
				},
				RequestData::Destroy => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(None)));
				},
				RequestData::Flush(data) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(Some(
						Self::handle_flush_request_sync(request.header, *data),
					))));
				},
				RequestData::Forget(data) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(
						self.handle_forget_request_sync(request.header, *data),
					)));
				},
				RequestData::GetAttr(_) => {
					provider_requests.push(ProviderRequest::GetAttr {
						id: request.header.nodeid,
					});
					scratch.provider_contexts.push((
						index,
						ProviderBatchResponseContext::GetAttr {
							nodeid: request.header.nodeid,
						},
					));
				},
				RequestData::GetXattr(data, name) => {
					let name = name
						.to_str()
						.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?
						.to_owned();
					provider_requests.push(ProviderRequest::GetXattr {
						id: request.header.nodeid,
						name,
					});
					scratch.provider_contexts.push((
						index,
						ProviderBatchResponseContext::GetXattr { request: *data },
					));
				},
				RequestData::ListXattr(data) => {
					provider_requests.push(ProviderRequest::ListXattrs {
						id: request.header.nodeid,
					});
					scratch.provider_contexts.push((
						index,
						ProviderBatchResponseContext::ListXattr { request: *data },
					));
				},
				RequestData::Lookup(name) => {
					let name = name
						.to_str()
						.map_err(|_| Error::from_raw_os_error(libc::ENOENT))?
						.to_owned();
					provider_requests.push(ProviderRequest::Lookup {
						id: request.header.nodeid,
						name,
					});
					scratch
						.provider_contexts
						.push((index, ProviderBatchResponseContext::Lookup));
				},
				RequestData::Open(_) => {
					provider_requests.push(ProviderRequest::Open {
						id: request.header.nodeid,
					});
					scratch
						.provider_contexts
						.push((index, ProviderBatchResponseContext::Open { fd }));
				},
				RequestData::OpenDir(_) => {
					provider_requests.push(ProviderRequest::OpenDir {
						id: request.header.nodeid,
					});
					scratch
						.provider_contexts
						.push((index, ProviderBatchResponseContext::OpenDir));
				},
				RequestData::Read(data) => {
					provider_requests.push(ProviderRequest::Read {
						handle: data.fh,
						position: data.offset,
						length: data.size.to_u64().unwrap(),
					});
					scratch
						.provider_contexts
						.push((index, ProviderBatchResponseContext::Read));
				},
				RequestData::ReadDir(data) => {
					provider_requests.push(ProviderRequest::ReadDir {
						handle: request.header.nodeid,
					});
					scratch.provider_contexts.push((
						index,
						ProviderBatchResponseContext::ReadDir { request: *data },
					));
				},
				RequestData::ReadDirPlus(data) => {
					provider_requests.push(ProviderRequest::ReadDirPlus {
						handle: request.header.nodeid,
					});
					scratch.provider_contexts.push((
						index,
						ProviderBatchResponseContext::ReadDirPlus { request: *data },
					));
				},
				RequestData::ReadLink => {
					provider_requests.push(ProviderRequest::ReadLink {
						id: request.header.nodeid,
					});
					scratch
						.provider_contexts
						.push((index, ProviderBatchResponseContext::ReadLink));
				},
				RequestData::Release(data) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(Some(
						self.handle_release_request_sync(fd, request.header, *data),
					))));
				},
				RequestData::ReleaseDir(data) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(Some(
						self.handle_release_dir_request_sync(fd, request.header, *data),
					))));
				},
				RequestData::Statfs => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(Some(
						Self::handle_statfs_request_sync(request.header),
					))));
				},
				RequestData::Statx(data) => {
					scratch.results[index] = Some(Self::result_to_sync_result(
						self.handle_statx_request_sync(request.header, *data),
					));
				},
				RequestData::Init(data) => {
					scratch.results[index] = Some(Self::result_to_sync_result(
						Self::handle_init_request_sync(request.header, *data),
					));
				},
				RequestData::Interrupt(data) => {
					scratch.results[index] = Some(Ok(SyncRequestResult::Handled(
						Self::handle_interrupt_request_sync(request.header, *data),
					)));
				},
				RequestData::Unsupported(opcode) => {
					scratch.results[index] = Some(Self::result_to_sync_result(
						Self::handle_unsupported_request_sync(request.header, *opcode),
					));
				},
			}
		}

		if !provider_requests.is_empty() {
			let provider_results = self.provider.handle_batch_sync(provider_requests);
			if provider_results.len() != scratch.provider_contexts.len() {
				return Err(Error::other("mismatched provider batch response length"));
			}
			for ((index, context), provider_result) in
				std::iter::zip(scratch.provider_contexts.drain(..), provider_results)
			{
				let result = match provider_result {
					Ok(response) => self.map_provider_batch_response(context, response),
					Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
						Ok(SyncRequestResult::Defer)
					},
					Err(error) => Err(error),
				};
				scratch.results[index] = Some(result);
			}
		}

		batch_results.clear();
		batch_results.reserve(requests.len());
		for result in &mut scratch.results {
			let result = result
				.take()
				.ok_or_else(|| Error::other("missing sync batch result"))?;
			batch_results.push(result);
		}
		Ok(())
	}

	fn map_provider_batch_response(
		&self,
		context: ProviderBatchResponseContext,
		response: ProviderResponse,
	) -> Result<SyncRequestResult> {
		match context {
			ProviderBatchResponseContext::GetAttr { nodeid } => {
				let ProviderResponse::GetAttr { attrs } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = Self::fuse_attr_out(nodeid, attrs);
				Ok(SyncRequestResult::Handled(Some(Response::GetAttr(out))))
			},
			ProviderBatchResponseContext::GetXattr { request } => {
				let ProviderResponse::GetXattr { value } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let attr = value
					.map(|value| value.to_vec())
					.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;
				if request.size == 0 {
					let response = fuse_getxattr_out {
						size: attr.len().to_u32().unwrap(),
						padding: 0,
					};
					Ok(SyncRequestResult::Handled(Some(Response::GetXattr(
						response.as_bytes().to_vec(),
					))))
				} else if request.size.to_usize().unwrap() < attr.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(SyncRequestResult::Handled(Some(Response::GetXattr(attr))))
				}
			},
			ProviderBatchResponseContext::ListXattr { request } => {
				let ProviderResponse::ListXattrs { names } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let attrs = names
					.into_iter()
					.flat_map(|name| {
						let mut bytes = name.into_bytes();
						bytes.push(0);
						bytes.into_iter()
					})
					.collect::<Vec<_>>();
				if request.size == 0 {
					let response = fuse_getxattr_out {
						size: attrs.len().to_u32().unwrap(),
						padding: 0,
					};
					Ok(SyncRequestResult::Handled(Some(Response::ListXattr(
						response.as_bytes().to_vec(),
					))))
				} else if request.size.to_usize().unwrap() < attrs.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(SyncRequestResult::Handled(Some(Response::ListXattr(attrs))))
				}
			},
			ProviderBatchResponseContext::Lookup => {
				let ProviderResponse::Lookup { id } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let node = id.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
				let out = self.fuse_entry_out_sync(node)?;
				Ok(SyncRequestResult::Handled(Some(Response::Lookup(out))))
			},
			ProviderBatchResponseContext::Open { fd } => {
				let ProviderResponse::Open { handle, backing_fd } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let mut open_flags = sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE;
				let mut backing_id = -1;
				if let Some(backing_fd) = backing_fd {
					match self.register_passthrough_backing(fd, handle, &backing_fd) {
						Ok(id) => {
							open_flags |= sys::FOPEN_PASSTHROUGH;
							backing_id = id;
						},
						Err(error) => {
							tracing::trace!(
								?error,
								fh = handle,
								"failed to register passthrough backing"
							);
						},
					}
				}
				let out = fuse_open_out {
					fh: handle,
					open_flags,
					backing_id,
				};
				Ok(SyncRequestResult::Handled(Some(Response::Open(out))))
			},
			ProviderBatchResponseContext::OpenDir => {
				let ProviderResponse::OpenDir { handle } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = fuse_open_out {
					fh: handle,
					open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
					backing_id: -1,
				};
				Ok(SyncRequestResult::Handled(Some(Response::OpenDir(out))))
			},
			ProviderBatchResponseContext::Read => {
				let ProviderResponse::Read { bytes } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				Ok(SyncRequestResult::Handled(Some(Response::Read(bytes))))
			},
			ProviderBatchResponseContext::ReadDir { request } => {
				let ProviderResponse::ReadDir { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = Self::build_read_dir_response_sync(entries, request);
				Ok(SyncRequestResult::Handled(Some(response)))
			},
			ProviderBatchResponseContext::ReadDirPlus { request } => {
				let ProviderResponse::ReadDirPlus { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = self.build_read_dir_plus_response_sync(entries, request);
				Ok(SyncRequestResult::Handled(Some(response)))
			},
			ProviderBatchResponseContext::ReadLink => {
				let ProviderResponse::ReadLink { target } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let target = CString::new(target.to_vec())
					.map_err(|_| Error::from_raw_os_error(libc::EIO))?;
				Ok(SyncRequestResult::Handled(Some(Response::ReadLink(target))))
			},
		}
	}

	fn result_to_sync_result(result: Result<Option<Response>>) -> Result<SyncRequestResult> {
		match result {
			Ok(response) => Ok(SyncRequestResult::Handled(response)),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				Ok(SyncRequestResult::Defer)
			},
			Err(error) => Err(error),
		}
	}

	fn handle_batch_forget_request_sync(
		&self,
		_header: fuse_in_header,
		request: fuse_batch_forget_in,
		entries: &[sys::fuse_forget_one],
	) -> Option<Response> {
		let count = request.count.to_usize().unwrap_or(0);
		for entry in entries.iter().take(count) {
			self.provider.forget_sync(entry.nodeid, entry.nlookup);
		}
		None
	}

	fn handle_flush_request_sync(_header: fuse_in_header, _request: fuse_flush_in) -> Response {
		Response::Flush
	}

	fn handle_forget_request_sync(
		&self,
		header: fuse_in_header,
		request: fuse_forget_in,
	) -> Option<Response> {
		self.provider.forget_sync(header.nodeid, request.nlookup);
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

	fn handle_init_request_sync(
		_header: fuse_in_header,
		request: fuse_init_in,
	) -> Result<Option<Response>> {
		let response = Self::init_response(request)?;
		Ok(Some(Response::Init(response)))
	}

	fn build_read_dir_response_sync(
		entries: Vec<(String, u64, crate::DirEntryType)>,
		request: fuse_read_in,
	) -> Response {
		let entries = entries
			.into_iter()
			.enumerate()
			.skip(request.offset.to_usize().unwrap());
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node, type_)) in entries {
			let type_ = Self::fuse_dirent_type(type_);
			let name = name.into_bytes();
			let struct_size = std::mem::size_of::<FuseDirentHeader>();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let dirent = FuseDirentHeader {
				ino: node,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};
			response.extend_from_slice(dirent.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}
		Response::ReadDir(response)
	}

	fn build_read_dir_plus_response_sync(
		&self,
		entries: Vec<(String, u64, crate::Attrs)>,
		request: fuse_read_in,
	) -> Response {
		let entries = entries
			.into_iter()
			.enumerate()
			.skip(request.offset.to_usize().unwrap());
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node, attr)) in entries {
			let name = name.into_bytes();
			let struct_size = std::mem::size_of::<FuseDirentPlusHeader>();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let type_ = match attr.typ {
				FileType::Directory => S_IFDIR,
				FileType::File { .. } => S_IFREG,
				FileType::Symlink => S_IFLNK,
			};

			let dirent = FuseDirentHeader {
				ino: node,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			self.provider.remember_sync(node);
			let entry = FuseDirentPlusHeader {
				entry_out: Self::fuse_entry_out_from_attrs(node, attr),
				dirent,
			};
			response.extend_from_slice(entry.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		Response::ReadDirPlus(response)
	}

	fn handle_release_request_sync(
		&self,
		fd: RawFd,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Response {
		self.close_passthrough_backing(fd, request.fh);
		self.provider.close_sync(request.fh);
		Response::Release
	}

	fn handle_release_dir_request_sync(
		&self,
		fd: RawFd,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Response {
		self.close_passthrough_backing(fd, request.fh);
		self.provider.close_sync(request.fh);
		Response::ReleaseDir
	}

	fn register_passthrough_backing(
		&self,
		fd: RawFd,
		fh: u64,
		backing_fd: &OwnedFd,
	) -> Result<i32> {
		let mut map = sys::fuse_backing_map {
			fd: backing_fd.as_raw_fd(),
			flags: 0,
			padding: 0,
		};
		let fd = unsafe { BorrowedFd::borrow_raw(fd) };
		let backing_id = unsafe {
			ioctl::ioctl(
				fd,
				IoctlPointerInt::<FUSE_DEV_IOC_BACKING_OPEN, _>::new(&mut map),
			)
		}
		.map_err(Error::from)?;
		let backing_id_u32 = backing_id
			.to_u32()
			.ok_or_else(|| Error::other("invalid backing id"))?;
		self.passthrough_backing_ids
			.lock()
			.unwrap()
			.insert(fh, backing_id_u32);
		Ok(backing_id)
	}

	fn close_passthrough_backing(&self, fd: RawFd, fh: u64) {
		let backing_id = self.passthrough_backing_ids.lock().unwrap().remove(&fh);
		let Some(backing_id) = backing_id else {
			return;
		};
		let mut backing_id = backing_id;
		let fd = unsafe { BorrowedFd::borrow_raw(fd) };
		let ret = unsafe {
			ioctl::ioctl(
				fd,
				IoctlPointerInt::<FUSE_DEV_IOC_BACKING_CLOSE, _>::new(&mut backing_id),
			)
		};
		if let Err(error) = ret {
			let error: Error = error.into();
			tracing::error!(?error, %fh, %backing_id, "failed to close passthrough backing");
		}
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
			RequestData::BatchForget(data, entries) => {
				self.handle_batch_forget_request(request.header, data, entries)
					.await
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
		const REQUIRED_FLAGS: u32 = sys::FUSE_INIT_EXT | sys::FUSE_MAX_PAGES;
		const REQUIRED_FLAGS2: u32 =
			((sys::FUSE_PASSTHROUGH | sys::FUSE_OVER_IO_URING) >> 32) as u32;
		const NEGOTIATED_FLAGS: u32 = sys::FUSE_ASYNC_READ
			| sys::FUSE_DO_READDIRPLUS
			| sys::FUSE_PARALLEL_DIROPS
			| sys::FUSE_CACHE_SYMLINKS
			| sys::FUSE_NO_OPENDIR_SUPPORT
			| sys::FUSE_SPLICE_MOVE
			| sys::FUSE_SPLICE_READ
			| sys::FUSE_MAX_PAGES
			| sys::FUSE_MAP_ALIGNMENT
			| sys::FUSE_INIT_EXT;
		const NEGOTIATED_FLAGS2: u32 =
			((sys::FUSE_PASSTHROUGH | sys::FUSE_OVER_IO_URING) >> 32) as u32;
		const MAX_PAGES: u16 = 256;
		const MAP_ALIGNMENT: u16 = 12;

		if request.major != FUSE_KERNEL_VERSION {
			return Err(Error::other("unsupported fuse major version"));
		}
		let missing_flags = REQUIRED_FLAGS & !request.flags;
		if missing_flags != 0 {
			return Err(Error::other(format!(
				"missing required fuse init flags, missing = {missing_flags:#x}",
			)));
		}
		let missing_flags2 = REQUIRED_FLAGS2 & !request.flags2;
		if missing_flags2 != 0 {
			return Err(Error::other(format!(
				"missing required fuse init flags2, missing = {missing_flags2:#x}",
			)));
		}
		let minor = request.minor.min(FUSE_KERNEL_MINOR_VERSION);
		let flags = NEGOTIATED_FLAGS & request.flags;
		let flags2 = NEGOTIATED_FLAGS2 & request.flags2;

		let response = fuse_init_out {
			major: FUSE_KERNEL_VERSION,
			minor,
			max_readahead: 1024 * 1024,
			flags,
			max_background: 0,
			congestion_threshold: 0,
			max_write: 1024 * 1024,
			time_gran: 0,
			max_pages: MAX_PAGES,
			map_alignment: MAP_ALIGNMENT,
			flags2,
			max_stack_depth: 0,
			request_timeout: 0,
			unused: [0; 11],
		};

		Ok(response)
	}

	async fn handle_batch_forget_request(
		&self,
		_header: fuse_in_header,
		request: fuse_batch_forget_in,
		entries: Vec<sys::fuse_forget_one>,
	) -> Result<Option<Response>> {
		let count = request.count.to_usize().unwrap_or(0);
		for entry in entries.into_iter().take(count) {
			self.provider.forget_sync(entry.nodeid, entry.nlookup);
		}
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
		header: fuse_in_header,
		request: fuse_forget_in,
	) -> Result<Option<Response>> {
		self.provider.forget_sync(header.nodeid, request.nlookup);
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
		header: fuse_in_header,
		request: fuse_read_in,
		plus: bool,
	) -> Result<Option<Response>> {
		let entries = self.provider_readdir(header.nodeid).await?;

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
		for (offset, (name, node, typ)) in entries {
			let type_ = if plus {
				let attr = self.provider_getattr(node).await?;
				match attr.typ {
					FileType::Directory => S_IFDIR,
					FileType::File { .. } => S_IFREG,
					FileType::Symlink => S_IFLNK,
				}
			} else {
				Self::fuse_dirent_type(typ)
			};
			let name = name.into_bytes();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

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
		self.provider.close(request.fh).await;
		Ok(Some(Response::Release))
	}

	async fn handle_release_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
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
		let (fuse_commfd, recvfd) = rustix::net::socketpair(
			AddressFamily::UNIX,
			SocketType::STREAM,
			SocketFlags::CLOEXEC,
			None,
		)
		.map_err(Error::from)?;

		let commfd_raw = fuse_commfd.as_raw_fd();
		let uid = rustix::process::getuid().as_raw();
		let gid = rustix::process::getgid().as_raw();
		let options = format!("rootmode=40755,user_id={uid},group_id={gid},default_permissions");
		// Safety: The pre_exec closure only calls async-signal-safe operations.
		let mut child = unsafe {
			std::process::Command::new("fusermount3")
				.args(["-o", &options, "--"])
				.arg(path)
				.env("_FUSE_COMMFD", commfd_raw.to_string())
				.stdin(std::process::Stdio::null())
				.stdout(std::process::Stdio::null())
				.stderr(std::process::Stdio::null())
				.pre_exec(move || {
					// Clear CLOEXEC on the comm fd so fusermount3 inherits it.
					let fd = BorrowedFd::borrow_raw(commfd_raw);
					rustix::io::fcntl_setfd(fd, FdFlags::empty()).map_err(Error::from)?;
					Ok(())
				})
				.spawn()?
		};
		drop(fuse_commfd);

		let mut read_buffer = [0u8; 8];
		let mut iovecs = [IoSliceMut::new(&mut read_buffer)];
		let mut cmsg_space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(1))];
		let mut cmsg_buffer = RecvAncillaryBuffer::new(&mut cmsg_space);
		let recv = rustix::net::recvmsg(&recvfd, &mut iovecs, &mut cmsg_buffer, RecvFlags::empty())
			.map_err(Error::from)?;
		if recv.bytes == 0 {
			return Err(Error::other("failed to read the control message"));
		}

		let mut fd = None;
		for message in cmsg_buffer.drain() {
			if let RecvAncillaryMessage::ScmRights(mut fds) = message {
				fd = fds.next();
				if fd.is_some() {
					break;
				}
			}
		}
		let fd = fd.ok_or_else(|| Error::other("missing control message"))?;
		rustix::io::fcntl_setfd(&fd, FdFlags::CLOEXEC).map_err(Error::from)?;

		child.wait()?;

		Ok(Arc::new(fd))
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

	async fn provider_read(&self, handle: u64, offset: u64, size: u64) -> Result<Bytes> {
		match self.provider.read_sync(handle, offset, size) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.read(handle, offset, size).await
			},
			Err(error) => Err(error),
		}
	}

	async fn provider_readdir(&self, node: u64) -> Result<Vec<(String, u64, crate::DirEntryType)>> {
		match self.provider.readdir_sync(node) {
			Ok(value) => Ok(value),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
				self.provider.readdir(node).await
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
			FileType::Directory => (0, S_IFDIR | 0o555),
			FileType::File { executable, size } => (
				size,
				S_IFREG | 0o444 | (if executable { 0o111 } else { 0o000 }),
			),
			FileType::Symlink => (0, S_IFLNK | 0o444),
		};
		let mode = mode.to_u32().unwrap();
		fuse_attr_out {
			attr_valid: u64::MAX,
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
		self.provider.remember_sync(node);
		Ok(Self::fuse_entry_out_from_attrs(node, attr))
	}

	async fn fuse_entry_out(&self, node: u64) -> Result<fuse_entry_out> {
		let attr = self.provider_getattr(node).await?;
		self.provider.remember_sync(node);
		Ok(Self::fuse_entry_out_from_attrs(node, attr))
	}

	fn fuse_entry_out_from_attrs(node: u64, attr: crate::Attrs) -> fuse_entry_out {
		let attr_out = Self::fuse_attr_out(node, attr);
		fuse_entry_out {
			nodeid: node,
			generation: 0,
			entry_valid: u64::MAX,
			attr_valid: u64::MAX,
			entry_valid_nsec: 0,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		}
	}

	fn fuse_dirent_type(type_: crate::DirEntryType) -> u32 {
		match type_ {
			crate::DirEntryType::Directory => S_IFDIR,
			crate::DirEntryType::File => S_IFREG,
			crate::DirEntryType::Symlink => S_IFLNK,
		}
	}
}

fn signal_eventfd(fd: RawFd) -> Result<()> {
	let value = 1u64.to_ne_bytes();
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	let ret = rustix::io::write(fd, &value).map_err(Error::from)?;
	if ret != value.len() {
		return Err(Error::other("failed to signal eventfd"));
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

fn write_response(fd: RawFd, unique: u64, response: &Response) -> std::io::Result<()> {
	let data = match response {
		Response::Flush | Response::Release | Response::ReleaseDir => &[],
		Response::GetAttr(data) => data.as_bytes(),
		Response::Init(data) => data.as_bytes(),
		Response::Lookup(data) => data.as_bytes(),
		Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
		Response::Read(data) => data.as_ref(),
		Response::ReadDir(data)
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
	let iov = [IoSlice::new(header), IoSlice::new(data)];
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	rustix::io::writev(fd, &iov).map_err(std::io::Error::from)?;
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
	let output = tokio::process::Command::new("fusermount3")
		.args(["-u", "-z"])
		.arg(path)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::piped())
		.output()
		.await?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(Error::other(format!("failed to unmount: {stderr}")));
	}
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
