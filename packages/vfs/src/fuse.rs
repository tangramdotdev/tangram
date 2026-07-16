use {
	self::sys::{
		fuse_attr, fuse_attr_out, fuse_entry_out, fuse_getxattr_out, fuse_in_header, fuse_init_out,
		fuse_open_in, fuse_open_out, fuse_out_header, fuse_read_in,
	},
	crate::{
		AttrsInner, Provider, Request as ProviderRequest, Response as ProviderResponse, Result,
	},
	bytes::Bytes,
	io_uring::{IoUring, opcode, types},
	num::ToPrimitive as _,
	rustix::{
		event::EventfdFlags,
		io::{Errno, FdFlags, IoSlice, IoSliceMut},
		ioctl,
		net::{
			AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, SocketFlags,
			SocketType,
		},
	},
	std::{
		collections::{BTreeSet, HashMap, VecDeque},
		ffi::{CString, OsString},
		io::Error,
		mem::{MaybeUninit, size_of},
		ops::Deref,
		os::fd::{AsRawFd as _, OwnedFd, RawFd},
		os::unix::ffi::OsStringExt as _,
		os::unix::process::CommandExt as _,
		path::Path,
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, Ordering},
		},
		time::Duration,
	},
	sys::{FUSE_KERNEL_MINOR_VERSION, FUSE_KERNEL_VERSION, fuse_interrupt_in},
	tokio_util::sync::CancellationToken,
	zerocopy::{FromBytes as _, FromZeros as _, IntoBytes as _},
};

mod connection;
mod protocol;
mod read_write;
mod request;
mod ring;
mod session;
#[cfg(test)]
mod tests;

pub mod sys;

const DEFAULT_MAX_WRITE: usize = 1024 * 1024;
const FUSE_MIN_READ_BUFFER: usize = 8192;
const READ_WRITE_ASYNC_CONCURRENCY: usize = 64;
const READ_WRITE_ASYNC_QUEUE_DEPTH: usize = 64;
const READ_WRITE_MAX_READER_COUNT: usize = 8;
const IO_URING_ENTRIES: u32 = 256;
const IO_URING_MAX_READER_COUNT: usize = 8;
const IO_URING_MAX_RETIRED_SLOTS_PER_WORKER: usize = 8;
const IO_URING_MAX_SLOTS_PER_QUEUE: usize = 4;
const IO_URING_PAYLOAD_MEMORY_BUDGET: usize = 256 * 1024 * 1024;
const IO_URING_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const THREAD_CQE_BATCH_SIZE: usize = 64;
const EVENTFD_USER_DATA: u64 = u64::MAX;
const SQPOLL_IDLE_MS: u32 = 2_000;
const EVENTFD_BUFFER_INDEX: u16 = 0;
const THREAD_FIXED_FUSE_FD: u32 = 0;
const THREAD_FIXED_EVENTFD_FD: u32 = 1;
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Options {
	pub io: Io,
	pub passthrough: Passthrough,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Io {
	#[default]
	Auto,
	IoUring,
	ReadWrite,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Passthrough {
	#[default]
	Auto,
	Disabled,
	Required,
}

pub struct Server<P>(Arc<State<P>>);

pub struct State<P> {
	active_requests: Mutex<HashMap<u64, CancellationToken>>,
	no_opendir_support: bool,
	pending_response_resources: Mutex<HashMap<u64, ResponseResources>>,
	passthrough_backing_ids: Mutex<HashMap<u64, u32>>,
	passthrough_enabled: bool,
	passthrough_permission_warning_emitted: AtomicBool,
	passthrough_required: bool,
	provider: P,
	task: Mutex<Option<tangram_futures::task::Shared<()>>>,
}

/// A request.
#[derive(Clone, Debug)]
struct Request {
	header: sys::fuse_in_header,
	data: RequestData,
}

#[derive(Clone, Debug)]
struct PendingRequest {
	slot: usize,
	request: Request,
}

enum WorkerEvent {
	Failed { error: Error, worker: String },
	Ready,
}

enum Dispatch {
	Deferred,
	Ready(Result<Response>),
}

/// A request's data.
#[derive(Clone, Debug)]
enum RequestData {
	BatchForget(sys::fuse_batch_forget_in, Vec<sys::fuse_forget_one>),
	Destroy,
	Flush,
	Forget(sys::fuse_forget_in),
	GetAttr,
	GetXattr(sys::fuse_getxattr_in, CString),
	Init(sys::fuse_init_in),
	Interrupt(sys::fuse_interrupt_in),
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
	Unsupported(u32),
}

/// A response.
#[derive(Clone, Debug)]
enum Response {
	BatchForget,
	Destroy,
	Flush,
	Forget,
	GetAttr(sys::fuse_attr_out),
	GetXattr(Vec<u8>),
	Interrupt,
	ListXattr(Vec<u8>),
	Lookup(sys::fuse_entry_out),
	Open(sys::fuse_open_out),
	OpenDir(sys::fuse_open_out),
	Read(Bytes),
	ReadDir(Vec<u8>),
	ReadDirPlus { data: Vec<u8>, nodes: Vec<u64> },
	ReadLink(Bytes),
	Release,
	ReleaseDir,
	Statfs(sys::fuse_statfs_out),
	Statx(sys::fuse_statx_out),
}

#[derive(Debug)]
struct AsyncResponse {
	opcode: sys::fuse_opcode,
	result: Result<Response>,
	slot: usize,
	unique: u64,
}

#[derive(Default)]
struct ResponseResources {
	handle: Option<u64>,
	nodes: Vec<u64>,
}

struct AsyncRequestContext {
	async_notification_pending: Arc<AtomicBool>,
	eventfd: Arc<OwnedFd>,
	fd: Arc<OwnedFd>,
	request: Request,
	runtime: tokio::runtime::Handle,
	sender: crossbeam_channel::Sender<AsyncResponse>,
	slot: usize,
	token: CancellationToken,
	worker_event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	worker_id: usize,
}

#[derive(Clone, Copy, Debug)]
struct Features {
	no_opendir_support: bool,
	over_io_uring: bool,
	passthrough: bool,
}

#[derive(Clone, Copy, Debug)]
struct RequestLimits {
	max_pages: u16,
	max_write: u32,
	payload_size: usize,
	request_buffer_size: usize,
}

#[derive(Clone, Copy, Debug)]
struct RingConfig {
	limits: RequestLimits,
	queue_count: usize,
	slots_per_queue: usize,
	worker_count: usize,
}

struct RingWorkerConfig {
	event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	payload_size: usize,
	queue_ids: Vec<u16>,
	slots_per_queue: usize,
	sqpoll_wq_fd: RawFd,
	worker_id: usize,
}

struct RingStartupContext<'a> {
	connection_id: u64,
	event_receiver: &'a mut tokio::sync::mpsc::UnboundedReceiver<WorkerEvent>,
	event_sender: tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	fd: Arc<OwnedFd>,
	path: &'a Path,
	ring_config: RingConfig,
	runtime: tokio::runtime::Handle,
	sqpoll_wq_fd: RawFd,
}

struct RingStartupFailure {
	disconnected: bool,
	error: Error,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseInitInV7p1 {
	major: u32,
	minor: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseInitInV7p6 {
	major: u32,
	minor: u32,
	max_readahead: u32,
	flags: u32,
}

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

#[derive(Clone, Copy, Debug)]
enum UringSlotState {
	Async {
		opcode: sys::fuse_opcode,
		unique: u64,
	},
	Commit {
		unique: u64,
	},
	Register,
	Request {
		opcode: sys::fuse_opcode,
		unique: u64,
	},
}

struct UringSlot {
	header: Box<sys::fuse_uring_req_header>,
	iovecs: [libc::iovec; URING_IOVEC_COUNT as usize],
	payload: Vec<u8>,
	qid: u16,
	state: UringSlotState,
}

struct IoctlPointerInt<'a, const OPCODE: rustix::ioctl::Opcode, T> {
	value: &'a mut T,
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub async fn start(provider: P, path: &Path, options: Options) -> Result<Self> {
		let mut options = options;
		let supports_no_opendir = provider.supports_no_opendir();
		let page_size = rustix::param::page_size();
		let mut ring_config = if options.io == Io::ReadWrite {
			None
		} else {
			match Self::ring_config(page_size) {
				Ok(config) => Some(config),
				Err(error) if options.io == Io::Auto => {
					tracing::warn!(
						%error,
						"failed to configure the FUSE io_uring transport; falling back to ReadWrite",
					);
					options.io = Io::ReadWrite;
					None
				},
				Err(error) => return Err(error),
			}
		};
		let mut limits = match ring_config {
			Some(config) => config.limits,
			None => Self::request_limits(page_size, DEFAULT_MAX_WRITE)?,
		};
		let (mut connection_id, mut fd, mut features, mut sqpoll_ring) = loop {
			// Unmount.
			Self::unmount(path).await.ok();

			// Mount.
			let fd = Self::mount(path)
				.await
				.inspect_err(|error| tracing::error!(%error, "failed to mount"))?;
			// Complete INIT before entering io_uring command mode.
			let features =
				match Self::init_handshake(fd.as_ref(), options, limits, supports_no_opendir) {
					Ok(features) => features,
					Err(error) => {
						drop(fd);
						Self::unmount(path).await.ok();
						return Err(Error::other(format!(
							"failed to complete init handshake: {error}",
						)));
					},
				};
			let connection_id = match Self::connection_id(path) {
				Ok(connection_id) => connection_id,
				Err(error) => {
					drop(fd);
					Self::unmount(path).await.ok();
					return Err(error);
				},
			};

			if !features.over_io_uring {
				break (connection_id, fd, features, None);
			}

			// Create a primary SQPOLL ring. Thread rings attach to this shared backend.
			let mut sqpoll_builder = IoUring::<io_uring::squeue::Entry128>::builder();
			sqpoll_builder
				.setup_sqpoll(SQPOLL_IDLE_MS)
				.setup_no_sqarray();
			match sqpoll_builder.build(IO_URING_ENTRIES) {
				Ok(ring) => break (connection_id, fd, features, Some(ring)),
				Err(error) if options.io == Io::Auto => {
					tracing::warn!(
						%error,
						"failed to build the FUSE io_uring SQPOLL ring; falling back to the ReadWrite transport"
					);
					drop(fd);
					Self::unmount(path).await.ok();
					options.io = Io::ReadWrite;
					ring_config = None;
					limits = Self::request_limits(page_size, DEFAULT_MAX_WRITE)?;
				},
				Err(error) => {
					drop(fd);
					Self::unmount(path).await.ok();
					return Err(Error::other(format!(
						"failed to build sqpoll ring: {error}"
					)));
				},
			}
		};

		// Create the server.
		let server = Self(Arc::new(State {
			active_requests: Mutex::new(HashMap::new()),
			no_opendir_support: features.no_opendir_support,
			pending_response_resources: Mutex::new(HashMap::new()),
			passthrough_backing_ids: Mutex::new(HashMap::default()),
			passthrough_enabled: features.passthrough,
			passthrough_permission_warning_emitted: AtomicBool::new(false),
			passthrough_required: options.passthrough == Passthrough::Required,
			provider,
			task: Mutex::new(None),
		}));

		let runtime = tokio::runtime::Handle::current();
		let (worker_event_sender, mut worker_event_receiver) =
			tokio::sync::mpsc::unbounded_channel();
		let mut thread_handles = Vec::new();
		let mut read_write_dispatcher = None;
		loop {
			if features.over_io_uring {
				let Some(config) = ring_config else {
					drop(fd);
					Self::unmount(path).await.ok();
					return Err(Error::other("missing the io_uring transport configuration"));
				};
				let context = RingStartupContext {
					connection_id,
					event_receiver: &mut worker_event_receiver,
					event_sender: worker_event_sender.clone(),
					fd: fd.clone(),
					path,
					ring_config: config,
					runtime: runtime.clone(),
					sqpoll_wq_fd: sqpoll_ring.as_ref().unwrap().as_raw_fd(),
				};
				match server.start_ring_transport(context).await {
					Ok(handles) => {
						thread_handles = handles;
						break;
					},
					Err(failure) if options.io == Io::Auto => {
						let RingStartupFailure {
							disconnected,
							error,
						} = failure;
						if !disconnected {
							return Err(Error::other(format!(
								"failed to stop the io_uring transport after startup failed: {error}",
							)));
						}
						tracing::warn!(
							%error,
							"failed to start the FUSE io_uring transport; falling back to ReadWrite",
						);
						drop(sqpoll_ring.take());
						drop(fd);
						options.io = Io::ReadWrite;
						ring_config = None;
						limits = Self::request_limits(page_size, DEFAULT_MAX_WRITE)?;
						fd = Self::mount(path).await?;
						connection_id = match Self::connection_id(path) {
							Ok(connection_id) => connection_id,
							Err(error) => {
								drop(fd);
								Self::unmount(path).await.ok();
								return Err(error);
							},
						};
						features = match Self::init_handshake(
							fd.as_ref(),
							options,
							limits,
							supports_no_opendir,
						) {
							Ok(features) => features,
							Err(error) => {
								drop(fd);
								Self::unmount(path).await.ok();
								return Err(Error::other(format!(
									"failed to complete the ReadWrite fallback init handshake: {error}",
								)));
							},
						};
						if features.no_opendir_support != server.no_opendir_support
							|| features.passthrough != server.passthrough_enabled
						{
							drop(fd);
							Self::unmount(path).await.ok();
							return Err(Error::other(
								"the ReadWrite fallback negotiated different FUSE features",
							));
						}
						continue;
					},
					Err(failure) => {
						drop(fd);
						drop(sqpoll_ring);
						return Err(failure.error);
					},
				}
			}

			let reader_fds = match Self::clone_read_write_fds(&fd) {
				Ok(reader_fds) => reader_fds,
				Err(error) => {
					drop(fd);
					Self::unmount(path).await.ok();
					return Err(error);
				},
			};
			let reader_count = reader_fds.len();
			let (request_sender, request_receiver) =
				tokio::sync::mpsc::channel(READ_WRITE_ASYNC_QUEUE_DEPTH);
			read_write_dispatcher = Some(
				server.spawn_read_write_dispatcher(request_receiver, worker_event_sender.clone()),
			);
			tracing::info!(
				async_concurrency = READ_WRITE_ASYNC_CONCURRENCY,
				async_queue_depth = READ_WRITE_ASYNC_QUEUE_DEPTH,
				reader_count,
				"started the FUSE ReadWrite transport",
			);
			let mut startup_error = None;
			for (reader_id, reader_fd) in reader_fds.into_iter().enumerate() {
				let request_sender = request_sender.clone();
				let server = server.clone();
				let worker_event_sender = worker_event_sender.clone();
				let thread = std::thread::Builder::new()
					.name(format!("tangram-fuse-read-write-{reader_id}"))
					.spawn(move || {
						worker_event_sender.send(WorkerEvent::Ready).ok();
						if let Err(error) = server.thread_loop_read_write(
							&reader_fd,
							limits.request_buffer_size,
							&request_sender,
						) {
							if error.raw_os_error() == Some(libc::ENOTCONN) {
								tracing::debug!(%error, %reader_id, "ReadWrite reader exited during shutdown");
							} else {
								tracing::error!(%error, %reader_id, "ReadWrite reader failed");
								worker_event_sender
									.send(WorkerEvent::Failed {
										error,
										worker: format!("ReadWrite reader {reader_id}"),
									})
									.ok();
							}
						}
					});
				match thread {
					Ok(thread) => thread_handles.push(thread),
					Err(error) => {
						startup_error = Some(Error::other(format!(
							"failed to spawn the ReadWrite reader {reader_id}: {error}",
						)));
						break;
					},
				}
			}
			drop(request_sender);

			if startup_error.is_none() {
				for _ in 0..thread_handles.len() {
					match worker_event_receiver.recv().await {
						Some(WorkerEvent::Ready) => {},
						Some(WorkerEvent::Failed { error, worker }) => {
							startup_error = Some(Error::other(format!(
								"{worker} failed during startup: {error}"
							)));
							break;
						},
						None => {
							startup_error =
								Some(Error::other("a ReadWrite reader failed during startup"));
							break;
						},
					}
				}
			}
			if let Some(error) = startup_error {
				server.cancel_async_requests();
				let disconnected = Self::disconnect_transport(path, connection_id).await;
				Self::join_transport_threads(&mut thread_handles, disconnected);
				if let Some(dispatcher) = read_write_dispatcher.take() {
					if !disconnected {
						dispatcher.abort();
					}
					dispatcher.await.ok();
				}
				if disconnected {
					server.rollback_all_response_resources(fd.as_ref());
				}
				drop(fd);
				drop(sqpoll_ring);
				return Err(error);
			}
			break;
		}
		drop(worker_event_sender);

		let path = path.to_owned();
		let shutdown_server = server.clone();
		let shutdown = async move {
			shutdown_server.cancel_async_requests();
			let disconnected = Self::disconnect_transport(&path, connection_id).await;
			Self::join_transport_threads(&mut thread_handles, disconnected);
			if let Some(read_write_dispatcher) = read_write_dispatcher {
				if !disconnected {
					read_write_dispatcher.abort();
				}
				read_write_dispatcher.await.ok();
			}
			if disconnected {
				shutdown_server.rollback_all_response_resources(fd.as_ref());
			}
			drop(fd);
			drop(sqpoll_ring);
		};

		// Spawn the task.
		let task = tangram_futures::task::Shared::spawn(|stop| async move {
			tokio::select! {
				() = stop.wait() => {},
				event = worker_event_receiver.recv() => {
					if let Some(WorkerEvent::Failed { error, worker }) = event {
						tracing::error!(%error, %worker, "a FUSE transport worker failed");
					}
				},
			}
			shutdown.await;
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}
}

impl<P> Clone for Server<P> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<P> Deref for Server<P> {
	type Target = State<P>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<'a, const OPCODE: rustix::ioctl::Opcode, T> IoctlPointerInt<'a, OPCODE, T> {
	const fn new(value: &'a mut T) -> Self {
		Self { value }
	}
}

// SAFETY: The wrapper exposes a valid mutable pointer for the duration of each ioctl call. Its
// constructor is private so each call site must pair the pointer type with the kernel opcode.
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
