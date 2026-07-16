use {
	self::sys::{
		fuse_attr, fuse_attr_out, fuse_batch_forget_in, fuse_entry_out, fuse_flush_in,
		fuse_forget_in, fuse_getattr_in, fuse_getxattr_in, fuse_getxattr_out, fuse_in_header,
		fuse_init_out, fuse_open_in, fuse_open_out, fuse_out_header, fuse_read_in, fuse_release_in,
	},
	crate::{
		AttrsInner, Provider, Request as ProviderRequest, Response as ProviderResponse, Result,
	},
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
	zerocopy::{FromBytes as _, IntoBytes as _},
};

mod protocol;
mod read_write;
mod request;
mod ring;

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
	pending_handles: Mutex<HashMap<u64, u64>>,
	pending_publications: Mutex<HashMap<u64, Vec<u64>>>,
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
	result: Result<Option<Response>>,
	slot: usize,
	unique: u64,
}

struct AsyncRequestContext {
	async_notification_pending: Arc<AtomicBool>,
	eventfd: Arc<OwnedFd>,
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
			pending_handles: Mutex::new(HashMap::new()),
			pending_publications: Mutex::new(HashMap::new()),
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
					server.rollback_all_response_resources(fd.as_raw_fd());
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
				shutdown_server.rollback_all_response_resources(fd.as_raw_fd());
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
		let options = format!("rootmode=40755,user_id={uid},group_id={gid},default_permissions,ro");
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

	async fn disconnect_transport(path: &Path, connection_id: u64) -> bool {
		let aborted = Self::abort_connection(connection_id)
			.inspect_err(|error| tracing::error!(%error, "failed to abort the FUSE connection"))
			.is_ok();
		Self::unmount(path)
			.await
			.inspect_err(|error| tracing::error!(%error, "failed to unmount"))
			.ok();

		aborted
	}

	fn abort_connection(connection_id: u64) -> Result<()> {
		let abort = Path::new("/sys/fs/fuse/connections")
			.join(connection_id.to_string())
			.join("abort");
		std::fs::write(abort, b"1")?;

		Ok(())
	}

	fn connection_id(path: &Path) -> Result<u64> {
		let path = if path.is_absolute() {
			path.to_owned()
		} else {
			std::env::current_dir()?.join(path)
		};
		let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;

		Self::parse_connection_id(&mountinfo, &path)
	}

	fn parse_connection_id(mountinfo: &str, path: &Path) -> Result<u64> {
		for line in mountinfo.lines().rev() {
			let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
			let Some(separator) = fields.iter().position(|field| *field == "-") else {
				continue;
			};
			if fields.len() <= separator + 1 || !fields[separator + 1].starts_with("fuse") {
				continue;
			}
			let Some(mountpoint) = fields.get(4) else {
				continue;
			};
			if Self::unescape_mountinfo_path(mountpoint) != path {
				continue;
			}
			let Some((major, minor)) = fields.get(2).and_then(|device| device.split_once(':'))
			else {
				continue;
			};
			let major = major.parse::<u32>().map_err(|error| {
				Error::other(format!(
					"failed to parse the FUSE device major number: {error}"
				))
			})?;
			let minor = minor.parse::<u32>().map_err(|error| {
				Error::other(format!(
					"failed to parse the FUSE device minor number: {error}"
				))
			})?;
			let connection_id = libc::makedev(major, minor);

			return Ok(connection_id);
		}

		Err(Error::other(
			"failed to find the FUSE connection in mountinfo",
		))
	}

	fn unescape_mountinfo_path(path: &str) -> std::path::PathBuf {
		let bytes = path.as_bytes();
		let mut output = Vec::with_capacity(bytes.len());
		let mut index = 0;
		while index < bytes.len() {
			if bytes[index] == b'\\'
				&& index + 3 < bytes.len()
				&& bytes[index + 1..=index + 3]
					.iter()
					.all(|byte| matches!(byte, b'0'..=b'7'))
			{
				let value = (bytes[index + 1] - b'0') * 64
					+ (bytes[index + 2] - b'0') * 8
					+ bytes[index + 3]
					- b'0';
				output.push(value);
				index += 4;
			} else {
				output.push(bytes[index]);
				index += 1;
			}
		}

		OsString::from_vec(output).into()
	}

	fn join_transport_threads(
		thread_handles: &mut Vec<std::thread::JoinHandle<()>>,
		disconnected: bool,
	) {
		if !disconnected {
			tracing::error!(
				"detaching FUSE workers because the connection could not be terminated"
			);
			thread_handles.clear();
			return;
		}
		for thread_handle in thread_handles.drain(..) {
			thread_handle.join().ok();
		}
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait().await.unwrap();
	}

	fn init_handshake(
		fd: &OwnedFd,
		options: Options,
		limits: RequestLimits,
		supports_no_opendir: bool,
	) -> Result<Features> {
		const FUSE_PASSTHROUGH_FLAGS2: u32 = (sys::FUSE_PASSTHROUGH >> 32) as u32;
		const FUSE_OVER_IO_URING_FLAGS2: u32 = (sys::FUSE_OVER_IO_URING >> 32) as u32;
		const REQUIRED_FLAGS: u32 = 0;
		const NEGOTIATED_FLAGS: u32 = sys::FUSE_ASYNC_READ
			| sys::FUSE_DO_READDIRPLUS
			| sys::FUSE_READDIRPLUS_AUTO
			| sys::FUSE_PARALLEL_DIROPS
			| sys::FUSE_CACHE_SYMLINKS
			| sys::FUSE_SPLICE_MOVE
			| sys::FUSE_SPLICE_READ
			| sys::FUSE_MAX_PAGES
			| sys::FUSE_MAP_ALIGNMENT
			| sys::FUSE_INIT_EXT;
		const MAP_ALIGNMENT: u16 = 12;
		// The kernel requires a non-zero max_stack_depth to enable FUSE_PASSTHROUGH.
		const MAX_STACK_DEPTH: u32 = 2;
		let mut negotiated_flags = NEGOTIATED_FLAGS;
		if supports_no_opendir {
			negotiated_flags |= sys::FUSE_NO_OPENDIR_SUPPORT;
		}
		let mut required_flags2 = 0;
		if options.io == Io::IoUring {
			required_flags2 |= FUSE_OVER_IO_URING_FLAGS2;
		}
		if options.passthrough == Passthrough::Required {
			required_flags2 |= FUSE_PASSTHROUGH_FLAGS2;
		}
		let mut negotiated_flags2 = 0;
		if options.io != Io::ReadWrite {
			negotiated_flags2 |= FUSE_OVER_IO_URING_FLAGS2;
		}
		if options.passthrough != Passthrough::Disabled {
			negotiated_flags2 |= FUSE_PASSTHROUGH_FLAGS2;
		}

		let mut buffer = vec![0u8; limits.request_buffer_size];
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

			if init.major != FUSE_KERNEL_VERSION {
				return Err(Error::other("unsupported fuse major version"));
			}
			let init_ext_available = (init.flags & sys::FUSE_INIT_EXT) != 0;
			let init_flags2 = if init_ext_available { init.flags2 } else { 0 };
			let missing_flags = REQUIRED_FLAGS & !init.flags;
			if missing_flags != 0 {
				return Err(Error::other(format!(
					"missing required fuse init flags, missing = {missing_flags:#x}",
				)));
			}
			let missing_flags2 = required_flags2 & !init_flags2;
			if missing_flags2 != 0 {
				return Err(Error::other(format!(
					"missing required fuse init flags2, missing = {missing_flags2:#x}",
				)));
			}

			let minor = init.minor.min(FUSE_KERNEL_MINOR_VERSION);
			let flags = negotiated_flags & init.flags;
			let flags2 = negotiated_flags2 & init_flags2;
			let max_pages = if (flags & sys::FUSE_MAX_PAGES) != 0 {
				limits.max_pages
			} else {
				0
			};
			let map_alignment = if (flags & sys::FUSE_MAP_ALIGNMENT) != 0 {
				MAP_ALIGNMENT
			} else {
				0
			};
			let max_stack_depth = if (flags2 & FUSE_PASSTHROUGH_FLAGS2) != 0 {
				MAX_STACK_DEPTH
			} else {
				0
			};
			let response = fuse_init_out {
				major: FUSE_KERNEL_VERSION,
				minor,
				max_readahead: limits.max_write,
				flags,
				max_background: 0,
				congestion_threshold: 0,
				max_write: limits.max_write,
				time_gran: 0,
				max_pages,
				map_alignment,
				flags2,
				max_stack_depth,
				request_timeout: 0,
				unused: [0; 11],
			};

			let data = &response.as_bytes()[..Self::init_response_size(minor)];
			let len = size_of::<fuse_out_header>() + data.len();
			let header = fuse_out_header {
				unique: request.header.unique,
				len: len.to_u32().unwrap(),
				error: 0,
			};
			let header = header.as_bytes();
			let iov = [IoSlice::new(header), IoSlice::new(data)];
			rustix::io::writev(fd, &iov).map_err(std::io::Error::from)?;
			return Ok(Features {
				no_opendir_support: (flags & sys::FUSE_NO_OPENDIR_SUPPORT) != 0,
				over_io_uring: (flags2 & FUSE_OVER_IO_URING_FLAGS2) != 0,
				passthrough: (flags2 & FUSE_PASSTHROUGH_FLAGS2) != 0,
			});
		}
	}

	fn init_response_size(minor: u32) -> usize {
		if minor < 5 {
			sys::FUSE_COMPAT_INIT_OUT_SIZE.to_usize().unwrap()
		} else if minor < 23 {
			sys::FUSE_COMPAT_22_INIT_OUT_SIZE.to_usize().unwrap()
		} else {
			size_of::<fuse_init_out>()
		}
	}

	fn is_clone_not_supported_error(error: &Error) -> bool {
		matches!(
			error.raw_os_error(),
			Some(libc::ENOSYS | libc::ENOTTY | libc::EINVAL)
		)
	}

	fn clone_thread_fd(fd: &OwnedFd) -> Result<OwnedFd> {
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

	fn clone_read_write_fds(fd: &Arc<OwnedFd>) -> Result<Vec<Arc<OwnedFd>>> {
		let reader_count = std::thread::available_parallelism()
			.map_or(1, std::num::NonZero::get)
			.clamp(1, READ_WRITE_MAX_READER_COUNT);
		let mut fds = Vec::with_capacity(reader_count);
		fds.push(fd.clone());
		for reader_id in 1..reader_count {
			match Self::clone_thread_fd(fd) {
				Ok(fd) => fds.push(Arc::new(fd)),
				Err(error) if Self::is_clone_not_supported_error(&error) => {
					tracing::warn!(
						?error,
						%reader_id,
						"FUSE fd cloning is not supported; using the available ReadWrite readers",
					);
					break;
				},
				Err(error) => {
					return Err(Error::other(format!(
						"failed to clone the ReadWrite reader fd {reader_id}: {error}",
					)));
				},
			}
		}

		Ok(fds)
	}

	fn register_async_request(&self, unique: u64) -> Result<CancellationToken> {
		let token = CancellationToken::new();
		let mut requests = self.active_requests.lock().unwrap();
		if requests.contains_key(&unique) {
			return Err(Error::other(format!(
				"a FUSE request with unique ID {unique} is already active",
			)));
		}
		requests.insert(unique, token.clone());

		Ok(token)
	}

	fn cancel_async_request(&self, unique: u64) -> bool {
		let token = self.active_requests.lock().unwrap().get(&unique).cloned();
		if let Some(token) = token {
			token.cancel();
			return true;
		}

		false
	}

	fn cancel_async_requests(&self) {
		let requests = std::mem::take(&mut *self.active_requests.lock().unwrap());
		for token in requests.into_values() {
			token.cancel();
		}
	}

	fn finish_async_request(&self, unique: u64) {
		self.active_requests.lock().unwrap().remove(&unique);
	}

	fn register_response_resources(
		&self,
		unique: u64,
		result: &Result<Option<Response>>,
	) -> Result<()> {
		let nodes = match result {
			Ok(Some(Response::Lookup(response))) => vec![response.nodeid],
			Ok(Some(Response::ReadDirPlus { nodes, .. })) => nodes.clone(),
			_ => Vec::new(),
		};
		let handle = match result {
			Ok(Some(Response::Open(response) | Response::OpenDir(response))) => Some(response.fh),
			_ => None,
		};
		if nodes.is_empty() && handle.is_none() {
			return Ok(());
		}
		if !nodes.is_empty() {
			let mut publications = self.pending_publications.lock().unwrap();
			if publications.contains_key(&unique) {
				drop(publications);
				self.rollback_nodes(nodes);
				return Err(Error::other(format!(
					"a FUSE response with unique ID {unique} already has pending publications",
				)));
			}
			publications.insert(unique, nodes);
		}
		if let Some(handle) = handle {
			let mut handles = self.pending_handles.lock().unwrap();
			if handles.contains_key(&unique) {
				drop(handles);
				self.provider.close_sync(handle);
				return Err(Error::other(format!(
					"a FUSE response with unique ID {unique} already has a pending handle",
				)));
			}
			handles.insert(unique, handle);
		}

		Ok(())
	}

	fn commit_response_resources(&self, unique: u64) {
		self.pending_handles.lock().unwrap().remove(&unique);
		self.pending_publications.lock().unwrap().remove(&unique);
	}

	fn rollback_response_resources(&self, fd: RawFd, unique: u64) {
		let handle = self.pending_handles.lock().unwrap().remove(&unique);
		if let Some(handle) = handle {
			self.close_passthrough_backing(fd, handle);
			self.provider.close_sync(handle);
		}
		self.rollback_response_publications(unique);
	}

	fn rollback_response_publications(&self, unique: u64) {
		let nodes = self.pending_publications.lock().unwrap().remove(&unique);
		if let Some(nodes) = nodes {
			self.rollback_nodes(nodes);
		}
	}

	fn rollback_all_response_resources(&self, fd: RawFd) {
		let handles = std::mem::take(&mut *self.pending_handles.lock().unwrap());
		for handle in handles.into_values() {
			self.close_passthrough_backing(fd, handle);
			self.provider.close_sync(handle);
		}
		let publications = std::mem::take(&mut *self.pending_publications.lock().unwrap());
		for nodes in publications.into_values() {
			self.rollback_nodes(nodes);
		}
	}

	fn rollback_nodes(&self, nodes: Vec<u64>) {
		for node in nodes {
			self.provider.forget_sync(node, 1);
		}
	}

	async fn handle_cancellable_request(
		&self,
		request: Request,
		token: CancellationToken,
	) -> Result<Option<Response>> {
		let unique = request.header.unique;
		let result = tokio::select! {
			biased;
			() = token.cancelled() => Err(Error::from_raw_os_error(libc::EINTR)),
			result = self.handle_request(request) => result,
		};
		self.finish_async_request(unique);

		result
	}

	fn thread_loop_control(&self, fd: &Arc<OwnedFd>, request_buffer_size: usize) -> Result<()> {
		let mut buffer = vec![0u8; request_buffer_size];
		loop {
			let size = loop {
				match rustix::io::read(fd.as_ref(), &mut buffer) {
					Ok(size) => break size,
					Err(Errno::INTR) => {},
					Err(Errno::NODEV | Errno::NOTCONN) => return Ok(()),
					Err(error) => return Err(error.into()),
				}
			};
			if size == 0 {
				return Ok(());
			}
			let size = size.to_usize().unwrap();
			let request = Self::deserialize_request(&buffer[..size])?;
			match request.data {
				RequestData::BatchForget(data, entries) => {
					self.handle_batch_forget_request_sync(request.header, data, &entries);
				},
				RequestData::Forget(data) => {
					self.handle_forget_request_sync(request.header, data);
				},
				RequestData::Interrupt(data) => {
					let result = if self.cancel_async_request(data.unique) {
						Ok(Some(Response::Interrupt))
					} else {
						Err(Error::from_raw_os_error(libc::EAGAIN))
					};
					if let Err(error) =
						Self::write_response(fd.as_raw_fd(), request.header.unique, result)
					{
						match error.raw_os_error() {
							Some(libc::EAGAIN | libc::EINTR | libc::ENOENT) => {},
							_ => return Err(error),
						}
					}
				},
				_ => {
					return Err(Error::other(format!(
						"received opcode {} on the io_uring control channel",
						request.header.opcode,
					)));
				},
			}
		}
	}

	fn write_response(fd: RawFd, unique: u64, result: Result<Option<Response>>) -> Result<()> {
		let (error, response) = match result {
			Ok(Some(response)) => {
				if !Self::requires_response(&response) {
					return Ok(());
				}
				(0, Some(response))
			},
			Ok(None) => (0, None),
			Err(error) => (error.raw_os_error().unwrap_or(libc::ENOSYS), None),
		};
		let payload = response.as_ref().map_or(&[][..], Self::response_bytes);
		let len = size_of::<fuse_out_header>() + payload.len();
		let header = fuse_out_header {
			unique,
			len: len.to_u32().unwrap(),
			error: -error,
		};
		let iov = [IoSlice::new(header.as_bytes()), IoSlice::new(payload)];
		let fd = unsafe { BorrowedFd::borrow_raw(fd) };
		let written = loop {
			match rustix::io::writev(fd, &iov) {
				Ok(written) => break written,
				Err(Errno::INTR) => {},
				Err(error) => return Err(error.into()),
			}
		};
		if written != len {
			return Err(Error::other("failed to write a complete response"));
		}
		Ok(())
	}

	fn requires_response(response: &Response) -> bool {
		!matches!(
			response,
			Response::BatchForget | Response::Forget | Response::Interrupt
		)
	}

	fn response_bytes(response: &Response) -> &[u8] {
		debug_assert!(Self::requires_response(response));
		match response {
			Response::BatchForget
			| Response::Destroy
			| Response::Flush
			| Response::Forget
			| Response::Interrupt
			| Response::Release
			| Response::ReleaseDir => &[],
			Response::GetAttr(data) => data.as_bytes(),
			Response::Lookup(data) => data.as_bytes(),
			Response::Open(data) | Response::OpenDir(data) => data.as_bytes(),
			Response::Read(data) => data.as_ref(),
			Response::ReadDir(data) | Response::GetXattr(data) | Response::ListXattr(data) => {
				data.as_bytes()
			},
			Response::ReadDirPlus { data, .. } => data.as_bytes(),
			Response::ReadLink(data) => data.as_bytes(),
			Response::Statfs(data) => data.as_bytes(),
			Response::Statx(data) => data.as_bytes(),
		}
	}

	fn is_expected_error(opcode: sys::fuse_opcode, error_code: i32) -> bool {
		(opcode == sys::fuse_opcode_FUSE_LOOKUP && error_code == libc::ENOENT)
			|| (opcode == sys::fuse_opcode_FUSE_GETXATTR
				&& (error_code == libc::ENODATA || error_code == libc::ERANGE))
			|| (opcode == sys::fuse_opcode_FUSE_LISTXATTR && error_code == libc::ERANGE)
			|| (matches!(
				opcode,
				sys::fuse_opcode_FUSE_OPEN | sys::fuse_opcode_FUSE_OPENDIR
			) && error_code == libc::EROFS)
			|| (opcode == sys::fuse_opcode_FUSE_INTERRUPT && error_code == libc::EAGAIN)
			|| matches!(error_code, libc::EINTR | libc::ENOSYS)
	}
}

fn notify_async_response(
	async_notification_pending: &AtomicBool,
	eventfd: RawFd,
	response: AsyncResponse,
	sender: &crossbeam_channel::Sender<AsyncResponse>,
	worker_event_sender: &tokio::sync::mpsc::UnboundedSender<WorkerEvent>,
	worker_id: usize,
) {
	if sender.send(response).is_err() || async_notification_pending.swap(true, Ordering::AcqRel) {
		return;
	}
	if let Err(error) = signal_eventfd(eventfd) {
		async_notification_pending.store(false, Ordering::Release);
		tracing::error!(?error, %worker_id, "failed to signal the io_uring eventfd");
		worker_event_sender
			.send(WorkerEvent::Failed {
				error,
				worker: format!("io_uring worker {worker_id} async notifier"),
			})
			.ok();
	}
}

fn signal_eventfd(fd: RawFd) -> Result<()> {
	let value = 1u64.to_ne_bytes();
	let fd = unsafe { BorrowedFd::borrow_raw(fd) };
	let ret = loop {
		match rustix::io::write(fd, &value) {
			Ok(ret) => break ret,
			Err(Errno::INTR) => {},
			Err(error) => return Err(error.into()),
		}
	};
	if ret != value.len() {
		return Err(Error::other("failed to signal eventfd"));
	}
	Ok(())
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

#[cfg(test)]
mod tests {
	use super::*;

	struct TestProvider;
	struct PassthroughProvider {
		closes: Arc<std::sync::atomic::AtomicUsize>,
	}
	struct SlowProvider {
		release: Arc<tokio::sync::Notify>,
		started: Arc<tokio::sync::Notify>,
	}

	impl Provider for TestProvider {
		fn handle_batch(
			&self,
			_requests: Vec<ProviderRequest>,
		) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
			std::future::ready(Vec::new())
		}

		fn handle_batch_sync(
			&self,
			_requests: Vec<ProviderRequest>,
		) -> Vec<Result<ProviderResponse>> {
			Vec::new()
		}
	}

	impl Provider for PassthroughProvider {
		fn handle_batch(
			&self,
			requests: Vec<ProviderRequest>,
		) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
			std::future::ready(self.handle_batch_sync(requests))
		}

		fn handle_batch_sync(
			&self,
			requests: Vec<ProviderRequest>,
		) -> Vec<Result<ProviderResponse>> {
			requests
				.into_iter()
				.map(|request| match request {
					ProviderRequest::Close { .. } => {
						self.closes.fetch_add(1, Ordering::Relaxed);
						Ok(ProviderResponse::Unit)
					},
					_ => Err(Error::from_raw_os_error(libc::ENOSYS)),
				})
				.collect()
		}
	}

	impl Provider for SlowProvider {
		fn handle_batch(
			&self,
			requests: Vec<ProviderRequest>,
		) -> impl futures::Future<Output = Vec<Result<ProviderResponse>>> + Send {
			let release = self.release.clone();
			let started = self.started.clone();
			async move {
				let mut responses = Vec::with_capacity(requests.len());
				for request in requests {
					let response = match request {
						ProviderRequest::GetAttr { .. } => Ok(ProviderResponse::GetAttr {
							attrs: crate::Attrs::new(AttrsInner::File {
								executable: false,
								size: 4,
							}),
						}),
						ProviderRequest::Read { .. } => {
							started.notify_one();
							release.notified().await;
							Ok(ProviderResponse::Read {
								bytes: Bytes::from_static(b"slow"),
							})
						},
						_ => Err(Error::from_raw_os_error(libc::ENOSYS)),
					};
					responses.push(response);
				}
				responses
			}
		}

		fn handle_batch_sync(
			&self,
			requests: Vec<ProviderRequest>,
		) -> Vec<Result<ProviderResponse>> {
			requests
				.into_iter()
				.map(|_| Err(Error::from_raw_os_error(libc::ENOSYS)))
				.collect()
		}
	}

	fn server_with_provider<P>(
		provider: P,
		passthrough_enabled: bool,
		passthrough_required: bool,
	) -> Server<P>
	where
		P: Provider + Send + Sync + 'static,
	{
		Server(Arc::new(State {
			active_requests: Mutex::new(HashMap::new()),
			no_opendir_support: false,
			pending_handles: Mutex::new(HashMap::new()),
			pending_publications: Mutex::new(HashMap::new()),
			passthrough_backing_ids: Mutex::new(HashMap::new()),
			passthrough_enabled,
			passthrough_permission_warning_emitted: AtomicBool::new(false),
			passthrough_required,
			provider,
			task: Mutex::new(None),
		}))
	}

	fn server() -> Server<TestProvider> {
		server_with_provider(TestProvider, false, false)
	}

	fn request_header(opcode: sys::fuse_opcode, unique: u64) -> fuse_in_header {
		fuse_in_header {
			gid: 0,
			len: 0,
			nodeid: 2,
			opcode,
			padding: 0,
			pid: 0,
			total_extlen: 0,
			uid: 0,
			unique,
		}
	}

	async fn read_response(fd: Arc<OwnedFd>) -> fuse_out_header {
		let response = tokio::task::spawn_blocking(move || {
			let mut bytes = Vec::new();
			loop {
				let mut buffer = [0u8; 4096];
				let size = rustix::io::read(fd.as_ref(), &mut buffer).unwrap();
				assert_ne!(size, 0);
				bytes.extend_from_slice(&buffer[..size]);
				if bytes.len() < size_of::<fuse_out_header>() {
					continue;
				}
				let (header, _) = fuse_out_header::read_from_prefix(&bytes).unwrap();
				if bytes.len() >= header.len.to_usize().unwrap() {
					return header;
				}
			}
		});
		tokio::time::timeout(Duration::from_secs(2), response)
			.await
			.unwrap()
			.unwrap()
	}

	#[test]
	fn parses_possible_cpu_sets() {
		assert_eq!(
			Server::<TestProvider>::parse_possible_cpu_count("0\n").unwrap(),
			1,
		);
		assert_eq!(
			Server::<TestProvider>::parse_possible_cpu_count("0-3,4-15\n").unwrap(),
			16,
		);
		assert!(Server::<TestProvider>::parse_possible_cpu_count("0-3,8-11").is_err());
	}

	#[test]
	fn readlink_responses_validate_targets_without_copying() {
		let target = Bytes::from_static(b"target");
		let pointer = target.as_ptr();
		let response = Server::<TestProvider>::read_link_response(target).unwrap();
		let Response::ReadLink(target) = response else {
			panic!("expected a readlink response");
		};
		assert_eq!(target.as_ptr(), pointer);

		let error = Server::<TestProvider>::read_link_response(Bytes::from_static(b"bad\0target"))
			.unwrap_err();
		assert_eq!(error.raw_os_error(), Some(libc::EIO));
	}

	#[test]
	fn stale_uring_slots_are_replaced_with_distinct_buffers() {
		let mut slots = vec![Box::new(UringSlot::new(7, 4096))];
		slots[0].state = UringSlotState::Commit { unique: 42 };
		let header = std::ptr::from_ref(slots[0].header.as_ref());
		let payload = slots[0].payload.as_ptr();
		let mut retired_slots = VecDeque::new();

		Server::<TestProvider>::replace_stale_uring_slot(&mut slots, &mut retired_slots, 4096, 0)
			.unwrap();

		assert_eq!(retired_slots.len(), 1);
		assert_eq!(slots[0].qid, 7);
		assert!(matches!(slots[0].state, UringSlotState::Register));
		assert_ne!(std::ptr::from_ref(slots[0].header.as_ref()), header);
		assert_ne!(slots[0].payload.as_ptr(), payload);
	}

	#[test]
	fn parses_fuse_connection_ids_without_accessing_the_mount() {
		let mountinfo = concat!(
			"20 1 8:1 / / rw - ext4 /dev/root rw\n",
			"21 20 0:42 / /tmp/mount\\040point rw - fuse tangram rw\n",
		);
		let connection_id =
			Server::<TestProvider>::parse_connection_id(mountinfo, Path::new("/tmp/mount point"))
				.unwrap();

		assert_eq!(connection_id, libc::makedev(0, 42));
	}

	#[test]
	fn derives_request_limits_from_page_size() {
		let limits = Server::<TestProvider>::request_limits(64 * 1024, DEFAULT_MAX_WRITE).unwrap();
		assert_eq!(limits.max_pages, 16);
		assert_eq!(limits.max_write, DEFAULT_MAX_WRITE.to_u32().unwrap());
		assert_eq!(limits.payload_size, DEFAULT_MAX_WRITE);
		assert_eq!(limits.request_buffer_size, DEFAULT_MAX_WRITE + 64 * 1024);
	}

	#[test]
	fn malformed_names_fail_individual_batch_requests() {
		let server = server();
		let lookup_header = fuse_in_header {
			gid: 0,
			len: 0,
			nodeid: 0,
			opcode: sys::fuse_opcode_FUSE_LOOKUP,
			padding: 0,
			pid: 0,
			total_extlen: 0,
			uid: 0,
			unique: 1,
		};
		let invalid_name = CString::new(vec![0xff]).unwrap();
		let getxattr_header = fuse_in_header {
			opcode: sys::fuse_opcode_FUSE_GETXATTR,
			unique: 2,
			..lookup_header
		};
		let statfs_header = fuse_in_header {
			opcode: sys::fuse_opcode_FUSE_STATFS,
			unique: 3,
			..lookup_header
		};
		let requests = [
			PendingRequest {
				request: Request {
					data: RequestData::Lookup(invalid_name.clone()),
					header: lookup_header,
				},
				slot: 0,
			},
			PendingRequest {
				request: Request {
					data: RequestData::GetXattr(
						fuse_getxattr_in {
							padding: 0,
							size: 0,
						},
						invalid_name,
					),
					header: getxattr_header,
				},
				slot: 1,
			},
			PendingRequest {
				request: Request {
					data: RequestData::Statfs,
					header: statfs_header,
				},
				slot: 2,
			},
		];
		let mut results = Vec::new();
		server
			.handle_request_sync_batch(-1, &requests, &mut results)
			.unwrap();

		assert_eq!(results.len(), 3);
		assert_eq!(
			results[0].as_ref().unwrap_err().raw_os_error(),
			Some(libc::ENOENT)
		);
		assert_eq!(
			results[1].as_ref().unwrap_err().raw_os_error(),
			Some(libc::ENODATA)
		);
		assert!(matches!(results[2], Ok(Some(Response::Statfs(_)))));
	}

	#[test]
	fn required_passthrough_failure_closes_provider_handle() {
		let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let server = server_with_provider(
			PassthroughProvider {
				closes: closes.clone(),
			},
			true,
			true,
		);
		let header = fuse_in_header {
			gid: 0,
			len: 0,
			nodeid: 1,
			opcode: sys::fuse_opcode_FUSE_OPEN,
			padding: 0,
			pid: 0,
			total_extlen: 0,
			uid: 0,
			unique: 1,
		};
		let request = Request {
			data: RequestData::Open(fuse_open_in {
				flags: 0,
				open_flags: 0,
			}),
			header,
		};
		let response = ProviderResponse::Open {
			backing_fd: None,
			handle: 7,
		};
		let error = server
			.map_provider_batch_response(-1, &request, response)
			.unwrap_err();

		assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP));
		assert_eq!(closes.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn failed_open_response_closes_provider_handle() {
		let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let server = server_with_provider(
			PassthroughProvider {
				closes: closes.clone(),
			},
			false,
			false,
		);
		let result = Ok(Some(Response::Open(fuse_open_out {
			backing_id: -1,
			fh: 11,
			open_flags: 0,
		})));
		server.register_response_resources(1, &result).unwrap();
		server.rollback_response_resources(-1, 1);

		assert_eq!(closes.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn statx_reports_supported_metadata() {
		let attrs = crate::Attrs {
			atime: crate::Timestamp { nanos: 2, secs: 1 },
			ctime: crate::Timestamp { nanos: 6, secs: 5 },
			gid: 8,
			inner: AttrsInner::Symlink { size: 513 },
			mtime: crate::Timestamp { nanos: 4, secs: 3 },
			uid: 7,
		};
		let attr = Server::<TestProvider>::fuse_attr_out(9, attrs);
		let statx = Server::<TestProvider>::fuse_statx_out(attr, 0);

		assert_eq!(statx.stat.mask, libc::STATX_BASIC_STATS);
		assert_eq!(statx.stat.mask & libc::STATX_BTIME, 0);
		assert_eq!(statx.stat.attributes_mask, 0);
		assert_eq!(statx.stat.atime.tv_sec, 1);
		assert_eq!(statx.stat.atime.tv_nsec, 2);
		assert_eq!(statx.stat.mtime.tv_sec, 3);
		assert_eq!(statx.stat.mtime.tv_nsec, 4);
		assert_eq!(statx.stat.ctime.tv_sec, 5);
		assert_eq!(statx.stat.ctime.tv_nsec, 6);
		assert_eq!(statx.stat.size, 513);
		assert_eq!(statx.stat.blocks, 2);
	}

	#[test]
	fn writable_opens_return_erofs() {
		let request = |flags| fuse_open_in {
			flags,
			open_flags: 0,
		};
		Server::<TestProvider>::validate_open_request(request(libc::O_RDONLY.to_u32().unwrap()))
			.unwrap();
		for flags in [libc::O_WRONLY, libc::O_RDWR] {
			let error =
				Server::<TestProvider>::validate_open_request(request(flags.to_u32().unwrap()))
					.unwrap_err();
			assert_eq!(error.raw_os_error(), Some(libc::EROFS));
		}
	}

	#[tokio::test]
	async fn read_write_dispatcher_handles_concurrency_and_cancellation() {
		let release = Arc::new(tokio::sync::Notify::new());
		let started = Arc::new(tokio::sync::Notify::new());
		let server = server_with_provider(
			SlowProvider {
				release: release.clone(),
				started: started.clone(),
			},
			false,
			false,
		);
		let (response_reader, response_writer) = rustix::net::socketpair(
			AddressFamily::UNIX,
			SocketType::STREAM,
			SocketFlags::CLOEXEC,
			None,
		)
		.unwrap();
		let response_reader = Arc::new(response_reader);
		let response_writer = Arc::new(response_writer);
		let (event_sender, _event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let (request_sender, request_receiver) = tokio::sync::mpsc::channel(4);
		let dispatcher = server.spawn_read_write_dispatcher(request_receiver, event_sender);
		let read = |unique| Request {
			data: RequestData::Read(fuse_read_in {
				fh: 1,
				flags: 0,
				lock_owner: 0,
				offset: 0,
				padding: 0,
				read_flags: 0,
				size: 4,
			}),
			header: request_header(sys::fuse_opcode_FUSE_READ, unique),
		};
		let getattr = |unique| Request {
			data: RequestData::GetAttr(fuse_getattr_in {
				dummy: 0,
				fh: 0,
				getattr_flags: 0,
			}),
			header: request_header(sys::fuse_opcode_FUSE_GETATTR, unique),
		};

		let token = server.register_async_request(1).unwrap();
		request_sender
			.send(read_write::ReadWriteRequest {
				fd: response_writer.clone(),
				request: read(1),
				token,
			})
			.await
			.unwrap();
		tokio::time::timeout(Duration::from_secs(2), started.notified())
			.await
			.unwrap();
		let token = server.register_async_request(2).unwrap();
		request_sender
			.send(read_write::ReadWriteRequest {
				fd: response_writer.clone(),
				request: getattr(2),
				token,
			})
			.await
			.unwrap();
		let response = read_response(response_reader.clone()).await;
		assert_eq!(response.unique, 2);
		assert_eq!(response.error, 0);
		release.notify_one();
		let response = read_response(response_reader.clone()).await;
		assert_eq!(response.unique, 1);
		assert_eq!(response.error, 0);

		let token = server.register_async_request(3).unwrap();
		request_sender
			.send(read_write::ReadWriteRequest {
				fd: response_writer.clone(),
				request: read(3),
				token,
			})
			.await
			.unwrap();
		tokio::time::timeout(Duration::from_secs(2), started.notified())
			.await
			.unwrap();
		assert!(server.cancel_async_request(3));
		let response = read_response(response_reader.clone()).await;
		assert_eq!(response.unique, 3);
		assert_eq!(response.error, -libc::EINTR);
		let token = server.register_async_request(4).unwrap();
		request_sender
			.send(read_write::ReadWriteRequest {
				fd: response_writer,
				request: getattr(4),
				token,
			})
			.await
			.unwrap();
		let response = read_response(response_reader).await;
		assert_eq!(response.unique, 4);
		assert_eq!(response.error, 0);

		drop(request_sender);
		dispatcher.await.unwrap();
	}

	#[test]
	fn unmatched_interrupts_return_eagain() {
		let server = server_with_provider(TestProvider, false, false);
		let header = request_header(sys::fuse_opcode_FUSE_INTERRUPT, 2);
		let request = fuse_interrupt_in { unique: 1 };
		let error = server
			.handle_interrupt_request_sync(header, request)
			.unwrap_err();
		assert_eq!(error.raw_os_error(), Some(libc::EAGAIN));

		let token = server.register_async_request(1).unwrap();
		let response = server
			.handle_interrupt_request_sync(header, request)
			.unwrap();
		assert!(matches!(response, Some(Response::Interrupt)));
		assert!(token.is_cancelled());
	}

	#[test]
	fn eventfd_failures_are_reported_to_the_supervisor() {
		let eventfd: OwnedFd = std::fs::File::open("/dev/null").unwrap().into();
		let pending = AtomicBool::new(false);
		let (sender, receiver) = crossbeam_channel::unbounded();
		let (worker_event_sender, mut worker_event_receiver) =
			tokio::sync::mpsc::unbounded_channel();
		let response = AsyncResponse {
			opcode: sys::fuse_opcode_FUSE_GETATTR,
			result: Err(Error::from_raw_os_error(libc::EIO)),
			slot: 0,
			unique: 1,
		};

		notify_async_response(
			&pending,
			eventfd.as_raw_fd(),
			response,
			&sender,
			&worker_event_sender,
			0,
		);

		assert!(receiver.try_recv().is_ok());
		assert!(!pending.load(Ordering::Acquire));
		let event = worker_event_receiver.try_recv().unwrap();
		assert!(matches!(event, WorkerEvent::Failed { .. }));
	}
}
