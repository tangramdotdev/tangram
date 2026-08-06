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
			AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags,
			SendAncillaryBuffer, SendAncillaryMessage, SendFlags, SocketFlags, SocketType,
		},
	},
	std::{
		collections::{BTreeSet, HashMap, VecDeque},
		ffi::{CString, OsString},
		io::Error,
		mem::{MaybeUninit, size_of},
		ops::Deref,
		os::fd::{AsFd as _, AsRawFd as _, OwnedFd},
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
mod startup;
#[cfg(test)]
mod tests;

pub mod sys;

const DEFAULT_MAX_WRITE: usize = 1024 * 1024;
const FUSE_DEV_IOC_MAGIC: u8 = 229;
const FUSE_DEV_IOC_CLONE: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::read::<u32>(FUSE_DEV_IOC_MAGIC, 0);
const FUSE_DEV_IOC_BACKING_OPEN: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::write::<sys::fuse_backing_map>(FUSE_DEV_IOC_MAGIC, 1);
const FUSE_DEV_IOC_BACKING_CLOSE: rustix::ioctl::Opcode =
	rustix::ioctl::opcode::write::<u32>(FUSE_DEV_IOC_MAGIC, 2);
const FUSE_MOUNT_READY_TIMEOUT: Duration = Duration::from_secs(5);
const S_IFDIR: u32 = 0o040_000;
const S_IFREG: u32 = 0o100_000;
const S_IFLNK: u32 = 0o120_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Options {
	pub io: Io,
	pub passthrough: Passthrough,
	pub sqpoll: bool,
}

impl Default for Options {
	fn default() -> Self {
		Self {
			io: Io::default(),
			passthrough: Passthrough::default(),
			sqpoll: true,
		}
	}
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
	auto_unmount: bool,
	no_opendir_support: bool,
	passthrough_backings: Mutex<PassthroughBackings>,
	passthrough_enabled: bool,
	passthrough_permission_warning_emitted: AtomicBool,
	passthrough_required: bool,
	pending_response_resources: Mutex<HashMap<u64, ResponseResources>>,
	provider: P,
	task: Mutex<Option<tangram_futures::task::Shared<()>>>,
}

#[derive(Default)]
struct PassthroughBackings {
	handles: HashMap<u64, u64>,
	nodes: HashMap<u64, PassthroughBacking>,
}

struct PassthroughBacking {
	id: u32,
	references: u64,
}

#[derive(Clone, Debug)]
struct Request {
	data: RequestData,
	header: sys::fuse_in_header,
}

#[derive(Clone, Debug)]
struct PendingRequest {
	request: Request,
	slot: usize,
}

enum WorkerEvent {
	Failed { error: Error, worker: String },
	Ready,
}

enum Dispatch {
	Deferred,
	Ready(Result<Response>),
}

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

// The field order matches the FUSE protocol ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseInitInV7p1 {
	major: u32,
	minor: u32,
}

// The field order matches the FUSE protocol ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseInitInV7p6 {
	major: u32,
	minor: u32,
	max_readahead: u32,
	flags: u32,
}

// The field order matches the FUSE protocol ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseDirentHeader {
	ino: u64,
	off: u64,
	namelen: u32,
	type_: u32,
}

// The field order matches the FUSE protocol ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
struct FuseDirentPlusHeader {
	entry_out: fuse_entry_out,
	dirent: FuseDirentHeader,
}

struct IoctlPointerInt<'a, const OPCODE: rustix::ioctl::Opcode, T> {
	value: &'a mut T,
}

pub fn mount_dev_fuse(sendfd: &OwnedFd, path: &Path) -> std::io::Result<()> {
	// Open the FUSE device.
	let fd: OwnedFd = std::fs::File::options()
		.read(true)
		.write(true)
		.open("/dev/fuse")?
		.into();

	// Mount the filesystem.
	let uid = rustix::process::getuid().as_raw();
	let gid = rustix::process::getgid().as_raw();
	let options = format!(
		"fd={},rootmode=40755,user_id={uid},group_id={gid},default_permissions",
		fd.as_raw_fd(),
	);
	let options = CString::new(options).map_err(|_| Error::other("invalid mount options"))?;
	rustix::mount::mount(
		"/dev/fuse",
		path,
		"fuse",
		rustix::mount::MountFlags::RDONLY
			| rustix::mount::MountFlags::NOSUID
			| rustix::mount::MountFlags::NODEV,
		options.as_c_str(),
	)
	.map_err(Error::from)?;

	// Find the connection ID in the mount table. Statting the mountpoint instead would block until the server answers the init request, which it cannot do before it receives the descriptor.
	let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
	let connection_id = connection::parse_connection_id(&mountinfo, path)?;

	// Send the descriptor and the connection ID to the server.
	let payload = connection_id.to_le_bytes();
	let iovecs = [IoSlice::new(&payload)];
	let fds = [fd.as_fd()];
	let mut cmsg_space = [MaybeUninit::<u8>::uninit(); rustix::cmsg_space!(ScmRights(1))];
	let mut cmsg_buffer = SendAncillaryBuffer::new(&mut cmsg_space);
	cmsg_buffer.push(SendAncillaryMessage::ScmRights(&fds));
	rustix::net::sendmsg(sendfd, &iovecs, &mut cmsg_buffer, SendFlags::NOSIGNAL)
		.map_err(Error::from)?;

	// Wait for the server to start the transport workers.
	let mut ready = [0u8; 1];
	let size = loop {
		match rustix::io::read(sendfd, &mut ready) {
			Err(Errno::INTR) => {},
			Err(error) => return Err(Error::from(error)),
			Ok(size) => break size,
		}
	};
	if size != 1 {
		return Err(Error::other(
			"the FUSE server stopped before starting the transport",
		));
	}

	// Confirm that the server can service a request through the mounted filesystem.
	let mut entries = std::fs::read_dir(path)?;
	entries.next().transpose()?;

	// Signal that the mount is ready.
	let size = rustix::net::send(sendfd, &[0], SendFlags::NOSIGNAL).map_err(Error::from)?;
	if size != 1 {
		return Err(Error::other(
			"failed to signal that the FUSE mount is ready",
		));
	}

	Ok(())
}

pub fn fusermount3(path: &Path) -> std::io::Result<OwnedFd> {
	// Create the helper control socket.
	let (fuse_commfd, recvfd) = rustix::net::socketpair(
		AddressFamily::UNIX,
		SocketType::STREAM,
		SocketFlags::CLOEXEC,
		None,
	)
	.map_err(Error::from)?;

	// Start the mount helper.
	let commfd_raw = fuse_commfd.as_raw_fd();
	let uid = rustix::process::getuid().as_raw();
	let gid = rustix::process::getgid().as_raw();
	let options = format!("rootmode=40755,user_id={uid},group_id={gid},default_permissions,ro");
	// SAFETY: The pre_exec closure only calls async-signal-safe operations.
	let child = unsafe {
		std::process::Command::new("fusermount3")
			.args(["-o", &options, "--"])
			.arg(path)
			.env("_FUSE_COMMFD", commfd_raw.to_string())
			.stdin(std::process::Stdio::null())
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::piped())
			.pre_exec(move || {
				// Clear CLOEXEC on the communication FD so fusermount3 inherits it.
				rustix::io::fcntl_setfd(&fuse_commfd, FdFlags::empty()).map_err(Error::from)?;
				Ok(())
			})
			.spawn()?
	};

	// Reap the mount helper and log its error output if it fails.
	std::thread::spawn(move || {
		if let Ok(output) = child.wait_with_output()
			&& !output.status.success()
		{
			let stderr = String::from_utf8_lossy(&output.stderr);
			tracing::error!(status = %output.status, stderr = %stderr.trim(), "the FUSE mount helper failed");
		}
	});

	Ok(recvfd)
}

impl<'a, const OPCODE: rustix::ioctl::Opcode, T> IoctlPointerInt<'a, OPCODE, T> {
	#[must_use]
	const fn new(value: &'a mut T) -> Self {
		Self { value }
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
