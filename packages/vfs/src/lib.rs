use {bytes::Bytes, futures::Future, std::os::fd::OwnedFd};

pub mod fuse;
pub mod nfs;

pub const ROOT_NODE_ID: u64 = 1;

/// A virtual filesystem provider.
pub trait Provider {
	/// Close an open file handle.
	fn close(&self, handle: u64) -> impl Future<Output = ()> + Send;

	/// Close an open file handle synchronously.
	fn close_sync(&self, _handle: u64) {}

	/// Get the attributes for a node.
	fn getattr(&self, id: u64) -> impl Future<Output = Result<Attrs>> + Send;

	/// Get the attributes for a node synchronously.
	fn getattr_sync(&self, _id: u64) -> Result<Attrs> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Get the value for an extended attribute for a node.
	fn getxattr(&self, id: u64, name: &str) -> impl Future<Output = Result<Option<Bytes>>> + Send;

	/// Get the value for an extended attribute for a node synchronously.
	fn getxattr_sync(&self, _id: u64, _name: &str) -> Result<Option<Bytes>> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// List extended attributes for a node.
	fn listxattrs(&self, id: u64) -> impl Future<Output = Result<Vec<String>>> + Send;

	/// List extended attributes for a node synchronously.
	fn listxattrs_sync(&self, _id: u64) -> Result<Vec<String>> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Look up a node.
	fn lookup(&self, id: u64, name: &str) -> impl Future<Output = Result<Option<u64>>> + Send;

	/// Look up a node synchronously.
	fn lookup_sync(&self, _id: u64, _name: &str) -> Result<Option<u64>> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Look up a node's parent.
	fn lookup_parent(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Look up a node's parent synchronously.
	fn lookup_parent_sync(&self, _id: u64) -> Result<u64> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Record one kernel lookup reference for a node.
	fn remember_sync(&self, _id: u64) {}

	/// Drop kernel lookup references for a node.
	fn forget_sync(&self, _id: u64, _nlookup: u64) {}

	/// Open a file.
	fn open(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Open a file synchronously.
	fn open_sync(&self, _id: u64) -> Result<(u64, Option<OwnedFd>)> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Open a directory.
	fn opendir(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Open a directory synchronously.
	fn opendir_sync(&self, _id: u64) -> Result<u64> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Read from a file.
	fn read(
		&self,
		handle: u64,
		position: u64,
		length: u64,
	) -> impl Future<Output = Result<Bytes>> + Send;

	/// Read from a file synchronously.
	fn read_sync(&self, _handle: u64, _position: u64, _length: u64) -> Result<Bytes> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Read from a directory.
	fn readdir(&self, handle: u64) -> impl Future<Output = Result<Vec<(String, u64)>>> + Send;

	/// Read from a directory synchronously.
	fn readdir_sync(&self, _handle: u64) -> Result<Vec<(String, u64)>> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	/// Read from a symlink.
	fn readlink(&self, id: u64) -> impl Future<Output = Result<Bytes>> + Send;

	/// Read from a symlink synchronously.
	fn readlink_sync(&self, _id: u64) -> Result<Bytes> {
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}
}

#[derive(Clone, Copy, Debug)]
pub enum FileType {
	File { executable: bool, size: u64 },
	Directory,
	Symlink,
}

/// Represents a set of  file attributes.
#[derive(Clone, Copy, Debug)]
pub struct Attrs {
	pub typ: FileType,
	pub atime: TimeSpec,
	pub mtime: TimeSpec,
	pub ctime: TimeSpec,
	pub uid: u32,
	pub gid: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TimeSpec {
	pub secs: u64,
	pub nanos: u32,
}

pub type Result<T> = std::io::Result<T>;

impl Attrs {
	#[must_use]
	pub fn new(typ: FileType) -> Self {
		Self {
			typ,
			atime: TimeSpec::default(),
			mtime: TimeSpec::default(),
			ctime: TimeSpec::default(),
			uid: unsafe { libc::getuid() },
			gid: unsafe { libc::getgid() },
		}
	}
}
