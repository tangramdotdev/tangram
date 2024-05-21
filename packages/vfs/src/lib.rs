use bytes::Bytes;
use futures::Future;

pub mod fuse;
pub mod nfs;
pub mod options;

pub type Result<T> = std::io::Result<T>;
pub const ROOT_NODE_ID: u64 = 1;

/// A virtual filesystem provider.
pub trait Provider {
	/// Look up a node.
	fn lookup(&self, id: u64, name: &str) -> impl Future<Output = Result<Option<u64>>> + Send;

	/// Look up a node's parent.
	fn lookup_parent(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Get the attributes for a node.
	fn getattr(&self, id: u64) -> impl Future<Output = Result<Attrs>> + Send;

	/// List extended attributes for a node.
	fn listxattrs(&self, id: u64) -> impl Future<Output = Result<Vec<String>>> + Send;

	/// Get the value for an extended attribute for a node.
	fn getxattr(&self, id: u64, name: &str) -> impl Future<Output = Option<String>> + Send;

	/// Open a directory.
	fn opendir(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Read from a directory.
	fn readdir(&self, id: u64) -> impl Future<Output = Result<Vec<(String, u64)>>> + Send;

	/// Open a file.
	fn open(&self, id: u64) -> impl Future<Output = Result<u64>> + Send;

	/// Read from a file.
	fn read(
		&self,
		handle: u64,
		position: u64,
		length: u64,
	) -> impl Future<Output = Result<Bytes>> + Send;

	/// Read from a symlink.
	fn readlink(&self, id: u64) -> impl Future<Output = Result<Bytes>> + Send;

	/// Close an open file handle.
	fn close(&self, id: u64) -> impl Future<Output = ()> + Send;
}

#[derive(Copy, Clone, Debug)]
pub enum FileType {
	File { executable: bool, size: u64 },
	Directory,
	Symlink,
}

/// Represents a set of  file attributes.
#[derive(Copy, Clone, Debug)]
pub struct Attrs {
	pub typ: FileType,
	pub atime: TimeSpec,
	pub mtime: TimeSpec,
	pub ctime: TimeSpec,
	pub uid: u32,
	pub gid: u32,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct TimeSpec {
	pub secs: u64,
	pub nanos: u32,
}

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
