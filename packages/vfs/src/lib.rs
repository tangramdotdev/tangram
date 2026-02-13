use {bytes::Bytes, futures::Future, std::io::Error, std::os::fd::OwnedFd};

pub mod fuse;
pub mod nfs;

pub const ROOT_NODE_ID: u64 = 1;

#[derive(Clone, Debug)]
pub enum Request {
	Close {
		handle: u64,
	},
	Forget {
		id: u64,
		nlookup: u64,
	},
	GetAttr {
		id: u64,
	},
	GetXattr {
		id: u64,
		name: String,
	},
	ListXattrs {
		id: u64,
	},
	Lookup {
		id: u64,
		name: String,
	},
	LookupParent {
		id: u64,
	},
	Open {
		id: u64,
	},
	OpenDir {
		id: u64,
	},
	Read {
		handle: u64,
		position: u64,
		length: u64,
	},
	ReadDir {
		handle: u64,
	},
	ReadDirPlus {
		handle: u64,
	},
	ReadLink {
		id: u64,
	},
	Remember {
		id: u64,
	},
}

#[derive(Debug)]
pub enum Response {
	GetAttr {
		attrs: Attrs,
	},
	GetXattr {
		value: Option<Bytes>,
	},
	ListXattrs {
		names: Vec<String>,
	},
	Lookup {
		id: Option<u64>,
	},
	LookupParent {
		id: u64,
	},
	Open {
		handle: u64,
		backing_fd: Option<OwnedFd>,
	},
	OpenDir {
		handle: u64,
	},
	Read {
		bytes: Bytes,
	},
	ReadDir {
		entries: Vec<(String, u64, DirEntryType)>,
	},
	ReadDirPlus {
		entries: Vec<(String, u64, Attrs)>,
	},
	ReadLink {
		target: Bytes,
	},
	Unit,
}

/// A virtual filesystem provider.
pub trait Provider {
	fn handle_batch(
		&self,
		requests: Vec<Request>,
	) -> impl Future<Output = Vec<Result<Response>>> + Send;

	fn handle_batch_sync(&self, requests: Vec<Request>) -> Vec<Result<Response>>;

	/// Close an open file handle.
	fn close(&self, handle: u64) -> impl Future<Output = ()> + Send
	where
		Self: Sync,
	{
		async move {
			let _ = self.handle_batch(vec![Request::Close { handle }]).await;
		}
	}

	/// Close an open file handle synchronously.
	fn close_sync(&self, handle: u64) {
		let _ = self.handle_batch_sync(vec![Request::Close { handle }]);
	}

	/// Get the attributes for a node.
	fn getattr(&self, id: u64) -> impl Future<Output = Result<Attrs>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::GetAttr { id }]).await)?;
			match response {
				Response::GetAttr { attrs } => Ok(attrs),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Get the attributes for a node synchronously.
	fn getattr_sync(&self, id: u64) -> Result<Attrs> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::GetAttr { id }]))?;
		match response {
			Response::GetAttr { attrs } => Ok(attrs),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Get the value for an extended attribute for a node.
	fn getxattr(&self, id: u64, name: &str) -> impl Future<Output = Result<Option<Bytes>>> + Send
	where
		Self: Sync,
	{
		let name = name.to_owned();
		async move {
			let response = take_single_response(
				self.handle_batch(vec![Request::GetXattr { id, name }])
					.await,
			)?;
			match response {
				Response::GetXattr { value } => Ok(value),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Get the value for an extended attribute for a node synchronously.
	fn getxattr_sync(&self, id: u64, name: &str) -> Result<Option<Bytes>> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::GetXattr {
			id,
			name: name.to_owned(),
		}]))?;
		match response {
			Response::GetXattr { value } => Ok(value),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// List extended attributes for a node.
	fn listxattrs(&self, id: u64) -> impl Future<Output = Result<Vec<String>>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::ListXattrs { id }]).await)?;
			match response {
				Response::ListXattrs { names } => Ok(names),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// List extended attributes for a node synchronously.
	fn listxattrs_sync(&self, id: u64) -> Result<Vec<String>> {
		let response =
			take_single_response(self.handle_batch_sync(vec![Request::ListXattrs { id }]))?;
		match response {
			Response::ListXattrs { names } => Ok(names),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Look up a node.
	fn lookup(&self, id: u64, name: &str) -> impl Future<Output = Result<Option<u64>>> + Send
	where
		Self: Sync,
	{
		let name = name.to_owned();
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::Lookup { id, name }]).await)?;
			match response {
				Response::Lookup { id } => Ok(id),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Look up a node synchronously.
	fn lookup_sync(&self, id: u64, name: &str) -> Result<Option<u64>> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::Lookup {
			id,
			name: name.to_owned(),
		}]))?;
		match response {
			Response::Lookup { id } => Ok(id),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Look up a node's parent.
	fn lookup_parent(&self, id: u64) -> impl Future<Output = Result<u64>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::LookupParent { id }]).await)?;
			match response {
				Response::LookupParent { id } => Ok(id),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Look up a node's parent synchronously.
	fn lookup_parent_sync(&self, id: u64) -> Result<u64> {
		let response =
			take_single_response(self.handle_batch_sync(vec![Request::LookupParent { id }]))?;
		match response {
			Response::LookupParent { id } => Ok(id),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Record one kernel lookup reference for a node.
	fn remember_sync(&self, id: u64) {
		let _ = self.handle_batch_sync(vec![Request::Remember { id }]);
	}

	/// Drop kernel lookup references for a node.
	fn forget_sync(&self, id: u64, nlookup: u64) {
		let _ = self.handle_batch_sync(vec![Request::Forget { id, nlookup }]);
	}

	/// Open a file.
	fn open(&self, id: u64) -> impl Future<Output = Result<u64>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::Open { id }]).await)?;
			match response {
				Response::Open { handle, .. } => Ok(handle),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Open a file synchronously.
	fn open_sync(&self, id: u64) -> Result<(u64, Option<OwnedFd>)> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::Open { id }]))?;
		match response {
			Response::Open { handle, backing_fd } => Ok((handle, backing_fd)),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Open a directory.
	fn opendir(&self, id: u64) -> impl Future<Output = Result<u64>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::OpenDir { id }]).await)?;
			match response {
				Response::OpenDir { handle } => Ok(handle),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Open a directory synchronously.
	fn opendir_sync(&self, id: u64) -> Result<u64> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::OpenDir { id }]))?;
		match response {
			Response::OpenDir { handle } => Ok(handle),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Read from a file.
	fn read(
		&self,
		handle: u64,
		position: u64,
		length: u64,
	) -> impl Future<Output = Result<Bytes>> + Send
	where
		Self: Sync,
	{
		async move {
			let response = take_single_response(
				self.handle_batch(vec![Request::Read {
					handle,
					position,
					length,
				}])
				.await,
			)?;
			match response {
				Response::Read { bytes } => Ok(bytes),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Read from a file synchronously.
	fn read_sync(&self, handle: u64, position: u64, length: u64) -> Result<Bytes> {
		let response = take_single_response(self.handle_batch_sync(vec![Request::Read {
			handle,
			position,
			length,
		}]))?;
		match response {
			Response::Read { bytes } => Ok(bytes),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Read from a directory.
	fn readdir(
		&self,
		handle: u64,
	) -> impl Future<Output = Result<Vec<(String, u64, DirEntryType)>>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::ReadDir { handle }]).await)?;
			match response {
				Response::ReadDir { entries } => Ok(entries),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Read from a directory synchronously.
	fn readdir_sync(&self, handle: u64) -> Result<Vec<(String, u64, DirEntryType)>> {
		let response =
			take_single_response(self.handle_batch_sync(vec![Request::ReadDir { handle }]))?;
		match response {
			Response::ReadDir { entries } => Ok(entries),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Read from a directory with attributes.
	fn readdirplus(
		&self,
		handle: u64,
	) -> impl Future<Output = Result<Vec<(String, u64, Attrs)>>> + Send
	where
		Self: Sync,
	{
		async move {
			let response = take_single_response(
				self.handle_batch(vec![Request::ReadDirPlus { handle }])
					.await,
			)?;
			match response {
				Response::ReadDirPlus { entries } => Ok(entries),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Read from a directory with attributes synchronously.
	fn readdirplus_sync(&self, handle: u64) -> Result<Vec<(String, u64, Attrs)>> {
		let response =
			take_single_response(self.handle_batch_sync(vec![Request::ReadDirPlus { handle }]))?;
		match response {
			Response::ReadDirPlus { entries } => Ok(entries),
			_ => Err(Error::other("unexpected response variant")),
		}
	}

	/// Read from a symlink.
	fn readlink(&self, id: u64) -> impl Future<Output = Result<Bytes>> + Send
	where
		Self: Sync,
	{
		async move {
			let response =
				take_single_response(self.handle_batch(vec![Request::ReadLink { id }]).await)?;
			match response {
				Response::ReadLink { target } => Ok(target),
				_ => Err(Error::other("unexpected response variant")),
			}
		}
	}

	/// Read from a symlink synchronously.
	fn readlink_sync(&self, id: u64) -> Result<Bytes> {
		let response =
			take_single_response(self.handle_batch_sync(vec![Request::ReadLink { id }]))?;
		match response {
			Response::ReadLink { target } => Ok(target),
			_ => Err(Error::other("unexpected response variant")),
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub enum FileType {
	File { executable: bool, size: u64 },
	Directory,
	Symlink,
}

#[derive(Clone, Copy, Debug)]
pub enum DirEntryType {
	File,
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

fn take_single_response(mut responses: Vec<Result<Response>>) -> Result<Response> {
	if responses.len() != 1 {
		return Err(Error::other("expected exactly one response"));
	}
	responses.remove(0)
}

impl Attrs {
	#[must_use]
	pub fn new(typ: FileType) -> Self {
		Self {
			typ,
			atime: TimeSpec::default(),
			mtime: TimeSpec::default(),
			ctime: TimeSpec::default(),
			uid: rustix::process::getuid().as_raw(),
			gid: rustix::process::getgid().as_raw(),
		}
	}
}
