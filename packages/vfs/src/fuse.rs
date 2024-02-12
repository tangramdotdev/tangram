use either::Either;
use fnv::FnvBuildHasher;
use num::ToPrimitive;
use std::{
	collections::HashMap,
	ffi::CString,
	io::{Read, SeekFrom, Write},
	os::{fd::FromRawFd, unix::prelude::OsStrExt},
	path::{Path, PathBuf},
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_error::{Result, Wrap, WrapErr};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use zerocopy::{AsBytes, FromBytes};
mod sys;

/// A FUSE server.
#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

type Map<K, V> = HashMap<K, V, FnvBuildHasher>;

struct Inner {
	path: std::path::PathBuf,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	tg: Box<dyn tg::Handle>,
	nodes: parking_lot::RwLock<Map<NodeId, Arc<Node>>>,
	node_index: std::sync::atomic::AtomicU64,
	handles: parking_lot::RwLock<Map<FileHandle, Arc<tokio::sync::RwLock<FileHandleData>>>>,
	handle_index: std::sync::atomic::AtomicU64,
}

/// A node in the file system.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
struct NodeId(u64);

/// The root node has ID 1.
const ROOT_NODE_ID: NodeId = NodeId(1);

/// A node.
#[derive(Debug)]
struct Node {
	id: NodeId,
	parent: Weak<Node>,
	kind: NodeKind,
}

/// An node's kind.
#[derive(Debug)]
enum NodeKind {
	Root {
		children: parking_lot::RwLock<Map<String, Arc<Node>>>,
	},
	Directory {
		directory: tg::Directory,
		children: parking_lot::RwLock<Vec<(String, Arc<Node>)>>,
	},
	File {
		file: tg::File,
		size: u64,
	},
	Symlink {
		symlink: tg::Symlink,
	},
	Checkout {
		path: PathBuf,
	},
}

/// A file handle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FileHandle(u64);

/// The data associated with a file handle.
enum FileHandleData {
	Directory,
	File {
		node: NodeId,
		reader: tg::blob::Reader,
	},
	Symlink,
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
	None,
	Open(sys::fuse_open_out),
	OpenDir(sys::fuse_open_out),
	Read(Vec<u8>),
	ReadDir(Vec<u8>),
	ReadDirPlus(Vec<u8>),
	ReadLink(CString),
	Release,
	ReleaseDir,
}

impl Server {
	pub async fn start(tg: &dyn tg::Handle, path: &Path) -> Result<Self> {
		// Create the server.
		let root = Arc::new_cyclic(|root| Node {
			id: ROOT_NODE_ID,
			parent: root.clone(),
			kind: NodeKind::Root {
				children: parking_lot::RwLock::new(Map::default()),
			},
		});
		let nodes = [(ROOT_NODE_ID, root)].into_iter().collect::<Map<_, _>>();
		let nodes = parking_lot::RwLock::new(nodes);
		let node_index = std::sync::atomic::AtomicU64::new(1000);
		let handles = parking_lot::RwLock::new(Map::default());
		let handle_index = std::sync::atomic::AtomicU64::new(0);
		let tg = tg.clone_box();
		let task = std::sync::Mutex::new(None);
		let server = Self {
			inner: Arc::new(Inner {
				path: path.to_owned(),
				task,
				tg,
				node_index,
				nodes,
				handle_index,
				handles,
			}),
		};

		// Mount.
		let fuse_file = Self::mount(path).await?;
		// let fuse_file = tokio::fs::File::from_std(fuse_file);

		// Spawn the task.
		let task = tokio::spawn({
			let server = server.clone();
			async move { server.serve(fuse_file).await }
		});
		server.inner.task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		if let Some(task) = self.inner.task.lock().unwrap().as_ref() {
			task.abort_handle().abort();
		};
	}

	pub async fn join(&self) -> Result<()> {
		// Join the task.
		let task = self.inner.task.lock().unwrap().take();
		if let Some(task) = task {
			match task.await {
				Ok(result) => Ok(result),
				Err(error) if error.is_cancelled() => Ok(Ok(())),
				Err(error) => Err(error),
			}
			.unwrap()?;
		}

		// Unmount.
		Self::unmount(&self.inner.path).await?;

		Ok(())
	}

	#[allow(clippy::too_many_lines)]
	async fn serve(&self, mut fuse_file: std::fs::File) -> Result<()> {
		// Create a buffer to read requests into.
		let mut request_buffer = vec![0u8; 1024 * 1024 + 4096];

		// Handle each request.
		let server = self.clone();
		tokio::task::spawn_blocking(move || {
			loop {
				// Read a request from the FUSE file.
				let request_size = match fuse_file.read(request_buffer.as_mut()) {
					Ok(request_size) => request_size,

					// Handle an error reading the request from the FUSE file.
					Err(error) => match error.raw_os_error() {
						// If the error is ENOENT, EINTR, or EAGAIN, then continue.
						Some(libc::ENOENT | libc::EINTR | libc::EAGAIN) => continue,

						// If the error is ENODEV, then the FUSE file has been unmounted.
						Some(libc::ENODEV) => return Ok(()),

						// Otherwise, return the error.
						_ => return Err(error.wrap("Failed to read the request.")),
					},
				};
				let request_bytes = &request_buffer[..request_size];

				// Deserialize the request.
				let request_header = sys::fuse_in_header::read_from_prefix(request_bytes)
					.wrap_err("Failed to deserialize the request header.")?;
				let request_header_len = std::mem::size_of::<sys::fuse_in_header>();
				let request_data = &request_bytes[request_header_len..];
				let request_data = match request_header.opcode {
					sys::fuse_opcode::FUSE_DESTROY => {
						break;
					},
					sys::fuse_opcode::FUSE_FLUSH => RequestData::Flush(read_data(request_data)?),
					sys::fuse_opcode::FUSE_FORGET => RequestData::Forget(read_data(request_data)?),
					sys::fuse_opcode::FUSE_BATCH_FORGET => {
						RequestData::BatchForget(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_GETATTR => {
						RequestData::GetAttr(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_INIT => RequestData::Init(read_data(request_data)?),
					sys::fuse_opcode::FUSE_LOOKUP => {
						let data = CString::from_vec_with_nul(request_data.to_owned())
							.wrap_err("Failed to deserialize the request.")?;
						RequestData::Lookup(data)
					},
					sys::fuse_opcode::FUSE_OPEN => RequestData::Open(read_data(request_data)?),
					sys::fuse_opcode::FUSE_OPENDIR => {
						RequestData::OpenDir(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_READ => RequestData::Read(read_data(request_data)?),
					sys::fuse_opcode::FUSE_READDIR => {
						RequestData::ReadDir(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_READDIRPLUS => {
						RequestData::ReadDirPlus(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_READLINK => RequestData::ReadLink,
					sys::fuse_opcode::FUSE_RELEASE => {
						RequestData::Release(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_RELEASEDIR => {
						RequestData::ReleaseDir(read_data(request_data)?)
					},
					sys::fuse_opcode::FUSE_GETXATTR => {
						let (fuse_getxattr_in, name) =
							request_data.split_at(std::mem::size_of::<sys::fuse_getxattr_in>());
						let fuse_getxattr_in = read_data(fuse_getxattr_in)?;
						let name = CString::from_vec_with_nul(name.to_owned())
							.wrap_err("Failed to deserialize the request.")?;
						RequestData::GetXattr(fuse_getxattr_in, name)
					},
					sys::fuse_opcode::FUSE_LISTXATTR => {
						RequestData::ListXattr(read_data(request_data)?)
					},
					_ => RequestData::Unsupported(request_header.opcode),
				};
				let request = Request {
					header: request_header,
					data: request_data,
				};

				// Spawn a task to handle the request.
				let mut fuse_file = fuse_file
					.try_clone()
					.wrap_err("Failed to clone the FUSE file.")?;
				let server = server.clone();
				tokio::spawn(async move {
					// Handle the request and get the response.
					let unique = request.header.unique;
					// Edge case: Don't send a reply if the kernel sends a FUSE_FORGET/FUSE_BATCH_FORGET request. We don't support these requests anyway (we cannot forget inodes, and duplicate inodes pointing to the same artifact are OK).
					if [
						sys::fuse_opcode::FUSE_FORGET,
						sys::fuse_opcode::FUSE_BATCH_FORGET,
					]
					.contains(&request.header.opcode)
					{
						tracing::warn!(?request, "Ignoring FORGET/FORGET_BATCH request.");
						return;
					}

					let result = server.handle_request(request).await;
					if let Err(e) = &result {
						tracing::error!(?e, "Request failed.");
					}

					// Serialize the response.
					let response_bytes = match result {
						Err(error) => {
							let len = std::mem::size_of::<sys::fuse_out_header>();
							let header = sys::fuse_out_header {
								unique,
								len: len.to_u32().unwrap(),
								error: -error,
							};
							header.as_bytes().to_owned()
						},
						Ok(data) => {
							let data_bytes = match &data {
								Response::Flush
								| Response::None
								| Response::Release
								| Response::ReleaseDir => &[],
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
							};
							let len =
								std::mem::size_of::<sys::fuse_out_header>() + data_bytes.len();
							let header = sys::fuse_out_header {
								unique,
								len: len.to_u32().unwrap(),
								error: 0,
							};
							let mut buffer = header.as_bytes().to_owned();
							buffer.extend_from_slice(data_bytes);
							buffer
						},
					};

					// Write the response.
					match fuse_file.write_all(&response_bytes) {
						Ok(()) => (),
						Err(error) => {
							tracing::error!(?error, "Failed to write the response.");
						},
					};
				});
			}
			Ok(())
		})
		.await
		.wrap_err("Failed to join task.")??;

		Ok(())
	}

	/// Handle a request.
	async fn handle_request(&self, request: Request) -> Result<Response, i32> {
		match request.data {
			RequestData::Flush(data) => self.handle_flush_request(request.header, data).await,
			RequestData::BatchForget(data) => {
				self.handle_batch_forget_request(request.header, data).await
			},
			RequestData::Forget(data) => self.handle_forget_request(request.header, data).await,
			RequestData::GetAttr(data) => self.handle_get_attr_request(request.header, data).await,
			RequestData::GetXattr(data, name) => {
				self.handle_get_xattr_request(request.header, data, name)
					.await
			},
			RequestData::ListXattr(data) => {
				self.handle_list_xattr_request(request.header, data).await
			},
			RequestData::Init(data) => self.handle_init_request(request.header, data).await,
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
			RequestData::Unsupported(opcode) => {
				self.handle_unsupported_request(request.header, opcode)
					.await
			},
		}
	}

	#[allow(clippy::unused_async)]
	async fn handle_flush_request(
		&self,
		_header: sys::fuse_in_header,
		_data: sys::fuse_flush_in,
	) -> Result<Response, i32> {
		Ok(Response::Flush)
	}

	#[allow(clippy::unused_async)]
	async fn handle_batch_forget_request(
		&self,
		_header: sys::fuse_in_header,
		_data: sys::fuse_batch_forget_in,
	) -> Result<Response, i32> {
		Ok(Response::None)
	}

	#[allow(clippy::unused_async)]
	async fn handle_forget_request(
		&self,
		_header: sys::fuse_in_header,
		_data: sys::fuse_forget_in,
	) -> Result<Response, i32> {
		Ok(Response::None)
	}

	async fn handle_get_attr_request(
		&self,
		header: sys::fuse_in_header,
		_data: sys::fuse_getattr_in,
	) -> Result<Response, i32> {
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;
		let response = node.fuse_attr_out(self.inner.tg.as_ref()).await?;
		Ok(Response::GetAttr(response))
	}

	async fn handle_get_xattr_request(
		&self,
		header: sys::fuse_in_header,
		data: sys::fuse_getxattr_in,
		name: CString,
	) -> Result<Response, i32> {
		// Get the node and check that it's a file.
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;
		let NodeKind::File { file, .. } = &node.kind else {
			return Err(libc::ENOTSUP);
		};
		let attr_name = name.to_str().map_err(|e| {
			tracing::error!(?e, "Failed to get string from xattr name.");
			libc::EINVAL
		})?;

		// Compute the attribute data.
		if attr_name != tg::file::TANGRAM_FILE_XATTR_NAME {
			return Err(libc::ENODATA);
		}
		let file_references = file.references(self.inner.tg.as_ref()).await.map_err(|e| {
			tracing::error!(?e, ?file, "Failed to get file references.");
			libc::EIO
		})?;
		let mut references = Vec::new();
		for artifact in file_references {
			let id = artifact.id(self.inner.tg.as_ref()).await.map_err(|e| {
				tracing::error!(?e, ?artifact, "Failed to get ID of artifact.");
				libc::EIO
			})?;
			references.push(id);
		}

		let attributes = tg::file::Attributes { references };
		let Ok(attributes) = serde_json::to_vec(&attributes) else {
			tracing::error!(?attributes, "Failed to serialize attributes.");
			return Err(libc::EIO);
		};

		// If "0" is passed in as an argument, the caller is requesting the size of this xattr.
		if data.size == 0 {
			let response = sys::fuse_getxattr_out {
				size: attributes.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Response::GetXattr(response))
		}
		// If the size is too small, return ERANGE.
		else if data.size.to_usize().unwrap() < attributes.len() {
			Err(libc::ERANGE)
		} else {
			Ok(Response::GetXattr(attributes))
		}
	}

	async fn handle_list_xattr_request(
		&self,
		header: sys::fuse_in_header,
		data: sys::fuse_getxattr_in,
	) -> Result<Response, i32> {
		// Get the node and check that it's a file.
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;
		let NodeKind::File { .. } = &node.kind else {
			return Err(libc::ENOTSUP);
		};
		let names = CString::new(tg::file::TANGRAM_FILE_XATTR_NAME).map_err(|e| {
			tracing::error!(?e, "Failed to create list of xattr names.");
			libc::EIO
		})?;
		let names = names.as_bytes_with_nul().to_vec();
		if data.size == 0 {
			let response = sys::fuse_getxattr_out {
				size: names.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Response::ListXattr(response))
		} else if data.size.to_usize().unwrap() < names.len() {
			Err(libc::ERANGE)
		} else {
			Ok(Response::ListXattr(names))
		}
	}

	#[allow(clippy::unused_async)]
	async fn handle_init_request(
		&self,
		_header: sys::fuse_in_header,
		data: sys::fuse_init_in,
	) -> Result<Response, i32> {
		let response = sys::fuse_init_out {
			major: 7,
			minor: 21,
			max_readahead: data.max_readahead,
			flags: sys::FUSE_DO_READDIRPLUS,
			max_background: 0,
			congestion_threshold: 0,
			max_write: 1024 * 1024,
			time_gran: 0,
			max_pages: 0,
			map_alignment: 0,
			flags2: 0,
			unused: [0; 7],
		};
		Ok(Response::Init(response))
	}

	async fn handle_lookup_request(
		&self,
		header: sys::fuse_in_header,
		data: CString,
	) -> Result<Response, i32> {
		// Get the parent node.
		let parent_node_id = NodeId(header.nodeid);
		let parent_node = self.get_node(parent_node_id).await?;

		// Get the name as a string.
		let name = String::from_utf8(data.into_bytes()).map_err(|_| libc::ENOENT)?;

		// Get or create the child node.
		let child_node = self.get_or_create_child_node(parent_node, &name).await?;

		// Create the response.
		let response = child_node.fuse_entry_out(self.inner.tg.as_ref()).await?;

		Ok(Response::Lookup(response))
	}

	#[allow(clippy::similar_names)]
	async fn handle_open_request(
		&self,
		header: sys::fuse_in_header,
		_data: sys::fuse_open_in,
	) -> Result<Response, i32> {
		// Get the node.
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;

		// Create the file handle.
		let file_handle_data = match &node.kind {
			NodeKind::Root { .. } => {
				return Err(libc::EPERM);
			},
			NodeKind::Directory { .. } => FileHandleData::Directory,
			NodeKind::File { file, .. } => {
				let reader = file
					.reader(self.inner.tg.as_ref())
					.await
					.map_err(|_| libc::EIO)?;
				FileHandleData::File {
					node: node_id,
					reader,
				}
			},
			NodeKind::Symlink { .. } | NodeKind::Checkout { .. } => FileHandleData::Symlink,
		};
		let file_handle_data = Arc::new(tokio::sync::RwLock::new(file_handle_data));

		// Add the file handle to the state.
		let file_handle = self.next_file_handle().await;
		self.inner
			.handles
			.write()
			.insert(file_handle, file_handle_data);

		// Create the response.
		let open_flags = if matches!(&node.kind, NodeKind::Directory { .. }) {
			sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE
		} else {
			sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE
		};

		let response = sys::fuse_open_out {
			fh: file_handle.0,
			open_flags,
			padding: 0,
		};

		Ok(Response::Open(response))
	}

	async fn handle_open_dir_request(
		&self,
		header: sys::fuse_in_header,
		data: sys::fuse_open_in,
	) -> Result<Response, i32> {
		self.handle_open_request(header, data)
			.await
			.map(|response| match response {
				Response::Open(response) => Response::OpenDir(response),
				_ => unreachable!(),
			})
	}

	async fn handle_read_request(
		&self,
		header: sys::fuse_in_header,
		data: sys::fuse_read_in,
	) -> Result<Response, i32> {
		let node_id = NodeId(header.nodeid);

		// Get the reader.
		let file_handle = FileHandle(data.fh);
		let file_handle_data = self
			.inner
			.handles
			.read()
			.get(&file_handle)
			.ok_or(libc::ENOENT)?
			.clone();

		// Get the reader, sanity checking that the file handle was not corrupted.
		let mut file_handle_data = file_handle_data.write().await;

		let blob_reader = match &mut *file_handle_data {
			FileHandleData::File { node, .. } if node.0 != node_id.0 => {
				tracing::error!(?file_handle, ?node, "File handle corrupted.");
				return Err(libc::EIO);
			},
			FileHandleData::File { reader, .. } => reader,
			FileHandleData::Directory => return Err(libc::EISDIR),
			FileHandleData::Symlink => return Err(libc::EINVAL),
		};

		// Seek to the offset.
		blob_reader
			.seek(SeekFrom::Start(data.offset))
			.await
			.map_err(|_| libc::EIO)?;

		// Read.
		let mut bytes = vec![0u8; data.size.to_usize().unwrap()];
		let mut size = 0;
		while size < data.size.to_usize().unwrap() {
			let n = blob_reader
				.read(&mut bytes[size..])
				.await
				.map_err(|_| libc::EIO)?;
			size += n;
			if n == 0 {
				break;
			}
		}
		bytes.truncate(size);

		Ok(Response::Read(bytes))
	}

	#[allow(clippy::unused_async)]
	async fn handle_read_dir_request(
		&self,
		header: sys::fuse_in_header,
		data: sys::fuse_read_in,
		plus: bool,
	) -> Result<Response, i32> {
		// Get the node.
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;

		// If the node is the root, then return an empty response.
		if let NodeKind::Root { .. } = &node.kind {
			return Ok(Response::ReadDir(vec![]));
		};

		// Otherwise, the node must be a directory.
		let NodeKind::Directory { directory, .. } = &node.kind else {
			return Err(libc::EIO);
		};

		// Create the response.
		let mut response = Vec::new();
		let names = directory
			.entries(self.inner.tg.as_ref())
			.await
			.map_err(|_| libc::EIO)?
			.keys()
			.map(AsRef::as_ref);

		for (offset, name) in [".", ".."]
			.into_iter()
			.chain(names)
			.enumerate()
			.skip(data.offset.to_usize().unwrap())
		{
			// Get the node.
			let node = match name {
				"." => node.clone(),
				".." => node.parent.upgrade().unwrap(),
				_ => self.get_or_create_child_node(node.clone(), name).await?,
			};

			// Compute the padding entry size.
			let struct_size = if plus {
				std::mem::size_of::<sys::fuse_direntplus>()
			} else {
				std::mem::size_of::<sys::fuse_dirent>()
			};
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;

			// If the response will exceed the specified size, then break.
			if response.len() + entry_size > data.size.to_usize().unwrap() {
				break;
			}

			// Otherwise, add the entry.
			let entry = sys::fuse_dirent {
				ino: node.id.0,
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_: node.type_(),
			};
			if plus {
				let entry = sys::fuse_direntplus {
					entry_out: node.fuse_entry_out(self.inner.tg.as_ref()).await?,
					dirent: entry,
				};
				response.extend_from_slice(entry.as_bytes());
			} else {
				response.extend_from_slice(entry.as_bytes());
			};
			response.extend_from_slice(name.as_bytes());
			response.extend((0..padding).map(|_| 0));
		}

		Ok(if plus {
			Response::ReadDirPlus(response)
		} else {
			Response::ReadDir(response)
		})
	}

	#[allow(clippy::unused_async)]
	async fn handle_read_link_request(&self, header: sys::fuse_in_header) -> Result<Response, i32> {
		// Get the node.
		let node_id = NodeId(header.nodeid);
		let node = self.get_node(node_id).await?;

		// Get the symlink.
		let symlink = match &node.kind {
			NodeKind::Symlink { symlink } => symlink,
			NodeKind::Checkout { path } => {
				let target =
					CString::new(path.as_os_str().as_bytes().to_vec()).map_err(|_| libc::EIO)?;
				return Ok(Response::ReadLink(target));
			},
			_ => return Err(libc::EINVAL),
		};

		// Render the target.
		let mut target = String::new();
		let Ok(artifact) = symlink.artifact(self.inner.tg.as_ref()).await else {
			return Err(libc::EIO);
		};
		let Ok(path) = symlink.path(self.inner.tg.as_ref()).await else {
			return Err(libc::EIO);
		};
		if let Some(artifact) = artifact {
			let Ok(id) = artifact.id(self.inner.tg.as_ref()).await else {
				return Err(libc::EIO);
			};
			for _ in 0..node.depth() - 1 {
				target.push_str("../");
			}
			target.push_str(&id.to_string());
		}
		if artifact.is_some() && path.is_some() {
			target.push('/');
		}
		if let Some(path) = path {
			target.push_str(path);
		}
		let target = CString::new(target).unwrap();

		Ok(Response::ReadLink(target))
	}

	#[allow(clippy::unused_async)]
	async fn handle_release_request(
		&self,
		_header: sys::fuse_in_header,
		data: sys::fuse_release_in,
	) -> Result<Response, i32> {
		let file_handle = FileHandle(data.fh);
		self.inner.handles.write().remove(&file_handle);
		Ok(Response::Release)
	}

	#[allow(clippy::unused_async)]
	async fn handle_release_dir_request(
		&self,
		_header: sys::fuse_in_header,
		data: sys::fuse_release_in,
	) -> Result<Response, i32> {
		let file_handle = FileHandle(data.fh);
		self.inner.handles.write().remove(&file_handle);
		Ok(Response::ReleaseDir)
	}

	#[allow(clippy::unused_async)]
	async fn handle_unsupported_request(
		&self,
		_header: sys::fuse_in_header,
		opcode: u32,
	) -> Result<Response, i32> {
		if opcode == sys::fuse_opcode::FUSE_IOCTL {
			return Err(libc::ENOTTY);
		}
		tracing::error!(?opcode, "Unsupported FUSE request.");
		Err(libc::ENOSYS)
	}

	#[allow(clippy::unused_async)]
	async fn get_node(&self, node_id: NodeId) -> Result<Arc<Node>, i32> {
		let Some(node) = self.inner.nodes.read().get(&node_id).cloned() else {
			return Err(libc::ENOENT);
		};
		Ok(node)
	}

	async fn get_or_create_child_node(
		&self,
		parent_node: Arc<Node>,
		name: &str,
	) -> Result<Arc<Node>, i32> {
		// Handle ".".
		if name == "." {
			return Ok(parent_node);
		}

		// Handle "..".
		if name == ".." {
			let parent_parent_node = parent_node.parent.upgrade().ok_or(libc::EIO)?;
			return Ok(parent_parent_node);
		}

		// If the child already exists, then return it.
		match &parent_node.kind {
			NodeKind::Root { children } => {
				if let Some(child) = children.read().get(name).cloned() {
					return Ok(child);
				}
			},
			NodeKind::Directory { children, .. } => {
				let child = children
					.read()
					.iter()
					.find_map(|(path_, node)| (path_.as_str() == name).then_some(node.clone()));
				if let Some(child) = child {
					return Ok(child);
				}
			},
			_ => return Err(libc::EIO),
		}

		// Get the child artifact or checkout path if it exists.
		let child = match &parent_node.kind {
			NodeKind::Root { .. } => {
				let id = name.parse::<tg::artifact::Id>().map_err(|_| libc::ENOENT)?;
				let path = Path::new("../checkouts").join(id.to_string());
				let exists = tokio::fs::symlink_metadata(self.inner.path.join(&path))
					.await
					.is_ok();
				if exists {
					Either::Left(path)
				} else {
					Either::Right(tg::Artifact::with_id(id))
				}
			},

			NodeKind::Directory { directory, .. } => {
				let entries = directory
					.entries(self.inner.tg.as_ref())
					.await
					.map_err(|_| libc::EIO)?;
				let artifact = entries.get(name).ok_or(libc::ENOENT)?.clone();
				Either::Right(artifact)
			},

			_ => return Err(libc::EIO),
		};

		// Create the child node.
		let node_id = self.next_node_id().await;
		let kind = match child {
			Either::Left(path) => NodeKind::Checkout { path },
			Either::Right(tg::Artifact::Directory(directory)) => {
				let children = parking_lot::RwLock::new(Vec::default());
				NodeKind::Directory {
					directory,
					children,
				}
			},
			Either::Right(tg::Artifact::File(file)) => {
				let size = file
					.size(self.inner.tg.as_ref())
					.await
					.map_err(|_| libc::EIO)?;
				NodeKind::File { file, size }
			},
			Either::Right(tg::Artifact::Symlink(symlink)) => NodeKind::Symlink { symlink },
		};
		let child_node = Node {
			id: node_id,
			parent: Arc::downgrade(&parent_node),
			kind,
		};
		let child_node = Arc::new(child_node);

		// Add the child node to the parent node.
		match &parent_node.kind {
			NodeKind::Root { children } => {
				children.write().insert(name.to_owned(), child_node.clone());
			},
			NodeKind::Directory { children, .. } => {
				children.write().push((name.to_owned(), child_node.clone()));
			},
			_ => return Err(libc::EIO),
		}

		// Add the child node to the nodes.
		self.inner
			.nodes
			.write()
			.insert(child_node.id, child_node.clone());

		Ok(child_node)
	}

	// Create a new NodeId.
	#[allow(clippy::unused_async)]
	async fn next_node_id(&self) -> NodeId {
		let node_id = self
			.inner
			.node_index
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		NodeId(node_id)
	}

	// Create a new reader id.
	#[allow(clippy::unused_async)]
	async fn next_file_handle(&self) -> FileHandle {
		let handle = self
			.inner
			.handle_index
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		FileHandle(handle)
	}

	#[allow(clippy::similar_names)]
	async fn mount(path: &Path) -> Result<std::fs::File> {
		Self::unmount(path).await?;
		unsafe {
			// Setup the arguments.
			let uid = libc::getuid();
			let gid = libc::getgid();
			let options = format!(
				"rootmode=40755,user_id={uid},group_id={gid},default_permissions,auto_unmount\0"
			);

			let mut fds = [0, 0];
			let ret = libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
			if ret != 0 {
				Err(std::io::Error::last_os_error())
					.wrap_err("Failed to create the socket pair.")?;
			}

			let fusermount3 = std::ffi::CString::new("/usr/bin/fusermount3").unwrap();
			let fuse_commfd = std::ffi::CString::new(fds[0].to_string()).unwrap();

			// Produce a null-terminated path.
			let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();

			// Fork.
			let pid = libc::fork();
			if pid == -1 {
				libc::close(fds[0]);
				libc::close(fds[1]);
				Err(std::io::Error::last_os_error()).wrap_err("Failed to fork.")?;
			}

			// Exec the program.
			if pid == 0 {
				let argv = [
					fusermount3.as_ptr(),
					b"-o\0".as_ptr().cast(),
					options.as_ptr().cast(),
					b"--\0".as_ptr().cast(),
					path.as_ptr().cast(),
					std::ptr::null(),
				];
				libc::close(fds[1]);
				libc::fcntl(fds[0], libc::F_SETFD, 0);
				libc::setenv(
					b"_FUSE_COMMFD\0".as_ptr().cast(),
					fuse_commfd.as_ptr().cast(),
					1,
				);
				libc::execv(argv[0], argv.as_ptr());
				libc::close(fds[0]);
				libc::exit(1);
			}
			libc::close(fds[0]);

			// Create the control message. We make sure all pointers are on the heap to avoid being overwritten by the call to recvmsg(). This is safe because it occurs only in the original process after the call to fork(), and the Box<> will live until the function exits.
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
				msg_controllen: std::mem::size_of_val(&control) as _,
				msg_flags: 0,
			});
			let msg: *mut libc::msghdr = msg.as_mut() as _;

			// Receive the control message.
			let ret = libc::recvmsg(fds[1], msg, 0);
			if ret == -1 {
				return Err(std::io::Error::last_os_error().wrap("Failed to receive the message."));
			}
			if ret == 0 {
				return Err(std::io::Error::new(
					std::io::ErrorKind::UnexpectedEof,
					"Unexpected EOF.",
				)
				.wrap("Unexpected EOF."));
			}

			// Read the file descriptor.
			let cmsg = libc::CMSG_FIRSTHDR(msg);
			if cmsg.is_null() {
				return Err(std::io::Error::new(
					std::io::ErrorKind::UnexpectedEof,
					"Unexpected EOF.",
				)
				.wrap("Unexpected EOF."));
			}
			let mut fd: std::os::unix::io::RawFd = 0;
			libc::memcpy(
				std::ptr::addr_of_mut!(fd).cast(),
				libc::CMSG_DATA(cmsg).cast(),
				std::mem::size_of_val(&fd),
			);

			if fd > 0 {
				libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
			}

			let file = std::fs::File::from_raw_fd(fd);
			Ok(file)
		}
	}

	async fn unmount(path: &Path) -> Result<()> {
		tokio::process::Command::new("/usr/bin/fusermount3")
			.arg("-q")
			.arg("-u")
			.arg(path)
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await
			.wrap_err("Failed to execute the unmount command.")?;
		Ok(())
	}
}

impl Node {
	fn type_(&self) -> u32 {
		match &self.kind {
			NodeKind::Root { .. } | NodeKind::Directory { .. } => libc::S_IFDIR as _,
			NodeKind::File { .. } => libc::S_IFREG as _,
			NodeKind::Symlink { .. } | NodeKind::Checkout { .. } => libc::S_IFLNK as _,
		}
	}

	async fn mode(&self, tg: &dyn tg::Handle) -> Result<u32, i32> {
		let mode = match &self.kind {
			NodeKind::Root { .. } | NodeKind::Directory { .. } => libc::S_IFDIR | 0o555,
			NodeKind::File { file, .. } => {
				let executable = file.executable(tg).await.map_err(|_| libc::EIO)?;
				libc::S_IFREG | 0o444 | (if executable { 0o111 } else { 0o000 })
			},
			NodeKind::Symlink { .. } | NodeKind::Checkout { .. } => libc::S_IFLNK | 0o444,
		};
		Ok(mode as _)
	}

	fn size(&self) -> u64 {
		match &self.kind {
			NodeKind::Root { .. }
			| NodeKind::Directory { .. }
			| NodeKind::Symlink { .. }
			| NodeKind::Checkout { .. } => 0,
			NodeKind::File { size, .. } => *size,
		}
	}

	fn depth(&self) -> usize {
		if self.id == ROOT_NODE_ID {
			0
		} else {
			self.parent.upgrade().unwrap().depth() + 1
		}
	}

	async fn fuse_entry_out(&self, tg: &dyn tg::Handle) -> Result<sys::fuse_entry_out, i32> {
		let nodeid = self.id.0;
		let attr_out = self.fuse_attr_out(tg).await?;
		let entry_out = sys::fuse_entry_out {
			nodeid,
			generation: 0,
			entry_valid: 1024,
			attr_valid: 0,
			entry_valid_nsec: 1024,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		};
		Ok(entry_out)
	}

	async fn fuse_attr_out(&self, tg: &dyn tg::Handle) -> Result<sys::fuse_attr_out, i32> {
		let nodeid = self.id.0;
		let nlink: u32 = match &self.kind {
			NodeKind::Root { .. } => 2,
			_ => 1,
		};
		let size = self.size();
		let mode = self.mode(tg).await?;
		let attr_out = sys::fuse_attr_out {
			attr_valid: 1024,
			attr_valid_nsec: 0,
			attr: sys::fuse_attr {
				ino: nodeid,
				size,
				blocks: 0,
				atime: 0,
				mtime: 0,
				ctime: 0,
				atimensec: 0,
				mtimensec: 0,
				ctimensec: 0,
				mode,
				nlink,
				uid: 1000,
				gid: 1000,
				rdev: 0,
				blksize: 512,
				flags: 0,
			},
			dummy: 0,
		};
		Ok(attr_out)
	}
}

fn read_data<T>(request_data: &[u8]) -> Result<T>
where
	T: FromBytes,
{
	T::read_from_prefix(request_data).wrap_err("Failed to deserialize the request data.")
}
