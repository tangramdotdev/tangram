#[cfg(feature = "lmdb")]
use heed as lmdb;
use {
	crate::Server,
	bytes::Bytes,
	dashmap::DashMap,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, HashMap},
		io::{Read as _, Seek as _},
		os::fd::OwnedFd,
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
		pin::pin,
		sync::atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
	tangram_vfs as vfs,
};

pub struct Provider {
	file_handle_count: AtomicU64,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	node_count: AtomicU64,
	nodes: Nodes,
	server: Server,
}

struct Nodes {
	nodes: DashMap<u64, Node, fnv::FnvBuildHasher>,
}

#[derive(Clone)]
struct Node {
	artifact: Option<tg::artifact::Id>,
	attrs: Option<vfs::Attrs>,
	children: HashMap<String, u64, fnv::FnvBuildHasher>,
	depth: u64,
	lookup_count: u64,
	name: Option<String>,
	parent: u64,
}

pub struct FileHandle {
	blob: tg::blob::Id,
}

#[cfg(feature = "lmdb")]
type Transaction<'a> = lmdb::RoTxn<'a>;
#[cfg(not(feature = "lmdb"))]
type Transaction<'a> = ();

impl Provider {
	pub async fn new(server: &Server, _options: crate::config::Vfs) -> tg::Result<Self> {
		// Create the nodes.
		let nodes = Nodes::new();

		// Create the provider.
		let file_handle_count = AtomicU64::new(1000);
		let file_handles = DashMap::default();
		let node_count = AtomicU64::new(1000);
		let server = server.clone();
		let provider = Self {
			file_handle_count,
			file_handles,
			node_count,
			nodes,
			server,
		};

		Ok(provider)
	}

	pub fn handle_batch(
		&self,
		requests: Vec<vfs::Request>,
	) -> impl std::future::Future<Output = Vec<std::io::Result<vfs::Response>>> + Send {
		async move {
			let mut responses = Vec::with_capacity(requests.len());
			for request in requests {
				let response = match request {
					vfs::Request::Close { handle } => {
						self.close(handle).await;
						Ok(vfs::Response::Unit)
					},
					vfs::Request::Forget { id, nlookup } => {
						self.forget_sync(id, nlookup);
						Ok(vfs::Response::Unit)
					},
					vfs::Request::GetAttr { id } => self
						.getattr(id)
						.await
						.map(|attrs| vfs::Response::GetAttr { attrs }),
					vfs::Request::GetXattr { id, name } => self
						.getxattr(id, &name)
						.await
						.map(|value| vfs::Response::GetXattr { value }),
					vfs::Request::ListXattrs { id } => self
						.listxattrs(id)
						.await
						.map(|names| vfs::Response::ListXattrs { names }),
					vfs::Request::Lookup { id, name } => self
						.lookup(id, &name)
						.await
						.map(|id| vfs::Response::Lookup { id }),
					vfs::Request::LookupParent { id } => self
						.lookup_parent(id)
						.await
						.map(|id| vfs::Response::LookupParent { id }),
					vfs::Request::Open { id } => {
						self.open(id).await.map(|handle| vfs::Response::Open {
							handle,
							backing_fd: None,
						})
					},
					vfs::Request::OpenDir { id } => self
						.opendir(id)
						.await
						.map(|handle| vfs::Response::OpenDir { handle }),
					vfs::Request::Read {
						handle,
						position,
						length,
					} => self
						.read(handle, position, length)
						.await
						.map(|bytes| vfs::Response::Read { bytes }),
					vfs::Request::ReadDir { handle } => self
						.readdir(handle)
						.await
						.map(|entries| vfs::Response::ReadDir { entries }),
					vfs::Request::ReadDirPlus { handle } => self
						.readdirplus(handle)
						.await
						.map(|entries| vfs::Response::ReadDirPlus { entries }),
					vfs::Request::ReadLink { id } => self
						.readlink(id)
						.await
						.map(|target| vfs::Response::ReadLink { target }),
					vfs::Request::Remember { id } => {
						self.remember_sync(id);
						Ok(vfs::Response::Unit)
					},
				};
				responses.push(response);
			}
			responses
		}
	}

	pub fn handle_batch_sync(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		#[cfg(feature = "lmdb")]
		if let crate::store::Store::Lmdb(store) = &self.server.store {
			let transaction = match store.env().read_txn() {
				Ok(transaction) => transaction,
				Err(error) => {
					tracing::error!(?error, "failed to begin an lmdb read transaction");
					return (0..requests.len())
						.map(|_| Err(std::io::Error::from_raw_os_error(libc::EIO)))
						.collect();
				},
			};
			return self.handle_batch_sync_inner(requests, Some(&transaction));
		}

		self.handle_batch_sync_inner(requests, None)
	}

	fn handle_batch_sync_inner(
		&self,
		requests: Vec<vfs::Request>,
		transaction: Option<&Transaction<'_>>,
	) -> Vec<std::io::Result<vfs::Response>> {
		let mut responses = Vec::with_capacity(requests.len());
		for request in requests {
			let response = match request {
				vfs::Request::Close { handle } => {
					self.close_sync(handle);
					Ok(vfs::Response::Unit)
				},
				vfs::Request::Forget { id, nlookup } => {
					self.forget_sync(id, nlookup);
					Ok(vfs::Response::Unit)
				},
				vfs::Request::GetAttr { id } => self
					.getattr_sync_inner(id, transaction)
					.map(|attrs| vfs::Response::GetAttr { attrs }),
				vfs::Request::GetXattr { id, name } => self
					.getxattr_sync_inner(id, &name, transaction)
					.map(|value| vfs::Response::GetXattr { value }),
				vfs::Request::ListXattrs { id } => self
					.listxattrs_sync_inner(id, transaction)
					.map(|names| vfs::Response::ListXattrs { names }),
				vfs::Request::Lookup { id, name } => self
					.lookup_sync_inner(id, &name, transaction)
					.map(|id| vfs::Response::Lookup { id }),
				vfs::Request::LookupParent { id } => self
					.lookup_parent_sync(id)
					.map(|id| vfs::Response::LookupParent { id }),
				vfs::Request::Open { id } => self
					.open_sync_inner(id, transaction)
					.map(|(handle, backing_fd)| vfs::Response::Open { handle, backing_fd }),
				vfs::Request::OpenDir { id } => self
					.opendir_sync(id)
					.map(|handle| vfs::Response::OpenDir { handle }),
				vfs::Request::Read {
					handle,
					position,
					length,
				} => self
					.read_sync_inner(handle, position, length, transaction)
					.map(|bytes| vfs::Response::Read { bytes }),
				vfs::Request::ReadDir { handle } => self
					.readdir_sync_inner(handle, transaction)
					.map(|entries| vfs::Response::ReadDir { entries }),
				vfs::Request::ReadDirPlus { handle } => self
					.readdirplus_sync_inner(handle, transaction)
					.map(|entries| vfs::Response::ReadDirPlus { entries }),
				vfs::Request::ReadLink { id } => self
					.readlink_sync_inner(id, transaction)
					.map(|target| vfs::Response::ReadLink { target }),
				vfs::Request::Remember { id } => {
					self.remember_sync(id);
					Ok(vfs::Response::Unit)
				},
			};
			responses.push(response);
		}
		responses
	}

	pub async fn close(&self, id: u64) {
		if self.file_handles.contains_key(&id) {
			self.file_handles.remove(&id);
		}
	}

	pub fn close_sync(&self, id: u64) {
		if self.file_handles.contains_key(&id) {
			self.file_handles.remove(&id);
		}
	}

	pub fn forget_sync(&self, id: u64, nlookup: u64) {
		drop(self.nodes.forget(id, nlookup));
	}

	pub async fn getattr(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		let node = self.get(id).await?;
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		let attrs = self.getattr_from_node_inner(&node).await?;
		self.nodes.set_attrs(id, attrs);
		Ok(attrs)
	}

	pub fn getattr_sync(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		self.getattr_sync_inner(id, None)
	}

	fn getattr_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<vfs::Attrs> {
		let node = self.get_sync(id)?;
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		let attrs = self.getattr_from_node_sync_inner(&node, transaction)?;
		self.nodes.set_attrs(id, attrs);
		Ok(attrs)
	}

	pub async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		let node = self.get(id).await?;
		let Some(artifact) = node.artifact else {
			return Ok(None);
		};
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(None);
		}
		let (file, _) = self.file_node_inner(&artifact).await?;
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			if file.dependencies.is_empty() {
				return Ok(None);
			}
			let references = file.dependencies.keys().cloned().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).unwrap();
			return Ok(Some(data.into()));
		}

		if name == tg::file::MODULE_XATTR_NAME {
			let Some(module) = file.module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	pub fn getxattr_sync(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		self.getxattr_sync_inner(id, name, None)
	}

	fn getxattr_sync_inner(
		&self,
		id: u64,
		name: &str,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<Bytes>> {
		let node = self.get_sync(id)?;
		let Some(artifact) = node.artifact else {
			return Ok(None);
		};
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(None);
		}
		let (file, _) = self.file_node_sync_inner(&artifact, transaction)?;
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			if file.dependencies.is_empty() {
				return Ok(None);
			}
			let references = file.dependencies.keys().cloned().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).unwrap();
			return Ok(Some(data.into()));
		}
		if name == tg::file::MODULE_XATTR_NAME {
			let Some(module) = file.module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	pub async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let node = self.get(id).await?;
		let Some(artifact) = node.artifact else {
			return Ok(Vec::new());
		};
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let (file, _) = self.file_node_inner(&artifact).await?;
		if file.dependencies.is_empty() {
			return Ok(Vec::new());
		}
		Ok(vec![tg::file::DEPENDENCIES_XATTR_NAME.to_owned()])
	}

	pub fn listxattrs_sync(&self, id: u64) -> std::io::Result<Vec<String>> {
		self.listxattrs_sync_inner(id, None)
	}

	fn listxattrs_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<String>> {
		let node = self.get_sync(id)?;
		let Some(artifact) = node.artifact else {
			return Ok(Vec::new());
		};
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let (file, _) = self.file_node_sync_inner(&artifact, transaction)?;
		if file.dependencies.is_empty() {
			return Ok(Vec::new());
		}
		Ok(vec![tg::file::DEPENDENCIES_XATTR_NAME.to_owned()])
	}

	pub async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			return Ok(Some(parent));
		} else if name == ".." {
			let id = self.lookup_parent(parent).await?;
			return Ok(Some(id));
		}

		// First, try to look up in the nodes storage.
		if let Some(id) = self.nodes.lookup(parent, name).await? {
			return Ok(Some(id));
		}

		// If the parent is the root, then create a new node.
		let entry = 'a: {
			if parent != vfs::ROOT_NODE_ID {
				break 'a None;
			}
			let name = None
				.or_else(|| name.strip_suffix(".tg.ts"))
				.or_else(|| name.strip_suffix(".tg.js"))
				.or_else(|| {
					std::path::Path::new(name)
						.file_stem()
						.and_then(|s| s.to_str())
				})
				.unwrap_or(name);
			let Ok(artifact) = name.parse() else {
				return Ok(None);
			};
			Some((artifact, 1))
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let entry = 'a: {
			if let Some(entry) = entry {
				break 'a Some(entry);
			}
			let Node {
				artifact, depth, ..
			} = self.get(parent).await?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			if !matches!(artifact.kind(), tg::artifact::Kind::Directory) {
				return Ok(None);
			}
			let entries = self.directory_entries_inner(&artifact).await?;
			let Some(artifact) = entries.get(name) else {
				return Ok(None);
			};
			Some((artifact.clone(), depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let attrs = Self::attrs_from_artifact(Some(&artifact));
		let id = self.put(parent, name, artifact, depth, attrs).await?;

		Ok(Some(id))
	}

	pub fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		self.lookup_sync_inner(parent, name, None)
	}

	fn lookup_sync_inner(
		&self,
		parent: u64,
		name: &str,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			return Ok(Some(parent));
		} else if name == ".." {
			let id = self.lookup_parent_sync(parent)?;
			return Ok(Some(id));
		}

		// First, try to look up in the nodes storage.
		if let Some(id) = self.nodes.lookup_sync(parent, name) {
			return Ok(Some(id));
		}

		// If the parent is the root, then create a new node.
		let entry = 'a: {
			if parent != vfs::ROOT_NODE_ID {
				break 'a None;
			}
			let name = None
				.or_else(|| name.strip_suffix(".tg.ts"))
				.or_else(|| name.strip_suffix(".tg.js"))
				.or_else(|| {
					std::path::Path::new(name)
						.file_stem()
						.and_then(|s| s.to_str())
				})
				.unwrap_or(name);
			let Ok(artifact) = name.parse() else {
				return Ok(None);
			};
			Some((artifact, 1))
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let entry = 'a: {
			if let Some(entry) = entry {
				break 'a Some(entry);
			}
			let Node {
				artifact, depth, ..
			} = self.get_sync(parent)?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			if !matches!(artifact.kind(), tg::artifact::Kind::Directory) {
				return Ok(None);
			}
			let entries = self.directory_entries_sync_inner(&artifact, None, transaction)?;
			let Some(artifact) = entries.get(name) else {
				return Ok(None);
			};
			Some((artifact.clone(), depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let attrs = self.try_precompute_node_attrs_sync_inner(&artifact, transaction);
		let id = self.put_sync(parent, name, &artifact, depth, attrs);
		Ok(Some(id))
	}

	pub async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.nodes.lookup_parent(id).await
	}

	pub fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		self.nodes.lookup_parent_sync(id)
	}

	pub async fn open(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Ensure it is a file.
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		}

		// Get the blob id.
		let (file, _) = self.file_node_inner(&artifact).await?;
		let Some(blob) = file.contents else {
			tracing::error!(%id, "file has no contents");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};

		// Create the file handle.
		let file_handle = FileHandle { blob };

		// Insert the file handle.
		let id = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(id, file_handle);

		Ok(id)
	}

	pub fn open_sync(&self, id: u64) -> std::io::Result<(u64, Option<OwnedFd>)> {
		self.open_sync_inner(id, None)
	}

	fn open_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(u64, Option<OwnedFd>)> {
		// Get the node.
		let Node { artifact, .. } = self.get_sync(id)?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Ensure it is a file.
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		}

		// Get the file object.
		let (file, _) = self.file_node_sync_inner(&artifact, transaction)?;
		let Some(blob) = file.contents else {
			tracing::error!(%id, "file has no contents");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};

		// Attempt to open a backing file for passthrough.
		let backing_fd = self.try_open_backing_fd_sync_inner(&blob, transaction)?;

		// Insert the file handle.
		let id = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(id, FileHandle { blob });

		Ok((id, backing_fd))
	}

	pub async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;
		match artifact {
			Some(artifact) if !matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			_ => {},
		}
		Ok(id)
	}

	pub fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get_sync(id)?;
		match artifact {
			Some(artifact) if !matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			_ => {},
		}
		Ok(id)
	}

	pub async fn read(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		// Get the file handle.
		let Some(file_handle) = self.file_handles.get(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};

		// Create the stream.
		let arg = tg::read::Arg {
			blob: file_handle.blob.clone(),
			options: tg::read::Options {
				position: Some(std::io::SeekFrom::Start(position)),
				length: Some(length),
				size: None,
			},
		};
		let stream = self
			.server
			.try_read(arg)
			.await
			.map_err(|error| {
				tracing::error!(%error, "failed to read the blob");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?
			.map_err(|error| {
				tracing::error!(%error, "failed to read a chunk");
				std::io::Error::from_raw_os_error(libc::EIO)
			});
		let mut stream = pin!(stream);
		let mut bytes = Vec::with_capacity(length.to_usize().unwrap());
		while let Some(chunk) = stream.try_next().await? {
			bytes.extend_from_slice(&chunk.bytes);
		}

		Ok(bytes.into())
	}

	pub fn read_sync(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		self.read_sync_inner(id, position, length, None)
	}

	fn read_sync_inner(
		&self,
		id: u64,
		position: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Bytes> {
		// Get the file handle.
		let Some(file_handle) = self.file_handles.get(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};

		let mut bytes = Vec::with_capacity(length.to_usize().unwrap());
		self.read_blob_range_sync_inner(
			&file_handle.blob,
			position,
			length,
			&mut bytes,
			transaction,
		)?;
		Ok(bytes.into())
	}

	pub async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		let Node { artifact, .. } = self.get(id).await?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => {
				tracing::error!(%id, "called readdir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let Some(directory) = directory else {
			return Ok(Vec::new());
		};
		let entries = self.directory_entries_inner(&directory).await?;
		let mut result = Vec::with_capacity(entries.len());
		result.push((".".to_owned(), id));
		result.push(("..".to_owned(), self.lookup_parent(id).await?));
		for name in entries.keys() {
			let entry = self.lookup(id, name).await?.ok_or_else(|| {
				tracing::error!(parent = id, %name, "failed to lookup directory entry");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			result.push((name.clone(), entry));
		}
		Ok(result)
	}

	pub fn readdir_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		self.readdir_sync_inner(id, None)
	}

	fn readdir_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64)>> {
		let Node { artifact, .. } = self.get_sync(id)?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => {
				tracing::error!(%id, "called readdir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let Some(directory) = directory else {
			return Ok(Vec::new());
		};
		let entries = self.directory_entries_sync_inner(&directory, None, transaction)?;
		let mut result = Vec::with_capacity(entries.len() + 2);
		result.push((".".to_owned(), id));
		result.push(("..".to_owned(), self.lookup_parent_sync(id)?));
		for name in entries.keys() {
			let entry = self
				.lookup_sync_inner(id, name, transaction)?
				.ok_or_else(|| {
					tracing::error!(parent = id, %name, "failed to lookup directory entry");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
			result.push((name.clone(), entry));
		}
		Ok(result)
	}

	pub async fn readdirplus(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let entries = self.readdir(id).await?;
		let mut entries_with_attrs = Vec::with_capacity(entries.len());
		for (name, node_id) in entries {
			let attrs = self.getattr(node_id).await?;
			entries_with_attrs.push((name, node_id, attrs));
		}
		Ok(entries_with_attrs)
	}

	pub fn readdirplus_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		self.readdirplus_sync_inner(id, None)
	}

	fn readdirplus_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let entries = self.readdir_sync_inner(id, transaction)?;
		let mut entries_with_attrs = Vec::with_capacity(entries.len());
		for (name, node_id) in entries {
			let attrs = self.getattr_sync_inner(node_id, transaction)?;
			entries_with_attrs.push((name, node_id, attrs));
		}
		Ok(entries_with_attrs)
	}

	pub async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		// Get the node.
		let Node {
			artifact, depth, ..
		} = self.get(id).await.map_err(|error| {
			tracing::error!(%error, "failed to lookup node");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};

		// Ensure it is a symlink.
		if !matches!(artifact.kind(), tg::artifact::Kind::Symlink) {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		}

		// Render the target.
		let (symlink, graph) = self.symlink_node_inner(&artifact).await?;
		let artifact = match symlink.artifact {
			Some(edge) => Some(Self::artifact_id_from_edge_inner(edge, graph.as_ref())?),
			None => None,
		};
		Self::build_symlink_target(depth, artifact, symlink.path)
	}

	pub fn readlink_sync(&self, id: u64) -> std::io::Result<Bytes> {
		self.readlink_sync_inner(id, None)
	}

	fn readlink_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Bytes> {
		// Get the node.
		let Node {
			artifact, depth, ..
		} = self.get_sync(id)?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};
		if !matches!(artifact.kind(), tg::artifact::Kind::Symlink) {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		}

		// Render the target.
		let (symlink, graph) = self.symlink_node_sync_inner(&artifact, transaction)?;
		let artifact = match symlink.artifact {
			Some(edge) => Some(Self::artifact_id_from_edge_inner(edge, graph.as_ref())?),
			None => None,
		};
		Self::build_symlink_target(depth, artifact, symlink.path)
	}

	pub fn remember_sync(&self, id: u64) {
		self.nodes.remember(id);
	}

	async fn get(&self, id: u64) -> std::io::Result<Node> {
		self.nodes.get(id).await
	}

	fn get_sync(&self, id: u64) -> std::io::Result<Node> {
		self.nodes.get_sync(id)
	}

	async fn put(
		&self,
		parent: u64,
		name: &str,
		artifact: tg::artifact::Id,
		depth: u64,
		attrs: Option<vfs::Attrs>,
	) -> std::io::Result<u64> {
		// Get a node ID.
		let id = self.node_count.fetch_add(1, Ordering::Relaxed);

		// Add the node to the nodes storage.
		self.nodes.insert(id, parent, name, artifact, depth, attrs);

		Ok(id)
	}

	fn put_sync(
		&self,
		parent: u64,
		name: &str,
		artifact: &tg::artifact::Id,
		depth: u64,
		attrs: Option<vfs::Attrs>,
	) -> u64 {
		// Get a node ID.
		let id = self.node_count.fetch_add(1, Ordering::Relaxed);

		// Add the node to the nodes storage.
		self.nodes
			.insert(id, parent, name, artifact.clone(), depth, attrs);

		id
	}

	fn build_symlink_target(
		depth: u64,
		artifact: Option<tg::artifact::Id>,
		path: Option<PathBuf>,
	) -> std::io::Result<Bytes> {
		let mut target = PathBuf::new();
		if let Some(artifact) = artifact {
			for _ in 0..depth.saturating_sub(1) {
				target.push("..");
			}
			target.push(artifact.to_string());
		}
		if let Some(path) = path {
			target.push(path);
		}
		if target == Path::new("") {
			tracing::error!("invalid symlink");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		let target = target.as_os_str().as_bytes().to_vec().into();
		Ok(target)
	}

	async fn getattr_from_node_inner(&self, node: &Node) -> std::io::Result<vfs::Attrs> {
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		let Some(artifact) = node.artifact.clone() else {
			return Ok(vfs::Attrs::new(vfs::FileType::Directory));
		};
		match artifact.kind() {
			tg::artifact::Kind::Directory => Ok(vfs::Attrs::new(vfs::FileType::Directory)),
			tg::artifact::Kind::File => {
				let (file, _) = self.file_node_inner(&artifact).await?;
				let size = if let Some(contents) = file.contents.as_ref() {
					self.blob_length_inner(contents).await?
				} else {
					0
				};
				Ok(vfs::Attrs::new(vfs::FileType::File {
					executable: file.executable,
					size,
				}))
			},
			tg::artifact::Kind::Symlink => Ok(vfs::Attrs::new(vfs::FileType::Symlink)),
		}
	}

	fn artifact_id_from_directory_edge_inner(
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::artifact::Id> {
		match edge {
			tg::graph::data::Edge::Object(directory) => Ok(directory.into()),
			tg::graph::data::Edge::Pointer(pointer) => Self::artifact_id_from_pointer_inner(
				&pointer,
				default_graph,
				Some(tg::artifact::Kind::Directory),
			),
		}
	}

	fn artifact_id_from_edge_inner(
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::artifact::Id> {
		match edge {
			tg::graph::data::Edge::Object(artifact) => Ok(artifact),
			tg::graph::data::Edge::Pointer(pointer) => {
				Self::artifact_id_from_pointer_inner(&pointer, default_graph, None)
			},
		}
	}

	fn artifact_id_from_pointer_inner(
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
		expected_kind: Option<tg::artifact::Kind>,
	) -> std::io::Result<tg::artifact::Id> {
		if let Some(expected_kind) = expected_kind
			&& pointer.kind != expected_kind
		{
			tracing::error!(kind = ?pointer.kind, expected = ?expected_kind, "invalid pointer kind");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		let pointer = Self::pointer_with_graph_inner(pointer, default_graph)?;
		let kind = pointer.kind;
		let data: tg::artifact::data::Artifact = match kind {
			tg::artifact::Kind::Directory => tg::directory::Data::Pointer(pointer).into(),
			tg::artifact::Kind::File => tg::file::Data::Pointer(pointer).into(),
			tg::artifact::Kind::Symlink => tg::symlink::Data::Pointer(pointer).into(),
		};
		let bytes = data.serialize().map_err(|error| {
			tracing::error!(error = %error.trace(), "failed to serialize the pointer artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		Ok(tg::artifact::Id::new(kind, &bytes))
	}

	async fn artifact_data_inner(
		&self,
		artifact: &tg::artifact::Id,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		let id: tg::object::Id = artifact.clone().into();
		let Some(data) = self.try_get_object_data_inner(&id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(%artifact, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	async fn directory_entries_inner(
		&self,
		directory: &tg::artifact::Id,
	) -> std::io::Result<BTreeMap<String, tg::artifact::Id>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![directory.clone()];
		while let Some(directory) = stack.pop() {
			let (directory, graph) = self.directory_node_inner(&directory).await?;
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = Self::artifact_id_from_edge_inner(edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = Self::artifact_id_from_directory_edge_inner(
							child.directory,
							graph.as_ref(),
						)?;
						stack.push(artifact);
					}
				},
			}
		}
		Ok(entries)
	}

	async fn directory_node_inner(
		&self,
		directory: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let data = self.artifact_data_inner(directory).await?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&pointer, None).await?;
				let tg::graph::data::Node::Directory(node) = node else {
					tracing::error!(pointer = ?pointer, "expected directory node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	async fn file_node_inner(
		&self,
		file: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let data = self.artifact_data_inner(file).await?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&pointer, None).await?;
				let tg::graph::data::Node::File(node) = node else {
					tracing::error!(pointer = ?pointer, "expected file node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	async fn graph_data_inner(&self, graph: &tg::graph::Id) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let Some(data) = self.try_get_object_data_inner(&id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(%graph, "expected graph data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn graph_id_from_pointer_inner(
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::graph::Id> {
		pointer
			.graph
			.clone()
			.or_else(|| default_graph.cloned())
			.ok_or_else(|| {
				tracing::error!(pointer = ?pointer, "missing pointer graph");
				std::io::Error::from_raw_os_error(libc::ENOSYS)
			})
	}

	fn pointer_with_graph_inner(
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::graph::data::Pointer> {
		let graph = pointer
			.graph
			.clone()
			.or_else(|| default_graph.cloned())
			.ok_or_else(|| {
				tracing::error!(pointer = ?pointer, "missing pointer graph");
				std::io::Error::from_raw_os_error(libc::ENOSYS)
			})?;
		Ok(tg::graph::data::Pointer {
			graph: Some(graph),
			index: pointer.index,
			kind: pointer.kind,
		})
	}

	async fn resolve_graph_node_inner(
		&self,
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = Self::graph_id_from_pointer_inner(pointer, default_graph)?;
		let graph_data = self.graph_data_inner(&graph).await?;
		let node = graph_data
			.nodes
			.get(pointer.index)
			.cloned()
			.ok_or_else(|| {
				tracing::error!(graph = %graph, pointer = ?pointer, "invalid graph node");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		if node.kind() != pointer.kind {
			tracing::error!(
				graph = %graph,
				pointer = ?pointer,
				kind = ?node.kind(),
				"invalid pointer kind"
			);
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok((node, graph))
	}

	async fn symlink_node_inner(
		&self,
		symlink: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let data = self.artifact_data_inner(symlink).await?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&pointer, None).await?;
				let tg::graph::data::Node::Symlink(node) = node else {
					tracing::error!(pointer = ?pointer, "expected symlink node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	async fn try_get_object_data_inner(
		&self,
		id: &tg::object::Id,
	) -> std::io::Result<Option<tg::object::Data>> {
		let output = self
			.server
			.try_get_object(id, tg::object::get::Arg::default())
			.await
			.map_err(|error| {
				tracing::error!(error = %error.trace(), %id, "failed to get object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let Some(output) = output else {
			return Ok(None);
		};
		let data = tg::object::Data::deserialize(id.kind(), output.bytes).map_err(|error| {
			tracing::error!(error = %error.trace(), %id, "failed to deserialize object data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		Ok(Some(data))
	}

	async fn blob_length_inner(&self, id: &tg::blob::Id) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let Some(data) = self.try_get_object_data_inner(&id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		let tg::object::Data::Blob(blob) = data else {
			tracing::error!(%id, "expected blob data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let length = match blob {
			tg::blob::Data::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
			tg::blob::Data::Branch(branch) => {
				branch.children.iter().map(|child| child.length).sum()
			},
		};
		Ok(length)
	}

	fn artifact_data_sync_inner(
		&self,
		artifact: &tg::artifact::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		let id: tg::object::Id = artifact.clone().into();
		let output = self.try_get_object_data(&id, transaction)?;
		let Some((_, data)) = output else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(%artifact, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn graph_data_sync_inner(
		&self,
		graph: &tg::graph::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let output = self.try_get_object_data(&id, transaction)?;
		let Some((_, data)) = output else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(%graph, "expected graph data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn resolve_graph_node_sync_inner(
		&self,
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = Self::graph_id_from_pointer_inner(pointer, default_graph)?;
		let graph_data = self.graph_data_sync_inner(&graph, transaction)?;
		let node = graph_data
			.nodes
			.get(pointer.index)
			.cloned()
			.ok_or_else(|| {
				tracing::error!(graph = %graph, pointer = ?pointer, "invalid graph node");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		if node.kind() != pointer.kind {
			tracing::error!(
				graph = %graph,
				pointer = ?pointer,
				kind = ?node.kind(),
				"invalid pointer kind"
			);
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		Ok((node, graph))
	}

	fn blob_length_sync_inner(
		&self,
		id: &tg::blob::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let output = self.try_get_object_data(&id, transaction)?;
		let Some((_, data)) = output else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		let tg::object::Data::Blob(blob) = data else {
			tracing::error!(%id, "expected blob data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let length = match blob {
			tg::blob::Data::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
			tg::blob::Data::Branch(branch) => {
				branch.children.iter().map(|child| child.length).sum()
			},
		};
		Ok(length)
	}

	fn directory_node_sync_inner(
		&self,
		directory: &tg::artifact::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let data = self.artifact_data_sync_inner(directory, transaction)?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&pointer, None, transaction)?;
				let tg::graph::data::Node::Directory(node) = node else {
					tracing::error!(pointer = ?pointer, "expected directory node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn read_blob_range_sync_inner(
		&self,
		id: &tg::blob::Id,
		position: u64,
		length: u64,
		output: &mut Vec<u8>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<()> {
		if length == 0 {
			return Ok(());
		}
		let object_id: tg::object::Id = id.clone().into();
		let object = self.try_get_object(&object_id, transaction)?;
		let Some(object) = object else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		if let Some(bytes) = object.bytes {
			let data =
				tg::object::Data::deserialize(object_id.kind(), &*bytes).map_err(|error| {
					tracing::error!(
						error = %error.trace(),
						id = %object_id,
						"failed to deserialize the object data"
					);
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
			let tg::object::Data::Blob(blob) = data else {
				tracing::error!(id = %object_id, "expected blob data");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			};
			match blob {
				tg::blob::Data::Leaf(leaf) => {
					let Some(start) = position.to_usize() else {
						return Ok(());
					};
					if start >= leaf.bytes.len() {
						return Ok(());
					}
					let available_length = (leaf.bytes.len() - start).to_u64().unwrap();
					let copy_length = std::cmp::min(length, available_length).to_usize().unwrap();
					output.extend_from_slice(&leaf.bytes[start..start + copy_length]);
				},
				tg::blob::Data::Branch(branch) => {
					let mut remaining = length;
					let mut child_position = position;
					for child in branch.children {
						if remaining == 0 {
							break;
						}
						if child_position >= child.length {
							child_position -= child.length;
							continue;
						}
						let child_length = std::cmp::min(remaining, child.length - child_position);
						self.read_blob_range_sync_inner(
							&child.blob,
							child_position,
							child_length,
							output,
							transaction,
						)?;
						remaining -= child_length;
						child_position = 0;
					}
				},
			}
			return Ok(());
		}
		let Some(cache_pointer) = object.cache_pointer else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		if position >= cache_pointer.length {
			return Ok(());
		}
		let read_length = std::cmp::min(length, cache_pointer.length - position);
		let mut path = self
			.server
			.cache_path()
			.join(cache_pointer.artifact.to_string());
		if let Some(path_) = cache_pointer.path {
			path.push(path_);
		}
		let mut file = match std::fs::File::open(&path) {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
			},
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open cache file");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			},
		};
		file.seek(std::io::SeekFrom::Start(cache_pointer.position + position))
			.map_err(|error| {
				tracing::error!(%error, path = %path.display(), "failed to seek while reading");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let mut bytes = vec![0; read_length.to_usize().unwrap()];
		let mut n = 0;
		while n < bytes.len() {
			let n_ = file.read(&mut bytes[n..]).map_err(|error| {
				tracing::error!(%error, path = %path.display(), "failed to read");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			if n_ == 0 {
				break;
			}
			n += n_;
		}
		bytes.truncate(n);
		output.extend_from_slice(&bytes);
		Ok(())
	}

	fn file_node_sync_inner(
		&self,
		file: &tg::artifact::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let data = self.artifact_data_sync_inner(file, transaction)?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&pointer, None, transaction)?;
				let tg::graph::data::Node::File(node) = node else {
					tracing::error!(pointer = ?pointer, "expected file node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn symlink_node_sync_inner(
		&self,
		symlink: &tg::artifact::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let data = self.artifact_data_sync_inner(symlink, transaction)?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&pointer, None, transaction)?;
				let tg::graph::data::Node::Symlink(node) = node else {
					tracing::error!(pointer = ?pointer, "expected symlink node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn directory_entries_sync_inner(
		&self,
		directory: &tg::artifact::Id,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<BTreeMap<String, tg::artifact::Id>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let (directory, graph) = self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = Self::artifact_id_from_edge_inner(edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = Self::artifact_id_from_directory_edge_inner(
							child.directory,
							graph.as_ref(),
						)?;
						stack.push((artifact, graph.clone()));
					}
				},
			}
		}
		Ok(entries)
	}

	fn getattr_from_node_sync_inner(
		&self,
		node: &Node,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		let artifact = node.artifact.clone();
		match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_sync_inner(&artifact, transaction)?;
				let size = file.contents.as_ref().map_or(Ok(0), |contents| {
					self.blob_length_sync_inner(contents, transaction)
				})?;
				Ok(vfs::Attrs::new(vfs::FileType::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::FileType::Directory))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Symlink) => {
				Ok(vfs::Attrs::new(vfs::FileType::Symlink))
			},
			None => Ok(vfs::Attrs::new(vfs::FileType::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}

	fn try_open_backing_fd_sync_inner(
		&self,
		id: &tg::blob::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<OwnedFd>> {
		let id: tg::object::Id = id.clone().into();
		let Some(object) = self.try_get_object(&id, transaction)? else {
			return Ok(None);
		};
		let Some(cache_pointer) = object.cache_pointer else {
			return Ok(None);
		};
		if cache_pointer.position != 0 {
			return Ok(None);
		}
		let mut path = self
			.server
			.cache_path()
			.join(cache_pointer.artifact.to_string());
		if let Some(path_) = cache_pointer.path {
			path.push(path_);
		}
		match std::fs::File::open(&path) {
			Ok(file) => Ok(Some(file.into())),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open cache file");
				Err(std::io::Error::from_raw_os_error(libc::EIO))
			},
		}
	}

	fn try_precompute_node_attrs_sync_inner(
		&self,
		artifact: &tg::artifact::Id,
		transaction: Option<&Transaction<'_>>,
	) -> Option<vfs::Attrs> {
		match artifact.kind() {
			tg::artifact::Kind::File => {
				let (file, _) = self.file_node_sync_inner(artifact, transaction).ok()?;
				let size = file.contents.as_ref().map_or(Some(0), |contents| {
					self.blob_length_sync_inner(contents, transaction).ok()
				})?;
				Some(vfs::Attrs::new(vfs::FileType::File {
					executable: file.executable,
					size,
				}))
			},
			_ => Self::attrs_from_artifact(Some(artifact)),
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<tangram_store::Object<'static>>> {
		#[cfg(feature = "lmdb")]
		if let (crate::store::Store::Lmdb(store), Some(transaction)) =
			(&self.server.store, transaction)
		{
			return store
				.try_get_object_with_transaction(transaction, id)
				.map_err(|error| Self::map_store_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		self.server
			.store
			.try_get_object_sync(id)
			.map_err(|error| Self::map_store_sync_error(&error))
	}

	fn try_get_object_data(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		#[cfg(feature = "lmdb")]
		if let (crate::store::Store::Lmdb(store), Some(transaction)) =
			(&self.server.store, transaction)
		{
			return store
				.try_get_object_data_with_transaction(transaction, id)
				.map_err(|error| Self::map_store_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		self.server
			.store
			.try_get_object_data_sync(id)
			.map_err(|error| Self::map_store_sync_error(&error))
	}

	fn map_store_sync_error(error: &tg::Error) -> std::io::Error {
		tracing::error!(error = %error.trace(), "failed to access local object data");
		std::io::Error::from_raw_os_error(libc::EIO)
	}

	fn attrs_from_artifact(artifact: Option<&tg::artifact::Id>) -> Option<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::File) => None,
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Some(vfs::Attrs::new(vfs::FileType::Directory))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Symlink) => {
				Some(vfs::Attrs::new(vfs::FileType::Symlink))
			},
			None => Some(vfs::Attrs::new(vfs::FileType::Directory)),
			_ => None,
		}
	}
}

impl Nodes {
	fn new() -> Self {
		let nodes = DashMap::default();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			Node {
				artifact: None,
				attrs: Some(vfs::Attrs::new(vfs::FileType::Directory)),
				children: HashMap::default(),
				depth: 0,
				lookup_count: u64::MAX,
				name: None,
				parent: vfs::ROOT_NODE_ID,
			},
		);
		Self { nodes }
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Ok(self.lookup_sync(parent, name))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.lookup_parent_sync(id)
	}

	async fn get(&self, id: u64) -> std::io::Result<Node> {
		self.get_sync(id)
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> Option<u64> {
		self.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
	}

	fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		self.nodes.get(&id).map(|node| node.parent).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
	}

	fn get_sync(&self, id: u64) -> std::io::Result<Node> {
		self.nodes.get(&id).map(|node| node.clone()).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
	}

	fn set_attrs(&self, id: u64, attrs: vfs::Attrs) {
		let Some(mut node) = self.nodes.get_mut(&id) else {
			return;
		};
		node.attrs = Some(attrs);
	}

	fn remember(&self, id: u64) {
		if id == vfs::ROOT_NODE_ID {
			return;
		}
		let Some(mut node) = self.nodes.get_mut(&id) else {
			return;
		};
		node.lookup_count = node.lookup_count.saturating_add(1);
	}

	fn forget(&self, id: u64, nlookup: u64) -> Vec<u64> {
		if id == vfs::ROOT_NODE_ID || nlookup == 0 {
			return Vec::new();
		}

		let Some(mut node) = self.nodes.get_mut(&id) else {
			return Vec::new();
		};
		node.lookup_count = node.lookup_count.saturating_sub(nlookup);
		let should_prune = node.lookup_count == 0 && node.children.is_empty();
		drop(node);
		if !should_prune {
			return Vec::new();
		}

		let mut removed = Vec::new();
		self.prune(id, &mut removed);
		removed
	}

	fn prune(&self, mut id: u64, removed: &mut Vec<u64>) {
		loop {
			if id == vfs::ROOT_NODE_ID {
				return;
			}

			let Some(node) = self.nodes.get(&id) else {
				return;
			};
			if node.lookup_count != 0 || !node.children.is_empty() {
				return;
			}
			let parent = node.parent;
			let name = node.name.clone();
			drop(node);

			self.nodes.remove(&id);
			removed.push(id);

			let Some(mut parent_node) = self.nodes.get_mut(&parent) else {
				return;
			};
			if let Some(name) = name {
				parent_node.children.remove(&name);
			}
			let prune_parent = parent != vfs::ROOT_NODE_ID
				&& parent_node.lookup_count == 0
				&& parent_node.children.is_empty();
			drop(parent_node);
			if !prune_parent {
				return;
			}
			id = parent;
		}
	}

	fn insert(
		&self,
		id: u64,
		parent: u64,
		name: &str,
		artifact: tg::artifact::Id,
		depth: u64,
		attrs: Option<vfs::Attrs>,
	) {
		// Insert the new node.
		self.nodes.insert(
			id,
			Node {
				artifact: Some(artifact),
				attrs,
				children: HashMap::default(),
				depth,
				lookup_count: 0,
				name: Some(name.to_owned()),
				parent,
			},
		);
		// Update the parent's children map.
		if let Some(mut parent_node) = self.nodes.get_mut(&parent) {
			parent_node.children.insert(name.to_owned(), id);
		}
	}
}

impl vfs::Provider for Provider {
	fn handle_batch(
		&self,
		requests: Vec<vfs::Request>,
	) -> impl std::future::Future<Output = Vec<std::io::Result<vfs::Response>>> + Send {
		Provider::handle_batch(self, requests)
	}

	fn handle_batch_sync(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		Provider::handle_batch_sync(self, requests)
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Provider::lookup(self, parent, name).await
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Provider::lookup_sync(self, parent, name)
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		Provider::lookup_parent(self, id).await
	}

	fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		Provider::lookup_parent_sync(self, id)
	}

	fn remember_sync(&self, id: u64) {
		Provider::remember_sync(self, id);
	}

	fn forget_sync(&self, id: u64, nlookup: u64) {
		Provider::forget_sync(self, id, nlookup);
	}

	async fn getattr(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		Provider::getattr(self, id).await
	}

	fn getattr_sync(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		Provider::getattr_sync(self, id)
	}

	async fn open(&self, id: u64) -> std::io::Result<u64> {
		Provider::open(self, id).await
	}

	fn open_sync(&self, id: u64) -> std::io::Result<(u64, Option<OwnedFd>)> {
		Provider::open_sync(self, id)
	}

	async fn read(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		Provider::read(self, id, position, length).await
	}

	fn read_sync(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		Provider::read_sync(self, id, position, length)
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		Provider::readlink(self, id).await
	}

	fn readlink_sync(&self, id: u64) -> std::io::Result<Bytes> {
		Provider::readlink_sync(self, id)
	}

	async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		Provider::listxattrs(self, id).await
	}

	fn listxattrs_sync(&self, id: u64) -> std::io::Result<Vec<String>> {
		Provider::listxattrs_sync(self, id)
	}

	async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		Provider::getxattr(self, id, name).await
	}

	fn getxattr_sync(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		Provider::getxattr_sync(self, id, name)
	}

	async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		Provider::opendir(self, id).await
	}

	fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		Provider::opendir_sync(self, id)
	}

	async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		Provider::readdir(self, id).await
	}

	fn readdir_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		Provider::readdir_sync(self, id)
	}

	async fn readdirplus(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus(self, id).await
	}

	fn readdirplus_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus_sync(self, id)
	}

	async fn close(&self, id: u64) {
		Provider::close(self, id).await;
	}

	fn close_sync(&self, id: u64) {
		Provider::close_sync(self, id);
	}
}
