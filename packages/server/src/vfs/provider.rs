#[cfg(feature = "lmdb")]
use heed as lmdb;
use {
	crate::Server,
	bytes::Bytes,
	dashmap::DashMap,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		os::fd::OwnedFd,
		os::unix::{ffi::OsStrExt as _, fs::FileExt as _},
		path::{Path, PathBuf},
		pin::pin,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_object_store::prelude::*,
	tangram_vfs as vfs,
};

pub struct Provider {
	directory_handles: DashMap<u64, DirectorySnapshot, fnv::FnvBuildHasher>,
	directory_snapshot_loads: DashMap<u64, Arc<tokio::sync::Mutex<()>>, fnv::FnvBuildHasher>,
	directory_snapshots: Mutex<vfs::cache::WeightedLruCache<u64, DirectorySnapshot>>,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	handle_count: AtomicU64,
	nodes: Nodes,
	server: Server,
}

#[derive(Clone)]
struct DirectorySnapshot {
	depth: u64,
	entries: Option<Arc<[DirectorySnapshotEntry]>>,
	node: u64,
	pageable: bool,
	parent: u64,
}

#[derive(Clone)]
struct DirectorySnapshotEntry {
	artifact: Option<ArtifactInfo>,
	kind: vfs::EntryKind,
	name: String,
	node: u64,
}

struct Nodes {
	state: Mutex<State>,
}

struct State {
	next: u64,
	nodes: BTreeMap<u64, Node>,
}

#[derive(Clone)]
struct Node {
	artifact: Option<ArtifactInfo>,
	attrs: Option<vfs::Attrs>,
	children: BTreeMap<String, u64>,
	depth: u64,
	lookup_count: u64,
	name: Option<String>,
	parent: u64,
}

#[derive(Clone)]
struct NodeInfo {
	artifact: Option<ArtifactInfo>,
	attrs: Option<vfs::Attrs>,
	depth: u64,
	parent: u64,
}

#[derive(Clone)]
struct ArtifactInfo {
	data: Option<tg::artifact::data::Artifact>,
	id: tg::artifact::Id,
}

struct PendingNodes<'a> {
	committed: bool,
	ids: Vec<u64>,
	provider: &'a Provider,
}

struct SnapshotLoad<'a> {
	id: u64,
	loads: &'a DashMap<u64, Arc<tokio::sync::Mutex<()>>, fnv::FnvBuildHasher>,
	mutex: Arc<tokio::sync::Mutex<()>>,
}

pub struct FileHandle {
	blob: tg::blob::Id,
}

#[cfg(feature = "lmdb")]
type Transaction<'a> = lmdb::RoTxn<'a>;
#[cfg(not(feature = "lmdb"))]
type Transaction<'a> = ();

const FUSE_DIRENT_HEADER_SIZE: usize = 24;
const FUSE_DIRENT_PLUS_HEADER_SIZE: usize = 152;
const DIRECTORY_SNAPSHOT_CACHE_CAPACITY: usize = 64 * 1024 * 1024;
const DIRECTORY_SNAPSHOT_ENTRY_OVERHEAD: usize = 256;
const DIRECTORY_SNAPSHOT_OVERHEAD: usize = 256;
const DIRECTORY_SNAPSHOT_READ_ENTRY_LIMIT: usize = 65_536;
const NAME_MAX: usize = 255;

impl Provider {
	pub async fn new(server: &Server) -> tg::Result<Self> {
		// Create the nodes.
		let nodes = Nodes::new();

		// Create the provider.
		let directory_handles = DashMap::default();
		let directory_snapshot_loads = DashMap::default();
		let directory_snapshots = Mutex::new(vfs::cache::WeightedLruCache::new(
			DIRECTORY_SNAPSHOT_CACHE_CAPACITY,
		));
		let file_handles = DashMap::default();
		let handle_count = AtomicU64::new(1000);
		let server = server.clone();
		let provider = Self {
			directory_handles,
			directory_snapshot_loads,
			directory_snapshots,
			file_handles,
			handle_count,
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
						.map(|id| vfs::Response::Lookup { attrs: None, id }),
					vfs::Request::LookupAndRemember { id, name } => {
						self.lookup_and_remember(id, &name).await.map(|entry| {
							let (id, attrs) = entry.unzip();
							vfs::Response::Lookup { attrs, id }
						})
					},
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
					vfs::Request::ReadDir {
						handle,
						length,
						offset,
					} => self
						.readdir(handle, offset, length)
						.await
						.map(|entries| vfs::Response::ReadDir { entries }),
					vfs::Request::ReadDirPlus {
						handle,
						length,
						offset,
					} => self
						.readdirplus(handle, offset, length)
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
		if let crate::object::Store::Lmdb(store) = &self.server.object_store {
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
					.lookup_sync_inner(id, &name, transaction, false)
					.map(|id| vfs::Response::Lookup { attrs: None, id }),
				vfs::Request::LookupAndRemember { id, name } => self
					.lookup_and_remember_sync_inner(id, &name, transaction)
					.map(|entry| {
						let (id, attrs) = entry.unzip();
						vfs::Response::Lookup { attrs, id }
					}),
				vfs::Request::LookupParent { id } => self
					.lookup_parent_sync(id)
					.map(|id| vfs::Response::LookupParent { id }),
				vfs::Request::Open { id } => self
					.open_sync_inner(id, transaction)
					.map(|(handle, backing_fd)| vfs::Response::Open { handle, backing_fd }),
				vfs::Request::OpenDir { id } => self
					.opendir_sync_inner(id, transaction)
					.map(|handle| vfs::Response::OpenDir { handle }),
				vfs::Request::Read {
					handle,
					position,
					length,
				} => self
					.read_sync_inner(handle, position, length, transaction)
					.map(|bytes| vfs::Response::Read { bytes }),
				vfs::Request::ReadDir {
					handle,
					length,
					offset,
				} => self
					.readdir_sync(handle, offset, length)
					.map(|entries| vfs::Response::ReadDir { entries }),
				vfs::Request::ReadDirPlus {
					handle,
					length,
					offset,
				} => self
					.readdirplus_sync_inner(handle, offset, length, transaction)
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
		self.directory_handles.remove(&id);
		self.file_handles.remove(&id);
	}

	pub fn close_sync(&self, id: u64) {
		self.directory_handles.remove(&id);
		self.file_handles.remove(&id);
	}

	pub fn forget_sync(&self, id: u64, nlookup: u64) {
		let removed = self.nodes.forget(id, nlookup);
		let mut cache = self.directory_snapshots.lock().unwrap();
		for id in removed {
			cache.remove(&id);
		}
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
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
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
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
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
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let (file, _) = self.file_node_inner(&artifact).await?;
		let mut names = Vec::with_capacity(2);
		if !file.dependencies.is_empty() {
			names.push(tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
		}
		if file.module.is_some() {
			names.push(tg::file::MODULE_XATTR_NAME.to_owned());
		}
		Ok(names)
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
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let (file, _) = self.file_node_sync_inner(&artifact, transaction)?;
		let mut names = Vec::with_capacity(2);
		if !file.dependencies.is_empty() {
			names.push(tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
		}
		if file.module.is_some() {
			names.push(tg::file::MODULE_XATTR_NAME.to_owned());
		}
		Ok(names)
	}

	pub async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		self.lookup_inner(parent, name, false).await
	}

	pub async fn lookup_and_remember(
		&self,
		parent: u64,
		name: &str,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		let Some(id) = self.lookup_inner(parent, name, true).await? else {
			return Ok(None);
		};
		let mut pending = PendingNodes::new(self);
		pending.push_acquired(id);
		let attrs = self.getattr(id).await?;
		pending.commit();

		Ok(Some((id, attrs)))
	}

	async fn lookup_inner(
		&self,
		parent: u64,
		name: &str,
		remember: bool,
	) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			if remember {
				self.nodes.remember_existing(parent)?;
			}
			return Ok(Some(parent));
		} else if name == ".." {
			let id = if remember {
				self.nodes.lookup_parent_and_remember_sync(parent)?
			} else {
				self.lookup_parent(parent).await?
			};
			return Ok(Some(id));
		}

		// First, try to look up in the nodes storage.
		let id = if remember {
			self.nodes.lookup_and_remember_sync(parent, name)
		} else {
			self.nodes.lookup(parent, name).await?
		};
		if let Some(id) = id {
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
				.or_else(|| Path::new(name).file_stem().and_then(|s| s.to_str()))
				.unwrap_or(name);
			let Ok(id) = name.parse() else {
				return Ok(None);
			};
			let artifact = ArtifactInfo { data: None, id };
			Some((artifact, 1))
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let entry = 'a: {
			if let Some(entry) = entry {
				break 'a Some(entry);
			}
			let NodeInfo {
				artifact, depth, ..
			} = self.get(parent).await?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			if !matches!(artifact.id.kind(), tg::artifact::Kind::Directory) {
				return Ok(None);
			}
			let artifact = self
				.directory_lookup_entry_inner(&artifact, name, None)
				.await?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			Some((artifact, depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let attrs = Self::attrs_from_artifact(Some(&artifact));
		let id = self
			.nodes
			.get_or_insert_child(parent, name, artifact, depth, attrs, remember)?;

		Ok(Some(id))
	}

	pub fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		self.lookup_sync_inner(parent, name, None, false)
	}

	pub fn lookup_and_remember_sync(
		&self,
		parent: u64,
		name: &str,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		self.lookup_and_remember_sync_inner(parent, name, None)
	}

	fn lookup_and_remember_sync_inner(
		&self,
		parent: u64,
		name: &str,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		let Some(id) = self.lookup_sync_inner(parent, name, transaction, true)? else {
			return Ok(None);
		};
		let mut pending = PendingNodes::new(self);
		pending.push_acquired(id);
		let attrs = self.getattr_sync_inner(id, transaction)?;
		pending.commit();

		Ok(Some((id, attrs)))
	}

	fn lookup_sync_inner(
		&self,
		parent: u64,
		name: &str,
		transaction: Option<&Transaction<'_>>,
		remember: bool,
	) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			if remember {
				self.nodes.remember_existing(parent)?;
			}
			return Ok(Some(parent));
		} else if name == ".." {
			let id = if remember {
				self.nodes.lookup_parent_and_remember_sync(parent)?
			} else {
				self.lookup_parent_sync(parent)?
			};
			return Ok(Some(id));
		}

		// First, try to look up in the nodes storage.
		let id = if remember {
			self.nodes.lookup_and_remember_sync(parent, name)
		} else {
			self.nodes.lookup_sync(parent, name)
		};
		if let Some(id) = id {
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
				.or_else(|| Path::new(name).file_stem().and_then(|s| s.to_str()))
				.unwrap_or(name);
			let Ok(id) = name.parse() else {
				return Ok(None);
			};
			let artifact = ArtifactInfo { data: None, id };
			Some((artifact, 1))
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let entry = 'a: {
			if let Some(entry) = entry {
				break 'a Some(entry);
			}
			let NodeInfo {
				artifact, depth, ..
			} = self.get_sync(parent)?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			if !matches!(artifact.id.kind(), tg::artifact::Kind::Directory) {
				return Ok(None);
			}
			let artifact =
				self.directory_lookup_entry_sync_inner(&artifact, name, None, transaction)?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			Some((artifact, depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let attrs = Self::attrs_from_artifact(Some(&artifact));
		let id = self
			.nodes
			.get_or_insert_child(parent, name, artifact, depth, attrs, remember)?;
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
		let NodeInfo { artifact, .. } = self.get(id).await?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Ensure it is a file.
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
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
		let id = self.handle_count.fetch_add(1, Ordering::Relaxed);
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
		let NodeInfo { artifact, .. } = self.get_sync(id)?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Ensure it is a file.
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
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
		let id = self.handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(id, FileHandle { blob });

		Ok((id, backing_fd))
	}

	pub async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		let snapshot = self.directory_snapshot(id).await?;
		let handle = self.insert_directory_handle(&snapshot);

		Ok(handle)
	}

	pub fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		self.opendir_sync_inner(id, None)
	}

	fn opendir_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<u64> {
		let snapshot = self.directory_snapshot_sync_inner(id, transaction)?;
		let handle = self.insert_directory_handle(&snapshot);

		Ok(handle)
	}

	async fn directory_snapshot(&self, id: u64) -> std::io::Result<DirectorySnapshot> {
		if let Some(snapshot) = self.directory_snapshots.lock().unwrap().get(&id) {
			return Ok(snapshot);
		}
		let load = SnapshotLoad::new(&self.directory_snapshot_loads, id);
		let _guard = load.mutex.clone().lock_owned().await;
		if let Some(snapshot) = self.directory_snapshots.lock().unwrap().get(&id) {
			return Ok(snapshot);
		}

		// Get the node.
		let NodeInfo {
			artifact,
			depth,
			parent,
			..
		} = self.get(id).await?;
		let entries = match &artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				if self.should_page_directory_inner(artifact).await? {
					None
				} else {
					Some(self.directory_entries_inner(artifact, None).await?)
				}
			},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			None => Some(BTreeMap::new()),
		};
		let pageable = artifact.is_some();
		let snapshot = Self::create_directory_snapshot(id, parent, depth, entries, pageable);
		let snapshot = if snapshot.weight() > DIRECTORY_SNAPSHOT_CACHE_CAPACITY {
			snapshot.paged()
		} else {
			snapshot
		};
		let weight = snapshot.weight();
		let snapshot = self
			.directory_snapshots
			.lock()
			.unwrap()
			.insert(id, snapshot, weight);

		Ok(snapshot)
	}

	fn directory_snapshot_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<DirectorySnapshot> {
		if let Some(snapshot) = self.directory_snapshots.lock().unwrap().get(&id) {
			return Ok(snapshot);
		}
		let load = SnapshotLoad::new(&self.directory_snapshot_loads, id);
		let _guard = futures::executor::block_on(load.mutex.clone().lock_owned());
		if let Some(snapshot) = self.directory_snapshots.lock().unwrap().get(&id) {
			return Ok(snapshot);
		}

		// Get the node.
		let NodeInfo {
			artifact,
			depth,
			parent,
			..
		} = self.get_sync(id)?;
		let entries = match &artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				if self.should_page_directory_sync_inner(artifact, transaction)? {
					None
				} else {
					Some(self.directory_entries_sync_inner(artifact, None, transaction)?)
				}
			},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			None => Some(BTreeMap::new()),
		};
		let pageable = artifact.is_some();
		let snapshot = Self::create_directory_snapshot(id, parent, depth, entries, pageable);
		let snapshot = if snapshot.weight() > DIRECTORY_SNAPSHOT_CACHE_CAPACITY {
			snapshot.paged()
		} else {
			snapshot
		};
		let weight = snapshot.weight();
		let snapshot = self
			.directory_snapshots
			.lock()
			.unwrap()
			.insert(id, snapshot, weight);

		Ok(snapshot)
	}

	fn create_directory_snapshot(
		node: u64,
		parent: u64,
		depth: u64,
		entries: Option<BTreeMap<String, ArtifactInfo>>,
		pageable: bool,
	) -> DirectorySnapshot {
		let entries = entries.map(|entries| {
			let mut snapshot = Vec::with_capacity(entries.len() + 2);
			snapshot.push(DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: ".".to_owned(),
				node,
			});
			snapshot.push(DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: "..".to_owned(),
				node: parent,
			});
			for (name, artifact) in entries {
				let kind = Self::entry_kind_from_artifact(&artifact);
				snapshot.push(DirectorySnapshotEntry {
					artifact: Some(artifact),
					kind,
					name,
					node: 0,
				});
			}
			snapshot
		});
		let entries = entries.map(Arc::from);

		DirectorySnapshot {
			depth,
			entries,
			node,
			pageable,
			parent,
		}
	}

	async fn should_page_directory_inner(&self, artifact: &ArtifactInfo) -> std::io::Result<bool> {
		let (directory, _) = self.directory_node_inner(artifact).await?;

		Ok(Self::directory_requires_paging(&directory))
	}

	fn should_page_directory_sync_inner(
		&self,
		artifact: &ArtifactInfo,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<bool> {
		let (directory, _) = self.directory_node_sync_inner(artifact, transaction)?;

		Ok(Self::directory_requires_paging(&directory))
	}

	fn directory_requires_paging(directory: &tg::graph::data::Directory) -> bool {
		let count = match directory {
			tg::graph::data::Directory::Branch(branch) => branch
				.children
				.iter()
				.fold(0u64, |count, child| count.saturating_add(child.count)),
			tg::graph::data::Directory::Leaf(leaf) => leaf.entries.len().to_u64().unwrap(),
		};
		let entry_weight = std::mem::size_of::<DirectorySnapshotEntry>()
			.saturating_add(DIRECTORY_SNAPSHOT_ENTRY_OVERHEAD)
			.saturating_add(NAME_MAX);
		let estimated_weight = count
			.to_usize()
			.unwrap_or(usize::MAX)
			.saturating_mul(entry_weight)
			.saturating_add(DIRECTORY_SNAPSHOT_OVERHEAD);

		estimated_weight > DIRECTORY_SNAPSHOT_CACHE_CAPACITY
	}

	fn directory_children_range(
		children: Vec<tg::graph::data::DirectoryChild>,
		mut offset: u64,
		mut limit: u64,
	) -> Vec<(tg::graph::data::Edge<tg::directory::Id>, u64)> {
		let mut output = Vec::new();
		for child in children {
			if offset >= child.count {
				offset -= child.count;
				continue;
			}
			let count = child.count.saturating_sub(offset);
			output.push((child.directory, offset));
			if count >= limit {
				break;
			}
			limit -= count;
			offset = 0;
		}

		output
	}

	fn insert_directory_handle(&self, snapshot: &DirectorySnapshot) -> u64 {
		let handle = self.handle_count.fetch_add(1, Ordering::Relaxed);
		let snapshot = snapshot.paged();
		self.directory_handles.insert(handle, snapshot);

		handle
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
			token: None,
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

	fn directory_handle(&self, handle: u64) -> std::io::Result<DirectorySnapshot> {
		self.directory_handles
			.get(&handle)
			.map(|handle| handle.clone())
			.ok_or_else(|| {
				tracing::error!(%handle, "tried to read from an invalid directory handle");
				std::io::Error::from_raw_os_error(libc::ENOENT)
			})
	}

	pub async fn readdir(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_handle(handle)?;

		self.read_directory_snapshot(&snapshot, offset, length)
			.await
	}

	pub fn readdir_sync(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_handle(handle)?;

		self.read_directory_snapshot_sync_inner(&snapshot, offset, length, None)
	}

	pub async fn readdir_node(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_snapshot(id).await?;

		self.read_directory_snapshot(&snapshot, offset, length)
			.await
	}

	pub fn readdir_node_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		#[cfg(feature = "lmdb")]
		if let crate::object::Store::Lmdb(store) = &self.server.object_store {
			let transaction = store.env().read_txn().map_err(|error| {
				tracing::error!(?error, "failed to begin an lmdb read transaction");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			return self.readdir_node_sync_inner(id, offset, length, Some(&transaction));
		}

		self.readdir_node_sync_inner(id, offset, length, None)
	}

	fn readdir_node_sync_inner(
		&self,
		id: u64,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_snapshot_sync_inner(id, transaction)?;

		self.read_directory_snapshot_sync_inner(&snapshot, offset, length, transaction)
	}

	async fn read_directory_snapshot(
		&self,
		snapshot: &DirectorySnapshot,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let limit = Self::directory_snapshot_entry_limit(length, FUSE_DIRENT_HEADER_SIZE);
		let snapshot_entries = self
			.directory_snapshot_entries_inner(snapshot, offset, limit)
			.await?;
		let mut entries = Vec::new();
		let mut size = 0;
		for entry in snapshot_entries {
			let entry_size = Self::readdir_entry_size(entry.name.len());
			if entry_size.saturating_add(size) > length.to_usize().unwrap_or(usize::MAX) {
				break;
			}
			size = size.saturating_add(entry_size);
			let node = self.readdir_entry_node(snapshot, &entry)?;
			entries.push((entry.name, node, entry.kind));
		}

		Ok(entries)
	}

	fn read_directory_snapshot_sync_inner(
		&self,
		snapshot: &DirectorySnapshot,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let limit = Self::directory_snapshot_entry_limit(length, FUSE_DIRENT_HEADER_SIZE);
		let snapshot_entries =
			self.directory_snapshot_entries_sync_inner(snapshot, offset, limit, transaction)?;
		let mut entries = Vec::new();
		let mut size = 0;
		for entry in snapshot_entries {
			let entry_size = Self::readdir_entry_size(entry.name.len());
			if entry_size.saturating_add(size) > length.to_usize().unwrap_or(usize::MAX) {
				break;
			}
			size = size.saturating_add(entry_size);
			let node = self.readdir_entry_node(snapshot, &entry)?;
			entries.push((entry.name, node, entry.kind));
		}

		Ok(entries)
	}

	/// Get the node ID to report for a readdir entry. Entries created from an artifact carry
	/// a node ID of zero until they are looked up, but readdir must report a nonzero inode
	/// number, because GNU make and other tools ignore directory entries whose inode number
	/// is zero. The node is inserted without a lookup reference, because readdir, unlike
	/// readdirplus, does not cause the kernel to hold one.
	fn readdir_entry_node(
		&self,
		snapshot: &DirectorySnapshot,
		entry: &DirectorySnapshotEntry,
	) -> std::io::Result<u64> {
		let Some(artifact) = &entry.artifact else {
			return Ok(entry.node);
		};
		let attrs = Self::attrs_from_artifact(Some(artifact));
		self.nodes.get_or_insert_child(
			snapshot.node,
			&entry.name,
			artifact.clone(),
			snapshot.depth + 1,
			attrs,
			false,
		)
	}

	async fn directory_snapshot_entries_inner(
		&self,
		snapshot: &DirectorySnapshot,
		offset: u64,
		limit: usize,
	) -> std::io::Result<Vec<DirectorySnapshotEntry>> {
		if let Some(entries) = &snapshot.entries {
			let entries = entries
				.iter()
				.skip(offset.to_usize().unwrap_or(usize::MAX))
				.take(limit)
				.cloned()
				.collect();

			return Ok(entries);
		}
		let NodeInfo { artifact, .. } = self.get(snapshot.node).await?;
		let Some(artifact) = artifact else {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let mut entries = snapshot.virtual_entries(offset, limit);
		let offset = offset.saturating_sub(2);
		let limit = limit.saturating_sub(entries.len());
		let artifact_entries = self
			.directory_entries_range_inner(&artifact, None, offset, limit)
			.await?;
		entries.extend(artifact_entries.into_iter().map(|(name, artifact)| {
			let kind = Self::entry_kind_from_artifact(&artifact);
			DirectorySnapshotEntry {
				artifact: Some(artifact),
				kind,
				name,
				node: 0,
			}
		}));

		Ok(entries)
	}

	fn directory_snapshot_entries_sync_inner(
		&self,
		snapshot: &DirectorySnapshot,
		offset: u64,
		limit: usize,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<DirectorySnapshotEntry>> {
		if let Some(entries) = &snapshot.entries {
			let entries = entries
				.iter()
				.skip(offset.to_usize().unwrap_or(usize::MAX))
				.take(limit)
				.cloned()
				.collect();

			return Ok(entries);
		}
		let NodeInfo { artifact, .. } = self.get_sync(snapshot.node)?;
		let Some(artifact) = artifact else {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let mut entries = snapshot.virtual_entries(offset, limit);
		let offset = offset.saturating_sub(2);
		let limit = limit.saturating_sub(entries.len());
		let artifact_entries =
			self.directory_entries_range_sync_inner(&artifact, None, offset, limit, transaction)?;
		entries.extend(artifact_entries.into_iter().map(|(name, artifact)| {
			let kind = Self::entry_kind_from_artifact(&artifact);
			DirectorySnapshotEntry {
				artifact: Some(artifact),
				kind,
				name,
				node: 0,
			}
		}));

		Ok(entries)
	}

	pub async fn readdirplus(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let directory = self.directory_handle(handle)?;

		self.readdirplus_inner(&directory, offset, length).await
	}

	pub async fn readdirplus_node(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let directory = self.directory_snapshot(id).await?;

		self.readdirplus_inner(&directory, offset, length).await
	}

	async fn readdirplus_inner(
		&self,
		directory: &DirectorySnapshot,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let limit = Self::directory_snapshot_entry_limit(length, FUSE_DIRENT_PLUS_HEADER_SIZE);
		let snapshot_entries = self
			.directory_snapshot_entries_inner(directory, offset, limit)
			.await?;
		let mut entries = Vec::new();
		let mut pending = PendingNodes::new(self);
		let mut size = 0;
		for entry in snapshot_entries {
			let entry_size = Self::readdirplus_entry_size(entry.name.len());
			if entry_size.saturating_add(size) > length.to_usize().unwrap_or(usize::MAX) {
				break;
			}
			let (node, attrs) = if let Some(artifact) = &entry.artifact {
				let attrs = self
					.compute_attrs_from_artifact_inner(Some(artifact), directory.depth + 1)
					.await?;
				let node = self.nodes.get_or_insert_child(
					directory.node,
					&entry.name,
					artifact.clone(),
					directory.depth + 1,
					Some(attrs),
					true,
				)?;
				pending.push_acquired(node);
				(node, attrs)
			} else {
				pending.acquire(entry.node)?;
				(entry.node, self.getattr(entry.node).await?)
			};
			size = size.saturating_add(entry_size);
			entries.push((entry.name, node, attrs));
		}
		pending.commit();

		Ok(entries)
	}

	pub fn readdirplus_sync(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		self.readdirplus_sync_inner(handle, offset, length, None)
	}

	pub fn readdirplus_node_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		#[cfg(feature = "lmdb")]
		if let crate::object::Store::Lmdb(store) = &self.server.object_store {
			let transaction = store.env().read_txn().map_err(|error| {
				tracing::error!(?error, "failed to begin an lmdb read transaction");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			return self.readdirplus_node_sync_inner(id, offset, length, Some(&transaction));
		}

		self.readdirplus_node_sync_inner(id, offset, length, None)
	}

	fn readdirplus_node_sync_inner(
		&self,
		id: u64,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let directory = self.directory_snapshot_sync_inner(id, transaction)?;

		self.read_directory_plus_snapshot_sync_inner(&directory, offset, length, transaction)
	}

	fn readdirplus_sync_inner(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let directory = self.directory_handle(handle)?;

		self.read_directory_plus_snapshot_sync_inner(&directory, offset, length, transaction)
	}

	fn read_directory_plus_snapshot_sync_inner(
		&self,
		directory: &DirectorySnapshot,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let limit = Self::directory_snapshot_entry_limit(length, FUSE_DIRENT_PLUS_HEADER_SIZE);
		let snapshot_entries =
			self.directory_snapshot_entries_sync_inner(directory, offset, limit, transaction)?;
		let mut entries = Vec::new();
		let mut pending = PendingNodes::new(self);
		let mut size = 0;
		for entry in snapshot_entries {
			let entry_size = Self::readdirplus_entry_size(entry.name.len());
			if entry_size.saturating_add(size) > length.to_usize().unwrap_or(usize::MAX) {
				break;
			}
			let (node, attrs) = if let Some(artifact) = &entry.artifact {
				let attrs = self.compute_attrs_from_artifact_sync_inner(
					Some(artifact),
					directory.depth + 1,
					transaction,
				)?;
				let node = self.nodes.get_or_insert_child(
					directory.node,
					&entry.name,
					artifact.clone(),
					directory.depth + 1,
					Some(attrs),
					true,
				)?;
				pending.push_acquired(node);
				(node, attrs)
			} else {
				pending.acquire(entry.node)?;
				(
					entry.node,
					self.getattr_sync_inner(entry.node, transaction)?,
				)
			};
			size = size.saturating_add(entry_size);
			entries.push((entry.name, node, attrs));
		}
		pending.commit();

		Ok(entries)
	}

	pub async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		// Get the node.
		let NodeInfo {
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
		if !matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		}

		// Render the target.
		let (symlink, graph) = self.symlink_node_inner(&artifact).await?;
		let artifact = match symlink.artifact {
			Some(edge) => Some(Self::artifact_from_edge_inner(edge, graph.as_ref())?.id),
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
		let NodeInfo {
			artifact, depth, ..
		} = self.get_sync(id)?;
		let Some(artifact) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};
		if !matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		}

		// Render the target.
		let (symlink, graph) = self.symlink_node_sync_inner(&artifact, transaction)?;
		let artifact = match symlink.artifact {
			Some(edge) => Some(Self::artifact_from_edge_inner(edge, graph.as_ref())?.id),
			None => None,
		};
		Self::build_symlink_target(depth, artifact, symlink.path)
	}

	pub fn remember_sync(&self, id: u64) {
		self.nodes.remember(id);
	}

	async fn get(&self, id: u64) -> std::io::Result<NodeInfo> {
		self.nodes.get(id).await
	}

	fn get_sync(&self, id: u64) -> std::io::Result<NodeInfo> {
		self.nodes.get_sync(id)
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

	async fn getattr_from_node_inner(&self, node: &NodeInfo) -> std::io::Result<vfs::Attrs> {
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		self.compute_attrs_from_artifact_inner(node.artifact.as_ref(), node.depth)
			.await
	}

	async fn compute_attrs_from_artifact_inner(
		&self,
		artifact: Option<&ArtifactInfo>,
		depth: u64,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_inner(artifact).await?;
				let size = if let Some(contents) = file.contents.as_ref() {
					self.blob_length_inner(contents).await?
				} else {
					0
				};
				Ok(vfs::Attrs::new(vfs::AttrsInner::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) => {
				let (symlink, graph) = self.symlink_node_inner(artifact).await?;
				let artifact = match symlink.artifact {
					Some(edge) => Some(Self::artifact_from_edge_inner(edge, graph.as_ref())?.id),
					None => None,
				};
				let target = Self::build_symlink_target(depth, artifact, symlink.path)?;
				let size = target.len().to_u64().unwrap();
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}

	fn artifact_from_directory_edge_inner(
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactInfo> {
		match edge {
			tg::graph::data::Edge::Object(directory) => Ok(ArtifactInfo {
				data: None,
				id: directory.into(),
			}),
			tg::graph::data::Edge::Pointer(pointer) => Self::artifact_from_pointer_inner(
				&pointer,
				default_graph,
				Some(tg::artifact::Kind::Directory),
			),
		}
	}

	fn artifact_from_edge_inner(
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactInfo> {
		match edge {
			tg::graph::data::Edge::Object(id) => Ok(ArtifactInfo { data: None, id }),
			tg::graph::data::Edge::Pointer(pointer) => {
				Self::artifact_from_pointer_inner(&pointer, default_graph, None)
			},
		}
	}

	fn artifact_from_pointer_inner(
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
		expected_kind: Option<tg::artifact::Kind>,
	) -> std::io::Result<ArtifactInfo> {
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
		let id = tg::artifact::Id::new(kind, &bytes);
		Ok(ArtifactInfo {
			data: Some(data),
			id,
		})
	}

	async fn artifact_data_inner(
		&self,
		artifact: &ArtifactInfo,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		if let Some(data) = &artifact.data {
			return Ok(data.clone());
		}
		let id: tg::object::Id = artifact.id.clone().into();
		let Some(data) = self.try_get_data_inner(&id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(artifact = %artifact.id, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	async fn directory_entries_inner(
		&self,
		directory: &ArtifactInfo,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<BTreeMap<String, ArtifactInfo>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let (directory, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = Self::artifact_from_directory_edge_inner(
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

	async fn directory_entries_range_inner(
		&self,
		directory: &ArtifactInfo,
		default_graph: Option<&tg::graph::Id>,
		offset: u64,
		limit: usize,
	) -> std::io::Result<Vec<(String, ArtifactInfo)>> {
		let mut entries = Vec::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned(), offset)];
		while entries.len() < limit {
			let Some((directory, default_graph, offset)) = stack.pop() else {
				break;
			};
			let (directory, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					let offset = offset.to_usize().unwrap_or(usize::MAX);
					let limit = limit.saturating_sub(entries.len());
					for (name, edge) in leaf.entries.into_iter().skip(offset).take(limit) {
						let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
						entries.push((name, artifact));
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					let limit = limit.saturating_sub(entries.len()).to_u64().unwrap();
					let mut children = Vec::new();
					for (directory, offset) in
						Self::directory_children_range(branch.children, offset, limit)
					{
						let artifact =
							Self::artifact_from_directory_edge_inner(directory, graph.as_ref())?;
						children.push((artifact, graph.clone(), offset));
					}
					stack.extend(children.into_iter().rev());
				},
			}
		}

		Ok(entries)
	}

	async fn directory_lookup_entry_inner(
		&self,
		directory: &ArtifactInfo,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<Option<ArtifactInfo>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let (directory_data, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
					return Ok(Some(artifact));
				},
				tg::graph::data::Directory::Branch(branch) => {
					let Some(child) = branch
						.children
						.into_iter()
						.find(|child| name <= child.last.as_str())
					else {
						return Ok(None);
					};
					directory =
						Self::artifact_from_directory_edge_inner(child.directory, graph.as_ref())?;
					default_graph = graph;
				},
			}
		}
	}

	async fn directory_node_inner(
		&self,
		directory: &ArtifactInfo,
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
		file: &ArtifactInfo,
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
		let Some(data) = self.try_get_data_inner(&id).await? else {
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
		symlink: &ArtifactInfo,
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

	async fn try_get_data_inner(
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
		let arg = crate::object::store::TryGetArg { id: id.clone() };
		let object = self
			.server
			.object_store
			.try_get(arg)
			.await
			.map_err(|error| {
				tracing::error!(error = %error.trace(), %id, "failed to get the object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let Some(object) = object.object else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		if let Some(cache_pointer) = object.cache_pointer {
			return Ok(cache_pointer.length);
		}
		let Some(bytes) = object.bytes else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		let data = tg::object::Data::deserialize(id.kind(), &*bytes).map_err(|error| {
			tracing::error!(error = %error.trace(), %id, "failed to deserialize object data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		Self::blob_length_from_data(&id, data)
	}

	fn blob_length_from_data(id: &tg::object::Id, data: tg::object::Data) -> std::io::Result<u64> {
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
		artifact: &ArtifactInfo,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		if let Some(data) = &artifact.data {
			return Ok(data.clone());
		}
		let id: tg::object::Id = artifact.id.clone().into();
		let output = self.try_get_data(&id, transaction)?;
		let Some((_, data)) = output else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(artifact = %artifact.id, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn graph_data_sync_inner(
		&self,
		graph: &tg::graph::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let output = self.try_get_data(&id, transaction)?;
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
		let object = self.try_get_object(&id, transaction)?;
		let Some(object) = object else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		if let Some(cache_pointer) = object.cache_pointer {
			return Ok(cache_pointer.length);
		}
		let Some(bytes) = object.bytes else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		let data = tg::object::Data::deserialize(id.kind(), &*bytes).map_err(|error| {
			tracing::error!(error = %error.trace(), %id, "failed to deserialize object data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		Self::blob_length_from_data(&id, data)
	}

	fn directory_node_sync_inner(
		&self,
		directory: &ArtifactInfo,
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
		let file = match std::fs::File::open(&path) {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
			},
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open cache file");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			},
		};
		let file_position = cache_pointer
			.position
			.checked_add(position)
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
		let output_start = output.len();
		output.resize(output_start + read_length.to_usize().unwrap(), 0);
		let mut n = 0;
		while output_start + n < output.len() {
			let offset = file_position
				.checked_add(n.to_u64().unwrap())
				.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
			let n_ = file
				.read_at(&mut output[output_start + n..], offset)
				.map_err(|error| {
					tracing::error!(%error, path = %path.display(), "failed to read");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
			if n_ == 0 {
				break;
			}
			n += n_;
		}
		output.truncate(output_start + n);
		Ok(())
	}

	fn file_node_sync_inner(
		&self,
		file: &ArtifactInfo,
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
		symlink: &ArtifactInfo,
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
		directory: &ArtifactInfo,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<BTreeMap<String, ArtifactInfo>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let (directory, graph) = self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = Self::artifact_from_directory_edge_inner(
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

	fn directory_entries_range_sync_inner(
		&self,
		directory: &ArtifactInfo,
		default_graph: Option<&tg::graph::Id>,
		offset: u64,
		limit: usize,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, ArtifactInfo)>> {
		let mut entries = Vec::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned(), offset)];
		while entries.len() < limit {
			let Some((directory, default_graph, offset)) = stack.pop() else {
				break;
			};
			let (directory, graph) = self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Leaf(leaf) => {
					let offset = offset.to_usize().unwrap_or(usize::MAX);
					let limit = limit.saturating_sub(entries.len());
					for (name, edge) in leaf.entries.into_iter().skip(offset).take(limit) {
						let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
						entries.push((name, artifact));
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					let limit = limit.saturating_sub(entries.len()).to_u64().unwrap();
					let mut children = Vec::new();
					for (directory, offset) in
						Self::directory_children_range(branch.children, offset, limit)
					{
						let artifact =
							Self::artifact_from_directory_edge_inner(directory, graph.as_ref())?;
						children.push((artifact, graph.clone(), offset));
					}
					stack.extend(children.into_iter().rev());
				},
			}
		}

		Ok(entries)
	}

	fn directory_lookup_entry_sync_inner(
		&self,
		directory: &ArtifactInfo,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<ArtifactInfo>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let (directory_data, graph) =
				self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact = Self::artifact_from_edge_inner(edge, graph.as_ref())?;
					return Ok(Some(artifact));
				},
				tg::graph::data::Directory::Branch(branch) => {
					let Some(child) = branch
						.children
						.into_iter()
						.find(|child| name <= child.last.as_str())
					else {
						return Ok(None);
					};
					directory =
						Self::artifact_from_directory_edge_inner(child.directory, graph.as_ref())?;
					default_graph = graph;
				},
			}
		}
	}

	fn getattr_from_node_sync_inner(
		&self,
		node: &NodeInfo,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		self.compute_attrs_from_artifact_sync_inner(node.artifact.as_ref(), node.depth, transaction)
	}

	fn compute_attrs_from_artifact_sync_inner(
		&self,
		artifact: Option<&ArtifactInfo>,
		depth: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_sync_inner(artifact, transaction)?;
				let size = file.contents.as_ref().map_or(Ok(0), |contents| {
					self.blob_length_sync_inner(contents, transaction)
				})?;
				Ok(vfs::Attrs::new(vfs::AttrsInner::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) => {
				let (symlink, graph) = self.symlink_node_sync_inner(artifact, transaction)?;
				let artifact = match symlink.artifact {
					Some(edge) => Some(Self::artifact_from_edge_inner(edge, graph.as_ref())?.id),
					None => None,
				};
				let target = Self::build_symlink_target(depth, artifact, symlink.path)?;
				let size = target.len().to_u64().unwrap();
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
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

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<tangram_object_store::Object<'static>>> {
		#[cfg(feature = "lmdb")]
		if let (crate::object::Store::Lmdb(store), Some(transaction)) =
			(&self.server.object_store, transaction)
		{
			let arg = crate::object::store::TryGetArg { id: id.clone() };
			return store
				.try_get_with_transaction(transaction, &arg)
				.map(|output| output.object)
				.map_err(|error| Self::map_store_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		let arg = crate::object::store::TryGetArg { id: id.clone() };
		self.server
			.object_store
			.try_get_sync(&arg)
			.map(|output| output.object)
			.map_err(|error| Self::map_store_sync_error(&error))
	}

	fn try_get_data(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		#[cfg(feature = "lmdb")]
		if let (crate::object::Store::Lmdb(store), Some(transaction)) =
			(&self.server.object_store, transaction)
		{
			return store
				.try_get_data_with_transaction(transaction, id)
				.map_err(|error| Self::map_store_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		self.server
			.object_store
			.try_get_data_sync(id)
			.map_err(|error| Self::map_store_sync_error(&error))
	}

	fn map_store_sync_error(error: &tg::Error) -> std::io::Error {
		tracing::error!(error = %error.trace(), "failed to access local object data");
		std::io::Error::from_raw_os_error(libc::EIO)
	}

	fn attrs_from_artifact(artifact: Option<&ArtifactInfo>) -> Option<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::File) => None,
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Some(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) => None,
			None => Some(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => None,
		}
	}

	fn entry_kind_from_artifact(artifact: &ArtifactInfo) -> vfs::EntryKind {
		match artifact.id.kind() {
			tg::artifact::Kind::Directory => vfs::EntryKind::Directory,
			tg::artifact::Kind::File => vfs::EntryKind::File,
			tg::artifact::Kind::Symlink => vfs::EntryKind::Symlink,
		}
	}

	fn readdir_entry_size(name_len: usize) -> usize {
		let padding = (8 - (FUSE_DIRENT_HEADER_SIZE + name_len) % 8) % 8;
		FUSE_DIRENT_HEADER_SIZE + name_len + padding
	}

	fn readdirplus_entry_size(name_len: usize) -> usize {
		let padding = (8 - (FUSE_DIRENT_PLUS_HEADER_SIZE + name_len) % 8) % 8;
		FUSE_DIRENT_PLUS_HEADER_SIZE + name_len + padding
	}

	fn directory_snapshot_entry_limit(length: u64, header_size: usize) -> usize {
		length
			.to_usize()
			.unwrap_or(usize::MAX)
			.checked_div(header_size)
			.unwrap()
			.min(DIRECTORY_SNAPSHOT_READ_ENTRY_LIMIT)
	}
}

impl DirectorySnapshot {
	fn paged(&self) -> Self {
		if !self.pageable || self.entries.is_none() {
			return self.clone();
		}
		Self {
			depth: self.depth,
			entries: None,
			node: self.node,
			pageable: self.pageable,
			parent: self.parent,
		}
	}

	fn virtual_entries(&self, offset: u64, limit: usize) -> Vec<DirectorySnapshotEntry> {
		let entries = [
			DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: ".".to_owned(),
				node: self.node,
			},
			DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: "..".to_owned(),
				node: self.parent,
			},
		];

		entries
			.into_iter()
			.skip(offset.to_usize().unwrap_or(usize::MAX))
			.take(limit)
			.collect()
	}

	fn weight(&self) -> usize {
		let mut weight = DIRECTORY_SNAPSHOT_OVERHEAD.saturating_add(std::mem::size_of::<Self>());
		let Some(entries) = &self.entries else {
			return weight;
		};
		weight = weight.saturating_add(std::mem::size_of_val(entries.as_ref()));
		for entry in entries.iter() {
			weight = weight.saturating_add(entry.name.capacity());
			let data_length = entry
				.artifact
				.as_ref()
				.and_then(|artifact| artifact.data.as_ref())
				.map_or(0, |data| {
					data.serialize()
						.map_or(DIRECTORY_SNAPSHOT_CACHE_CAPACITY, |data| data.len())
				});
			weight = weight
				.saturating_add(DIRECTORY_SNAPSHOT_ENTRY_OVERHEAD)
				.saturating_add(data_length);
		}

		weight
	}
}

impl<'a> SnapshotLoad<'a> {
	fn new(
		loads: &'a DashMap<u64, Arc<tokio::sync::Mutex<()>>, fnv::FnvBuildHasher>,
		id: u64,
	) -> Self {
		let mutex = loads
			.entry(id)
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone();

		Self { id, loads, mutex }
	}
}

impl Drop for SnapshotLoad<'_> {
	fn drop(&mut self) {
		self.loads.remove_if(&self.id, |_, mutex| {
			Arc::ptr_eq(mutex, &self.mutex) && Arc::strong_count(mutex) == 2
		});
	}
}

impl<'a> PendingNodes<'a> {
	fn new(provider: &'a Provider) -> Self {
		Self {
			committed: false,
			ids: Vec::new(),
			provider,
		}
	}

	fn acquire(&mut self, id: u64) -> std::io::Result<()> {
		self.provider.nodes.remember_existing(id)?;
		self.ids.push(id);

		Ok(())
	}

	fn push_acquired(&mut self, id: u64) {
		self.ids.push(id);
	}

	fn commit(mut self) {
		self.committed = true;
	}
}

impl Drop for PendingNodes<'_> {
	fn drop(&mut self) {
		if self.committed {
			return;
		}
		for &id in self.ids.iter().rev() {
			self.provider.forget_sync(id, 1);
		}
	}
}

impl Nodes {
	fn new() -> Self {
		let mut nodes = BTreeMap::new();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			Node {
				artifact: None,
				attrs: Some(vfs::Attrs::new(vfs::AttrsInner::Directory)),
				children: BTreeMap::new(),
				depth: 0,
				lookup_count: u64::MAX,
				name: None,
				parent: vfs::ROOT_NODE_ID,
			},
		);
		let state = Mutex::new(State { next: 1000, nodes });
		Self { state }
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Ok(self.lookup_sync(parent, name))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.lookup_parent_sync(id)
	}

	async fn get(&self, id: u64) -> std::io::Result<NodeInfo> {
		self.get_sync(id)
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> Option<u64> {
		self.state
			.lock()
			.unwrap()
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
	}

	fn lookup_and_remember_sync(&self, parent: u64, name: &str) -> Option<u64> {
		let mut state = self.state.lock().unwrap();
		let id = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())?;
		let node = state.nodes.get_mut(&id)?;
		node.lookup_count = node.lookup_count.saturating_add(1);
		Some(id)
	}

	fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		self.state
			.lock()
			.unwrap()
			.nodes
			.get(&id)
			.map(|node| node.parent)
			.ok_or_else(|| {
				tracing::error!(%id, "node not found");
				std::io::Error::from_raw_os_error(libc::ENOENT)
			})
	}

	fn lookup_parent_and_remember_sync(&self, id: u64) -> std::io::Result<u64> {
		let mut state = self.state.lock().unwrap();
		let parent = state
			.nodes
			.get(&id)
			.map(|node| node.parent)
			.ok_or_else(|| {
				tracing::error!(%id, "node not found");
				std::io::Error::from_raw_os_error(libc::ENOENT)
			})?;
		if parent != vfs::ROOT_NODE_ID {
			let Some(node) = state.nodes.get_mut(&parent) else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			node.lookup_count = node.lookup_count.saturating_add(1);
		}

		Ok(parent)
	}

	fn get_sync(&self, id: u64) -> std::io::Result<NodeInfo> {
		self.state
			.lock()
			.unwrap()
			.nodes
			.get(&id)
			.map(|node| NodeInfo {
				artifact: node.artifact.clone(),
				attrs: node.attrs,
				depth: node.depth,
				parent: node.parent,
			})
			.ok_or_else(|| {
				tracing::error!(%id, "node not found");
				std::io::Error::from_raw_os_error(libc::ENOENT)
			})
	}

	fn set_attrs(&self, id: u64, attrs: vfs::Attrs) {
		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&id) else {
			return;
		};
		node.attrs = Some(attrs);
	}

	fn remember(&self, id: u64) {
		self.remember_existing(id).ok();
	}

	fn remember_existing(&self, id: u64) -> std::io::Result<()> {
		if id == vfs::ROOT_NODE_ID {
			return Ok(());
		}
		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&id) else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
		node.lookup_count = node.lookup_count.saturating_add(1);

		Ok(())
	}

	fn forget(&self, id: u64, nlookup: u64) -> Vec<u64> {
		if id == vfs::ROOT_NODE_ID || nlookup == 0 {
			return Vec::new();
		}

		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&id) else {
			return Vec::new();
		};
		node.lookup_count = node.lookup_count.saturating_sub(nlookup);
		let should_prune = node.lookup_count == 0 && node.children.is_empty();
		if !should_prune {
			return Vec::new();
		}

		let mut removed = Vec::new();
		Self::prune(&mut state, id, &mut removed);
		removed
	}

	fn prune(state: &mut State, mut id: u64, removed: &mut Vec<u64>) {
		loop {
			if id == vfs::ROOT_NODE_ID {
				return;
			}

			let Some(node) = state.nodes.get(&id) else {
				return;
			};
			if node.lookup_count != 0 || !node.children.is_empty() {
				return;
			}
			let parent = node.parent;
			let name = node.name.clone();

			state.nodes.remove(&id);
			removed.push(id);

			let prune_parent = {
				let Some(parent_node) = state.nodes.get_mut(&parent) else {
					return;
				};
				if let Some(name) = name {
					parent_node.children.remove(&name);
				}
				parent != vfs::ROOT_NODE_ID
					&& parent_node.lookup_count == 0
					&& parent_node.children.is_empty()
			};
			if !prune_parent {
				return;
			}
			id = parent;
		}
	}

	fn get_or_insert_child(
		&self,
		parent: u64,
		name: &str,
		artifact: ArtifactInfo,
		depth: u64,
		attrs: Option<vfs::Attrs>,
		remember: bool,
	) -> std::io::Result<u64> {
		let mut state = self.state.lock().unwrap();
		if let Some(id) = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
		{
			if remember {
				let node = state.nodes.get_mut(&id).unwrap();
				node.lookup_count = node.lookup_count.saturating_add(1);
			}
			return Ok(id);
		}

		if !state.nodes.contains_key(&parent) {
			tracing::error!(%parent, "node not found");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}

		let id = state.next;
		state.next += 1;
		state.nodes.insert(
			id,
			Node {
				artifact: Some(artifact),
				attrs,
				children: BTreeMap::new(),
				depth,
				lookup_count: u64::from(remember),
				name: Some(name.to_owned()),
				parent,
			},
		);
		state
			.nodes
			.get_mut(&parent)
			.unwrap()
			.children
			.insert(name.to_owned(), id);
		Ok(id)
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

	fn supports_no_opendir(&self) -> bool {
		true
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Provider::lookup(self, parent, name).await
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Provider::lookup_sync(self, parent, name)
	}

	async fn lookup_and_remember(
		&self,
		parent: u64,
		name: &str,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		Provider::lookup_and_remember(self, parent, name).await
	}

	fn lookup_and_remember_sync(
		&self,
		parent: u64,
		name: &str,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		Provider::lookup_and_remember_sync(self, parent, name)
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

	async fn readdir(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir(self, id, offset, length).await
	}

	fn readdir_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_sync(self, id, offset, length)
	}

	async fn readdir_node(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_node(self, id, offset, length).await
	}

	fn readdir_node_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_node_sync(self, id, offset, length)
	}

	async fn readdirplus(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus(self, id, offset, length).await
	}

	fn readdirplus_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus_sync(self, id, offset, length)
	}

	async fn readdirplus_node(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus_node(self, id, offset, length).await
	}

	fn readdirplus_node_sync(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		Provider::readdirplus_node_sync(self, id, offset, length)
	}

	async fn close(&self, id: u64) {
		Provider::close(self, id).await;
	}

	fn close_sync(&self, id: u64) {
		Provider::close_sync(self, id);
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		std::sync::{Arc, Barrier},
	};

	#[test]
	fn directory_snapshots_discard_entries_for_handles() {
		let entries = ["a", "b", "c"].map(|name| DirectorySnapshotEntry {
			artifact: None,
			kind: vfs::EntryKind::File,
			name: name.to_owned(),
			node: 0,
		});
		let snapshot = DirectorySnapshot {
			depth: 0,
			entries: Some(Arc::from(entries)),
			node: vfs::ROOT_NODE_ID,
			pageable: true,
			parent: vfs::ROOT_NODE_ID,
		};

		let snapshot = snapshot.paged();
		assert!(snapshot.entries.is_none());
		let entries = snapshot.virtual_entries(1, 1);
		assert_eq!(entries[0].name, "..");
	}

	#[test]
	fn directory_snapshot_loads_are_removed_after_the_last_user() {
		let loads = DashMap::default();
		let first = SnapshotLoad::new(&loads, 1);
		let second = SnapshotLoad::new(&loads, 1);
		assert!(Arc::ptr_eq(&first.mutex, &second.mutex));
		drop(first);
		assert!(loads.contains_key(&1));
		drop(second);
		assert!(loads.is_empty());
	}

	#[test]
	fn directory_branch_ranges_cross_child_boundaries() {
		let children = [3, 4, 5]
			.into_iter()
			.enumerate()
			.map(|(index, count)| tg::graph::data::DirectoryChild {
				count,
				directory: tg::graph::data::Edge::Object(tg::directory::Id::new(&[index
					.to_u8()
					.unwrap()])),
				last: index.to_string(),
			})
			.collect();

		let children = Provider::directory_children_range(children, 2, 6);
		let offsets = children
			.into_iter()
			.map(|(_, offset)| offset)
			.collect::<Vec<_>>();
		assert_eq!(offsets, vec![2, 0, 0]);
	}

	#[test]
	fn large_directory_branches_are_paged() {
		let entry_weight = std::mem::size_of::<DirectorySnapshotEntry>()
			+ DIRECTORY_SNAPSHOT_ENTRY_OVERHEAD
			+ NAME_MAX;
		let count = (DIRECTORY_SNAPSHOT_CACHE_CAPACITY / entry_weight + 1)
			.to_u64()
			.unwrap();
		let directory = tg::graph::data::Directory::Branch(tg::graph::data::DirectoryBranch {
			children: vec![tg::graph::data::DirectoryChild {
				count,
				directory: tg::graph::data::Edge::Object(tg::directory::Id::new(b"directory")),
				last: "z".to_owned(),
			}],
		});

		assert!(Provider::directory_requires_paging(&directory));
	}

	#[test]
	fn pending_node_references_are_independent() {
		let nodes = Nodes::new();
		let child = 1000;
		let mut state = nodes.state.lock().unwrap();
		state.nodes.insert(
			child,
			Node {
				artifact: None,
				attrs: None,
				children: BTreeMap::new(),
				depth: 1,
				lookup_count: 2,
				name: Some("child".to_owned()),
				parent: vfs::ROOT_NODE_ID,
			},
		);
		state
			.nodes
			.get_mut(&vfs::ROOT_NODE_ID)
			.unwrap()
			.children
			.insert("child".to_owned(), child);
		drop(state);

		assert!(nodes.forget(child, 1).is_empty());
		assert_eq!(nodes.lookup_sync(vfs::ROOT_NODE_ID, "child"), Some(child));
		assert_eq!(nodes.forget(child, 1), vec![child]);
		assert_eq!(nodes.lookup_sync(vfs::ROOT_NODE_ID, "child"), None);
	}

	#[test]
	fn lookup_publication_is_atomic_with_forget() {
		for _ in 0..128 {
			let nodes = Arc::new(Nodes::new());
			let child = 1000;
			{
				let mut state = nodes.state.lock().unwrap();
				state.nodes.insert(
					child,
					Node {
						artifact: None,
						attrs: None,
						children: BTreeMap::new(),
						depth: 1,
						lookup_count: 1,
						name: Some("child".to_owned()),
						parent: vfs::ROOT_NODE_ID,
					},
				);
				state
					.nodes
					.get_mut(&vfs::ROOT_NODE_ID)
					.unwrap()
					.children
					.insert("child".to_owned(), child);
			}

			let barrier = Arc::new(Barrier::new(3));
			let lookup = {
				let barrier = barrier.clone();
				let nodes = nodes.clone();
				std::thread::spawn(move || {
					barrier.wait();
					nodes.lookup_and_remember_sync(vfs::ROOT_NODE_ID, "child")
				})
			};
			let forget = {
				let barrier = barrier.clone();
				let nodes = nodes.clone();
				std::thread::spawn(move || {
					barrier.wait();
					nodes.forget(child, 1);
				})
			};
			barrier.wait();
			let lookup = lookup.join().unwrap();
			forget.join().unwrap();

			if lookup.is_some() {
				assert_eq!(nodes.lookup_sync(vfs::ROOT_NODE_ID, "child"), Some(child),);
			} else {
				assert_eq!(nodes.lookup_sync(vfs::ROOT_NODE_ID, "child"), None);
			}
		}
	}
}
