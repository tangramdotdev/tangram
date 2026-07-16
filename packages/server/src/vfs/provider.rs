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
		io::{Read as _, Seek as _},
		os::fd::OwnedFd,
		os::unix::ffi::OsStrExt as _,
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
	directory_snapshots: Mutex<vfs::cache::WeightedLruCache<u64, DirectorySnapshot>>,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	handle_count: AtomicU64,
	nodes: Nodes,
	server: Server,
}

#[derive(Clone)]
struct DirectorySnapshot {
	depth: u64,
	entries: Arc<[DirectorySnapshotEntry]>,
	node: u64,
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

pub struct FileHandle {
	blob: tg::blob::Id,
}

#[cfg(feature = "lmdb")]
type Transaction<'a> = lmdb::RoTxn<'a>;
#[cfg(not(feature = "lmdb"))]
type Transaction<'a> = ();

const FUSE_DIRENT_PLUS_HEADER_SIZE: usize = 152;
const DIRECTORY_SNAPSHOT_CACHE_CAPACITY: usize = 64 * 1024 * 1024;
const DIRECTORY_SNAPSHOT_CACHE_ENTRY_OVERHEAD: usize = 256;

impl Provider {
	pub async fn new(server: &Server) -> tg::Result<Self> {
		// Create the nodes.
		let nodes = Nodes::new();

		// Create the provider.
		let directory_handles = DashMap::default();
		let directory_snapshots = Mutex::new(vfs::cache::WeightedLruCache::new(
			DIRECTORY_SNAPSHOT_CACHE_CAPACITY,
		));
		let file_handles = DashMap::default();
		let handle_count = AtomicU64::new(1000);
		let server = server.clone();
		let provider = Self {
			directory_handles,
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
						.map(|id| vfs::Response::Lookup { id }),
					vfs::Request::LookupAndRemember { id, name } => self
						.lookup_and_remember(id, &name)
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
					.map(|id| vfs::Response::Lookup { id }),
				vfs::Request::LookupAndRemember { id, name } => self
					.lookup_and_remember_sync_inner(id, &name, transaction)
					.map(|id| vfs::Response::Lookup { id }),
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
				vfs::Request::ReadDir { handle } => self
					.readdir_sync(handle)
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
	) -> std::io::Result<Option<u64>> {
		self.lookup_inner(parent, name, true).await
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
	) -> std::io::Result<Option<u64>> {
		self.lookup_and_remember_sync_inner(parent, name, None)
	}

	fn lookup_and_remember_sync_inner(
		&self,
		parent: u64,
		name: &str,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<u64>> {
		self.lookup_sync_inner(parent, name, transaction, true)
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
		let handle = self.insert_directory_handle(snapshot);

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
		let handle = self.insert_directory_handle(snapshot);

		Ok(handle)
	}

	async fn directory_snapshot(&self, id: u64) -> std::io::Result<DirectorySnapshot> {
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
		let entries = match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Some(self.directory_entries_inner(&artifact, None).await?)
			},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			None => None,
		};
		let snapshot = Self::create_directory_snapshot(id, parent, depth, entries);
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

		// Get the node.
		let NodeInfo {
			artifact,
			depth,
			parent,
			..
		} = self.get_sync(id)?;
		let entries = match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Some(self.directory_entries_sync_inner(&artifact, None, transaction)?)
			},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
			None => None,
		};
		let snapshot = Self::create_directory_snapshot(id, parent, depth, entries);
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
	) -> DirectorySnapshot {
		let entries = entries.map_or_else(Vec::new, |entries| {
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
		let entries = Arc::from(entries);

		DirectorySnapshot {
			depth,
			entries,
			node,
		}
	}

	fn insert_directory_handle(&self, snapshot: DirectorySnapshot) -> u64 {
		let handle = self.handle_count.fetch_add(1, Ordering::Relaxed);
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
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		self.readdir_sync(handle)
	}

	pub fn readdir_sync(&self, handle: u64) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_handle(handle)?;

		Ok(Self::read_directory_snapshot(&snapshot))
	}

	pub async fn readdir_node(
		&self,
		id: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_snapshot(id).await?;

		Ok(Self::read_directory_snapshot(&snapshot))
	}

	pub fn readdir_node_sync(
		&self,
		id: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		#[cfg(feature = "lmdb")]
		if let crate::object::Store::Lmdb(store) = &self.server.object_store {
			let transaction = store.env().read_txn().map_err(|error| {
				tracing::error!(?error, "failed to begin an lmdb read transaction");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			return self.readdir_node_sync_inner(id, Some(&transaction));
		}

		self.readdir_node_sync_inner(id, None)
	}

	fn readdir_node_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_snapshot_sync_inner(id, transaction)?;

		Ok(Self::read_directory_snapshot(&snapshot))
	}

	fn read_directory_snapshot(snapshot: &DirectorySnapshot) -> Vec<(String, u64, vfs::EntryKind)> {
		snapshot
			.entries
			.iter()
			.map(|entry| (entry.name.clone(), entry.node, entry.kind))
			.collect()
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
		let mut entries = Vec::new();
		let mut size = 0;
		for entry in directory.entries.iter().skip(offset.to_usize().unwrap()) {
			let entry_size = Self::readdirplus_entry_size(entry.name.len());
			if entry_size + size > length.to_usize().unwrap() {
				break;
			}
			let (node, attrs) = if let Some(artifact) = &entry.artifact {
				let attrs = self
					.compute_attrs_from_artifact_inner(Some(artifact))
					.await?;
				let node = self.nodes.get_or_insert_child(
					directory.node,
					&entry.name,
					artifact.clone(),
					directory.depth + 1,
					Some(attrs),
					false,
				)?;
				(node, attrs)
			} else {
				(entry.node, self.getattr(entry.node).await?)
			};
			size += entry_size;
			entries.push((entry.name.clone(), node, attrs));
		}
		self.remember_readdirplus_entries(&entries)?;

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
		let mut entries = Vec::new();
		let mut size = 0;
		for entry in directory.entries.iter().skip(offset.to_usize().unwrap()) {
			let entry_size = Self::readdirplus_entry_size(entry.name.len());
			if entry_size + size > length.to_usize().unwrap() {
				break;
			}
			let (node, attrs) = if let Some(artifact) = &entry.artifact {
				let attrs =
					self.compute_attrs_from_artifact_sync_inner(Some(artifact), transaction)?;
				let node = self.nodes.get_or_insert_child(
					directory.node,
					&entry.name,
					artifact.clone(),
					directory.depth + 1,
					Some(attrs),
					false,
				)?;
				(node, attrs)
			} else {
				(
					entry.node,
					self.getattr_sync_inner(entry.node, transaction)?,
				)
			};
			size += entry_size;
			entries.push((entry.name.clone(), node, attrs));
		}
		self.remember_readdirplus_entries(&entries)?;

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
		self.compute_attrs_from_artifact_inner(node.artifact.as_ref())
			.await
	}

	async fn compute_attrs_from_artifact_inner(
		&self,
		artifact: Option<&ArtifactInfo>,
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
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink))
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
		self.compute_attrs_from_artifact_sync_inner(node.artifact.as_ref(), transaction)
	}

	fn compute_attrs_from_artifact_sync_inner(
		&self,
		artifact: Option<&ArtifactInfo>,
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
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink))
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
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) => {
				Some(vfs::Attrs::new(vfs::AttrsInner::Symlink))
			},
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

	fn remember_readdirplus_entries(
		&self,
		entries: &[(String, u64, vfs::Attrs)],
	) -> std::io::Result<()> {
		self.nodes
			.remember_many(entries.iter().map(|(_, id, _)| *id))
	}

	fn readdirplus_entry_size(name_len: usize) -> usize {
		let padding = (8 - (FUSE_DIRENT_PLUS_HEADER_SIZE + name_len) % 8) % 8;
		FUSE_DIRENT_PLUS_HEADER_SIZE + name_len + padding
	}
}

impl DirectorySnapshot {
	fn weight(&self) -> usize {
		let mut weight = DIRECTORY_SNAPSHOT_CACHE_ENTRY_OVERHEAD
			.saturating_add(std::mem::size_of::<Self>())
			.saturating_add(std::mem::size_of_val(self.entries.as_ref()));
		for entry in self.entries.iter() {
			weight = weight.saturating_add(entry.name.capacity());
			let data_length = entry
				.artifact
				.as_ref()
				.and_then(|artifact| artifact.data.as_ref())
				.and_then(|data| data.serialize().ok())
				.map_or(0, |data| data.len());
			weight = weight.saturating_add(data_length);
		}

		weight
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

	fn remember_many(&self, ids: impl IntoIterator<Item = u64>) -> std::io::Result<()> {
		let ids = ids.into_iter().collect::<Vec<_>>();
		let mut state = self.state.lock().unwrap();
		if ids
			.iter()
			.any(|id| *id != vfs::ROOT_NODE_ID && !state.nodes.contains_key(id))
		{
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		for id in ids {
			if id == vfs::ROOT_NODE_ID {
				continue;
			}
			let node = state.nodes.get_mut(&id).unwrap();
			node.lookup_count = node.lookup_count.saturating_add(1);
		}

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

	async fn lookup_and_remember(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Provider::lookup_and_remember(self, parent, name).await
	}

	fn lookup_and_remember_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
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

	async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir(self, id).await
	}

	fn readdir_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_sync(self, id)
	}

	async fn readdir_node(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_node(self, id).await
	}

	fn readdir_node_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		Provider::readdir_node_sync(self, id)
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
