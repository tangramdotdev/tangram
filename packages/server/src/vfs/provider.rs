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
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_vfs as vfs,
};

struct Nodes {
	nodes: DashMap<u64, MemoryNode, fnv::FnvBuildHasher>,
}

#[derive(Clone)]
struct MemoryNode {
	parent: u64,
	artifact: Option<tg::artifact::Id>,
	depth: u64,
	lookup_count: u64,
	children: HashMap<String, u64, fnv::FnvBuildHasher>,
}

pub struct Provider {
	node_cache: moka::sync::Cache<u64, Node, fnv::FnvBuildHasher>,
	node_count: AtomicU64,
	file_handle_count: AtomicU64,
	nodes: Nodes,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	pending_nodes: Arc<DashMap<u64, Node, fnv::FnvBuildHasher>>,
	server: Server,
}

pub struct FileHandle {
	blob: tg::blob::Id,
}

#[derive(Clone)]
struct Node {
	parent: u64,
	artifact: Option<tg::Artifact>,
	depth: u64,
}

impl vfs::Provider for Provider {
	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			return Ok(Some(parent));
		} else if name == ".." {
			let id = self.lookup_parent(parent).await?;
			return Ok(Some(id));
		}

		// Check the cache to see if the node is there to avoid going to the database if we don't need to.
		if let Some(Node {
			artifact: Some(tg::Artifact::Directory(object)),
			..
		}) = self.node_cache.get(&parent)
		{
			let entries = object.entries(&self.server).await.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to get directory entries");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			if !entries.contains_key(name) {
				return Ok(None);
			}
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
			let artifact = tg::Artifact::with_id(artifact);
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
			let Some(tg::Artifact::Directory(parent)) = artifact else {
				return Ok(None);
			};
			let entries = parent.entries(&self.server).await.map_err(|error| {
				tracing::error!(%error, "failed to get parent directory entries");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			let Some(artifact) = entries.get(name) else {
				return Ok(None);
			};
			Some((artifact.clone(), depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let id = self.put(parent, name, artifact, depth).await?;

		Ok(Some(id))
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		self.check_sync_capability()?;

		// Handle "." and "..".
		if name == "." {
			return Ok(Some(parent));
		} else if name == ".." {
			let id = self.lookup_parent_sync(parent)?;
			return Ok(Some(id));
		}

		// Check the cache to see if the node is there to avoid going to storage if we do not need to.
		if let Some(Node {
			artifact: Some(tg::Artifact::Directory(directory)),
			..
		}) = self.node_cache.get(&parent)
		{
			let entries = self.directory_entries_sync(&directory)?;
			if !entries.contains_key(name) {
				return Ok(None);
			}
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
			let artifact = tg::Artifact::with_id(artifact);
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
			let Some(tg::Artifact::Directory(parent)) = artifact else {
				return Ok(None);
			};
			let entries = self.directory_entries_sync(&parent)?;
			let Some(artifact) = entries.get(name) else {
				return Ok(None);
			};
			Some((artifact.clone(), depth + 1))
		};

		// Insert the node.
		let (artifact, depth) = entry.unwrap();
		let id = self.put_sync(parent, name, &artifact, depth);
		Ok(Some(id))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		// Lookup the parent in the cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.parent);
		}

		// Lookup the node in the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.parent);
		}

		self.nodes.lookup_parent(id).await
	}

	fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		self.check_sync_capability()?;

		// Lookup the parent in the cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.parent);
		}

		// Lookup the node in the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.parent);
		}

		self.nodes.lookup_parent_sync(id)
	}

	fn remember_sync(&self, id: u64) {
		self.nodes.remember(id);
	}

	fn forget_sync(&self, id: u64, nlookup: u64) {
		let removed = self.nodes.forget(id, nlookup);
		for id in removed {
			self.node_cache.invalidate(&id);
			self.pending_nodes.remove(&id);
		}
	}

	async fn getattr(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		match self.get(id).await? {
			Node {
				artifact: Some(tg::Artifact::File(file)),
				..
			} => {
				let executable = file.executable(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file's executable bit");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				let size = file.length(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file's size");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				Ok(vfs::Attrs::new(vfs::FileType::File { executable, size }))
			},
			Node {
				artifact: Some(tg::Artifact::Directory(_)) | None,
				..
			} => Ok(vfs::Attrs::new(vfs::FileType::Directory)),
			Node {
				artifact: Some(tg::Artifact::Symlink(_)),
				..
			} => Ok(vfs::Attrs::new(vfs::FileType::Symlink)),
		}
	}

	fn getattr_sync(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		self.check_sync_capability()?;
		let node = self.get_sync(id)?;
		self.getattr_from_node_sync(&node)
	}

	async fn open(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;

		// Ensure it is a file.
		let Some(tg::Artifact::File(file)) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Get the blob id.
		let blob = file
			.contents(&self.server)
			.await
			.map_err(|error| {
				tracing::error!(%error, ?file, "failed to get blob for file");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.id();

		// Create the file handle.
		let file_handle = FileHandle { blob };

		// Insert the file handle.
		let id = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(id, file_handle);

		Ok(id)
	}

	fn open_sync(&self, id: u64) -> std::io::Result<(u64, Option<OwnedFd>)> {
		self.check_sync_capability()?;

		// Get the node.
		let Node { artifact, .. } = self.get_sync(id)?;

		// Ensure it is a file.
		let Some(tg::Artifact::File(file)) = artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Get the file object.
		let file = self.file_node_sync(&file)?;
		let Some(blob) = file.contents else {
			tracing::error!(%id, "file has no contents");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};

		// Attempt to open a backing file for passthrough.
		let backing_fd = self.try_open_backing_fd_sync(&blob)?;

		// Insert the file handle.
		let id = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(id, FileHandle { blob });

		Ok((id, backing_fd))
	}

	async fn read(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
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

	fn read_sync(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		self.check_sync_capability()?;

		// Get the file handle.
		let Some(file_handle) = self.file_handles.get(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};

		// Ensure the object exists locally before using the sync reader.
		let blob_id: tg::object::Id = file_handle.blob.clone().into();
		let object = self
			.server
			.store
			.try_get_object_sync(&blob_id)
			.map_err(|error| Self::map_store_sync_error(&error))?;
		if object.is_none() {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		}

		let mut reader = crate::read::Reader::new_sync(
			&self.server,
			tg::Blob::with_id(file_handle.blob.clone()),
		)
		.map_err(|error| Self::map_store_sync_error(&error))?;
		reader
			.seek(std::io::SeekFrom::Start(position))
			.map_err(|error| {
				tracing::error!(%error, "failed to seek while reading");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let mut bytes = vec![0u8; length.to_usize().unwrap()];
		let mut n = 0;
		while n < bytes.len() {
			let n_ = reader.read(&mut bytes[n..]).map_err(|error| {
				tracing::error!(%error, "failed to read");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			if n_ == 0 {
				break;
			}
			n += n_;
		}
		bytes.truncate(n);
		Ok(bytes.into())
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		// Get the node.
		let Node {
			artifact, depth, ..
		} = self.get(id).await.map_err(|error| {
			tracing::error!(%error, "failed to lookup node");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Ensure it is a symlink.
		let Some(tg::Artifact::Symlink(symlink)) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};

		// Render the target.
		let Ok(artifact) = symlink.artifact(&self.server).await else {
			tracing::error!("failed to get the symlink's artifact");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let Ok(path) = symlink.path(&self.server).await else {
			tracing::error!("failed to get the symlink's path");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let mut target = PathBuf::new();
		if let Some(artifact) = artifact.as_ref() {
			for _ in 0..depth - 1 {
				target.push("..");
			}
			target.push(artifact.id().to_string());
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

	fn readlink_sync(&self, id: u64) -> std::io::Result<Bytes> {
		self.check_sync_capability()?;

		// Get the node.
		let Node {
			artifact, depth, ..
		} = self.get_sync(id)?;

		// Ensure it is a symlink.
		let Some(tg::Artifact::Symlink(symlink)) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};

		// Render the target.
		let symlink = self.symlink_node_sync(&symlink)?;
		let mut target = PathBuf::new();
		if let Some(artifact) = symlink.artifact {
			let artifact = match artifact {
				tg::graph::data::Edge::Object(artifact) => artifact,
				tg::graph::data::Edge::Pointer(_) => {
					return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
				},
			};
			for _ in 0..depth - 1 {
				target.push("..");
			}
			target.push(artifact.to_string());
		}
		if let Some(path) = symlink.path {
			target.push(path);
		}
		if target == Path::new("") {
			tracing::error!("invalid symlink");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		let target = target.as_os_str().as_bytes().to_vec().into();

		Ok(target)
	}

	async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let node = self.get(id).await?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(Vec::new());
		};
		let dependencies = file.dependencies(&self.server).await.map_err(|error| {
			tracing::error!(error = %error.trace(), "failed to get file dependencies");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		if dependencies.is_empty() {
			return Ok(Vec::new());
		}
		Ok(vec![tg::file::DEPENDENCIES_XATTR_NAME.to_owned()])
	}

	fn listxattrs_sync(&self, id: u64) -> std::io::Result<Vec<String>> {
		self.check_sync_capability()?;
		let node = self.get_sync(id)?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(Vec::new());
		};
		let file = self.file_node_sync(&file)?;
		if file.dependencies.is_empty() {
			return Ok(Vec::new());
		}
		Ok(vec![tg::file::DEPENDENCIES_XATTR_NAME.to_owned()])
	}

	async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		let node = self.get(id).await?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(None);
		};
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			let dependencies = file.dependencies(&self.server).await.map_err(|error| {
				tracing::error!(error = %error.trace(), "failed to get file dependencies");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			if dependencies.is_empty() {
				return Ok(None);
			}
			let references = dependencies.keys().cloned().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).unwrap();
			return Ok(Some(data.into()));
		}

		if name == tg::file::MODULE_XATTR_NAME {
			let module = file.module(&self.server).await.map_err(|error| {
				tracing::error!(error = %error.trace(), "failed to get file's module");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			let Some(module) = module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	fn getxattr_sync(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		self.check_sync_capability()?;
		let node = self.get_sync(id)?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(None);
		};
		let file = self.file_node_sync(&file)?;
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

	async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;
		match artifact {
			Some(tg::Artifact::Directory(_)) | None => {},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		}
		Ok(id)
	}

	fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		self.check_sync_capability()?;

		// Get the node.
		let Node { artifact, .. } = self.get_sync(id)?;
		match artifact {
			Some(tg::Artifact::Directory(_)) | None => {},
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		}
		Ok(id)
	}

	async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		let Node { artifact, .. } = self.get(id).await?;
		let directory = match artifact {
			Some(tg::Artifact::Directory(directory)) => Some(directory),
			None => None,
			Some(_) => {
				tracing::error!(%id, "called readdir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let Some(directory) = directory.as_ref() else {
			return Ok(Vec::new());
		};
		let entries = directory.entries(&self.server).await.map_err(|error| {
			tracing::error!(%error, "failed to get directory entries");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
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

	fn readdir_sync(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		self.check_sync_capability()?;

		let Node { artifact, .. } = self.get_sync(id)?;
		let directory = match artifact {
			Some(tg::Artifact::Directory(directory)) => Some(directory),
			None => None,
			Some(_) => {
				tracing::error!(%id, "called readdir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let Some(directory) = directory.as_ref() else {
			return Ok(Vec::new());
		};
		let entries = self.directory_entries_sync(directory)?;
		let mut result = Vec::with_capacity(entries.len() + 2);
		result.push((".".to_owned(), id));
		result.push(("..".to_owned(), self.lookup_parent_sync(id)?));
		for name in entries.keys() {
			let entry = self.lookup_sync(id, name)?.ok_or_else(|| {
				tracing::error!(parent = id, %name, "failed to lookup directory entry");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			result.push((name.clone(), entry));
		}
		Ok(result)
	}

	async fn close(&self, id: u64) {
		if self.file_handles.contains_key(&id) {
			self.file_handles.remove(&id);
		}
	}

	fn close_sync(&self, id: u64) {
		if self.file_handles.contains_key(&id) {
			self.file_handles.remove(&id);
		}
	}
}

impl Provider {
	pub async fn new(server: &Server, options: crate::config::Vfs) -> tg::Result<Self> {
		// Create the cache.
		let cache = moka::sync::CacheBuilder::new(options.cache_size.to_u64().unwrap())
			.time_to_idle(options.cache_ttl)
			.build_with_hasher(fnv::FnvBuildHasher::default());

		// Create the nodes.
		let nodes = Nodes::new();

		// Create the provider.
		let node_count = AtomicU64::new(1000);
		let file_handle_count = AtomicU64::new(1000);
		let file_handles = DashMap::default();
		let pending_nodes = Arc::new(DashMap::default());
		let server = server.clone();
		let provider = Self {
			node_cache: cache,
			node_count,
			file_handle_count,
			nodes,
			file_handles,
			pending_nodes,
			server,
		};

		Ok(provider)
	}

	async fn get(&self, id: u64) -> std::io::Result<Node> {
		// Attempt to get the node from the node cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.clone());
		}

		// Attempt to get the node from the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.clone());
		}

		// Get the node from the nodes storage.
		let data = self.nodes.get(id).await?;
		let node = Node {
			parent: data.parent,
			artifact: data.artifact.map(tg::Artifact::with_id),
			depth: data.depth,
		};

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		Ok(node)
	}

	fn get_sync(&self, id: u64) -> std::io::Result<Node> {
		// Attempt to get the node from the node cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.clone());
		}

		// Attempt to get the node from the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.clone());
		}

		// Get the node from the nodes storage.
		let data = self.nodes.get_sync(id)?;
		let node = Node {
			parent: data.parent,
			artifact: data.artifact.map(tg::Artifact::with_id),
			depth: data.depth,
		};

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		Ok(node)
	}

	async fn put(
		&self,
		parent: u64,
		name: &str,
		artifact: tg::Artifact,
		depth: u64,
	) -> std::io::Result<u64> {
		// Create the node.
		let node = Node {
			parent,
			artifact: Some(artifact.clone()),
			depth,
		};

		// Get the artifact id.
		let artifact_id = artifact.id();

		// Get a node ID.
		let id = self.node_count.fetch_add(1, Ordering::Relaxed);

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		// Insert into the pending nodes.
		self.pending_nodes.insert(id, node);

		// Insert the node into the nodes storage.
		self.nodes.insert(id, parent, name, artifact_id, depth);

		// Remove from pending nodes after storage insertion completes.
		let pending_nodes = self.pending_nodes.clone();
		tokio::spawn(async move {
			// Give storage time to persist.
			tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			pending_nodes.remove(&id);
		});

		Ok(id)
	}

	fn put_sync(&self, parent: u64, name: &str, artifact: &tg::Artifact, depth: u64) -> u64 {
		// Create the node.
		let node = Node {
			parent,
			artifact: Some(artifact.clone()),
			depth,
		};

		// Get the artifact id.
		let artifact_id = artifact.id();

		// Get a node ID.
		let id = self.node_count.fetch_add(1, Ordering::Relaxed);

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		// Insert into the pending nodes and nodes storage.
		self.pending_nodes.insert(id, node);
		self.nodes.insert(id, parent, name, artifact_id, depth);

		// Storage insertion is synchronous for memory-backed nodes, so remove the pending node now.
		self.pending_nodes.remove(&id);

		id
	}

	fn check_sync_capability(&self) -> std::io::Result<()> {
		#[cfg(feature = "lmdb")]
		{
			if matches!(&self.server.store, crate::store::Store::Lmdb(_))
				&& matches!(&self.server.index, crate::index::Index::Lmdb(_))
			{
				return Ok(());
			}
		}
		Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
	}

	fn map_store_sync_error(error: &tg::Error) -> std::io::Error {
		tracing::error!(error = %error.trace(), "failed to access local object data");
		std::io::Error::from_raw_os_error(libc::EIO)
	}

	fn artifact_data_sync(&self, artifact: &tg::Artifact) -> std::io::Result<tg::object::Data> {
		let id: tg::object::Id = artifact.id().into();
		let output = self
			.server
			.store
			.try_get_object_data_sync(&id)
			.map_err(|error| Self::map_store_sync_error(&error))?;
		let Some((_, data)) = output else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		Ok(data)
	}

	fn blob_length_sync(&self, id: &tg::blob::Id) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let output = self
			.server
			.store
			.try_get_object_data_sync(&id)
			.map_err(|error| Self::map_store_sync_error(&error))?;
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

	fn file_node_sync(&self, file: &tg::File) -> std::io::Result<tg::graph::data::File> {
		let artifact = tg::Artifact::File(file.clone());
		let data = self.artifact_data_sync(&artifact)?;
		let tg::object::Data::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok(node),
			tg::file::Data::Pointer(_) => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}

	fn directory_node_sync(
		&self,
		directory: &tg::Directory,
	) -> std::io::Result<tg::graph::data::Directory> {
		let artifact = tg::Artifact::Directory(directory.clone());
		let data = self.artifact_data_sync(&artifact)?;
		let tg::object::Data::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok(node),
			tg::directory::Data::Pointer(_) => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}

	fn symlink_node_sync(
		&self,
		symlink: &tg::Symlink,
	) -> std::io::Result<tg::graph::data::Symlink> {
		let artifact = tg::Artifact::Symlink(symlink.clone());
		let data = self.artifact_data_sync(&artifact)?;
		let tg::object::Data::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok(node),
			tg::symlink::Data::Pointer(_) => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}

	fn directory_entries_sync(
		&self,
		directory: &tg::Directory,
	) -> std::io::Result<BTreeMap<String, tg::Artifact>> {
		let directory = self.directory_node_sync(directory)?;
		match directory {
			tg::graph::data::Directory::Leaf(leaf) => leaf
				.entries
				.into_iter()
				.map(|(name, edge)| {
					let artifact = match edge {
						tg::graph::data::Edge::Object(artifact) => tg::Artifact::with_id(artifact),
						tg::graph::data::Edge::Pointer(_) => {
							return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
						},
					};
					Ok((name, artifact))
				})
				.collect(),
			tg::graph::data::Directory::Branch(branch) => {
				let mut entries = BTreeMap::new();
				for child in branch.children {
					let directory = match child.directory {
						tg::graph::data::Edge::Object(directory) => directory,
						tg::graph::data::Edge::Pointer(_) => {
							return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
						},
					};
					let child = tg::Directory::with_id(directory);
					entries.extend(self.directory_entries_sync(&child)?);
				}
				Ok(entries)
			},
		}
	}

	fn getattr_from_node_sync(&self, node: &Node) -> std::io::Result<vfs::Attrs> {
		match &node.artifact {
			Some(tg::Artifact::File(file)) => {
				let file = self.file_node_sync(file)?;
				let size = file
					.contents
					.as_ref()
					.map_or(Ok(0), |contents| self.blob_length_sync(contents))?;
				Ok(vfs::Attrs::new(vfs::FileType::File {
					executable: file.executable,
					size,
				}))
			},
			Some(tg::Artifact::Directory(_)) | None => {
				Ok(vfs::Attrs::new(vfs::FileType::Directory))
			},
			Some(tg::Artifact::Symlink(_)) => Ok(vfs::Attrs::new(vfs::FileType::Symlink)),
		}
	}

	fn try_open_backing_fd_sync(&self, id: &tg::blob::Id) -> std::io::Result<Option<OwnedFd>> {
		let id: tg::object::Id = id.clone().into();
		let Some(object) = self
			.server
			.store
			.try_get_object_sync(&id)
			.map_err(|error| Self::map_store_sync_error(&error))?
		else {
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
}

impl Nodes {
	fn new() -> Self {
		let nodes = DashMap::default();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			MemoryNode {
				parent: vfs::ROOT_NODE_ID,
				artifact: None,
				depth: 0,
				lookup_count: u64::MAX,
				children: HashMap::default(),
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

	async fn get(&self, id: u64) -> std::io::Result<MemoryNode> {
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

	fn get_sync(&self, id: u64) -> std::io::Result<MemoryNode> {
		self.nodes.get(&id).map(|node| node.clone()).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
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
			drop(node);

			self.nodes.remove(&id);
			removed.push(id);

			let Some(mut parent_node) = self.nodes.get_mut(&parent) else {
				return;
			};
			parent_node.children.retain(|_, child_id| *child_id != id);
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

	fn insert(&self, id: u64, parent: u64, name: &str, artifact: tg::artifact::Id, depth: u64) {
		// Insert the new node.
		self.nodes.insert(
			id,
			MemoryNode {
				parent,
				artifact: Some(artifact),
				depth,
				lookup_count: 0,
				children: HashMap::default(),
			},
		);
		// Update the parent's children map.
		if let Some(mut parent_node) = self.nodes.get_mut(&parent) {
			parent_node.children.insert(name.to_owned(), id);
		}
	}
}
