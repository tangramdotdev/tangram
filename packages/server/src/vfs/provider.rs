use {
	crate::Server,
	bytes::Bytes,
	dashmap::DashMap,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		collections::HashMap,
		os::{fd::OwnedFd, unix::ffi::OsStrExt as _},
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
	nodes: DashMap<u64, StorageNode, fnv::FnvBuildHasher>,
}

#[derive(Clone)]
struct StorageNode {
	parent: u64,
	artifact: Option<tg::artifact::Id>,
	depth: u64,
	children: HashMap<String, u64, fnv::FnvBuildHasher>,
	nlookup: u64,
}

pub struct Provider {
	node_cache: moka::sync::Cache<u64, Node, fnv::FnvBuildHasher>,
	node_count: AtomicU64,
	file_handle_count: AtomicU64,
	nodes: Nodes,
	directory_handles: DashMap<u64, DirectoryHandle, fnv::FnvBuildHasher>,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	pending_nodes: Arc<DashMap<u64, Node, fnv::FnvBuildHasher>>,
	server: Server,
}

pub struct DirectoryHandle {
	node: u64,
	directory: Option<tg::Directory>,
}

pub struct FileHandle {
	blob: tg::blob::Id,
}

#[derive(Clone)]
struct Node {
	parent: u64,
	artifact: Option<tg::Artifact>,
	depth: u64,
	attrs: Option<vfs::Attrs>,
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
		if let Some(id) = self.nodes.lookup(parent, name) {
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

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		// Lookup the parent in the cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.parent);
		}

		// Lookup the node in the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.parent);
		}

		self.nodes.lookup_parent(id)
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

	async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;
		let directory = match artifact {
			Some(tg::Artifact::Directory(directory)) => Some(directory),
			None => None,
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let handle_id = self.file_handle_count.fetch_add(1, Ordering::SeqCst);
		let handle = DirectoryHandle {
			node: id,
			directory,
		};
		self.directory_handles.insert(handle_id, handle);
		Ok(handle_id)
	}

	async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		let Some(handle) = self.directory_handles.get(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
		let Some(directory) = handle.directory.as_ref() else {
			return Ok(Vec::new());
		};
		let entries = directory.entries(&self.server).await.map_err(|error| {
			tracing::error!(%error, "failed to get directory entries");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let mut result = Vec::with_capacity(entries.len());
		result.push((".".to_owned(), handle.node));
		result.push(("..".to_owned(), self.lookup_parent(handle.node).await?));
		for name in entries.keys() {
			let id = self.lookup(handle.node, name).await?.unwrap();
			result.push((name.clone(), id));
		}
		Ok(result)
	}

	async fn close(&self, id: u64) {
		if self.directory_handles.contains_key(&id) {
			self.directory_handles.remove(&id);
		}
		if self.file_handles.contains_key(&id) {
			self.file_handles.remove(&id);
		}
	}

	fn close_sync(&self, handle: u64) {
		self.directory_handles.remove(&handle);
		self.file_handles.remove(&handle);
	}

	fn getattr_sync(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		let node = self.get_sync(id)?;

		// Return cached attrs if available.
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}

		let attrs = match &node {
			Node {
				artifact: Some(tg::Artifact::File(file)),
				..
			} => {
				let (executable, size) = self.get_file_metadata_sync(file)?;
				vfs::Attrs::new(vfs::FileType::File { executable, size })
			},
			Node {
				artifact: Some(tg::Artifact::Directory(_)) | None,
				..
			} => vfs::Attrs::new(vfs::FileType::Directory),
			Node {
				artifact: Some(tg::Artifact::Symlink(_)),
				..
			} => vfs::Attrs::new(vfs::FileType::Symlink),
		};

		// Update the cached node with the computed attrs.
		let mut updated_node = node;
		updated_node.attrs = Some(attrs);
		self.node_cache.insert(id, updated_node);

		Ok(attrs)
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		// Handle "." and "..".
		if name == "." {
			return Ok(Some(parent));
		} else if name == ".." {
			let id = self.lookup_parent_sync(parent)?;
			return Ok(Some(id));
		}

		// Check the nodes storage.
		if let Some(id) = self.nodes.lookup(parent, name) {
			return Ok(Some(id));
		}

		// If the parent is the root, then create a new node.
		if parent == vfs::ROOT_NODE_ID {
			let name_stripped = None
				.or_else(|| name.strip_suffix(".tg.ts"))
				.or_else(|| name.strip_suffix(".tg.js"))
				.or_else(|| {
					std::path::Path::new(name)
						.file_stem()
						.and_then(|s| s.to_str())
				})
				.unwrap_or(name);
			let Ok(artifact) = name_stripped.parse() else {
				return Ok(None);
			};
			let artifact = tg::Artifact::with_id(artifact);
			let id = self.put_sync(parent, name, artifact, 1);
			return Ok(Some(id));
		}

		// Otherwise, get the parent artifact and attempt to look up the child.
		let Node {
			artifact, depth, ..
		} = self.get_sync(parent)?;
		let Some(tg::Artifact::Directory(parent_dir)) = artifact else {
			return Ok(None);
		};

		// Get directory entries synchronously.
		let entries = self.get_directory_entries_sync(&parent_dir)?;
		let Some(artifact_id) = entries.get(name) else {
			return Ok(None);
		};

		// Create the child node and speculatively compute its attrs.
		let artifact = tg::Artifact::with_id(artifact_id.clone());
		let attrs = self.try_compute_attrs_sync(&artifact);
		let id = self.put_sync_with_attrs(parent, name, artifact, depth + 1, attrs);
		Ok(Some(id))
	}

	fn lookup_parent_sync(&self, id: u64) -> std::io::Result<u64> {
		// Check the node cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.parent);
		}

		// Check the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.parent);
		}

		self.nodes.lookup_parent(id)
	}

	fn open_sync(&self, id: u64) -> std::io::Result<(u64, Option<OwnedFd>)> {
		// Get the node.
		let node = self.get_sync(id)?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		// Get the blob id synchronously.
		let blob_id = self.get_file_blob_sync(&file)?;

		// Check for a cache pointer for passthrough.
		let backing_fd = self.try_get_passthrough_fd_sync(&blob_id);

		// Create the file handle.
		let file_handle = FileHandle { blob: blob_id };
		let handle_id = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(handle_id, file_handle);

		Ok((handle_id, backing_fd))
	}

	fn read_sync(&self, handle: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		// Get the file handle.
		let Some(file_handle) = self.file_handles.get(&handle) else {
			tracing::error!(%handle, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};

		// Create a sync reader.
		let blob = tg::Blob::with_id(file_handle.blob.clone());
		let mut reader = crate::read::Reader::new_sync(&self.server, blob).map_err(|error| {
			tracing::error!(%error, "failed to create the sync reader");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Seek and read.
		std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(position))?;
		let mut buf = vec![0u8; length.to_usize().unwrap()];
		let mut total = 0;
		while total < buf.len() {
			let n = std::io::Read::read(&mut reader, &mut buf[total..])?;
			if n == 0 {
				break;
			}
			total += n;
		}
		buf.truncate(total);
		Ok(buf.into())
	}

	fn readlink_sync(&self, id: u64) -> std::io::Result<Bytes> {
		// Get the node.
		let Node {
			artifact, depth, ..
		} = self.get_sync(id)?;
		let Some(tg::Artifact::Symlink(symlink)) = artifact else {
			tracing::error!(%id, "tried to readlink on a non-symlink");
			return Err(std::io::Error::other("expected a symlink"));
		};

		// Get the symlink data synchronously.
		let (artifact_id, path) = self.get_symlink_data_sync(&symlink)?;

		// Render the target.
		let mut target = PathBuf::new();
		if let Some(artifact_id) = artifact_id.as_ref() {
			for _ in 0..depth - 1 {
				target.push("..");
			}
			target.push(artifact_id.to_string());
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

	fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		// Get the node.
		let Node { artifact, .. } = self.get_sync(id)?;
		let directory = match artifact {
			Some(tg::Artifact::Directory(directory)) => Some(directory),
			None => None,
			Some(_) => {
				tracing::error!(%id, "called opendir on a file or symlink");
				return Err(std::io::Error::other("expected a directory"));
			},
		};
		let handle_id = self.file_handle_count.fetch_add(1, Ordering::SeqCst);
		let handle = DirectoryHandle {
			node: id,
			directory,
		};
		self.directory_handles.insert(handle_id, handle);
		Ok(handle_id)
	}

	fn readdir_sync(&self, handle: u64) -> std::io::Result<Vec<(String, u64)>> {
		let Some(handle) = self.directory_handles.get(&handle) else {
			tracing::error!(%handle, "tried to read from an invalid directory handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
		let Some(directory) = handle.directory.as_ref() else {
			return Ok(Vec::new());
		};

		// Get directory entries synchronously.
		let entries = self.get_directory_entries_sync(directory)?;
		let node_id = handle.node;
		let node_depth = self.get_sync(node_id)?.depth;

		// Collect new entries that need object data to be fetched.
		let mut new_entries = Vec::new();
		let mut existing_entries = Vec::new();
		for (name, artifact_id) in &entries {
			if let Some(existing_id) = self.nodes.lookup(node_id, name) {
				existing_entries.push((name.clone(), existing_id));
			} else {
				new_entries.push((name, artifact_id));
			}
		}

		// Batch fetch object data for all new entries in a single LMDB transaction.
		let object_ids: Vec<tg::object::Id> = new_entries
			.iter()
			.map(|(_, artifact_id)| (*artifact_id).clone().into())
			.collect();
		let mut file_handle = None;
		let batch_results = self
			.server
			.try_get_object_batch_sync(&object_ids, &mut file_handle)
			.unwrap_or_else(|_| vec![None; object_ids.len()]);

		let mut result = Vec::with_capacity(entries.len() + 2);
		result.push((".".to_owned(), node_id));
		result.push(("..".to_owned(), self.lookup_parent_sync(node_id)?));

		// Add existing entries.
		result.extend(existing_entries);

		// Create nodes for new entries with pre-computed attrs.
		for ((name, artifact_id), object_data) in
			new_entries.into_iter().zip(batch_results.into_iter())
		{
			let artifact = tg::Artifact::with_id(artifact_id.clone());
			let attrs = object_data.and_then(|output| {
				self.compute_attrs_from_bytes(&artifact_id.clone().into(), &output.bytes)
			});
			let id = self.put_sync_with_attrs(node_id, name, artifact, node_depth + 1, attrs);
			result.push((name.clone(), id));
		}
		Ok(result)
	}

	fn getxattr_sync(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		let node = self.get_sync(id)?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(None);
		};

		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			let file_data = self.get_file_data_sync(&file)?;
			if file_data.dependencies.is_empty() {
				return Ok(None);
			}
			let references = file_data.dependencies.keys().cloned().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).unwrap();
			return Ok(Some(data.into()));
		}

		if name == tg::file::MODULE_XATTR_NAME {
			let file_data = self.get_file_data_sync(&file)?;
			let Some(module) = file_data.module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}

		Ok(None)
	}

	fn listxattrs_sync(&self, id: u64) -> std::io::Result<Vec<String>> {
		let node = self.get_sync(id)?;
		let Some(tg::Artifact::File(file)) = node.artifact else {
			return Ok(Vec::new());
		};
		let file_data = self.get_file_data_sync(&file)?;
		if file_data.dependencies.is_empty() {
			return Ok(Vec::new());
		}
		Ok(vec![tg::file::DEPENDENCIES_XATTR_NAME.to_owned()])
	}

	fn forget(&self, id: u64, nlookup: u64) {
		self.node_cache.invalidate(&id);
		self.pending_nodes.remove(&id);
		self.nodes.forget(id, nlookup);
	}

	fn forget_multi(&self, forgets: &[(u64, u64)]) {
		for &(id, nlookup) in forgets {
			self.forget(id, nlookup);
		}
	}

	fn increment_nlookup(&self, id: u64) {
		self.nodes.increment_nlookup(id);
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
		let directory_handles = DashMap::default();
		let file_handles = DashMap::default();
		let pending_nodes = Arc::new(DashMap::default());
		let server = server.clone();
		let provider = Self {
			node_cache: cache,
			node_count,
			file_handle_count,
			nodes,
			directory_handles,
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
		let data = self.nodes.get(id)?;
		let node = Node {
			parent: data.parent,
			artifact: data.artifact.map(tg::Artifact::with_id),
			depth: data.depth,
			attrs: None,
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
			attrs: None,
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

	/// Get a node synchronously. Checks the node cache, pending nodes, and storage.
	fn get_sync(&self, id: u64) -> std::io::Result<Node> {
		// Check the node cache.
		if let Some(node) = self.node_cache.get(&id) {
			return Ok(node.clone());
		}

		// Check the pending nodes.
		if let Some(node) = self.pending_nodes.get(&id) {
			return Ok(node.clone());
		}

		// Get the node from nodes storage.
		let data = self.nodes.get(id)?;
		let node = Node {
			parent: data.parent,
			artifact: data.artifact.map(tg::Artifact::with_id),
			depth: data.depth,
			attrs: None,
		};

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		Ok(node)
	}

	/// Insert a node synchronously.
	fn put_sync(&self, parent: u64, name: &str, artifact: tg::Artifact, depth: u64) -> u64 {
		self.put_sync_with_attrs(parent, name, artifact, depth, None)
	}

	/// Insert a node synchronously with pre-computed attrs.
	fn put_sync_with_attrs(
		&self,
		parent: u64,
		name: &str,
		artifact: tg::Artifact,
		depth: u64,
		attrs: Option<vfs::Attrs>,
	) -> u64 {
		// Get the artifact id.
		let artifact_id = artifact.id();

		// Create the node.
		let node = Node {
			parent,
			artifact: Some(artifact),
			depth,
			attrs,
		};

		// Get a node id.
		let id = self.node_count.fetch_add(1, Ordering::Relaxed);

		// Add the node to the cache.
		self.node_cache.insert(id, node.clone());

		// Insert into the pending nodes.
		self.pending_nodes.insert(id, node);

		// Insert the node into the nodes storage.
		self.nodes.insert(id, parent, name, artifact_id, depth);

		// Schedule cleanup of pending nodes if a tokio runtime is available.
		let pending_nodes = self.pending_nodes.clone();
		if let Ok(runtime) = tokio::runtime::Handle::try_current() {
			runtime.spawn(async move {
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
				pending_nodes.remove(&id);
			});
		}

		id
	}

	/// Get the file data synchronously by fetching the object from the store and deserializing it.
	fn get_file_data_sync(&self, file: &tg::File) -> std::io::Result<tg::graph::data::File> {
		let id: tg::object::Id = file.id().into();
		let mut file_handle = None;
		let output = self
			.server
			.try_get_object_sync(&id, &mut file_handle)
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the file object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.ok_or_else(|| {
				tracing::error!(%id, "file object not found");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let data = tg::file::Data::deserialize(output.bytes).map_err(|error| {
			tracing::error!(%error, %id, "failed to deserialize the file data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		match data {
			tg::file::Data::Node(node) => Ok(node),
			// Pointer variants require resolving a graph, which needs async handling.
			tg::file::Data::Pointer(_) => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}

	/// Get the executable bit and size for a file synchronously.
	fn get_file_metadata_sync(&self, file: &tg::File) -> std::io::Result<(bool, u64)> {
		let file_data = self.get_file_data_sync(file)?;
		let executable = file_data.executable;
		let size = if let Some(blob_id) = &file_data.contents {
			self.get_blob_length_sync(blob_id)?
		} else {
			0
		};
		Ok((executable, size))
	}

	/// Compute attrs from raw object bytes. Returns None if deserialization fails.
	fn compute_attrs_from_bytes(
		&self,
		id: &tg::object::Id,
		bytes: &bytes::Bytes,
	) -> Option<vfs::Attrs> {
		// Determine the artifact type from the object id.
		let kind = id.kind();
		match kind {
			tg::object::Kind::File => {
				let data = tg::file::Data::deserialize(bytes.clone()).ok()?;
				match data {
					tg::file::Data::Node(node) => {
						let executable = node.executable;
						let size = node
							.contents
							.as_ref()
							.and_then(|blob_id| self.get_blob_length_sync(blob_id).ok())
							.unwrap_or(0);
						Some(vfs::Attrs::new(vfs::FileType::File { executable, size }))
					},
					tg::file::Data::Pointer(_) => None,
				}
			},
			tg::object::Kind::Directory => Some(vfs::Attrs::new(vfs::FileType::Directory)),
			tg::object::Kind::Symlink => Some(vfs::Attrs::new(vfs::FileType::Symlink)),
			_ => None,
		}
	}

	/// Try to compute attrs for an artifact synchronously. Returns None if the computation fails.
	fn try_compute_attrs_sync(&self, artifact: &tg::Artifact) -> Option<vfs::Attrs> {
		match artifact {
			tg::Artifact::File(file) => {
				let (executable, size) = self.get_file_metadata_sync(file).ok()?;
				Some(vfs::Attrs::new(vfs::FileType::File { executable, size }))
			},
			tg::Artifact::Directory(_) => Some(vfs::Attrs::new(vfs::FileType::Directory)),
			tg::Artifact::Symlink(_) => Some(vfs::Attrs::new(vfs::FileType::Symlink)),
		}
	}

	/// Get the blob id for a file synchronously.
	fn get_file_blob_sync(&self, file: &tg::File) -> std::io::Result<tg::blob::Id> {
		let file_data = self.get_file_data_sync(file)?;
		file_data.contents.ok_or_else(|| {
			tracing::error!("file has no contents");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	/// Get the length of a blob synchronously.
	fn get_blob_length_sync(&self, blob_id: &tg::blob::Id) -> std::io::Result<u64> {
		let id: tg::object::Id = blob_id.clone().into();
		let mut file_handle = None;
		let output = self
			.server
			.try_get_object_sync(&id, &mut file_handle)
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the blob object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.ok_or_else(|| {
				tracing::error!(%id, "blob object not found");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let data = tg::blob::Data::deserialize(output.bytes).map_err(|error| {
			tracing::error!(%error, %id, "failed to deserialize the blob data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let object = tg::blob::Object::try_from_data(data).map_err(|error| {
			tracing::error!(%error, %id, "failed to create the blob object");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let length = match &object {
			tg::blob::Object::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
			tg::blob::Object::Branch(branch) => {
				branch.children.iter().map(|child| child.length).sum()
			},
		};
		Ok(length)
	}

	/// Try to get a passthrough file descriptor for a blob synchronously.
	fn try_get_passthrough_fd_sync(&self, blob_id: &tg::blob::Id) -> Option<OwnedFd> {
		let id: tg::object::Id = blob_id.clone().into();
		let object = self.server.store.try_get_object_sync(&id).ok()??;
		let cache_pointer = object.cache_pointer?;
		// Only blobs that start at position 0 are eligible for passthrough.
		if cache_pointer.position != 0 {
			return None;
		}
		let mut path = self
			.server
			.cache_path()
			.join(cache_pointer.artifact.to_string());
		if let Some(path_) = &cache_pointer.path {
			path.push(path_);
		}
		let file = std::fs::File::open(&path).ok()?;
		Some(OwnedFd::from(file))
	}

	/// Get symlink artifact and path synchronously.
	fn get_symlink_data_sync(
		&self,
		symlink: &tg::Symlink,
	) -> std::io::Result<(Option<tg::artifact::Id>, Option<PathBuf>)> {
		let id: tg::object::Id = symlink.id().into();
		let mut file_handle = None;
		let output = self
			.server
			.try_get_object_sync(&id, &mut file_handle)
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the symlink object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.ok_or_else(|| {
				tracing::error!(%id, "symlink object not found");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let data = tg::symlink::Data::deserialize(output.bytes).map_err(|error| {
			tracing::error!(%error, %id, "failed to deserialize the symlink data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		match data {
			tg::symlink::Data::Node(node) => {
				let artifact_id = node.artifact.and_then(|edge| match edge {
					tg::graph::data::Edge::Object(id) => Some(id),
					tg::graph::data::Edge::Pointer(_) => None,
				});
				Ok((artifact_id, node.path))
			},
			// Pointer variants require resolving a graph, which needs async handling.
			tg::symlink::Data::Pointer(_) => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}

	/// Get directory entries synchronously. Only works for leaf directories with object edges.
	fn get_directory_entries_sync(
		&self,
		directory: &tg::Directory,
	) -> std::io::Result<HashMap<String, tg::artifact::Id, fnv::FnvBuildHasher>> {
		let id: tg::object::Id = directory.id().into();
		let mut file_handle = None;
		let output = self
			.server
			.try_get_object_sync(&id, &mut file_handle)
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the directory object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.ok_or_else(|| {
				tracing::error!(%id, "directory object not found");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		let data = tg::directory::Data::deserialize(output.bytes).map_err(|error| {
			tracing::error!(%error, %id, "failed to deserialize the directory data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		match data {
			tg::directory::Data::Node(tg::graph::data::Directory::Leaf(leaf)) => {
				let mut entries = HashMap::with_hasher(fnv::FnvBuildHasher::default());
				for (name, edge) in leaf.entries {
					match edge {
						tg::graph::data::Edge::Object(id) => {
							entries.insert(name, id);
						},
						// Pointer edges need graph resolution, which requires async.
						tg::graph::data::Edge::Pointer(_) => {
							return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
						},
					}
				}
				Ok(entries)
			},
			// Branch directories and pointer variants need async resolution.
			_ => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
		}
	}
}

impl Nodes {
	fn new() -> Self {
		let nodes = DashMap::default();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			StorageNode {
				parent: vfs::ROOT_NODE_ID,
				artifact: None,
				depth: 0,
				children: HashMap::default(),
				nlookup: u64::MAX,
			},
		);
		Self { nodes }
	}

	fn lookup(&self, parent: u64, name: &str) -> Option<u64> {
		self.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
	}

	fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.nodes.get(&id).map(|node| node.parent).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
	}

	fn get(&self, id: u64) -> std::io::Result<StorageNode> {
		self.nodes.get(&id).map(|n| n.clone()).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
	}

	fn insert(&self, id: u64, parent: u64, name: &str, artifact: tg::artifact::Id, depth: u64) {
		// Insert the new node.
		self.nodes.insert(
			id,
			StorageNode {
				parent,
				artifact: Some(artifact),
				depth,
				children: HashMap::default(),
				nlookup: 0,
			},
		);
		// Update the parent's children map.
		if let Some(mut parent_node) = self.nodes.get_mut(&parent) {
			parent_node.children.insert(name.to_owned(), id);
		}
	}

	/// Increment the nlookup count for a node. Called when a `fuse_entry_out` is sent to the kernel.
	fn increment_nlookup(&self, id: u64) {
		if let Some(mut node) = self.nodes.get_mut(&id) {
			node.nlookup += 1;
		}
	}

	/// Decrement the nlookup count for a node. If it reaches zero, remove the node from the map
	/// and from its parent's children.
	fn forget(&self, id: u64, nlookup: u64) {
		let should_remove = {
			let Some(mut node) = self.nodes.get_mut(&id) else {
				return;
			};
			node.nlookup = node.nlookup.saturating_sub(nlookup);
			node.nlookup == 0
		};
		if should_remove
			&& id != vfs::ROOT_NODE_ID
			&& let Some((_, removed)) = self.nodes.remove(&id)
			&& let Some(mut parent) = self.nodes.get_mut(&removed.parent)
		{
			parent.children.retain(|_, &mut child_id| child_id != id);
		}
	}
}
