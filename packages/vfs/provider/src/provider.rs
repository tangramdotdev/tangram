use {
	bytes::Bytes,
	futures::TryStreamExt as _,
	heed as lmdb,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		io::{Read as _, Seek as _, SeekFrom},
		os::unix::ffi::OsStrExt as _,
		path::{Path, PathBuf},
		pin::pin,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
	tangram_object_store as object_store,
	tangram_uri::Uri,
	tangram_vfs as vfs,
	tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _},
};

/// The name of the cache directory within the data directory. The server uses this name only while a vfs is mounted, which is the only circumstance in which this provider runs.
const CACHE_DIRECTORY_NAME: &str = "cache";

/// The default interval at which expired cache-only nodes are swept.
const DEFAULT_NODE_EVICTION_INTERVAL: Duration = Duration::from_secs(30);

/// The default duration a cache-only node created by directory enumeration is retained after its most recent access before it becomes eligible for eviction.
const DEFAULT_NODE_TTL: Duration = Duration::from_mins(1);

/// The default map size with which the object store is opened. It is the server's default, and the server sends its own if it is configured with another.
const DEFAULT_OBJECT_STORE_MAP_SIZE: usize = 1_099_511_627_776;

/// The default path of the object store within the data directory. It is the server's default, and the server sends its own if it is configured with another.
const DEFAULT_OBJECT_STORE_PATH: &str = "objects";

/// The size of a FUSE directory entry header.
const FUSE_DIRENT_HEADER_SIZE: usize = 24;

/// The configuration for a provider.
#[derive(Clone)]
pub struct Config {
	/// The server's data directory, which the fast path reads the object store and the cache directory from. If it is `None`, then the provider uses the client for every request.
	pub data_directory: Option<PathBuf>,

	/// The interval at which expired cache-only nodes are swept.
	pub node_eviction_interval: Duration,

	/// The duration a cache-only node created by directory enumeration is retained after its most recent access before it becomes eligible for eviction.
	pub node_ttl: Duration,

	/// The map size with which to open the object store. LMDB requires a reader to use a map size at least as large as the writer's, so this must be at least the server's.
	pub object_store_map_size: usize,

	/// The path of the object store, which is joined to the data directory.
	pub object_store_path: PathBuf,

	/// An optional base name for the object store's POSIX lock semaphores. It must match the name the server opens the object store with so that the sandboxed provider and the server share the same lock.
	pub object_store_posix_sem_prefix: Option<String>,
}

pub struct Provider {
	inner: Arc<Inner>,
	runtime: tokio::runtime::Runtime,
}

struct Inner {
	client: tg::Client,
	fast: Option<Fast>,
	file_handle_count: AtomicU64,
	file_handles: Mutex<BTreeMap<u64, FileHandle>>,
	nodes: Nodes,
}

/// The state the fast path requires. It reads the object store and the cache directory directly instead of sending a request to the server.
struct Fast {
	cache_path: PathBuf,
	store: object_store::lmdb::Store,
}

struct FileHandle {
	blob: tg::blob::Id,
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
	accessed: Instant,
	artifact: Option<tg::artifact::Id>,
	attrs: Option<vfs::Attrs>,
	children: BTreeMap<String, u64>,
	depth: u64,
	lookup_count: u64,
	name: Option<String>,
	parent: u64,
}

#[derive(Clone)]
struct NodeInfo {
	artifact: Option<tg::artifact::Id>,
	attrs: Option<vfs::Attrs>,
	depth: u64,
	parent: u64,
}

struct ReaddirPlusPage {
	length: usize,
	offset: usize,
	position: usize,
	size: usize,
}

impl Provider {
	pub fn new(socket: &str, config: &Config) -> std::io::Result<Self> {
		// Initialize the logging and runtime.
		init_logging();
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()?;

		// Create the client URL with the builder so socket paths are not mangled by percent-encoding round trips.
		let url = Uri::builder()
			.scheme("http+unix")
			.authority(socket)
			.path("")
			.build()
			.map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
		let arg = tg::Arg {
			pool: None,
			reconnect: None,
			retry: None,
			sync: tg::sync::Config::default(),
			token: None,
			url: Some(url),
			version: None,
		};
		// Connect lazily so mounting does not depend on the server already serving on its socket.
		let client = tg::Client::new(arg).map_err(std::io::Error::other)?;

		// Initialize the provider state.
		let fast = config
			.data_directory
			.as_deref()
			.and_then(|data_directory| Fast::new(data_directory, config));
		let inner = Arc::new(Inner {
			client,
			fast,
			file_handle_count: AtomicU64::new(1000),
			file_handles: Mutex::new(BTreeMap::new()),
			nodes: Nodes::new(),
		});

		// Spawn a background task that evicts cache-only nodes the kernel never referenced, bounding node table growth; it is aborted when the runtime is dropped with the provider.
		runtime.spawn({
			let inner = inner.clone();
			let ttl = config.node_ttl;
			let interval = config.node_eviction_interval;
			async move {
				loop {
					tokio::time::sleep(interval).await;
					inner.nodes.evict_expired(ttl);
				}
			}
		});
		let provider = Self { inner, runtime };

		Ok(provider)
	}

	pub(crate) fn handle_batch_sync(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		vfs::Provider::handle_batch_sync(&*self.inner, requests)
	}

	pub(crate) fn submit_batch<F>(&self, requests: Vec<vfs::Request>, on_complete: F)
	where
		F: FnOnce(Vec<std::io::Result<vfs::Response>>) + Send + 'static,
	{
		let inner = self.inner.clone();
		self.runtime.spawn(async move {
			let results = inner.handle_batch(requests).await;
			on_complete(results);
		});
	}
}

impl Inner {
	async fn handle_batch(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		// Handle each request.
		let mut responses = Vec::with_capacity(requests.len());
		for request in requests {
			tracing::debug!(?request, "handling vfs request");
			let response = match request {
				vfs::Request::Close { handle } => {
					self.close(handle);
					Ok(vfs::Response::Unit)
				},
				vfs::Request::Forget { id, nlookup } => {
					self.nodes.forget(id, nlookup);
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
					.nodes
					.lookup_parent(id)
					.map(|id| vfs::Response::LookupParent { id }),
				vfs::Request::Open { id } => {
					self.open(id).await.map(|handle| vfs::Response::Open {
						backing_fd: None,
						handle,
					})
				},
				vfs::Request::OpenDir { id } => self
					.opendir(id)
					.await
					.map(|handle| vfs::Response::OpenDir { handle }),
				vfs::Request::Read {
					handle,
					length,
					position,
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
					self.nodes.remember(id);
					Ok(vfs::Response::Unit)
				},
			};
			if let Err(error) = &response {
				tracing::debug!(%error, "vfs request failed");
			} else {
				tracing::debug!("vfs request ok");
			}
			responses.push(response);
		}

		responses
	}

	async fn getattr(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		let node = self.nodes.get(id)?;
		if let Some(attrs) = node.attrs {
			return Ok(attrs);
		}
		let attrs = self
			.compute_attrs_for_node(node.artifact.as_ref(), node.depth)
			.await?;
		self.nodes.set_attrs(id, attrs);
		Ok(attrs)
	}

	/// Computes the attributes for a node. A symlink's target is rendered so that its size is the length of the target, which the file system extension uses to size the readlink result.
	async fn compute_attrs_for_node(
		&self,
		artifact: Option<&tg::artifact::Id>,
		depth: u64,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(artifact) = artifact
			&& matches!(artifact.kind(), tg::artifact::Kind::Symlink)
		{
			let target = self.resolve_symlink(artifact, depth).await?;
			let size = target.len().to_u64().unwrap();
			return Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size }));
		}
		self.compute_attrs_with_fallback(artifact).await
	}

	/// Computes the attributes with the fast path, falling back to the client if the fast path is unavailable.
	async fn compute_attrs_with_fallback(
		&self,
		artifact: Option<&tg::artifact::Id>,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(fast) = &self.fast {
			match fast.compute_attrs(artifact) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		self.compute_attrs(artifact).await
	}

	/// Gets the entries of a directory with the fast path, falling back to the client if the fast path is unavailable.
	async fn directory_entry_ids(
		&self,
		artifact: &tg::artifact::Id,
	) -> std::io::Result<BTreeMap<String, tg::artifact::Id>> {
		if let Some(fast) = &self.fast {
			match fast.directory_entries(artifact) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		let entries = self.directory_entries(artifact).await?;
		Ok(entries
			.into_iter()
			.map(|(name, artifact)| (name, artifact.id()))
			.collect())
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		self.lookup_inner(parent, name, false).await
	}

	async fn lookup_and_remember(
		&self,
		parent: u64,
		name: &str,
	) -> std::io::Result<Option<(u64, vfs::Attrs)>> {
		let Some(id) = self.lookup_inner(parent, name, true).await? else {
			return Ok(None);
		};
		let attrs = match self.getattr(id).await {
			Err(error) => {
				self.nodes.forget(id, 1);
				return Err(error);
			},
			Ok(attrs) => attrs,
		};

		Ok(Some((id, attrs)))
	}

	async fn lookup_inner(
		&self,
		parent: u64,
		name: &str,
		remember: bool,
	) -> std::io::Result<Option<u64>> {
		// Handle the special directory names.
		if name == "." {
			if remember {
				self.nodes.remember(parent);
			}
			return Ok(Some(parent));
		}
		if name == ".." {
			let id = self.nodes.lookup_parent(parent)?;
			if remember {
				self.nodes.remember(id);
			}
			return Ok(Some(id));
		}

		// Look up an existing node.
		let id = if remember {
			self.nodes.lookup_and_remember(parent, name)
		} else {
			self.nodes.lookup(parent, name)
		};
		if let Some(id) = id {
			return Ok(Some(id));
		}

		// Resolve a child artifact, treating a root child as an artifact ID with an optional module extension.
		let entry = if parent == vfs::ROOT_NODE_ID {
			let name = None
				.or_else(|| name.strip_suffix(".tg.ts"))
				.or_else(|| name.strip_suffix(".tg.js"))
				.or_else(|| Path::new(name).file_stem().and_then(|name| name.to_str()))
				.unwrap_or(name);
			name.parse().ok().map(|artifact| (artifact, 1))
		} else {
			None
		};

		let entry = if entry.is_some() {
			entry
		} else {
			let NodeInfo {
				artifact, depth, ..
			} = self.nodes.get(parent)?;
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			if !matches!(artifact.kind(), tg::artifact::Kind::Directory) {
				return Ok(None);
			}
			// The fast path descends the directory tree to the named entry, so unlike the fallback it does not enumerate the whole directory.
			let artifact = 'a: {
				if let Some(fast) = &self.fast {
					match fast.directory_lookup_entry(&artifact, name) {
						Err(error) if is_fallback(&error) => (),
						result => break 'a result?,
					}
				}
				let entries = self.directory_entries(&artifact).await?;
				entries.get(name).map(tg::Artifact::id)
			};
			let Some(artifact) = artifact else {
				return Ok(None);
			};
			Some((artifact, depth + 1))
		};

		let Some((artifact, depth)) = entry else {
			return Ok(None);
		};

		// Insert the resolved node.
		let attrs = attrs_from_artifact(Some(&artifact));
		let id = self
			.nodes
			.get_or_insert_child(parent, name, artifact, depth, attrs, remember)?;

		Ok(Some(id))
	}

	async fn open(&self, id: u64) -> std::io::Result<u64> {
		// Resolve the file contents.
		let NodeInfo { artifact, .. } = self.nodes.get(id)?;
		let Some(artifact) = artifact else {
			return Err(std::io::Error::other("expected a file"));
		};
		let blob = 'a: {
			if let Some(fast) = &self.fast {
				match fast.file_contents(&artifact) {
					Err(error) if is_fallback(&error) => (),
					result => break 'a result?,
				}
			}
			let tg::Artifact::File(file) = tg::Artifact::with_id(artifact) else {
				return Err(std::io::Error::other("expected a file"));
			};
			file.contents_with_handle(&self.client)
				.await
				.map_err(eio)?
				.id()
		};

		// Create a file handle.
		let handle = self.file_handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles
			.lock()
			.unwrap()
			.insert(handle, FileHandle { blob });

		Ok(handle)
	}

	async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		let NodeInfo { artifact, .. } = self.nodes.get(id)?;
		if let Some(artifact) = artifact
			&& !matches!(artifact.kind(), tg::artifact::Kind::Directory)
		{
			return Err(std::io::Error::other("expected a directory"));
		}
		Ok(id)
	}

	async fn read(&self, handle: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		// Resolve the file handle.
		let blob = {
			let file_handles = self.file_handles.lock().unwrap();
			let Some(file_handle) = file_handles.get(&handle) else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			file_handle.blob.clone()
		};

		// Try the fast path.
		if let Some(fast) = &self.fast {
			match fast.read(&blob, position, length) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}

		// Read through the client.
		let arg = tg::read::Arg {
			blob,
			options: tg::read::Options {
				length: Some(length),
				position: Some(SeekFrom::Start(position)),
				size: None,
			},
			token: None,
		};
		let stream = self
			.client
			.try_read(arg)
			.await
			.map_err(eio)?
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?
			.map_err(eio);
		let mut stream = pin!(stream);
		let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(0));
		while let Some(chunk) = stream.try_next().await? {
			bytes.extend_from_slice(&chunk.bytes);
		}
		let bytes = bytes.into();

		Ok(bytes)
	}

	async fn readdir(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		// Resolve the directory.
		let NodeInfo {
			artifact, parent, ..
		} = self.nodes.get(id)?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => return Err(std::io::Error::other("expected a directory")),
		};
		let Some(directory) = directory else {
			return Ok(Vec::new());
		};

		// Collect the directory entries.
		let children = self.directory_entry_ids(&directory).await?;
		let mut entries = Vec::with_capacity(children.len() + 2);
		entries.push((".".to_owned(), id, vfs::EntryKind::Directory));
		entries.push(("..".to_owned(), parent, vfs::EntryKind::Directory));
		for (name, artifact) in children {
			let kind = match artifact.kind() {
				tg::artifact::Kind::Directory => vfs::EntryKind::Directory,
				tg::artifact::Kind::File => vfs::EntryKind::File,
				tg::artifact::Kind::Symlink => vfs::EntryKind::Symlink,
			};
			entries.push((name, 0, kind));
		}

		// Select the requested page.
		let offset = offset.to_usize().unwrap_or(usize::MAX);
		let length = length.to_usize().unwrap_or(usize::MAX);
		let mut size = 0usize;
		let entries = entries
			.into_iter()
			.skip(offset)
			.take_while(|(name, _, _)| {
				let entry_size = readdir_entry_size(name.len());
				let fits = size.saturating_add(entry_size) <= length;
				if fits {
					size = size.saturating_add(entry_size);
				}
				fits
			})
			.collect();

		Ok(entries)
	}

	async fn readdirplus(
		&self,
		id: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		// Resolve the directory.
		let NodeInfo {
			artifact,
			depth,
			parent,
			..
		} = self.nodes.get(id)?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => return Err(std::io::Error::other("expected a directory")),
		};
		let Some(directory) = directory else {
			return Ok(Vec::new());
		};

		// Initialize the requested page.
		let children = self.directory_entry_ids(&directory).await?;
		let mut page = ReaddirPlusPage {
			length: usize::try_from(length).unwrap_or(usize::MAX),
			offset: usize::try_from(offset).unwrap_or(usize::MAX),
			position: 0,
			size: 0,
		};
		let mut entries = Vec::new();

		// Add the special directory entries.
		let attrs = self.getattr(id).await?;
		if page.push(&mut entries, ".".to_owned(), id, attrs) {
			return Ok(entries);
		}
		let parent_attrs = self.getattr(parent).await?;
		if page.push(&mut entries, "..".to_owned(), parent, parent_attrs) {
			return Ok(entries);
		}

		// Add the child entries.
		for (name, artifact) in children {
			if page.position < page.offset {
				page.position += 1;
				continue;
			}
			let attrs = self
				.compute_attrs_for_node(Some(&artifact), depth + 1)
				.await?;
			if ReaddirPlusPage::entry_size(name.len()) + page.size > page.length {
				break;
			}
			let node_id = self.nodes.get_or_insert_child(
				id,
				&name,
				artifact,
				depth + 1,
				Some(attrs),
				false,
			)?;
			page.size += ReaddirPlusPage::entry_size(name.len());
			page.position += 1;
			entries.push((name, node_id, attrs));
		}

		Ok(entries)
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		let NodeInfo {
			artifact, depth, ..
		} = self.nodes.get(id)?;
		let Some(artifact) = artifact else {
			return Err(std::io::Error::other("expected a symlink"));
		};
		self.resolve_symlink(&artifact, depth).await
	}

	/// Renders a symlink's target with the fast path, falling back to the client if the fast path is unavailable.
	async fn resolve_symlink(
		&self,
		artifact: &tg::artifact::Id,
		depth: u64,
	) -> std::io::Result<Bytes> {
		if let Some(fast) = &self.fast {
			match fast.readlink(artifact, depth) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		let tg::Artifact::Symlink(symlink) = tg::Artifact::with_id(artifact.clone()) else {
			return Err(std::io::Error::other("expected a symlink"));
		};
		let artifact = symlink
			.artifact_with_handle(&self.client)
			.await
			.map_err(eio)?
			.map(|artifact| artifact.id());
		let path = symlink.path_with_handle(&self.client).await.map_err(eio)?;
		render_symlink(depth, artifact, path)
	}

	async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let NodeInfo { artifact, .. } = self.nodes.get(id)?;
		let Some(artifact) = artifact else {
			return Ok(Vec::new());
		};
		if let Some(fast) = &self.fast {
			match fast.listxattrs(&artifact) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		let tg::Artifact::File(file) = tg::Artifact::with_id(artifact) else {
			return Ok(Vec::new());
		};
		let dependencies = file
			.dependencies_with_handle(&self.client)
			.await
			.map_err(eio)?;
		let module = file.module_with_handle(&self.client).await.map_err(eio)?;
		let mut names = Vec::with_capacity(2);
		if !dependencies.is_empty() {
			names.push(tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
		}
		if module.is_some() {
			names.push(tg::file::MODULE_XATTR_NAME.to_owned());
		}
		Ok(names)
	}

	async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		let NodeInfo { artifact, .. } = self.nodes.get(id)?;
		let Some(artifact) = artifact else {
			return Ok(None);
		};
		if let Some(fast) = &self.fast {
			match fast.getxattr(&artifact, name) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		let tg::Artifact::File(file) = tg::Artifact::with_id(artifact) else {
			return Ok(None);
		};
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			let dependencies = file
				.dependencies_with_handle(&self.client)
				.await
				.map_err(eio)?;
			if dependencies.is_empty() {
				return Ok(None);
			}
			let references = dependencies.into_keys().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).map_err(eio)?;
			return Ok(Some(data.into()));
		}
		if name == tg::file::MODULE_XATTR_NAME {
			let Some(module) = file.module_with_handle(&self.client).await.map_err(eio)? else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	fn close(&self, handle: u64) {
		self.file_handles.lock().unwrap().remove(&handle);
	}

	async fn compute_attrs(
		&self,
		artifact: Option<&tg::artifact::Id>,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::File) => {
				let tg::Artifact::File(file) = tg::Artifact::with_id(artifact.clone()) else {
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				let size = file.length_with_handle(&self.client).await.map_err(eio)?;
				let executable = file
					.executable_with_handle(&self.client)
					.await
					.map_err(eio)?;
				Ok(vfs::Attrs::new(vfs::AttrsInner::File { executable, size }))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Symlink) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size: 0 }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}

	async fn directory_entries(
		&self,
		artifact: &tg::artifact::Id,
	) -> std::io::Result<BTreeMap<String, tg::Artifact>> {
		let tg::Artifact::Directory(directory) = tg::Artifact::with_id(artifact.clone()) else {
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		directory
			.entries_with_handle(&self.client)
			.await
			.map_err(eio)
	}
}

impl Fast {
	/// Opens the object store read only and locates the cache directory. Returns `None` if the object store cannot be opened, in which case the provider uses the client for every request.
	fn new(data_directory: &Path, config: &Config) -> Option<Self> {
		// Open the object store.
		let path = data_directory.join(&config.object_store_path);
		let config = object_store::lmdb::Config {
			map_size: config.object_store_map_size,
			path,
			posix_sem_prefix: config.object_store_posix_sem_prefix.clone(),
		};
		let store = match object_store::lmdb::Store::new_readonly(&config) {
			Err(error) => {
				tracing::warn!(
					error = %error.trace(),
					"failed to open the object store, so the fast path is disabled"
				);
				return None;
			},
			Ok(store) => store,
		};

		// Locate the cache directory.
		let cache_path = data_directory.join(CACHE_DIRECTORY_NAME);
		tracing::info!(cache_path = %cache_path.display(), "enabled the fast path");
		let fast = Self { cache_path, store };

		Some(fast)
	}

	/// Begins a read transaction. Every request opens its own transaction, because the driver submits one request per batch.
	fn transaction(&self) -> std::io::Result<lmdb::RoTxn<'_, lmdb::WithTls>> {
		self.store.env().read_txn().map_err(|error| {
			tracing::debug!(?error, "failed to begin a transaction");
			fallback()
		})
	}

	fn compute_attrs(&self, artifact: Option<&tg::artifact::Id>) -> std::io::Result<vfs::Attrs> {
		let transaction = self.transaction()?;
		self.compute_attrs_with_transaction(&transaction, artifact)
	}

	fn directory_entries(
		&self,
		artifact: &tg::artifact::Id,
	) -> std::io::Result<BTreeMap<String, tg::artifact::Id>> {
		let transaction = self.transaction()?;
		self.directory_entries_with_transaction(&transaction, artifact, None)
	}

	fn directory_lookup_entry(
		&self,
		artifact: &tg::artifact::Id,
		name: &str,
	) -> std::io::Result<Option<tg::artifact::Id>> {
		let transaction = self.transaction()?;
		self.directory_lookup_entry_with_transaction(&transaction, artifact, name, None)
	}

	fn file_contents(&self, artifact: &tg::artifact::Id) -> std::io::Result<tg::blob::Id> {
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Err(std::io::Error::other("expected a file"));
		}
		let transaction = self.transaction()?;
		let (file, _) = self.file_node_with_transaction(&transaction, artifact)?;
		// A file with no contents is empty. The fallback handles it, so that this path does not have to represent a handle with no blob.
		file.contents.ok_or_else(fallback)
	}

	fn read(&self, blob: &tg::blob::Id, position: u64, length: u64) -> std::io::Result<Bytes> {
		let transaction = self.transaction()?;
		let mut output = Vec::with_capacity(length.to_usize().unwrap_or(0));
		self.read_blob_range_with_transaction(&transaction, blob, position, length, &mut output)?;
		Ok(output.into())
	}

	fn readlink(&self, artifact: &tg::artifact::Id, depth: u64) -> std::io::Result<Bytes> {
		if !matches!(artifact.kind(), tg::artifact::Kind::Symlink) {
			return Err(std::io::Error::other("expected a symlink"));
		}
		let transaction = self.transaction()?;
		let (symlink, graph) = self.symlink_node_with_transaction(&transaction, artifact)?;
		let artifact = match symlink.artifact {
			None => None,
			Some(edge) => Some(Self::artifact_id_from_edge(edge, graph.as_ref())?),
		};
		render_symlink(depth, artifact, symlink.path)
	}

	fn listxattrs(&self, artifact: &tg::artifact::Id) -> std::io::Result<Vec<String>> {
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let transaction = self.transaction()?;
		let (file, _) = self.file_node_with_transaction(&transaction, artifact)?;
		let mut names = Vec::with_capacity(2);
		if !file.dependencies.is_empty() {
			names.push(tg::file::DEPENDENCIES_XATTR_NAME.to_owned());
		}
		if file.module.is_some() {
			names.push(tg::file::MODULE_XATTR_NAME.to_owned());
		}
		Ok(names)
	}

	fn getxattr(&self, artifact: &tg::artifact::Id, name: &str) -> std::io::Result<Option<Bytes>> {
		if !matches!(artifact.kind(), tg::artifact::Kind::File) {
			return Ok(None);
		}
		let transaction = self.transaction()?;
		let (file, _) = self.file_node_with_transaction(&transaction, artifact)?;
		if name == tg::file::DEPENDENCIES_XATTR_NAME {
			if file.dependencies.is_empty() {
				return Ok(None);
			}
			let references = file.dependencies.keys().cloned().collect::<Vec<_>>();
			let data = serde_json::to_vec(&references).map_err(eio)?;
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

	fn compute_attrs_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		artifact: Option<&tg::artifact::Id>,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_with_transaction(transaction, artifact)?;
				let size = file.contents.as_ref().map_or(Ok(0), |contents| {
					self.blob_length_with_transaction(transaction, contents)
				})?;
				Ok(vfs::Attrs::new(vfs::AttrsInner::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Symlink) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size: 0 }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}

	fn try_get_object(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> std::io::Result<Option<object_store::Object<'static>>> {
		let arg = object_store::TryGetArg { id: id.clone() };
		self.store
			.try_get_with_transaction(transaction, &arg)
			.map(|output| output.object)
			.map_err(eio)
	}

	fn try_get_data(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		self.store
			.try_get_data_with_transaction(transaction, id)
			.map_err(eio)
	}

	fn artifact_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		artifact: &tg::artifact::Id,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		let id: tg::object::Id = artifact.clone().into();
		let Some((_, data)) = self.try_get_data(transaction, &id)? else {
			return Err(fallback());
		};
		data.try_into().map_err(|_| {
			tracing::error!(%artifact, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn graph_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		graph: &tg::graph::Id,
	) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let Some((_, data)) = self.try_get_data(transaction, &id)? else {
			return Err(fallback());
		};
		data.try_into().map_err(|_| {
			tracing::error!(%graph, "expected graph data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn resolve_graph_node_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = Self::graph_id_from_pointer(pointer, default_graph)?;
		let graph_data = self.graph_data_with_transaction(transaction, &graph)?;
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

	fn directory_node_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		directory: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let data = self.artifact_data_with_transaction(transaction, directory)?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_with_transaction(transaction, &pointer, None)?;
				let tg::graph::data::Node::Directory(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a directory node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn file_node_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		file: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let data = self.artifact_data_with_transaction(transaction, file)?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_with_transaction(transaction, &pointer, None)?;
				let tg::graph::data::Node::File(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a file node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn symlink_node_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		symlink: &tg::artifact::Id,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let data = self.artifact_data_with_transaction(transaction, symlink)?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_with_transaction(transaction, &pointer, None)?;
				let tg::graph::data::Node::Symlink(node) = node else {
					tracing::error!(pointer = ?pointer, "expected a symlink node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn directory_entries_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		directory: &tg::artifact::Id,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<BTreeMap<String, tg::artifact::Id>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let (directory, graph) =
				self.directory_node_with_transaction(transaction, &directory)?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact =
							Self::artifact_id_from_directory_edge(child.directory, graph.as_ref())?;
						stack.push((artifact, graph.clone()));
					}
				},
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = Self::artifact_id_from_edge(edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
			}
		}
		Ok(entries)
	}

	fn directory_lookup_entry_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		directory: &tg::artifact::Id,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<Option<tg::artifact::Id>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let (directory_data, graph) =
				self.directory_node_with_transaction(transaction, &directory)?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Branch(branch) => {
					let Some(child) = branch
						.children
						.into_iter()
						.find(|child| name <= child.last.as_str())
					else {
						return Ok(None);
					};
					directory =
						Self::artifact_id_from_directory_edge(child.directory, graph.as_ref())?;
					default_graph = graph;
				},
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact = Self::artifact_id_from_edge(edge, graph.as_ref())?;
					return Ok(Some(artifact));
				},
			}
		}
	}

	fn blob_length_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::blob::Id,
	) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let Some(object) = self.try_get_object(transaction, &id)? else {
			return Err(fallback());
		};
		if let Some(cache_pointer) = object.cache_pointer {
			return Ok(cache_pointer.length);
		}
		let Some(bytes) = object.bytes else {
			return Err(fallback());
		};
		let data = tg::object::Data::deserialize(id.kind(), &*bytes).map_err(|error| {
			tracing::error!(error = %error.trace(), %id, "failed to deserialize the object data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let tg::object::Data::Blob(blob) = data else {
			tracing::error!(%id, "expected blob data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let length = match blob {
			tg::blob::Data::Branch(branch) => {
				branch.children.iter().map(|child| child.length).sum()
			},
			tg::blob::Data::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
		};
		Ok(length)
	}

	fn read_blob_range_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::blob::Id,
		position: u64,
		length: u64,
		output: &mut Vec<u8>,
	) -> std::io::Result<()> {
		if length == 0 {
			return Ok(());
		}
		let object_id: tg::object::Id = id.clone().into();
		let Some(object) = self.try_get_object(transaction, &object_id)? else {
			return Err(fallback());
		};

		// Read the blob from the object's bytes if it has them.
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
						self.read_blob_range_with_transaction(
							transaction,
							&child.blob,
							child_position,
							child_length,
							output,
						)?;
						remaining -= child_length;
						child_position = 0;
					}
				},
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
			}
			return Ok(());
		}

		// Otherwise, read the blob from the cache directory.
		let Some(cache_pointer) = object.cache_pointer else {
			return Err(fallback());
		};
		if position >= cache_pointer.length {
			return Ok(());
		}
		let read_length = std::cmp::min(length, cache_pointer.length - position);
		let mut path = self.cache_path.join(cache_pointer.artifact.to_string());
		if let Some(path_) = cache_pointer.path {
			path.push(path_);
		}
		let mut file = match std::fs::File::open(&path) {
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(fallback());
			},
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open the cache file");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			},
			Ok(file) => file,
		};
		file.seek(SeekFrom::Start(cache_pointer.position + position))
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

	fn graph_id_from_pointer(
		pointer: &tg::graph::data::Pointer,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::graph::Id> {
		pointer
			.graph
			.clone()
			.or_else(|| default_graph.cloned())
			.ok_or_else(|| {
				tracing::error!(pointer = ?pointer, "missing the pointer graph");
				fallback()
			})
	}

	fn artifact_id_from_edge(
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::artifact::Id> {
		match edge {
			tg::graph::data::Edge::Object(artifact) => Ok(artifact),
			tg::graph::data::Edge::Pointer(pointer) => {
				Self::artifact_id_from_pointer(&pointer, default_graph, None)
			},
		}
	}

	fn artifact_id_from_directory_edge(
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<tg::artifact::Id> {
		match edge {
			tg::graph::data::Edge::Object(directory) => Ok(directory.into()),
			tg::graph::data::Edge::Pointer(pointer) => Self::artifact_id_from_pointer(
				&pointer,
				default_graph,
				Some(tg::artifact::Kind::Directory),
			),
		}
	}

	fn artifact_id_from_pointer(
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
		let graph = Self::graph_id_from_pointer(pointer, default_graph)?;
		let pointer = tg::graph::data::Pointer {
			graph: Some(graph),
			index: pointer.index,
			kind: pointer.kind,
		};
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
}

impl Nodes {
	fn new() -> Self {
		let mut nodes = BTreeMap::new();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			Node {
				accessed: Instant::now(),
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

	fn lookup(&self, parent: u64, name: &str) -> Option<u64> {
		let mut state = self.state.lock().unwrap();
		let id = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())?;
		// Refresh the access time so a node handed to the kernel is not evicted before the driver records its lookup reference.
		if let Some(node) = state.nodes.get_mut(&id) {
			node.accessed = Instant::now();
		}
		Some(id)
	}

	fn lookup_and_remember(&self, parent: u64, name: &str) -> Option<u64> {
		let mut state = self.state.lock().unwrap();
		let id = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())?;
		let node = state.nodes.get_mut(&id)?;
		node.accessed = Instant::now();
		node.lookup_count = node.lookup_count.saturating_add(1);
		Some(id)
	}

	fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.state
			.lock()
			.unwrap()
			.nodes
			.get(&id)
			.map(|node| node.parent)
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))
	}

	fn get(&self, id: u64) -> std::io::Result<NodeInfo> {
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
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))
	}

	fn set_attrs(&self, id: u64, attrs: vfs::Attrs) {
		let mut state = self.state.lock().unwrap();
		if let Some(node) = state.nodes.get_mut(&id) {
			node.attrs = Some(attrs);
		}
	}

	fn remember(&self, id: u64) {
		if id == vfs::ROOT_NODE_ID {
			return;
		}
		let mut state = self.state.lock().unwrap();
		if let Some(node) = state.nodes.get_mut(&id) {
			node.lookup_count = node.lookup_count.saturating_add(1);
		}
	}

	fn forget(&self, id: u64, nlookup: u64) {
		if id == vfs::ROOT_NODE_ID || nlookup == 0 {
			return;
		}
		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&id) else {
			return;
		};
		node.lookup_count = node.lookup_count.saturating_sub(nlookup);
		if node.lookup_count == 0 && node.children.is_empty() {
			Self::prune(&mut state, id);
		}
	}

	fn prune(state: &mut State, mut id: u64) {
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

	fn evict_expired(&self, ttl: Duration) {
		let now = Instant::now();
		let mut state = self.state.lock().unwrap();
		// Collect the cache-only leaf nodes whose time to live has elapsed; a node with a nonzero lookup count is still referenced and a node with children is not a leaf, so neither is eligible.
		let expired = state
			.nodes
			.iter()
			.filter_map(|(&id, node)| {
				let evict = id != vfs::ROOT_NODE_ID
					&& node.lookup_count == 0
					&& node.children.is_empty()
					&& now.duration_since(node.accessed) >= ttl;
				evict.then_some((id, node.parent, node.name.clone()))
			})
			.collect::<Vec<_>>();
		// Remove only the collected nodes; eviction is not cascaded into a parent, which may have just been handed to the kernel, so an emptied parent is collected by a later sweep once its own time to live has elapsed.
		for (id, parent, name) in expired {
			state.nodes.remove(&id);
			if let Some(name) = name
				&& let Some(parent_node) = state.nodes.get_mut(&parent)
			{
				parent_node.children.remove(&name);
			}
		}
	}

	fn get_or_insert_child(
		&self,
		parent: u64,
		name: &str,
		artifact: tg::artifact::Id,
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
			// Refresh the access time so an actively enumerated entry is not evicted.
			if let Some(node) = state.nodes.get_mut(&id) {
				node.accessed = Instant::now();
				if remember {
					node.lookup_count = node.lookup_count.saturating_add(1);
				}
			}
			return Ok(id);
		}
		if !state.nodes.contains_key(&parent) {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		let id = state.next;
		state.next += 1;
		state.nodes.insert(
			id,
			Node {
				accessed: Instant::now(),
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

impl ReaddirPlusPage {
	const FUSE_DIRENT_PLUS_HEADER_SIZE: usize = 152;

	fn push(
		&mut self,
		entries: &mut Vec<(String, u64, vfs::Attrs)>,
		name: String,
		id: u64,
		attrs: vfs::Attrs,
	) -> bool {
		if self.position < self.offset {
			self.position += 1;
			return false;
		}
		let entry_size = Self::entry_size(name.len());
		if self.size + entry_size > self.length {
			return true;
		}
		self.position += 1;
		self.size += entry_size;
		entries.push((name, id, attrs));
		false
	}

	fn entry_size(name_len: usize) -> usize {
		let padding = (8 - (Self::FUSE_DIRENT_PLUS_HEADER_SIZE + name_len) % 8) % 8;
		Self::FUSE_DIRENT_PLUS_HEADER_SIZE + name_len + padding
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			data_directory: None,
			node_eviction_interval: DEFAULT_NODE_EVICTION_INTERVAL,
			node_ttl: DEFAULT_NODE_TTL,
			object_store_map_size: DEFAULT_OBJECT_STORE_MAP_SIZE,
			object_store_path: PathBuf::from(DEFAULT_OBJECT_STORE_PATH),
			object_store_posix_sem_prefix: None,
		}
	}
}

impl vfs::Provider for Inner {
	fn handle_batch(
		&self,
		requests: Vec<vfs::Request>,
	) -> impl std::future::Future<Output = Vec<std::io::Result<vfs::Response>>> + Send {
		Inner::handle_batch(self, requests)
	}

	fn handle_batch_sync(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		// Serve operations without asynchronous I/O and return `ENOSYS` for the caller to retry on the asynchronous path.
		requests
			.into_iter()
			.map(|request| match request {
				vfs::Request::Close { handle } => {
					self.close(handle);
					Ok(vfs::Response::Unit)
				},
				vfs::Request::Forget { id, nlookup } => {
					self.nodes.forget(id, nlookup);
					Ok(vfs::Response::Unit)
				},
				vfs::Request::LookupParent { id } => self
					.nodes
					.lookup_parent(id)
					.map(|id| vfs::Response::LookupParent { id }),
				vfs::Request::Remember { id } => {
					self.nodes.remember(id);
					Ok(vfs::Response::Unit)
				},
				_ => Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
			})
			.collect()
	}
}

fn attrs_from_artifact(artifact: Option<&tg::artifact::Id>) -> Option<vfs::Attrs> {
	match artifact {
		Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Directory) => {
			Some(vfs::Attrs::new(vfs::AttrsInner::Directory))
		},
		// A file's size is not known without resolving the artifact, so leave its attributes to be computed on demand.
		Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::File) => None,
		// A symlink's target length is not known without resolving the artifact, so leave its attributes to be computed on demand.
		Some(artifact) if matches!(artifact.kind(), tg::artifact::Kind::Symlink) => None,
		None => Some(vfs::Attrs::new(vfs::AttrsInner::Directory)),
		_ => None,
	}
}

fn readdir_entry_size(name_len: usize) -> usize {
	let padding = (8 - (FUSE_DIRENT_HEADER_SIZE + name_len) % 8) % 8;
	FUSE_DIRENT_HEADER_SIZE + name_len + padding
}

fn render_symlink(
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
		return Err(std::io::Error::from_raw_os_error(libc::EIO));
	}
	Ok(target.as_os_str().as_bytes().to_vec().into())
}

fn init_logging() {
	static ONCE: std::sync::Once = std::sync::Once::new();
	ONCE.call_once(|| {
		let filter = tracing_subscriber::EnvFilter::new(
			"warn,tangram_vfs_provider=debug,tangram_client=debug,hyper=debug",
		);
		let layer = tracing_oslog::OsLogger::new("tangram.fskit", "provider");
		let _ = tracing_subscriber::registry()
			.with(filter)
			.with(layer)
			.try_init();
	});
}

/// Creates the error the fast path returns when it cannot serve a request, because the object is not in the local store or is not cached. The provider falls back to the client, which fetches from a remote if necessary. This matches the sync path of the server's provider, which the fuse server retries asynchronously on `ENOSYS` alone.
fn fallback() -> std::io::Error {
	std::io::Error::from_raw_os_error(libc::ENOSYS)
}

/// Returns whether an error from the fast path means the provider should fall back to the client. Any other error is returned to the caller, so that a corrupt object is not masked by a request to the server.
fn is_fallback(error: &std::io::Error) -> bool {
	error.raw_os_error() == Some(libc::ENOSYS)
}

fn eio(error: impl std::fmt::Debug) -> std::io::Error {
	tracing::error!("{error:?}");
	std::io::Error::from_raw_os_error(libc::EIO)
}
