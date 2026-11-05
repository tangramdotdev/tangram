use {
	authorization::{Authority, Authorizations},
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
		time::{Duration, Instant, SystemTime, UNIX_EPOCH},
	},
	tangram_cache as cache,
	tangram_client::prelude::*,
	tangram_uri::Uri,
	tangram_vfs as vfs,
	tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _},
};

mod authorization;
mod client;

/// The name of the checkouts directory within the data directory. The server uses this name only while a vfs is mounted, which is the only circumstance in which this provider runs.
const CHECKOUTS_DIRECTORY_NAME: &str = "checkouts";

/// The default interval at which expired cache-only nodes are swept.
const DEFAULT_NODE_EVICTION_INTERVAL: Duration = Duration::from_secs(30);

/// The default duration a cache-only node created by directory enumeration is retained after its most recent access before it becomes eligible for eviction.
const DEFAULT_NODE_TTL: Duration = Duration::from_mins(1);

/// The default map size with which the cache is opened. It is the server's default, and the server sends its own if it is configured with another.
const DEFAULT_CACHE_MAP_SIZE: usize = 1_099_511_627_776;

/// The default path of the cache within the data directory. It is the server's default, and the server sends its own if it is configured with another.
const DEFAULT_CACHE_PATH: &str = "cache.lmdb";

/// The size of a FUSE directory entry header.
const FUSE_DIRENT_HEADER_SIZE: usize = 24;

/// The configuration for a provider.
#[derive(Clone)]
pub struct Config {
	/// The backend used by the local cache.
	pub cache_kind: CacheKind,

	/// The map size with which to open the cache. LMDB requires a reader to use a map size at least as large as the writer's, so this must be at least the server's.
	pub cache_map_size: usize,

	/// The path of the cache, which is joined to the data directory.
	pub cache_path: PathBuf,

	/// An optional base name for the cache's POSIX lock semaphores. It must match the name the server opens the cache with so that the sandboxed provider and the server share the same lock.
	pub cache_posix_sem_prefix: Option<String>,

	/// The server's data directory, which the fast path reads the cache and the checkouts directory from. If it is `None`, then the provider uses the client for every request.
	pub data_directory: Option<PathBuf>,

	/// The interval at which expired cache-only nodes are swept.
	pub node_eviction_interval: Duration,

	/// The duration a cache-only node created by directory enumeration is retained after its most recent access before it becomes eligible for eviction.
	pub node_ttl: Duration,

	/// The principal the mount serves. When it is `None` or the root principal, the mount is unenforced and every artifact is accessible. Otherwise the provider authorizes access to each artifact subtree.
	pub principal: Option<tg::Principal>,

	/// The authorization tokens the mount holds, which authorize access to their artifacts and, by extension, their subtrees without consulting the server.
	pub tokens: Vec<tg::authorization::Token>,
}

#[derive(Clone, Copy, Default)]
pub enum CacheKind {
	Fjall,
	#[default]
	Lmdb,
	Rocksdb,
}

pub struct Provider {
	inner: Arc<Inner>,
	runtime: tokio::runtime::Runtime,
}

struct Inner {
	authorizations: Mutex<Authorizations>,

	client: tg::Client,

	directory_handles: Mutex<BTreeMap<u64, u64>>,

	fast: Option<Fast>,

	file_handles: Mutex<BTreeMap<u64, FileHandle>>,

	handle_count: AtomicU64,

	nodes: Nodes,

	principal: Option<tg::Principal>,

	#[cfg(target_os = "macos")]
	proxy: Option<PathBuf>,
}

/// The state the fast path requires. It reads the cache and the checkouts directory directly instead of sending a request to the server.
struct Fast {
	cache: Cache,
	checkout_path: PathBuf,
	secondary_path: Option<tangram_util::fs::Temp>,
}

enum Cache {
	Lmdb(tangram_cache_lmdb::Cache),
	#[cfg(feature = "rocksdb")]
	Rocksdb(tangram_cache_rocksdb::Cache),
}

enum Transaction<'a> {
	Lmdb(lmdb::RoTxn<'a, lmdb::WithTls>),
	#[cfg(feature = "rocksdb")]
	Rocksdb(tangram_cache_rocksdb::transaction::Transaction<'a>),
}

struct FileHandle {
	blob: tg::blob::Id,
	file: ArtifactState,
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
	artifact: Option<ArtifactState>,
	attrs: Option<vfs::Attrs>,
	children: BTreeMap<String, u64>,
	dependencies: Vec<ArtifactState>,
	depth: u64,
	lookup_count: u64,
	name: Option<String>,
	named: Option<NamedNodeInfo>,
	parent: u64,
}

#[derive(Clone)]
struct NodeInfo {
	artifact: Option<ArtifactState>,
	attrs: Option<vfs::Attrs>,
	depth: u64,
	named: Option<NamedNodeInfo>,
	parent: u64,
}

#[derive(Clone)]
struct ArtifactState {
	authority: Arc<Authority>,
	data: Option<tg::artifact::data::Artifact>,
	id: tg::artifact::Id,
}

#[derive(Clone)]
struct NamedNodeInfo {
	specifier: tg::Specifier,
	suffix: Option<String>,
	target: Option<tg::Referent<tg::Id>>,
}

struct NamedNodeChild {
	name: tg::specifier::Component,
	target: Option<tg::Referent<tg::Id>>,
}

enum NamedNodeEntry {
	Directory,
	Symlink(tg::Referent<tg::Id>),
}

struct ReaddirPlusPage {
	length: usize,
	offset: usize,
	position: usize,
	size: usize,
}

impl Provider {
	pub fn new(socket: &str, config: &Config) -> std::io::Result<Self> {
		#[cfg(target_os = "macos")]
		let (socket, proxy) = socket
			.strip_prefix("proxy:")
			.map_or((socket, None), |socket| {
				(socket, Some(PathBuf::from(socket)))
			});

		// Initialize the logging and runtime.
		init_logging();
		let mut runtime = tokio::runtime::Builder::new_multi_thread();
		#[cfg(target_os = "macos")]
		if proxy.is_some() {
			runtime.worker_threads(2);
		}
		let runtime = runtime.enable_all().build()?;

		// Create the client URL with the builder so socket paths are not mangled by percent-encoding round trips.
		let url = Uri::builder()
			.scheme("http+unix")
			.authority(socket)
			.path("")
			.build()
			.map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
		let arg = tg::Arg {
			url: Some(url),
			..Default::default()
		};
		// Connect lazily so mounting does not depend on the server already serving on its socket.
		let client = tg::Client::new(arg).map_err(std::io::Error::other)?;

		// Initialize the provider state.
		let data_directory = config.data_directory.as_deref();
		#[cfg(target_os = "macos")]
		let data_directory = data_directory.filter(|_| proxy.is_none());
		let fast = data_directory.and_then(|data_directory| Fast::new(data_directory, config));

		// Retain the mount's authorization proofs.
		let mut authorizations = Authorizations::default();
		for token in &config.tokens {
			let tokens = tg::authorization::Tokens::with_authorization(vec![token.clone()]);
			authorizations.insert(&token.body.resource, &tokens, true);
		}
		let inner = Inner {
			authorizations: Mutex::new(authorizations),

			client,

			directory_handles: Mutex::new(BTreeMap::new()),

			fast,

			file_handles: Mutex::new(BTreeMap::new()),

			handle_count: AtomicU64::new(1000),

			nodes: Nodes::new(),

			principal: config.principal.clone(),

			#[cfg(target_os = "macos")]
			proxy,
		};
		let inner = Arc::new(inner);

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
		#[cfg(target_os = "macos")]
		if let Some(path) = &self.proxy {
			let result = async {
				let stream = tokio::net::UnixStream::connect(path).await?;
				vfs::proxy::request(stream, &requests).await
			}
			.await;
			return proxy_responses(result, requests.len());
		}

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
				vfs::Request::Lookup { id, name } => {
					let immutable = self.nodes.is_immutable(id);
					self.lookup(id, &name)
						.await
						.map(|id| vfs::Response::Lookup {
							attrs: None,
							id,
							immutable,
						})
				},
				vfs::Request::LookupAndRemember { id, name } => {
					let immutable = self.nodes.is_immutable(id);
					self.lookup_and_remember(id, &name).await.map(|entry| {
						let (id, attrs) = entry.unzip();
						vfs::Response::Lookup {
							attrs,
							id,
							immutable,
						}
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
					.map(|(handle, immutable)| vfs::Response::OpenDir { handle, immutable }),
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
		let node = self.get(id).await?;
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
		artifact: Option<&ArtifactState>,
		depth: u64,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(artifact) = artifact
			&& !self.authorize(artifact).await
		{
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		if let Some(artifact) = artifact
			&& matches!(artifact.id.kind(), tg::artifact::Kind::Symlink)
		{
			let target = self.resolve_symlink(artifact, depth).await?.0;
			let size = target.len().to_u64().unwrap();
			return Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size }));
		}
		self.compute_attrs_with_fallback(artifact).await
	}

	/// Computes the attributes with the fast path, falling back to the client if the fast path is unavailable.
	async fn compute_attrs_with_fallback(
		&self,
		artifact: Option<&ArtifactState>,
	) -> std::io::Result<vfs::Attrs> {
		if let Some(fast) = &self.fast
			&& self
				.principal
				.as_ref()
				.is_none_or(|principal| matches!(principal, tg::Principal::Root))
		{
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
		artifact: &ArtifactState,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let entries = 'a: {
			if let Some(fast) = &self.fast {
				match fast.directory_entries(artifact) {
					Err(error) if is_fallback(&error) => (),
					result => break 'a result?,
				}
			}
			self.directory_entries(artifact).await?
		};
		Ok(entries)
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

		// Resolve root entries and named nodes using the flat store namespace.
		let parent_node = self.get(parent).await?;
		if parent == vfs::ROOT_NODE_ID {
			match tg::store::path::parse_component(name) {
				Ok(tg::store::path::Component::Id { id, .. }) => {
					let artifact = self.nodes.root_artifact(&id);
					if !self.authorize(&artifact).await {
						return Ok(None);
					}
					let attrs = attrs_from_artifact(Some(&id));
					let id = self
						.nodes
						.get_or_insert_child(parent, name, artifact, 1, attrs, remember)?;
					return Ok(Some(id));
				},
				Ok(tg::store::path::Component::Tag { component, suffix }) => {
					return self
						.lookup_named_node(parent, name, None, component, suffix, remember)
						.await;
				},
				Err(_) => return Ok(None),
			}
		}
		if let Some(named_node) = &parent_node.named {
			if named_node.target.is_some() {
				return Ok(None);
			}
			let Ok(tg::store::path::Component::Tag { component, suffix }) =
				tg::store::path::parse_component(name)
			else {
				return Ok(None);
			};
			return self
				.lookup_named_node(
					parent,
					name,
					Some(&named_node.specifier),
					component,
					suffix,
					remember,
				)
				.await;
		}

		// Look up an existing artifact node.
		let id = if remember {
			self.nodes.lookup_and_remember(parent, name)
		} else {
			self.nodes.lookup(parent, name)
		};
		if let Some(id) = id {
			return Ok(Some(id));
		}

		let Some(artifact) = parent_node.artifact else {
			return Ok(None);
		};
		if !matches!(artifact.id.kind(), tg::artifact::Kind::Directory) {
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
			entries.get(name).cloned()
		};
		let Some(artifact) = artifact else {
			return Ok(None);
		};
		let depth = parent_node.depth + 1;

		// Insert the resolved node.
		let attrs = attrs_from_artifact(Some(&artifact.id));
		let id = self
			.nodes
			.get_or_insert_child(parent, name, artifact, depth, attrs, remember)?;

		Ok(Some(id))
	}

	async fn lookup_named_node(
		&self,
		parent: u64,
		name: &str,
		parent_specifier: Option<&tg::Specifier>,
		component: tg::specifier::Component,
		suffix: Option<&str>,
		remember: bool,
	) -> std::io::Result<Option<u64>> {
		let specifier = match parent_specifier {
			Some(parent) => format!("{parent}/{component}"),
			None => component.to_string(),
		};
		let specifier: tg::Specifier = specifier.parse().map_err(|error| {
			std::io::Error::other(tg::error!(!error, "invalid named node specifier"))
		})?;
		let Some(entry) = self.get_named_node_entry(&specifier).await? else {
			return Ok(None);
		};
		let depth = self.get(parent).await?.depth + 1;
		let (attrs, target) = match entry {
			NamedNodeEntry::Directory if suffix.is_none() => (
				vfs::Attrs::new(vfs::AttrsInner::Directory).cacheable(false),
				None,
			),
			NamedNodeEntry::Directory => return Ok(None),
			NamedNodeEntry::Symlink(target) => {
				self.remember_artifact_tag_target_token(&target);
				let target_path = render_tag(depth, &target.node, suffix);
				let size = target_path.len().to_u64().unwrap();
				(
					vfs::Attrs::new(vfs::AttrsInner::Symlink { size }).cacheable(false),
					Some(target),
				)
			},
		};
		let named_node = NamedNodeInfo {
			specifier,
			suffix: suffix.map(ToOwned::to_owned),
			target,
		};
		let id = self
			.nodes
			.get_or_insert_named_node_child(parent, name, depth, attrs, named_node, remember)?;
		Ok(Some(id))
	}

	async fn get_named_node_entry(
		&self,
		specifier: &tg::Specifier,
	) -> std::io::Result<Option<NamedNodeEntry>> {
		let location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let pattern = specifier
			.to_string()
			.parse()
			.map_err(std::io::Error::other)?;
		let entry = tg::reference::Options {
			location: location.clone(),
			..Default::default()
		};
		let reference =
			tg::Reference::with_node_and_options(tg::reference::Node::Specifier(pattern), entry);
		let output = self.try_get_reference(&reference).await?;
		let Some(output) = output else {
			return Ok(None);
		};
		let tg::get::Node::Id(id) = output.node else {
			return Ok(None);
		};
		if id.kind() != tg::id::Kind::Tag {
			return Ok(matches!(
				id.kind(),
				tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User
			)
			.then_some(NamedNodeEntry::Directory));
		}

		let id = tg::tag::Id::try_from(id).map_err(std::io::Error::other)?;
		let selector = tg::tag::Selector::Id(id);
		let arg = tg::tag::get::Arg {
			location,
			tokens: output.options.tokens,
			..Default::default()
		};
		let session = self.client.session(self.client.context());
		let output = session.try_get_tag(&selector, arg).await.map_err(eio)?;
		let Some(output) = output else {
			return Ok(None);
		};
		let node = match output.data.target {
			tg::tag::data::Target::Object(target) => target.into(),
			tg::tag::data::Target::Process(target) => target.into(),
		};
		let options = tg::referent::Options {
			location: output.location,
			tokens: output.tokens,
			..Default::default()
		};
		let target = tg::Referent::new(node, options);

		Ok(Some(NamedNodeEntry::Symlink(target)))
	}

	async fn list_named_node_children(
		&self,
		parent: Option<tg::Specifier>,
		position: u64,
		length: u64,
	) -> std::io::Result<Vec<NamedNodeChild>> {
		let session = self.client.session(self.client.context());
		let root = parent.is_none();
		let location = tg::Location::Local(tg::location::Local::default());
		let node = if let Some(parent) = parent {
			let options = tg::reference::Options {
				location: Some(location.clone().into()),
				..tg::reference::Options::default()
			};
			let reference = tg::Reference::with_node_and_options(
				tg::reference::Node::Specifier(parent.into()),
				options,
			);
			let node = self
				.try_get_reference(&reference)
				.await?
				.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
			let node = node
				.try_map(|node| match node {
					tg::get::Node::Id(id) => Ok(id),
					tg::get::Node::Pointer(_) => Err(tg::error!("the list node must be an ID")),
				})
				.map_err(eio)?;
			Some(node)
		} else {
			None
		};
		let arg = tg::list::Arg {
			cached: false,
			cursor: None,
			groups: true,
			limit: None,
			location: Some(location.into()),
			node,
			organizations: root,
			recursive: false,
			reverse: false,
			tags: true,
			ttl: tg::remote::cache::Ttl::default(),
			users: root,
		};
		let output = session.list_all(arg).await.map_err(eio)?;
		let children = output
			.data
			.into_iter()
			.skip(usize::try_from(position).unwrap_or(usize::MAX))
			.take(usize::try_from(length).unwrap_or(usize::MAX))
			.filter_map(|entry| {
				let name = entry.name().parse().ok()?;
				let target = entry.target.map(|target| {
					let tg::Referent { node, options } = target;
					let node = match node {
						tg::Either::Left(target) => target.into(),
						tg::Either::Right(target) => target.into(),
					};
					tg::Referent::new(node, options)
				});
				Some(NamedNodeChild { name, target })
			})
			.collect();

		Ok(children)
	}

	fn remember_artifact_tag_target_token(&self, target: &tg::Referent<tg::Id>) {
		self.authorizations
			.lock()
			.unwrap()
			.insert(&target.node, &target.options.tokens, true);
	}

	async fn authorize(&self, artifact: &ArtifactState) -> bool {
		let id = match &artifact.data {
			Some(
				tg::artifact::data::Artifact::Directory(tg::directory::Data::Pointer(pointer))
				| tg::artifact::data::Artifact::File(tg::file::Data::Pointer(pointer))
				| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Pointer(pointer)),
			) => pointer.graph.clone().into(),
			_ => artifact.id.clone().into(),
		};
		self.authorize_object(&artifact.authority, &id).await
	}

	async fn authorize_object(&self, authority: &Arc<Authority>, id: &tg::object::Id) -> bool {
		if self
			.principal
			.as_ref()
			.is_none_or(|principal| matches!(principal, tg::Principal::Root))
		{
			return true;
		}
		if self
			.authorizations
			.lock()
			.unwrap()
			.authorized(authority, id)
		{
			return true;
		}
		if self.tokens(authority, id).is_empty() {
			return false;
		}
		self.get_object(authority, id).await.is_ok()
	}

	async fn get(&self, id: u64) -> std::io::Result<NodeInfo> {
		let node = self.nodes.get(id)?;
		if let Some(artifact) = &node.artifact
			&& !self.authorize(artifact).await
		{
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		Ok(node)
	}

	async fn open(&self, id: u64) -> std::io::Result<u64> {
		// Resolve the file contents.
		let NodeInfo { artifact, .. } = self.get(id).await?;
		let Some(artifact) = artifact else {
			return Err(std::io::Error::other("expected a file"));
		};
		let file = artifact.clone();
		let (data, graph) = 'a: {
			if let Some(fast) = &self.fast {
				let result = fast.transaction().and_then(|transaction| {
					fast.file_node_with_transaction(&transaction, &artifact)
				});
				match result {
					Err(error) if is_fallback(&error) => (),
					result => break 'a result?,
				}
			}
			self.file_node(&artifact).await?
		};
		let (_, dependencies) = self
			.file_dependency_references(&artifact, &data, graph.as_ref())
			.await?;
		for dependency in dependencies {
			self.nodes.insert_dependency(id, &dependency);
		}
		let blob = data
			.contents
			.ok_or_else(|| std::io::Error::other("the file has no contents"))?;

		// Create a file handle.
		let handle = self.handle_count.fetch_add(1, Ordering::Relaxed);
		let file_handle = FileHandle { blob, file };
		self.file_handles
			.lock()
			.unwrap()
			.insert(handle, file_handle);

		Ok(handle)
	}

	async fn opendir(&self, id: u64) -> std::io::Result<(u64, bool)> {
		let NodeInfo { artifact, .. } = self.get(id).await?;
		let immutable = artifact.is_some();
		if let Some(artifact) = &artifact
			&& !matches!(artifact.id.kind(), tg::artifact::Kind::Directory)
		{
			return Err(std::io::Error::other("expected a directory"));
		}

		let handle = self.handle_count.fetch_add(1, Ordering::Relaxed);
		self.nodes.remember_existing(id)?;
		self.directory_handles.lock().unwrap().insert(handle, id);

		Ok((handle, immutable))
	}

	async fn read(&self, handle: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		// Resolve the file handle.
		let (blob, file) = {
			let file_handles = self.file_handles.lock().unwrap();
			let Some(file_handle) = file_handles.get(&handle) else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			(file_handle.blob.clone(), file_handle.file.clone())
		};

		if !self.authorize(&file).await {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		if !self
			.authorize_object(&file.authority, &blob.clone().into())
			.await
		{
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}

		// Try the fast path.
		if let Some(fast) = &self.fast {
			match fast.read(&blob, position, length) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}

		// Read through the client.
		let arg = tg::read::Arg {
			blob: blob.clone(),
			options: tg::read::Options {
				length: Some(length),
				position: Some(SeekFrom::Start(position)),
				size: None,
			},
			tokens: self.tokens(&file.authority, &blob.into()),
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
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let id = self
			.directory_handles
			.lock()
			.unwrap()
			.get(&handle)
			.copied()
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

		// Resolve the directory.
		let NodeInfo {
			artifact,
			named: named_node,
			parent,
			..
		} = self.get(id).await?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => return Err(std::io::Error::other("expected a directory")),
		};
		// Collect the directory entries.
		let mut entries = Vec::new();
		entries.push((".".to_owned(), id, vfs::EntryKind::Directory));
		entries.push(("..".to_owned(), parent, vfs::EntryKind::Directory));
		let mut page_offset = offset;
		if let Some(directory) = directory {
			let children = self.directory_entry_ids(&directory).await?;
			for (name, artifact) in children {
				let kind = match artifact.id.kind() {
					tg::artifact::Kind::Directory => vfs::EntryKind::Directory,
					tg::artifact::Kind::File => vfs::EntryKind::File,
					tg::artifact::Kind::Symlink => vfs::EntryKind::Symlink,
				};
				entries.push((name, 0, kind));
			}
		} else if id == vfs::ROOT_NODE_ID
			|| named_node
				.as_ref()
				.is_some_and(|named_node| named_node.target.is_none())
		{
			let specifier = named_node.map(|named_node| named_node.specifier);
			let position = offset.saturating_sub(2);
			let limit = length / FUSE_DIRENT_HEADER_SIZE.to_u64().unwrap();
			let children = self
				.list_named_node_children(specifier, position, limit)
				.await?;
			for child in &children {
				if let Some(target) = &child.target {
					self.remember_artifact_tag_target_token(target);
				}
			}
			entries.extend(children.into_iter().map(|child| {
				let kind = if child.target.is_some() {
					vfs::EntryKind::Symlink
				} else {
					vfs::EntryKind::Directory
				};
				(child.name.to_string(), 0, kind)
			}));
			page_offset = offset.min(2);
		} else {
			return Err(std::io::Error::other("expected a directory"));
		}

		// Select the requested page.
		let offset = page_offset.to_usize().unwrap_or(usize::MAX);
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
		handle: u64,
		offset: u64,
		length: u64,
	) -> std::io::Result<Vec<(String, u64, vfs::Attrs)>> {
		let id = self
			.directory_handles
			.lock()
			.unwrap()
			.get(&handle)
			.copied()
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

		// Resolve the directory.
		let NodeInfo {
			artifact,
			depth,
			named: named_node,
			parent,
			..
		} = self.get(id).await?;
		let directory = match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Some(artifact)
			},
			None => None,
			Some(_) => return Err(std::io::Error::other("expected a directory")),
		};
		// Initialize the requested page.
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
		if let Some(directory) = directory {
			let children = self.directory_entry_ids(&directory).await?;
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
		} else if id == vfs::ROOT_NODE_ID
			|| named_node
				.as_ref()
				.is_some_and(|named_node| named_node.target.is_none())
		{
			let parent_specifier = named_node.map(|named_node| named_node.specifier);
			let position = page.offset.saturating_sub(page.position).to_u64().unwrap();
			let length = page
				.length
				.saturating_sub(page.size)
				.checked_div(ReaddirPlusPage::FUSE_DIRENT_PLUS_HEADER_SIZE)
				.unwrap()
				.to_u64()
				.unwrap();
			let children = self
				.list_named_node_children(parent_specifier.clone(), position, length)
				.await?;
			page.position = page.position.max(page.offset);
			for child in children {
				let name = child.name.to_string();
				if ReaddirPlusPage::entry_size(name.len()) + page.size > page.length {
					break;
				}
				let specifier = match &parent_specifier {
					Some(parent) => format!("{parent}/{}", child.name),
					None => child.name.to_string(),
				}
				.parse()
				.map_err(std::io::Error::other)?;
				let target = child.target;
				let attrs = if let Some(target) = &target {
					self.remember_artifact_tag_target_token(target);
					let target_path = render_tag(depth + 1, &target.node, None);
					let size = target_path.len().to_u64().unwrap();
					vfs::Attrs::new(vfs::AttrsInner::Symlink { size }).cacheable(false)
				} else {
					vfs::Attrs::new(vfs::AttrsInner::Directory).cacheable(false)
				};
				let named_node = NamedNodeInfo {
					specifier,
					suffix: None,
					target,
				};
				let node = self.nodes.get_or_insert_named_node_child(
					id,
					&name,
					depth + 1,
					attrs,
					named_node,
					false,
				)?;
				page.size += ReaddirPlusPage::entry_size(name.len());
				page.position += 1;
				entries.push((name, node, attrs));
			}
		} else {
			return Err(std::io::Error::other("expected a directory"));
		}

		Ok(entries)
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		let NodeInfo {
			artifact,
			depth,
			named: named_node,
			..
		} = self.get(id).await?;
		if let Some(named_node) = named_node {
			let Some(NamedNodeEntry::Symlink(target)) =
				self.get_named_node_entry(&named_node.specifier).await?
			else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			self.remember_artifact_tag_target_token(&target);
			return Ok(render_tag(
				depth,
				&target.node,
				named_node.suffix.as_deref(),
			));
		}
		let Some(artifact) = artifact else {
			return Err(std::io::Error::other("expected a symlink"));
		};
		let (target, dependency) = self.resolve_symlink(&artifact, depth).await?;
		if let Some(dependency) = dependency {
			self.nodes.insert_dependency(id, &dependency);
		}
		Ok(target)
	}

	/// Renders a symlink's target with the fast path, falling back to the client if the fast path is unavailable.
	async fn resolve_symlink(
		&self,
		artifact: &ArtifactState,
		depth: u64,
	) -> std::io::Result<(Bytes, Option<ArtifactState>)> {
		let (symlink, graph) = 'a: {
			if let Some(fast) = &self.fast {
				let result = fast.transaction().and_then(|transaction| {
					fast.symlink_node_with_transaction(&transaction, artifact)
				});
				match result {
					Err(error) if is_fallback(&error) => (),
					result => break 'a result?,
				}
			}
			self.symlink_node(artifact).await?
		};
		let target = match symlink.artifact {
			Some(edge) => Some(
				self.artifact_from_edge(&artifact.authority, edge, graph.as_ref())
					.await?,
			),
			None => None,
		};
		let path = render_symlink(
			depth,
			target.as_ref().map(|target| target.id.clone()),
			symlink.path,
		)?;
		Ok((path, target))
	}

	async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let NodeInfo { artifact, .. } = self.get(id).await?;
		let Some(artifact) =
			artifact.filter(|artifact| artifact.id.kind() == tg::artifact::Kind::File)
		else {
			return Ok(Vec::new());
		};
		let (file, graph) = self.file_node(&artifact).await?;
		let (references, dependencies) = self
			.file_dependency_references(&artifact, &file, graph.as_ref())
			.await?;
		for dependency in dependencies {
			self.nodes.insert_dependency(id, &dependency);
		}
		let mut names =
			tg::file::xattrs::encode_dependencies(&references, tg::file::xattrs::MAX_VALUE_SIZE)
				.map_err(eio)?
				.into_iter()
				.map(|xattr| xattr.name)
				.collect::<Vec<_>>();
		if file.module.is_some() {
			names.push(tg::file::xattrs::MODULE_NAME.to_owned());
		}
		if self.file_token(&artifact).is_some() {
			names.push(tg::file::xattrs::TOKEN_NAME.to_owned());
		}
		Ok(names)
	}

	async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<Bytes>> {
		let NodeInfo { artifact, .. } = self.get(id).await?;
		let Some(artifact) =
			artifact.filter(|artifact| artifact.id.kind() == tg::artifact::Kind::File)
		else {
			return Ok(None);
		};
		if let Some(fast) = &self.fast {
			match fast.getxattr(&artifact, name) {
				Err(error) if is_fallback(&error) => (),
				result => return result,
			}
		}
		let (file, graph) = self.file_node(&artifact).await?;
		if name == tg::file::xattrs::TOKEN_NAME {
			return Ok(self.file_token(&artifact).map(Bytes::from));
		}
		if tg::file::xattrs::is_dependencies_name(name) {
			let (references, dependencies) = self
				.file_dependency_references(&artifact, &file, graph.as_ref())
				.await?;
			for dependency in dependencies {
				self.nodes.insert_dependency(id, &dependency);
			}
			let xattrs = tg::file::xattrs::encode_dependencies(
				&references,
				tg::file::xattrs::MAX_VALUE_SIZE,
			)
			.map_err(eio)?;
			return Ok(xattrs
				.into_iter()
				.find_map(|xattr| (xattr.name == name).then_some(xattr.value)));
		}
		if name == tg::file::xattrs::MODULE_NAME {
			return Ok(file.module.map(|module| Bytes::from(module.to_string())));
		}
		Ok(None)
	}

	fn file_token(&self, artifact: &ArtifactState) -> Option<String> {
		let resource = tg::Id::from(artifact.id.clone());
		self.tokens(&artifact.authority, &artifact.id.clone().into())
			.local_authorization()
			.iter()
			.filter(|token| token.authorizes_object_subtree(&resource))
			.max_by_key(|token| token.body.expires_at)
			.map(ToString::to_string)
	}

	async fn file_dependency_references(
		&self,
		artifact: &ArtifactState,
		file: &tg::graph::data::File,
		graph: Option<&tg::graph::Id>,
	) -> std::io::Result<(Vec<tg::Reference>, Vec<ArtifactState>)> {
		let mut artifacts = Vec::new();
		let mut references = Vec::with_capacity(file.dependencies.len());
		for (reference, dependency) in &file.dependencies {
			let mut reference = reference.clone();
			if let Some(dependency) = dependency {
				let mut options = reference.options().clone();
				options.tokens.inherit(&dependency.0.options.tokens);
				let object = match &dependency.0.node {
					Some(tg::graph::data::Edge::Index(index)) => Some(
						self.artifact_from_index(&artifact.authority, *index, graph, None)
							.await?,
					),
					Some(tg::graph::data::Edge::Object(object)) => {
						if let Ok(id) = tg::artifact::Id::try_from(object.clone()) {
							Some(ArtifactState::new(id, Some(&artifact.authority)))
						} else {
							options
								.tokens
								.inherit(&self.tokens(&artifact.authority, object));
							options.tokens.normalize(Some(&object.clone().into()));
							None
						}
					},
					Some(tg::graph::data::Edge::Pointer(pointer)) => Some(
						client::artifact_from_pointer(&artifact.authority, pointer, None)?,
					),
					None => None,
				};
				if let Some(dependency) = object {
					let object: tg::object::Id = dependency.id.clone().into();
					self.authorizations.lock().unwrap().insert(
						&object.clone().into(),
						&options.tokens,
						false,
					);
					options
						.tokens
						.inherit(&self.tokens(&dependency.authority, &object));
					options.tokens.normalize(Some(&object.into()));
					artifacts.push(dependency);
				}
				reference.set_options(options);
			}
			references.push(reference);
		}
		Ok((references, artifacts))
	}

	fn close(&self, handle: u64) {
		self.file_handles.lock().unwrap().remove(&handle);
		if let Some(id) = self.directory_handles.lock().unwrap().remove(&handle) {
			self.nodes.forget(id, 1);
		}
	}

	async fn compute_attrs(&self, artifact: Option<&ArtifactState>) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if artifact.id.kind() == tg::artifact::Kind::Directory => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if artifact.id.kind() == tg::artifact::Kind::File => {
				let (file, _) = self.file_node(artifact).await?;
				let size = if let Some(blob) = &file.contents {
					self.blob_length(&artifact.authority, blob).await?
				} else {
					0
				};
				Ok(vfs::Attrs::new(vfs::AttrsInner::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if artifact.id.kind() == tg::artifact::Kind::Symlink => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size: 0 }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}
}

impl Fast {
	/// Opens the cache read only and locates the checkouts directory. Returns `None` if the cache cannot be opened, in which case the provider uses the client for every request.
	fn new(data_directory: &Path, config: &Config) -> Option<Self> {
		// Open the cache.
		let path = data_directory.join(&config.cache_path);
		let (cache, secondary_path) = match config.cache_kind {
			CacheKind::Fjall => return None,
			CacheKind::Lmdb => {
				let config = tangram_cache_lmdb::Config {
					map_size: config.cache_map_size,
					max_readers: 1_000,
					path,
					posix_sem_prefix: config.cache_posix_sem_prefix.clone(),
					read_batch_size: 64,
					read_channel_capacity: 256,
					read_concurrency: 1,
					write_batch_size: 8_000,
					write_channel_capacity: 256,
				};
				let cache = tangram_cache_lmdb::Cache::new_readonly(&config).map(Cache::Lmdb);
				(cache, None)
			},
			#[cfg(feature = "rocksdb")]
			CacheKind::Rocksdb => {
				let config = tangram_cache_rocksdb::Config {
					path,
					read_batch_size: 64,
					read_channel_capacity: 256,
					read_concurrency: 1,
					write_batch_size: 8_000,
					write_channel_capacity: 256,
				};
				let temp = tangram_util::fs::Temp::new().ok()?;
				let result = tangram_cache_rocksdb::Cache::new_readonly(&config, temp.path())
					.map(Cache::Rocksdb);
				(result, Some(temp))
			},
			#[cfg(not(feature = "rocksdb"))]
			CacheKind::Rocksdb => return None,
		};
		let cache = match cache {
			Err(error) => {
				tracing::warn!(
					error = %error.trace(),
					"failed to open the cache, so the fast path is disabled"
				);
				return None;
			},
			Ok(cache) => cache,
		};

		// Locate the checkouts directory.
		let checkout_path = data_directory.join(CHECKOUTS_DIRECTORY_NAME);
		let fast = Self {
			cache,
			checkout_path,
			secondary_path,
		};

		let secondary_path = fast
			.secondary_path
			.as_ref()
			.map(tangram_util::fs::Temp::path);
		tracing::info!(
			checkout_path = %fast.checkout_path.display(),
			?secondary_path,
			"enabled the fast path",
		);

		Some(fast)
	}

	/// Begins a read transaction. Every request opens its own transaction, because the driver submits one request per batch.
	fn transaction(&self) -> std::io::Result<Transaction<'_>> {
		let result = match &self.cache {
			Cache::Lmdb(cache) => cache
				.env()
				.read_txn()
				.map(Transaction::Lmdb)
				.map_err(|error| tg::error!(!error, "failed to begin a transaction")),
			#[cfg(feature = "rocksdb")]
			Cache::Rocksdb(cache) => Ok(Transaction::Rocksdb(cache.read_transaction())),
		};
		result.map_err(|error| {
			tracing::debug!(?error, "failed to begin a transaction");
			fallback()
		})
	}

	fn compute_attrs(&self, artifact: Option<&ArtifactState>) -> std::io::Result<vfs::Attrs> {
		let transaction = self.transaction()?;
		self.compute_attrs_with_transaction(&transaction, artifact)
	}

	fn directory_entries(
		&self,
		artifact: &ArtifactState,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let transaction = self.transaction()?;
		self.directory_entries_with_transaction(&transaction, artifact, None)
	}

	fn directory_lookup_entry(
		&self,
		artifact: &ArtifactState,
		name: &str,
	) -> std::io::Result<Option<ArtifactState>> {
		let transaction = self.transaction()?;
		self.directory_lookup_entry_with_transaction(&transaction, artifact, name, None)
	}

	fn read(&self, blob: &tg::blob::Id, position: u64, length: u64) -> std::io::Result<Bytes> {
		let transaction = self.transaction()?;
		let mut output = Vec::with_capacity(length.to_usize().unwrap_or(0));
		self.read_blob_range_with_transaction(&transaction, blob, position, length, &mut output)?;

		Ok(output.into())
	}

	fn getxattr(&self, artifact: &ArtifactState, name: &str) -> std::io::Result<Option<Bytes>> {
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
			return Ok(None);
		}
		if tg::file::xattrs::is_dependencies_name(name) || name == tg::file::xattrs::TOKEN_NAME {
			// Use the token-aware client path for dependency references.
			return Err(fallback());
		}
		let transaction = self.transaction()?;
		let (file, _) = self.file_node_with_transaction(&transaction, artifact)?;
		if name == tg::file::xattrs::MODULE_NAME {
			let Some(module) = file.module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	fn compute_attrs_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		artifact: Option<&ArtifactState>,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Directory) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Directory))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_with_transaction(transaction, artifact)?;
				let size = file.contents.as_ref().map_or(Ok(0), |contents| {
					self.blob_length_with_transaction(transaction, contents)
				})?;
				Ok(vfs::Attrs::new(vfs::AttrsInner::File {
					executable: file.executable,
					size,
				}))
			},
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::Symlink) => {
				Ok(vfs::Attrs::new(vfs::AttrsInner::Symlink { size: 0 }))
			},
			None => Ok(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			_ => Err(std::io::Error::from_raw_os_error(libc::EIO)),
		}
	}

	fn try_get_object(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::object::Id,
	) -> std::io::Result<Option<cache::object::Object<'static>>> {
		let arg = cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let output = match (&self.cache, transaction) {
			(Cache::Lmdb(cache), Transaction::Lmdb(transaction)) => {
				cache.try_get_object_with_transaction(transaction, &arg)
			},
			#[cfg(feature = "rocksdb")]
			(Cache::Rocksdb(cache), Transaction::Rocksdb(transaction)) => {
				cache.try_get_object_with_transaction(transaction, &arg)
			},
			#[cfg(feature = "rocksdb")]
			_ => unreachable!(),
		};
		output.map(|output| output.object).map_err(eio)
	}

	fn try_get_data(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::object::Id,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		let output = match (&self.cache, transaction) {
			(Cache::Lmdb(cache), Transaction::Lmdb(transaction)) => {
				cache.try_get_object_data_with_transaction(transaction, id)
			},
			#[cfg(feature = "rocksdb")]
			(Cache::Rocksdb(cache), Transaction::Rocksdb(transaction)) => {
				cache.try_get_object_data_with_transaction(transaction, id)
			},
			#[cfg(feature = "rocksdb")]
			_ => unreachable!(),
		};
		output.map_err(eio)
	}

	fn artifact_data_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		artifact: &ArtifactState,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		if let Some(data) = &artifact.data {
			return Ok(data.clone());
		}
		let id: tg::object::Id = artifact.id.clone().into();
		let Some((_, data)) = self.try_get_data(transaction, &id)? else {
			return Err(fallback());
		};
		data.try_into().map_err(|_| {
			tracing::error!(artifact = %artifact.id, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	fn graph_data_with_transaction(
		&self,
		transaction: &Transaction<'_>,
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

	/// Get the node at an index of a graph, loading only the leaf that contains it.
	fn graph_node_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		graph: &tg::graph::Id,
		index: usize,
	) -> std::io::Result<Option<tg::graph::data::Node>> {
		let mut current = graph.clone();
		let mut offset = index;
		loop {
			let data = self.graph_data_with_transaction(transaction, &current)?;
			let branch = match data {
				tg::graph::Data::Leaf(leaf) => return Ok(leaf.nodes.into_iter().nth(offset)),
				tg::graph::Data::Branch(branch) => branch,
			};
			let mut child_graph = None;
			for child in branch.children {
				let count = child.count.to_usize().unwrap();
				if offset < count {
					child_graph = Some(child.graph);
					break;
				}
				offset -= count;
			}
			let Some(child_graph) = child_graph else {
				return Ok(None);
			};
			current = child_graph;
		}
	}

	fn resolve_graph_node_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		pointer: &tg::graph::data::Pointer,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = pointer.graph.clone();
		let node = self
			.graph_node_with_transaction(transaction, &graph, pointer.index)?
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
		transaction: &Transaction<'_>,
		directory: &ArtifactState,
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
					self.resolve_graph_node_with_transaction(transaction, &pointer)?;
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
		transaction: &Transaction<'_>,
		file: &ArtifactState,
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
					self.resolve_graph_node_with_transaction(transaction, &pointer)?;
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
		transaction: &Transaction<'_>,
		symlink: &ArtifactState,
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
					self.resolve_graph_node_with_transaction(transaction, &pointer)?;
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
		transaction: &Transaction<'_>,
		directory: &ArtifactState,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let authority = directory.authority.clone();
			let (directory, graph) =
				self.directory_node_with_transaction(transaction, &directory)?;
			let graph = graph.or(default_graph);
			match directory {
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = self.artifact_from_directory_edge(
							transaction,
							&authority,
							child.directory,
							graph.as_ref(),
						)?;
						stack.push((artifact, graph.clone()));
					}
				},
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact =
							self.artifact_from_edge(transaction, &authority, edge, graph.as_ref())?;
						entries.insert(name, artifact);
					}
				},
			}
		}
		Ok(entries)
	}

	fn directory_lookup_entry_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		directory: &ArtifactState,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<Option<ArtifactState>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let authority = directory.authority.clone();
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
					directory = self.artifact_from_directory_edge(
						transaction,
						&authority,
						child.directory,
						graph.as_ref(),
					)?;
					default_graph = graph;
				},
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact =
						self.artifact_from_edge(transaction, &authority, edge, graph.as_ref())?;
					return Ok(Some(artifact));
				},
			}
		}
	}

	fn blob_length_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::blob::Id,
	) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let Some(object) = self.try_get_object(transaction, &id)? else {
			return Err(fallback());
		};
		if let Some(length) = object.length {
			return Ok(length);
		}
		if let Some(checkout_pointer) = object.checkout_pointer {
			return Ok(checkout_pointer.length);
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
		transaction: &Transaction<'_>,
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

		// Otherwise, read the blob from the checkouts directory.
		let Some(checkout_pointer) = object.checkout_pointer else {
			return Err(fallback());
		};
		if position >= checkout_pointer.length {
			return Ok(());
		}
		let read_length = std::cmp::min(length, checkout_pointer.length - position);
		let mut path = self
			.checkout_path
			.join(checkout_pointer.artifact.to_string());
		if let Some(path_) = checkout_pointer.path {
			path.push(path_);
		}
		let mut file = match std::fs::File::open(&path) {
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(fallback());
			},
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open the checkout file");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			},
			Ok(file) => file,
		};
		file.seek(SeekFrom::Start(checkout_pointer.position + position))
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

	fn artifact_from_edge(
		&self,
		transaction: &Transaction<'_>,
		authority: &Arc<Authority>,
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => self.artifact_from_index_with_transaction(
				transaction,
				authority,
				index,
				default_graph,
				None,
			),
			tg::graph::data::Edge::Object(artifact) => {
				Ok(ArtifactState::new(artifact, Some(authority)))
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				client::artifact_from_pointer(authority, &pointer, None)
			},
		}
	}

	fn artifact_from_directory_edge(
		&self,
		transaction: &Transaction<'_>,
		authority: &Arc<Authority>,
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => self.artifact_from_index_with_transaction(
				transaction,
				authority,
				index,
				default_graph,
				Some(tg::artifact::Kind::Directory),
			),
			tg::graph::data::Edge::Object(directory) => {
				Ok(ArtifactState::new(directory.into(), Some(authority)))
			},
			tg::graph::data::Edge::Pointer(pointer) => client::artifact_from_pointer(
				authority,
				&pointer,
				Some(tg::artifact::Kind::Directory),
			),
		}
	}
	fn artifact_from_index_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		authority: &Arc<Authority>,
		index: usize,
		graph: Option<&tg::graph::Id>,
		expected_kind: Option<tg::artifact::Kind>,
	) -> std::io::Result<ArtifactState> {
		let graph = graph.ok_or_else(fallback)?;
		let node = self
			.graph_node_with_transaction(transaction, graph, index)?
			.ok_or_else(fallback)?;
		let pointer = tg::graph::data::Pointer {
			graph: graph.clone(),
			index,
			kind: node.kind(),
		};
		client::artifact_from_pointer(authority, &pointer, expected_kind)
	}
}

impl Nodes {
	fn new() -> Self {
		let mut nodes = BTreeMap::new();
		let entry = Node {
			accessed: Instant::now(),
			artifact: None,
			attrs: Some(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth: 0,
			lookup_count: u64::MAX,
			name: None,
			named: None,
			parent: vfs::ROOT_NODE_ID,
		};
		nodes.insert(vfs::ROOT_NODE_ID, entry);
		let state = Mutex::new(State { next: 1000, nodes });
		Self { state }
	}

	fn root_artifact(&self, id: &tg::artifact::Id) -> ArtifactState {
		let state = self.state.lock().unwrap();
		let root = &state.nodes[&vfs::ROOT_NODE_ID];
		if let Some(source) = root
			.children
			.get(&id.to_string())
			.and_then(|id| state.nodes.get(id))
		{
			if let Some(artifact) = &source.artifact
				&& &artifact.id == id
			{
				return artifact.clone();
			}
			if let Some(artifact) = source
				.dependencies
				.iter()
				.find(|artifact| &artifact.id == id)
			{
				return artifact.clone();
			}
		}
		ArtifactState::new(id.clone(), None)
	}

	fn insert_dependency(&self, source: u64, artifact: &ArtifactState) {
		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&source) else {
			return;
		};
		if let Some(existing) = node
			.dependencies
			.iter()
			.find(|dependency| dependency.id == artifact.id)
		{
			existing.authority.adopt(&artifact.authority);
		} else {
			node.dependencies.push(artifact.clone());
		}
		let entry = *state
			.nodes
			.get_mut(&vfs::ROOT_NODE_ID)
			.unwrap()
			.children
			.entry(artifact.id.to_string())
			.or_insert(source);

		// Authorize an artifact already loaded at the root through the path that reached the dependency, so that its loaded descendants are authorized as well.
		if let Some(node) = state.nodes.get(&entry) {
			let loaded = node
				.artifact
				.iter()
				.chain(&node.dependencies)
				.filter(|loaded| loaded.id == artifact.id);
			for loaded in loaded {
				loaded.authority.adopt(&artifact.authority);
			}
		}
	}

	fn lookup(&self, parent: u64, name: &str) -> Option<u64> {
		let mut state = self.state.lock().unwrap();
		let id = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())?;
		let node = state.nodes.get(&id)?;
		if node.parent != parent || node.name.as_deref() != Some(name) {
			return None;
		}
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
		if node.parent != parent || node.name.as_deref() != Some(name) {
			return None;
		}
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
				named: node.named.clone(),
				parent: node.parent,
			})
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))
	}

	fn is_immutable(&self, id: u64) -> bool {
		// The root and named directories gain entries over time, but artifacts never change.
		self.state
			.lock()
			.unwrap()
			.nodes
			.get(&id)
			.is_some_and(|node| node.artifact.is_some())
	}

	fn set_attrs(&self, id: u64, attrs: vfs::Attrs) {
		let mut state = self.state.lock().unwrap();
		if let Some(node) = state.nodes.get_mut(&id) {
			node.attrs = Some(attrs);
		}
	}

	fn remember_existing(&self, id: u64) -> std::io::Result<()> {
		let mut state = self.state.lock().unwrap();
		let node = state
			.nodes
			.get_mut(&id)
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
		node.lookup_count = node.lookup_count.saturating_add(1);
		Ok(())
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

			Self::remove(state, id);

			let prune_parent = {
				let Some(parent_node) = state.nodes.get_mut(&parent) else {
					return;
				};
				if let Some(name) = name
					&& parent_node.children.get(&name) == Some(&id)
				{
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

	fn remove(state: &mut State, id: u64) {
		let parent = state.nodes[&id].parent;
		let node = state.nodes.remove(&id).unwrap();
		let mut dependencies = node.dependencies;
		if parent == vfs::ROOT_NODE_ID
			&& let Some(artifact) = node.artifact
		{
			dependencies.push(artifact);
		}
		for dependency in dependencies {
			let name = dependency.id.to_string();
			if state.nodes[&vfs::ROOT_NODE_ID].children.get(&name) != Some(&id) {
				continue;
			}
			// Preserve a shared dependency only while another existing inode supplies its tokens.
			let source = state.nodes.iter().find_map(|(id, node)| {
				node.dependencies
					.iter()
					.any(|other| other.id == dependency.id)
					.then_some(*id)
			});
			let root = state.nodes.get_mut(&vfs::ROOT_NODE_ID).unwrap();
			if let Some(source) = source {
				root.children.insert(name, source);
			} else {
				root.children.remove(&name);
			}
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
			Self::remove(&mut state, id);
			if let Some(name) = name
				&& let Some(parent_node) = state.nodes.get_mut(&parent)
				&& parent_node.children.get(&name) == Some(&id)
			{
				parent_node.children.remove(&name);
			}
		}
	}

	fn get_or_insert_child(
		&self,
		parent: u64,
		name: &str,
		artifact: ArtifactState,
		depth: u64,
		attrs: Option<vfs::Attrs>,
		remember: bool,
	) -> std::io::Result<u64> {
		let mut state = self.state.lock().unwrap();
		if let Some(id) = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
			&& state.nodes[&id].parent == parent
			&& state.nodes[&id].name.as_deref() == Some(name)
		{
			// Refresh the access time so an actively enumerated entry is not evicted.
			if let Some(node) = state.nodes.get_mut(&id) {
				node.accessed = Instant::now();
				if let Some(existing) = &node.artifact {
					existing.authority.adopt(&artifact.authority);
				}
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
		let entry = Node {
			accessed: Instant::now(),
			artifact: Some(artifact),
			attrs,
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth,
			lookup_count: u64::from(remember),
			name: Some(name.to_owned()),
			named: None,
			parent,
		};
		state.nodes.insert(id, entry);
		state
			.nodes
			.get_mut(&parent)
			.unwrap()
			.children
			.insert(name.to_owned(), id);
		Ok(id)
	}

	fn get_or_insert_named_node_child(
		&self,
		parent: u64,
		name: &str,
		depth: u64,
		attrs: vfs::Attrs,
		named_node: NamedNodeInfo,
		remember: bool,
	) -> std::io::Result<u64> {
		let mut state = self.state.lock().unwrap();
		if let Some(id) = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
		{
			let node = state.nodes.get_mut(&id).unwrap();
			node.accessed = Instant::now();
			node.artifact = None;
			node.attrs = Some(attrs);
			node.named = Some(named_node);
			if remember {
				node.lookup_count = node.lookup_count.saturating_add(1);
			}
			return Ok(id);
		}
		if !state.nodes.contains_key(&parent) {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		let id = state.next;
		state.next += 1;
		let entry = Node {
			accessed: Instant::now(),
			artifact: None,
			attrs: Some(attrs),
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth,
			lookup_count: u64::from(remember),
			name: Some(name.to_owned()),
			named: Some(named_node),
			parent,
		};
		state.nodes.insert(id, entry);
		state
			.nodes
			.get_mut(&parent)
			.unwrap()
			.children
			.insert(name.to_owned(), id);
		Ok(id)
	}
}

impl ArtifactState {
	#[must_use]
	fn new(id: tg::artifact::Id, parent: Option<&Arc<Authority>>) -> Self {
		let authority = Authority::new(id.clone().into(), parent);
		Self {
			authority,
			data: None,
			id,
		}
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
			cache_kind: CacheKind::Lmdb,
			cache_map_size: DEFAULT_CACHE_MAP_SIZE,
			cache_path: PathBuf::from(DEFAULT_CACHE_PATH),
			cache_posix_sem_prefix: None,
			data_directory: None,
			node_eviction_interval: DEFAULT_NODE_EVICTION_INTERVAL,
			node_ttl: DEFAULT_NODE_TTL,
			principal: None,
			tokens: Vec::new(),
		}
	}
}

impl vfs::Provider for Inner {
	fn handle_batch(
		&self,
		requests: Vec<vfs::Request>,
	) -> impl Future<Output = Vec<std::io::Result<vfs::Response>>> + Send {
		Inner::handle_batch(self, requests)
	}

	fn handle_batch_sync(
		&self,
		requests: Vec<vfs::Request>,
	) -> Vec<std::io::Result<vfs::Response>> {
		#[cfg(target_os = "macos")]
		if let Some(path) = &self.proxy {
			let result = std::os::unix::net::UnixStream::connect(path)
				.and_then(|stream| vfs::proxy::request_sync(stream, &requests));
			return proxy_responses(result, requests.len());
		}

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

fn render_tag(depth: u64, id: &tg::Id, suffix: Option<&str>) -> Bytes {
	let mut target = PathBuf::new();
	for _ in 0..depth.saturating_sub(1) {
		target.push("..");
	}
	let suffix = suffix.unwrap_or_default();
	target.push(format!("{id}{suffix}"));
	target.as_os_str().as_bytes().to_vec().into()
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

/// Creates the error the fast path returns when it cannot serve a request, because the object is not in the local cache or is not checked out. The provider falls back to the client, which fetches from a remote if necessary. This matches the sync path of the server's provider, which the fuse server retries asynchronously on `ENOSYS` alone.
fn fallback() -> std::io::Error {
	std::io::Error::from_raw_os_error(libc::ENOSYS)
}

/// Returns whether an error from the fast path means the provider should fall back to the client. Any other error is returned to the caller, so that a corrupt object is not masked by a request to the server.
fn is_fallback(error: &std::io::Error) -> bool {
	error.raw_os_error() == Some(libc::ENOSYS)
}

#[cfg(target_os = "macos")]
fn proxy_responses(
	result: std::io::Result<Vec<std::io::Result<vfs::Response>>>,
	length: usize,
) -> Vec<std::io::Result<vfs::Response>> {
	match result {
		Ok(responses) => responses,
		Err(error) => {
			tracing::error!(%error, "failed to proxy the VFS requests");
			let errno = error.raw_os_error().unwrap_or(libc::EIO);
			(0..length)
				.map(|_| Err(std::io::Error::from_raw_os_error(errno)))
				.collect()
		},
	}
}

fn now() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| {
			duration.as_secs().to_i64().unwrap_or(i64::MAX)
		})
}

fn eio(error: impl std::fmt::Debug) -> std::io::Error {
	tracing::error!("{error:?}");
	std::io::Error::from_raw_os_error(libc::EIO)
}

#[cfg(test)]
mod tests {
	use {super::*, tangram_util::fs::Temp};

	struct Fixture {
		child: tg::artifact::Id,
		directory: tg::artifact::Id,
		provider: Provider,
		temp: Temp,
	}

	#[test]
	fn authorized_root_allows_descendant_traversal() {
		let fixture = fixture(true);
		assert!(fixture.temp.path().exists());
		let inner = fixture.provider.inner.clone();
		let root = fixture
			.provider
			.runtime
			.block_on(inner.lookup(vfs::ROOT_NODE_ID, &fixture.directory.to_string()))
			.unwrap()
			.unwrap();
		let child = fixture
			.provider
			.runtime
			.block_on(inner.lookup(root, "child"))
			.unwrap()
			.unwrap();
		let info = inner.nodes.get(child).unwrap();

		assert_eq!(
			info.artifact.map(|artifact| artifact.id),
			Some(fixture.child.clone())
		);
	}

	#[test]
	fn directory_handles_remain_independent_and_pin_the_node() {
		let fixture = fixture(true);
		let inner = &fixture.provider.inner;
		fixture.provider.runtime.block_on(async {
			let id = inner
				.lookup(vfs::ROOT_NODE_ID, &fixture.directory.to_string())
				.await
				.unwrap()
				.unwrap();
			let (first, immutable) = inner.opendir(id).await.unwrap();
			let (second, _) = inner.opendir(id).await.unwrap();
			assert!(immutable);
			assert_ne!(first, second);
			inner.nodes.evict_expired(Duration::ZERO);
			let entries = inner.readdir(first, 0, 4096).await.unwrap();
			assert!(entries.iter().any(|(name, _, _)| name == "child"));
			inner.close(first);
			assert!(inner.readdir(first, 0, 4096).await.is_err());
			assert!(inner.readdir(second, 0, 4096).await.is_ok());
			inner.close(second);
			assert!(inner.nodes.get(id).is_err());
		});
	}

	#[test]
	fn eviction_releases_pointer_data_and_authority_paths() {
		let nodes = Nodes::new();
		let parent = ArtifactState::new(tg::directory::Id::new(b"parent").into(), None);
		let parent_authority = Arc::downgrade(&parent.authority);
		let source = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, "parent", parent.clone(), 1, None, false)
			.unwrap();
		let pointer = tg::graph::data::Pointer {
			graph: tg::graph::Id::new(b"graph"),
			index: 0,
			kind: tg::artifact::Kind::File,
		};
		let dependency = client::artifact_from_pointer(&parent.authority, &pointer, None).unwrap();
		let dependency_authority = Arc::downgrade(&dependency.authority);
		let id = dependency.id.clone();
		nodes.insert_dependency(source, &dependency);
		let loaded = nodes.root_artifact(&id);
		assert!(loaded.data.is_some());
		assert!(Arc::ptr_eq(&loaded.authority, &dependency.authority));
		drop(loaded);
		drop(dependency);
		drop(parent);
		nodes.evict_expired(Duration::ZERO);
		assert!(parent_authority.upgrade().is_none());
		assert!(dependency_authority.upgrade().is_none());
		assert!(nodes.root_artifact(&id).data.is_none());
	}

	#[test]
	fn shared_dependencies_survive_until_their_last_source_is_forgotten() {
		let nodes = Nodes::new();
		let first = ArtifactState::new(tg::file::Id::new(b"first").into(), None);
		let second = ArtifactState::new(tg::file::Id::new(b"second").into(), None);
		let dependency = ArtifactState::new(
			tg::file::Id::new(b"dependency").into(),
			Some(&first.authority),
		);
		let id = dependency.id.clone();
		let first = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, "first", first, 1, None, true)
			.unwrap();
		let second = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, "second", second, 1, None, true)
			.unwrap();
		nodes.insert_dependency(first, &dependency);
		nodes.insert_dependency(second, &dependency);
		nodes.forget(first, 1);
		assert!(Arc::ptr_eq(
			&nodes.root_artifact(&id).authority,
			&dependency.authority
		));
		nodes.forget(second, 1);
		assert!(!Arc::ptr_eq(
			&nodes.root_artifact(&id).authority,
			&dependency.authority
		));
	}

	#[test]
	fn denied_root_is_hidden() {
		let fixture = fixture(false);
		let inner = fixture.provider.inner.clone();
		let root = fixture
			.provider
			.runtime
			.block_on(inner.lookup(vfs::ROOT_NODE_ID, &fixture.directory.to_string()))
			.unwrap();

		assert_eq!(root, None);
		assert_eq!(
			inner
				.nodes
				.lookup(vfs::ROOT_NODE_ID, &fixture.directory.to_string()),
			None,
		);
	}

	#[test]
	fn renders_non_artifact_tag_targets() {
		let blob: tg::Id = tg::blob::Id::new(b"blob").into();
		let process: tg::Id = tg::process::Id::new().into();

		assert_eq!(
			render_tag(3, &blob, None),
			Bytes::from(format!("../../{blob}"))
		);
		assert_eq!(
			render_tag(2, &process, Some(".tg.ts")),
			Bytes::from(format!("../{process}.tg.ts"))
		);
	}

	#[test]
	fn fjall_disables_the_fast_path() {
		let temp = Temp::new().unwrap();
		let config = Config {
			cache_kind: CacheKind::Fjall,
			cache_path: PathBuf::from("cache.fjall"),
			..Config::default()
		};
		assert!(Fast::new(temp.path(), &config).is_none());
		assert!(!temp.path().exists());
	}

	#[cfg(not(feature = "rocksdb"))]
	#[test]
	fn rocksdb_disables_the_fast_path_without_the_feature() {
		let temp = Temp::new().unwrap();
		let config = Config {
			cache_kind: CacheKind::Rocksdb,
			cache_path: PathBuf::from("cache.rocksdb"),
			..Config::default()
		};
		assert!(Fast::new(temp.path(), &config).is_none());
		assert!(!temp.path().exists());
	}

	#[cfg(feature = "rocksdb")]
	#[test]
	fn rocksdb_fast_path_catches_up_on_a_miss() {
		let temp = Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let cache_path = PathBuf::from("cache.rocksdb");
		let config = tangram_cache_rocksdb::Config {
			path: temp.path().join(&cache_path),
			read_batch_size: 64,
			read_channel_capacity: 256,
			read_concurrency: 1,
			write_batch_size: 8_000,
			write_channel_capacity: 256,
		};
		let cache = tangram_cache_rocksdb::Cache::new(&config).unwrap();
		let config = Config {
			cache_kind: CacheKind::Rocksdb,
			cache_path,
			..Config::default()
		};
		let fast = Fast::new(temp.path(), &config).unwrap();

		// Publish an object after opening the secondary.
		let data = tg::object::Data::from(tg::blob::Data::Leaf(tg::blob::data::Leaf {
			bytes: Bytes::from_static(b"hello"),
		}));
		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(tg::object::Kind::Blob, &bytes);
		let arg = cache::object::put::Arg {
			bytes: Some(bytes.clone()),
			checkout_pointer: None,
			id: id.clone(),
			length: Some(5),
			put: [1; 16],
		};
		cache.put_object_sync(arg).unwrap();

		// Leave an unflushed object for the provider to fetch through its server fallback.
		let transaction = fast.transaction().unwrap();
		assert!(fast.try_get_object(&transaction, &id).unwrap().is_none());
		assert!(fast.try_get_data(&transaction, &id).unwrap().is_none());
		cache.flush_sync().unwrap();

		// Serve the flushed object through the fast path without contacting the server.
		let transaction = fast.transaction().unwrap();
		let (_, data) = fast.try_get_data(&transaction, &id).unwrap().unwrap();
		assert_eq!(data.serialize().unwrap(), bytes);
		let object = fast.try_get_object(&transaction, &id).unwrap().unwrap();
		assert_eq!(object.bytes.unwrap().as_ref(), bytes.as_ref());
	}

	fn fixture(authorized: bool) -> Fixture {
		// Create a temporary cache containing a directory and one child.
		let temp = Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let child: tg::artifact::Id = tg::file::Id::new(b"child").into();
		let entries = BTreeMap::from([(
			"child".to_owned(),
			tg::graph::data::Edge::Object(child.clone()),
		)]);
		let directory =
			tg::graph::data::Directory::Leaf(tg::graph::data::DirectoryLeaf { entries });
		let data: tg::object::Data = tg::directory::Data::Node(directory).into();
		let bytes = data.serialize().unwrap();
		let directory: tg::artifact::Id = tg::directory::Id::new(&bytes).into();
		let cache_path = PathBuf::from("cache.lmdb");
		let cache = tangram_cache_lmdb::Cache::new(&tangram_cache_lmdb::Config {
			map_size: 10 * 1024 * 1024,
			max_readers: 1_000,
			path: temp.path().join(&cache_path),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_channel_capacity: 256,
			read_concurrency: 1,
			write_batch_size: 8_000,
			write_channel_capacity: 256,
		})
		.unwrap();
		cache
			.put_object_sync(cache::object::put::Arg {
				bytes: Some(bytes),
				checkout_pointer: None,
				id: tg::object::Id::from(directory.clone()),
				length: None,
				put: [0; 16],
			})
			.unwrap();
		drop(cache);

		// Create a provider whose anonymous principal optionally holds the directory's subtree token.
		let tokens = authorized.then(|| token(&directory)).into_iter().collect();
		let config = Config {
			data_directory: Some(temp.path().to_owned()),
			cache_map_size: 10 * 1024 * 1024,
			cache_path,
			principal: Some(tg::Principal::Anonymous),
			tokens,
			..Config::default()
		};
		let provider = Provider::new("/tmp/tangram-vfs-provider.sock", &config).unwrap();

		Fixture {
			child,
			directory,
			provider,
			temp,
		}
	}

	fn token(artifact: &tg::artifact::Id) -> tg::authorization::Token {
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![permission],
				resource: tg::object::Id::from(artifact.clone()).into(),
			},
			secret_id: 0,
			signature: vec![0; 16],
		}
	}
}
