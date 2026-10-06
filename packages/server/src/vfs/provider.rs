#[cfg(feature = "lmdb")]
use heed as lmdb;
use {
	crate::{Server, Session, context::Context},
	bytes::Bytes,
	dashmap::DashMap,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		collections::BTreeMap,
		ops::Deref,
		os::fd::OwnedFd,
		os::unix::{ffi::OsStrExt as _, fs::FileExt as _},
		path::{Path, PathBuf},
		pin::pin,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
	},
	tangram_cache::prelude::*,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_vfs as vfs,
};

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

#[derive(Clone)]
pub struct Provider(Arc<Inner>);

#[derive(Clone)]
pub struct Weak(std::sync::Weak<Inner>);

pub struct Inner {
	directory_handles: DashMap<u64, DirectorySnapshot, fnv::FnvBuildHasher>,
	directory_snapshot_loads: DashMap<u64, Arc<tokio::sync::Mutex<()>>, fnv::FnvBuildHasher>,
	directory_snapshots: Mutex<vfs::cache::WeightedLruCache<u64, DirectorySnapshot>>,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	handle_count: AtomicU64,
	nodes: Nodes,
	origin: crate::Origin,
	principal: Mutex<Option<tg::Principal>>,
	remote_tokens: Mutex<tg::authorization::Tokens>,
	runtime: tokio::runtime::Handle,
	server: Server,
}

#[derive(Clone)]
struct DirectorySnapshot {
	depth: u64,
	entries: Option<Arc<[DirectorySnapshotEntry]>>,
	named: Option<NamedNodeInfo>,
	named_directory: bool,
	node: u64,
	pageable: bool,
	parent: u64,
}

#[derive(Clone)]
struct DirectorySnapshotEntry {
	artifact: Option<ArtifactState>,
	kind: vfs::EntryKind,
	name: String,
	named: Option<NamedNodeInfo>,
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
	artifact: Option<ArtifactState>,
	attrs: Option<vfs::Attrs>,
	children: BTreeMap<String, u64>,
	dependencies: Vec<ArtifactState>,
	depth: u64,
	lookup_count: u64,
	name: Option<String>,
	named: Option<NamedNodeInfo>,
	parent: u64,
	tokens: Vec<tg::authorization::Token>,
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
	branch_children: Arc<Mutex<BTreeMap<tg::artifact::Id, BranchChild>>>,
	children_expires_at: Arc<Mutex<Option<i64>>>,
	data: Option<tg::artifact::data::Artifact>,
	id: tg::artifact::Id,
	tokens: Arc<Mutex<Vec<tg::authorization::Token>>>,
}

struct BranchChild {
	artifact: ArtifactState,
	parent_tokens: Vec<tg::authorization::Token>,
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
	tokens: Arc<Mutex<Vec<tg::authorization::Token>>>,
}

impl Provider {
	pub async fn new(
		server: &Server,
		origin: crate::Origin,
		principal: Option<tg::Principal>,
	) -> tg::Result<Self> {
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
		let principal = Mutex::new(principal);
		let remote_tokens = Mutex::new(tg::authorization::Tokens::default());
		let runtime = tokio::runtime::Handle::current();
		let server = server.clone();
		let provider = Inner {
			directory_handles,
			directory_snapshot_loads,
			directory_snapshots,
			file_handles,
			handle_count,
			nodes,
			origin,
			principal,
			remote_tokens,
			runtime,
			server,
		};

		let provider = Self(Arc::new(provider));

		Ok(provider)
	}

	#[cfg(target_os = "linux")]
	#[must_use]
	pub fn downgrade(&self) -> Weak {
		Weak(Arc::downgrade(&self.0))
	}

	#[cfg(target_os = "linux")]
	pub fn set_principal(&self, principal: tg::Principal) {
		self.principal.lock().unwrap().replace(principal);
	}

	pub fn seed_tokens(
		&self,
		session: &Session,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<()> {
		self.inherit_remote_tokens(tokens);
		let tokens = tg::authorization::Tokens::with_authorization(
			tokens
				.local_authorization()
				.iter()
				.filter(|token| session.verify_token(token))
				.cloned(),
		);
		self.nodes.insert_tokens(session, &tokens)?;
		Ok(())
	}

	fn inherit_remote_tokens(&self, tokens: &tg::authorization::Tokens) {
		let mut incoming = tokens.clone();
		incoming.remove_local();
		self.remote_tokens.lock().unwrap().inherit(&incoming);
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
						.opendir_inner(id)
						.await
						.map(|(handle, immutable)| vfs::Response::OpenDir { handle, immutable }),
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
		let mut transaction = None;
		let mut requests = requests.into_iter();
		let mut responses = Vec::with_capacity(requests.len());
		while let Some(request) = requests.next() {
			// Defer a named node request before opening a cache transaction.
			if let Some(response) = self.try_defer_named_request_sync(&request) {
				responses.push(response);
				continue;
			}

			#[cfg(feature = "lmdb")]
			let transaction = if let crate::cache::Cache::Lmdb(cache) = &self.server.cache {
				if transaction.is_none() {
					transaction = match cache.env().read_txn() {
						Ok(transaction) => Some(transaction),
						Err(error) => {
							tracing::error!(?error, "failed to begin an lmdb read transaction");
							responses.push(Err(std::io::Error::from_raw_os_error(libc::EIO)));
							responses.extend(
								requests.map(|_| Err(std::io::Error::from_raw_os_error(libc::EIO))),
							);
							return responses;
						},
					};
				}
				transaction.as_deref()
			} else {
				None
			};
			#[cfg(not(feature = "lmdb"))]
			let transaction: Option<&Transaction<'_>> = None;

			let response = self.handle_request_sync_inner(request, transaction);
			responses.push(response);
		}

		responses
	}

	fn try_defer_named_request_sync(
		&self,
		request: &vfs::Request,
	) -> Option<std::io::Result<vfs::Response>> {
		let named = match request {
			vfs::Request::Lookup { id, name } | vfs::Request::LookupAndRemember { id, name } => {
				if name == "." || name == ".." {
					return None;
				}
				let named_component = matches!(
					tg::store::path::parse_component(name),
					Ok(tg::store::path::Component::Tag { .. })
				);
				if !named_component {
					return None;
				}
				if *id == vfs::ROOT_NODE_ID {
					Ok(true)
				} else {
					self.get_sync(*id).map(|node| {
						node.named
							.is_some_and(|named_node| named_node.target.is_none())
					})
				}
			},
			vfs::Request::ReadDir { handle, .. } | vfs::Request::ReadDirPlus { handle, .. } => self
				.directory_handle(*handle)
				.map(|snapshot| snapshot.named_directory),
			vfs::Request::ReadLink { id } => self.get_sync(*id).map(|node| node.named.is_some()),
			_ => return None,
		};
		match named {
			Err(error) => Some(Err(error)),
			Ok(false) => None,
			Ok(true) => Some(Err(std::io::Error::from_raw_os_error(libc::ENOSYS))),
		}
	}

	fn handle_request_sync_inner(
		&self,
		request: vfs::Request,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<vfs::Response> {
		match request {
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
			vfs::Request::Lookup { id, name } => {
				let immutable = self.nodes.is_immutable(id);
				self.lookup_sync_inner(id, &name, transaction, false)
					.map(|id| vfs::Response::Lookup {
						attrs: None,
						id,
						immutable,
					})
			},
			vfs::Request::LookupAndRemember { id, name } => {
				let immutable = self.nodes.is_immutable(id);
				self.lookup_and_remember_sync_inner(id, &name, transaction)
					.map(|entry| {
						let (id, attrs) = entry.unzip();
						vfs::Response::Lookup {
							attrs,
							id,
							immutable,
						}
					})
			},
			vfs::Request::LookupParent { id } => self
				.lookup_parent_sync(id)
				.map(|id| vfs::Response::LookupParent { id }),
			vfs::Request::Open { id } => self
				.open_sync_inner(id, transaction)
				.map(|(handle, backing_fd)| vfs::Response::Open { handle, backing_fd }),
			vfs::Request::OpenDir { id } => self
				.opendir_sync_inner(id, transaction)
				.map(|(handle, immutable)| vfs::Response::OpenDir { handle, immutable }),
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
				.readdir_sync_inner(handle, offset, length, transaction)
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
		}
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
		drop(cache);
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
		let (file, graph) = self.file_node_inner(&artifact).await?;
		if tg::file::xattrs::is_dependencies_name(name) {
			if file.dependencies.is_empty() {
				return Ok(None);
			}
			let references =
				self.file_dependency_references(&artifact.tokens, &file, graph.as_ref(), None)?;
			let xattrs = tg::file::xattrs::encode_dependencies(
				&references,
				tg::file::xattrs::MAX_VALUE_SIZE,
			)
			.map_err(|error| std::io::Error::other(error.to_string()))?;
			let value = xattrs
				.into_iter()
				.find_map(|xattr| (xattr.name == name).then_some(xattr.value));
			return Ok(value);
		}
		if name == tg::file::xattrs::TOKEN_NAME {
			let token = self.artifact_token(&artifact)?;
			let value = token.map(|token| Bytes::from(token.to_string()));
			return Ok(value);
		}
		if name == tg::file::xattrs::MODULE_NAME {
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
		let (file, graph) = self.file_node_sync_inner(&artifact, transaction)?;
		if tg::file::xattrs::is_dependencies_name(name) {
			if file.dependencies.is_empty() {
				return Ok(None);
			}
			let references = self.file_dependency_references(
				&artifact.tokens,
				&file,
				graph.as_ref(),
				transaction,
			)?;
			let xattrs = tg::file::xattrs::encode_dependencies(
				&references,
				tg::file::xattrs::MAX_VALUE_SIZE,
			)
			.map_err(|error| std::io::Error::other(error.to_string()))?;
			let value = xattrs
				.into_iter()
				.find_map(|xattr| (xattr.name == name).then_some(xattr.value));
			return Ok(value);
		}
		if name == tg::file::xattrs::TOKEN_NAME {
			let token = self.artifact_token(&artifact)?;
			let value = token.map(|token| Bytes::from(token.to_string()));
			return Ok(value);
		}
		if name == tg::file::xattrs::MODULE_NAME {
			let Some(module) = file.module else {
				return Ok(None);
			};
			return Ok(Some(module.to_string().as_bytes().to_vec().into()));
		}
		Ok(None)
	}

	fn artifact_token(
		&self,
		artifact: &ArtifactState,
	) -> std::io::Result<Option<tg::authorization::Token>> {
		self.authorize(&artifact.tokens, &artifact.id.clone().into())?;
		let token = artifact
			.tokens
			.lock()
			.unwrap()
			.iter()
			.filter(|token| token.body.resource == artifact.id.clone().into())
			.max_by_key(|token| token.body.expires_at)
			.cloned();
		Ok(token)
	}

	fn file_dependency_references(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		file: &tg::graph::data::File,
		graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<tg::Reference>> {
		let mut references = Vec::with_capacity(file.dependencies.len());
		for (reference, dependency) in &file.dependencies {
			let mut reference = reference.clone();
			let Some(dependency) =
				self.file_dependency_artifact(tokens, dependency.as_ref(), graph, transaction)?
			else {
				references.push(reference);
				continue;
			};
			let mut options = reference.options().clone();
			options
				.tokens
				.inherit(&tg::authorization::Tokens::with_authorization(
					dependency.tokens.lock().unwrap().clone(),
				));
			reference.set_options(options);
			references.push(reference);
		}

		Ok(references)
	}

	fn file_dependency_artifact(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		dependency: Option<&tg::graph::data::Dependency>,
		graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<ArtifactState>> {
		let Some(edge) = dependency.and_then(|dependency| dependency.0.node.as_ref()) else {
			return Ok(None);
		};
		let Ok(edge) = tg::graph::data::Edge::<tg::artifact::Id>::try_from(edge.clone()) else {
			return Ok(None);
		};
		let artifact = self.artifact_from_edge_inner(tokens, edge, graph, transaction)?;
		Ok(Some(artifact))
	}

	pub async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let node = self.get(id).await?;
		let Some(artifact) = node.artifact else {
			return Ok(Vec::new());
		};
		if !matches!(artifact.id.kind(), tg::artifact::Kind::File) {
			return Ok(Vec::new());
		}
		let (file, graph) = self.file_node_inner(&artifact).await?;
		let mut names = Vec::new();
		if !file.dependencies.is_empty() {
			let references =
				self.file_dependency_references(&artifact.tokens, &file, graph.as_ref(), None)?;
			let xattrs = tg::file::xattrs::encode_dependencies(
				&references,
				tg::file::xattrs::MAX_VALUE_SIZE,
			)
			.map_err(|error| std::io::Error::other(error.to_string()))?;
			names.extend(xattrs.into_iter().map(|xattr| xattr.name));
		}
		if file.module.is_some() {
			names.push(tg::file::xattrs::MODULE_NAME.to_owned());
		}
		let token = self.artifact_token(&artifact)?;
		if token.is_some() {
			names.push(tg::file::xattrs::TOKEN_NAME.to_owned());
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
		let (file, graph) = self.file_node_sync_inner(&artifact, transaction)?;
		let mut names = Vec::new();
		if !file.dependencies.is_empty() {
			let references = self.file_dependency_references(
				&artifact.tokens,
				&file,
				graph.as_ref(),
				transaction,
			)?;
			let xattrs = tg::file::xattrs::encode_dependencies(
				&references,
				tg::file::xattrs::MAX_VALUE_SIZE,
			)
			.map_err(|error| std::io::Error::other(error.to_string()))?;
			names.extend(xattrs.into_iter().map(|xattr| xattr.name));
		}
		if file.module.is_some() {
			names.push(tg::file::xattrs::MODULE_NAME.to_owned());
		}
		let token = self.artifact_token(&artifact)?;
		if token.is_some() {
			names.push(tg::file::xattrs::TOKEN_NAME.to_owned());
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

		// Resolve root entries and named node components using the flat store namespace.
		let parent_node = self.get(parent).await?;
		if parent == vfs::ROOT_NODE_ID {
			match tg::store::path::parse_component(name) {
				Ok(tg::store::path::Component::Id { id, .. }) => {
					let artifact = self.nodes.root_artifact(&id);
					if self
						.authorize_inner(&artifact.tokens, &id.clone().into())
						.await
						.is_err()
					{
						return Ok(None);
					}
					let attrs = Self::attrs_from_artifact(Some(&artifact));
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
			self.nodes.lookup_and_remember_sync(parent, name)
		} else {
			self.nodes.lookup(parent, name).await?
		};
		if let Some(id) = id {
			return Ok(Some(id));
		}

		// Resolve an artifact directory entry.
		let Some(artifact) = parent_node.artifact else {
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
		let depth = parent_node.depth + 1;

		// Insert the node.
		let attrs = Self::attrs_from_artifact(Some(&artifact));
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
		let (attrs, target) = match entry {
			NamedNodeEntry::Directory if suffix.is_none() => (
				vfs::Attrs::new(vfs::AttrsInner::Directory).cacheable(false),
				None,
			),
			NamedNodeEntry::Directory => return Ok(None),
			NamedNodeEntry::Symlink(target) => {
				let depth = self.nodes.get_sync(parent)?.depth + 1;
				self.register_target_tokens(&target)?;
				let target_path = Self::build_tag_target(depth, &target.node, suffix);
				let size = target_path.len().to_u64().unwrap();
				(
					vfs::Attrs::new(vfs::AttrsInner::Symlink { size }).cacheable(false),
					Some(target),
				)
			},
		};
		let depth = self.nodes.get_sync(parent)?.depth + 1;
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
		let Some(session) = self.named_node_session() else {
			return Ok(None);
		};
		let location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let id = self
			.server
			.index
			.try_get_id_for_specifier(specifier)
			.await
			.map_err(|error| named_node_error(&error))?;
		let Some(id) = id else {
			return Ok(None);
		};
		if id.kind() != tg::id::Kind::Tag {
			if !matches!(
				id.kind(),
				tg::id::Kind::Group | tg::id::Kind::Organization | tg::id::Kind::User
			) {
				return Ok(None);
			}
			let permission = Session::read_permission_for_resource(&id)
				.map_err(|error| named_node_error(&error))?;
			let authorized = session
				.authorize(tg::Selector::Id(id), permission)
				.await
				.map_err(|error| named_node_error(&error))?
				.permissions
				.contains(permission);

			return Ok(authorized.then_some(NamedNodeEntry::Directory));
		}

		let id = tg::tag::Id::try_from(id).map_err(std::io::Error::other)?;
		let selector = tg::tag::Selector::Id(id);
		let arg = tg::tag::get::Arg {
			location,
			..Default::default()
		};
		if let Some(output) = session
			.try_get_tag(&selector, arg)
			.await
			.map_err(|error| named_node_error(&error))?
		{
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

			return Ok(Some(NamedNodeEntry::Symlink(target)));
		}

		Ok(None)
	}

	async fn list_named_node_children(
		&self,
		parent: Option<tg::Specifier>,
		position: u64,
		length: u64,
	) -> std::io::Result<Vec<NamedNodeChild>> {
		let Some(session) = self.named_node_session() else {
			return Ok(Vec::new());
		};
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
			let node = reference
				.get_with_instance(&session)
				.await
				.map_err(|error| named_node_error(&error))?;
			let node = node
				.try_map(|node| match node {
					tg::get::Node::Id(id) => Ok(id),
					tg::get::Node::Pointer(_) => Err(tg::error!("the list node must be an ID")),
				})
				.map_err(|error| named_node_error(&error))?;
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
		let output = session
			.list_all(arg)
			.await
			.map_err(|error| named_node_error(&error))?;
		let entries = output
			.data
			.into_iter()
			.skip(usize::try_from(position).unwrap_or(usize::MAX))
			.take(usize::try_from(length).unwrap_or(usize::MAX))
			.collect::<Vec<_>>();
		let children = entries
			.into_iter()
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

	fn register_target_tokens(&self, target: &tg::Referent<tg::Id>) -> std::io::Result<()> {
		self.seed_tokens(&self.session(), &target.options.tokens)
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		Ok(())
	}

	fn session(&self) -> Session {
		// Fetch as the mount principal with the tokens retained by its nodes.
		let context = Context {
			billing_ready: false,
			id: None,
			origin: self.origin,
			principal: self
				.principal
				.lock()
				.unwrap()
				.clone()
				.unwrap_or(tg::Principal::Anonymous),
			stopper: None,
			token: None,
		};

		Session::new(self.server.clone(), context)
	}

	fn named_node_session(&self) -> Option<Session> {
		let principal = self.principal.lock().unwrap().clone()?;
		// The provider is a host service acting as the mount's principal.
		let context = Context {
			billing_ready: false,
			id: None,
			origin: crate::Origin::Host,
			principal,
			stopper: None,
			token: None,
		};

		Some(Session::new(self.server.clone(), context))
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

		// Resolve named node components on the server runtime so they always reflect the database.
		let parent_node = self.get_sync(parent)?;
		let tag_component = match tg::store::path::parse_component(name) {
			Ok(tg::store::path::Component::Tag { component, suffix }) => Some((component, suffix)),
			_ => None,
		};
		if parent == vfs::ROOT_NODE_ID {
			if let Some((component, suffix)) = tag_component {
				return self.runtime.block_on(
					self.lookup_named_node(parent, name, None, component, suffix, remember),
				);
			}
		} else if let Some(named_node) = parent_node.named {
			if named_node.target.is_some() {
				return Ok(None);
			}
			let Some((component, suffix)) = tag_component else {
				return Ok(None);
			};
			return self.runtime.block_on(self.lookup_named_node(
				parent,
				name,
				Some(&named_node.specifier),
				component,
				suffix,
				remember,
			));
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

		// If the parent is the root, then create a new artifact node.
		let entry = 'a: {
			if parent != vfs::ROOT_NODE_ID {
				break 'a None;
			}
			let Ok(tg::store::path::Component::Id { id, .. }) =
				tg::store::path::parse_component(name)
			else {
				return Ok(None);
			};
			// Return not found if the principal is not authorized to access the artifact.
			let artifact = self.nodes.root_artifact(&id);
			if self
				.authorize_sync(&artifact.tokens, &id.clone().into())
				.is_err()
			{
				return Ok(None);
			}
			Some((artifact, 1))
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let entry = 'a: {
			if let Some(entry) = entry {
				break 'a Some(entry);
			}
			let NodeInfo {
				artifact, depth, ..
			} = parent_node;
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

	fn authorize(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
	) -> std::io::Result<crate::authorization::Output> {
		let principal = self.principal.lock().unwrap().clone();
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let Some(principal) = principal else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
		let now = self
			.server
			.clock
			.unix_timestamp()
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		let resource = tg::Id::from(id.clone());
		let expires_at = tokens
			.lock()
			.unwrap()
			.iter()
			.find(|token| {
				token.body.resource == resource
					&& token.body.expires_at >= now
					&& token.body.authorizes(subtree)
			})
			.map(|token| token.body.expires_at);
		if expires_at.is_none() && !matches!(principal, tg::Principal::Root) {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}
		let output = crate::authorization::Output {
			expires_at,
			outcome: crate::authorization::Outcome::Satisfied,
			permissions: subtree.into(),
		};
		Ok(output)
	}

	async fn authorize_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
	) -> std::io::Result<crate::authorization::Output> {
		if let Ok(output) = self.authorize(tokens, id) {
			return Ok(output);
		}
		if self.principal.lock().unwrap().is_none() {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}

		// Fall back to the existing authorization path when the available tokens are insufficient.
		let mut available = self.nodes.state.lock().unwrap().nodes[&vfs::ROOT_NODE_ID]
			.tokens
			.clone();
		available.extend(tokens.lock().unwrap().iter().cloned());
		let tokens_arg = tg::authorization::Tokens::with_authorization(available);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), tokens_arg);
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let requested = tg::authorization::permission::Set::from(permission);
		let session = self.session();
		let output = session
			.authorize_with_permissions(resource, requested, requested, requested.empty_like())
			.await
			.and_then(crate::authorization::Output::check_exhaustion)
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		if !output.permissions.contains(permission) {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		}

		// Retain the result on the node so subsequent accesses can use its token.
		let expires_at = self.token_expiration(output.expires_at)?;
		if let Some(token) = session
			.create_token(id.clone().into(), vec![permission], expires_at)
			.map_err(|error| Self::map_cache_sync_error(&error))?
		{
			let mut tokens = tokens.lock().unwrap();
			let now = self
				.server
				.clock
				.unix_timestamp()
				.map_err(|error| Self::map_cache_sync_error(&error))?;
			tokens.retain(|token| token.body.expires_at >= now);
			Self::insert_tokens(&mut tokens, &[token]);
		}
		Ok(output)
	}

	fn authorize_sync(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
	) -> std::io::Result<crate::authorization::Output> {
		self.authorize(tokens, id)
			.or_else(|_| self.runtime.block_on(self.authorize_inner(tokens, id)))
	}

	fn token_expiration(&self, expiration: Option<i64>) -> std::io::Result<i64> {
		let now = self
			.server
			.clock
			.unix_timestamp()
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		let ttl = i64::try_from(self.server.config.object.permission_time_to_live.as_secs())
			.map_err(std::io::Error::other)?;
		let expires_at = now
			.checked_add(ttl)
			.ok_or_else(|| std::io::Error::other("the token expiration overflowed"))?;
		Ok(expiration.map_or(expires_at, |expiration| expiration.min(expires_at)))
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
		let (file, graph) = self.file_node_inner(&artifact).await?;
		let Some(blob) = file.contents else {
			tracing::error!(%id, "file has no contents");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};

		// Register the file's dependencies so that a process can open them by store path.
		self.register_file_dependencies(id, &artifact, &file.dependencies, graph.as_ref(), None)?;

		// Create the file handle.
		let file_handle = FileHandle {
			blob,
			tokens: artifact.tokens.clone(),
		};

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
		let (file, graph) = self.file_node_sync_inner(&artifact, transaction)?;
		let Some(blob) = file.contents else {
			tracing::error!(%id, "file has no contents");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};

		// Register the file's dependencies so that a process can open them by store path.
		self.register_file_dependencies(
			id,
			&artifact,
			&file.dependencies,
			graph.as_ref(),
			transaction,
		)?;

		// Attempt to open a backing file for passthrough.
		let backing_fd = self.try_open_backing_fd_sync_inner(&blob, transaction)?;

		// Insert the file handle.
		let id = self.handle_count.fetch_add(1, Ordering::Relaxed);
		self.file_handles.insert(
			id,
			FileHandle {
				blob,
				tokens: artifact.tokens.clone(),
			},
		);

		Ok((id, backing_fd))
	}

	pub async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		let (handle, _) = self.opendir_inner(id).await?;

		Ok(handle)
	}

	async fn opendir_inner(&self, id: u64) -> std::io::Result<(u64, bool)> {
		let snapshot = self.directory_snapshot(id).await?;
		let handle = self.insert_directory_handle(&snapshot);
		let immutable = !snapshot.named_directory;

		Ok((handle, immutable))
	}

	pub fn opendir_sync(&self, id: u64) -> std::io::Result<u64> {
		let (handle, _) = self.opendir_sync_inner(id, None)?;

		Ok(handle)
	}

	fn opendir_sync_inner(
		&self,
		id: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(u64, bool)> {
		let snapshot = self.directory_snapshot_sync_inner(id, transaction)?;
		let handle = self.insert_directory_handle(&snapshot);
		let immutable = !snapshot.named_directory;

		Ok((handle, immutable))
	}

	async fn directory_snapshot(&self, id: u64) -> std::io::Result<DirectorySnapshot> {
		let node = self.get(id).await?;
		if node.artifact.is_none() {
			return Self::named_node_directory_snapshot(id, node.parent, node.depth, node.named);
		}
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
		let node = self.get_sync(id)?;
		if node.artifact.is_none() {
			return Self::named_node_directory_snapshot(id, node.parent, node.depth, node.named);
		}
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
		entries: Option<BTreeMap<String, ArtifactState>>,
		pageable: bool,
	) -> DirectorySnapshot {
		let entries = entries.map(|entries| {
			let mut snapshot = Vec::with_capacity(entries.len() + 2);
			snapshot.push(DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: ".".to_owned(),
				named: None,
				node,
			});
			snapshot.push(DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: "..".to_owned(),
				named: None,
				node: parent,
			});
			for (name, artifact) in entries {
				let kind = Self::entry_kind_from_artifact(&artifact);
				snapshot.push(DirectorySnapshotEntry {
					artifact: Some(artifact),
					kind,
					name,
					named: None,
					node: 0,
				});
			}
			snapshot
		});
		let entries = entries.map(Arc::from);

		DirectorySnapshot {
			depth,
			entries,
			named: None,
			named_directory: false,
			node,
			pageable,
			parent,
		}
	}

	fn named_node_directory_snapshot(
		node: u64,
		parent: u64,
		depth: u64,
		named_node: Option<NamedNodeInfo>,
	) -> std::io::Result<DirectorySnapshot> {
		if named_node
			.as_ref()
			.is_some_and(|named_node| named_node.target.is_some())
		{
			return Err(std::io::Error::other("expected a directory"));
		}

		Ok(DirectorySnapshot {
			depth,
			entries: None,
			named: named_node,
			named_directory: true,
			node,
			pageable: true,
			parent,
		})
	}

	fn named_node_child_snapshot_entry(
		parent: Option<&tg::Specifier>,
		child: NamedNodeChild,
	) -> std::io::Result<DirectorySnapshotEntry> {
		let specifier = match parent {
			Some(parent) => format!("{parent}/{}", child.name),
			None => child.name.to_string(),
		}
		.parse()
		.map_err(std::io::Error::other)?;
		let kind = if child.target.is_some() {
			vfs::EntryKind::Symlink
		} else {
			vfs::EntryKind::Directory
		};
		let named_node = NamedNodeInfo {
			specifier,
			suffix: None,
			target: child.target,
		};
		let entry = DirectorySnapshotEntry {
			artifact: None,
			kind,
			name: child.name.to_string(),
			named: Some(named_node),
			node: 0,
		};

		Ok(entry)
	}

	async fn should_page_directory_inner(&self, artifact: &ArtifactState) -> std::io::Result<bool> {
		let (directory, _) = self.directory_node_inner(artifact).await?;

		Ok(Self::directory_requires_paging(&directory))
	}

	fn should_page_directory_sync_inner(
		&self,
		artifact: &ArtifactState,
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

		self.authorize_inner(&file_handle.tokens, &file_handle.blob.clone().into())
			.await?;

		// Create the stream.
		let mut tokens = tg::authorization::Tokens::with_authorization(
			file_handle.tokens.lock().unwrap().clone(),
		);
		tokens.inherit_with_resource(
			&self.remote_tokens.lock().unwrap(),
			Some(&file_handle.blob.clone().into()),
		);
		let options = tg::read::Options {
			length: Some(length),
			position: Some(std::io::SeekFrom::Start(position)),
			size: None,
		};
		let arg = tg::read::Arg {
			blob: file_handle.blob.clone(),
			options,
			tokens,
		};
		let stream = self
			.session()
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
			&file_handle.tokens,
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
		self.readdir_sync_inner(handle, offset, length, None)
	}

	fn readdir_sync_inner(
		&self,
		handle: u64,
		offset: u64,
		length: u64,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, u64, vfs::EntryKind)>> {
		let snapshot = self.directory_handle(handle)?;

		self.read_directory_snapshot_sync_inner(&snapshot, offset, length, transaction)
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
		if let crate::cache::Cache::Lmdb(cache) = &self.server.cache {
			let transaction = cache.env().read_txn().map_err(|error| {
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
			entries.push((entry.name, entry.node, entry.kind));
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
			entries.push((entry.name, entry.node, entry.kind));
		}

		Ok(entries)
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
		if snapshot.named_directory {
			let mut entries = snapshot.virtual_entries(offset, limit);
			let position = offset.saturating_sub(2);
			let length = limit.saturating_sub(entries.len()).to_u64().unwrap();
			let parent = snapshot
				.named
				.as_ref()
				.map(|named_node| named_node.specifier.clone());
			let children = self
				.list_named_node_children(parent.clone(), position, length)
				.await?;
			for child in &children {
				if let Some(target) = &child.target {
					self.register_target_tokens(target)?;
				}
			}
			let children = children
				.into_iter()
				.map(|child| Self::named_node_child_snapshot_entry(parent.as_ref(), child))
				.collect::<std::io::Result<Vec<_>>>()?;
			entries.extend(children);

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
				named: None,
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
		if snapshot.named_directory {
			let mut entries = snapshot.virtual_entries(offset, limit);
			let position = offset.saturating_sub(2);
			let length = limit.saturating_sub(entries.len()).to_u64().unwrap();
			let parent = snapshot
				.named
				.as_ref()
				.map(|named_node| named_node.specifier.clone());
			let children = self.runtime.block_on(self.list_named_node_children(
				parent.clone(),
				position,
				length,
			))?;
			for child in &children {
				if let Some(target) = &child.target {
					self.register_target_tokens(target)?;
				}
			}
			let children = children
				.into_iter()
				.map(|child| Self::named_node_child_snapshot_entry(parent.as_ref(), child))
				.collect::<std::io::Result<Vec<_>>>()?;
			entries.extend(children);

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
				named: None,
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
				let artifact = if self
					.authorize(&artifact.tokens, &artifact.id.clone().into())
					.is_ok()
				{
					artifact.snapshot()
				} else {
					let parent = self
						.get(directory.node)
						.await?
						.artifact
						.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
					self.directory_lookup_entry_inner(&parent, &entry.name, None)
						.await?
						.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?
				};
				let attrs = self
					.compute_attrs_from_artifact_inner(Some(&artifact), directory.depth + 1)
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
			} else if let Some(named_node) = entry.named {
				let attrs = if let Some(target) = &named_node.target {
					self.register_target_tokens(target)?;
					let target = Self::build_tag_target(directory.depth + 1, &target.node, None);
					let size = target.len().to_u64().unwrap();
					vfs::Attrs::new(vfs::AttrsInner::Symlink { size }).cacheable(false)
				} else {
					vfs::Attrs::new(vfs::AttrsInner::Directory).cacheable(false)
				};
				let node = self.nodes.get_or_insert_named_node_child(
					directory.node,
					&entry.name,
					directory.depth + 1,
					attrs,
					named_node,
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
		if let crate::cache::Cache::Lmdb(cache) = &self.server.cache {
			let transaction = cache.env().read_txn().map_err(|error| {
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
				let artifact = if self
					.authorize(&artifact.tokens, &artifact.id.clone().into())
					.is_ok()
				{
					artifact.snapshot()
				} else {
					let parent = self
						.get_sync(directory.node)?
						.artifact
						.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
					self.directory_lookup_entry_sync_inner(&parent, &entry.name, None, transaction)?
						.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?
				};
				let attrs = self.compute_attrs_from_artifact_sync_inner(
					Some(&artifact),
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
			} else if let Some(named_node) = entry.named {
				let attrs = if let Some(target) = &named_node.target {
					self.register_target_tokens(target)?;
					let target = Self::build_tag_target(directory.depth + 1, &target.node, None);
					let size = target.len().to_u64().unwrap();
					vfs::Attrs::new(vfs::AttrsInner::Symlink { size }).cacheable(false)
				} else {
					vfs::Attrs::new(vfs::AttrsInner::Directory).cacheable(false)
				};
				let node = self.nodes.get_or_insert_named_node_child(
					directory.node,
					&entry.name,
					directory.depth + 1,
					attrs,
					named_node,
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
			artifact,
			depth,
			named: named_node,
			..
		} = self.get(id).await.map_err(|error| {
			tracing::error!(%error, "failed to lookup node");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		if let Some(named_node) = named_node {
			// Keep the target from the lookup stable for the kernel's symlink cache.
			let Some(target) = named_node.target else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			self.register_target_tokens(&target)?;
			return Ok(Self::build_tag_target(
				depth,
				&target.node,
				named_node.suffix.as_deref(),
			));
		}
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
			Some(edge) => {
				let target =
					self.artifact_from_edge_inner(&artifact.tokens, edge, graph.as_ref(), None)?;
				self.register_dependency(id, &target)?;
				Some(target.id)
			},
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
			artifact,
			depth,
			named: named_node,
			..
		} = self.get_sync(id)?;
		if let Some(named_node) = named_node {
			// Keep the target from the lookup stable for the kernel's symlink cache.
			let Some(target) = named_node.target else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
			};
			self.register_target_tokens(&target)?;
			return Ok(Self::build_tag_target(
				depth,
				&target.node,
				named_node.suffix.as_deref(),
			));
		}
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
			Some(edge) => {
				let target = self.artifact_from_edge_inner(
					&artifact.tokens,
					edge,
					graph.as_ref(),
					transaction,
				)?;
				self.register_dependency(id, &target)?;
				Some(target.id)
			},
			None => None,
		};
		Self::build_symlink_target(depth, artifact, symlink.path)
	}

	pub fn remember_sync(&self, id: u64) {
		self.nodes.remember(id);
	}

	async fn get(&self, id: u64) -> std::io::Result<NodeInfo> {
		let node = self.nodes.get_sync(id)?;
		if let Some(artifact) = &node.artifact {
			self.authorize_inner(&artifact.tokens, &artifact.id.clone().into())
				.await?;
		}
		Ok(node)
	}

	fn get_sync(&self, id: u64) -> std::io::Result<NodeInfo> {
		let node = self.nodes.get_sync(id)?;
		if let Some(artifact) = &node.artifact {
			self.authorize_sync(&artifact.tokens, &artifact.id.clone().into())?;
		}
		Ok(node)
	}

	fn register_file_dependencies(
		&self,
		source: u64,
		artifact: &ArtifactState,
		dependencies: &BTreeMap<tg::Reference, Option<tg::graph::data::Dependency>>,
		graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<()> {
		for dependency in dependencies.values() {
			let Some(dependency) = self.file_dependency_artifact(
				&artifact.tokens,
				dependency.as_ref(),
				graph,
				transaction,
			)?
			else {
				continue;
			};
			self.register_dependency(source, &dependency)?;
		}
		Ok(())
	}

	fn register_dependency(&self, source: u64, dependency: &ArtifactState) -> std::io::Result<()> {
		self.nodes.insert_dependency(source, dependency);
		let tokens = dependency.tokens.lock().unwrap().clone();
		self.nodes
			.refresh_node_tokens(&self.session(), &tokens)
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		Ok(())
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

	fn build_tag_target(depth: u64, id: &tg::Id, suffix: Option<&str>) -> Bytes {
		let mut target = PathBuf::new();
		for _ in 0..depth.saturating_sub(1) {
			target.push("..");
		}
		let suffix = suffix.unwrap_or_default();
		target.push(format!("{id}{suffix}"));
		target.as_os_str().as_bytes().to_vec().into()
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
		artifact: Option<&ArtifactState>,
		depth: u64,
	) -> std::io::Result<vfs::Attrs> {
		match artifact {
			Some(artifact) if matches!(artifact.id.kind(), tg::artifact::Kind::File) => {
				let (file, _) = self.file_node_inner(artifact).await?;
				let size = if let Some(contents) = file.contents.as_ref() {
					self.blob_length_inner(&artifact.tokens, contents).await?
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
					Some(edge) => Some(
						self.artifact_from_edge_inner(
							&artifact.tokens,
							edge,
							graph.as_ref(),
							None,
						)?
						.id,
					),
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
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		edge: tg::graph::data::Edge<tg::directory::Id>,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => {
				let graph =
					default_graph.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOSYS))?;
				let data = self.graph_data_sync_inner(tokens, graph, transaction)?;
				let node = data
					.nodes
					.get(index)
					.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
				let pointer = tg::graph::data::Pointer {
					graph: graph.clone(),
					index,
					kind: node.kind(),
				};
				self.artifact_from_pointer_inner(
					tokens,
					&pointer,
					Some(tg::artifact::Kind::Directory),
				)
			},
			tg::graph::data::Edge::Object(directory) => {
				Ok(Self::artifact(directory.into(), tokens))
			},
			tg::graph::data::Edge::Pointer(pointer) => self.artifact_from_pointer_inner(
				tokens,
				&pointer,
				Some(tg::artifact::Kind::Directory),
			),
		}
	}

	fn artifact_from_edge_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		edge: tg::graph::data::Edge<tg::artifact::Id>,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<ArtifactState> {
		match edge {
			tg::graph::data::Edge::Index(index) => {
				let graph =
					default_graph.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOSYS))?;
				let data = self.graph_data_sync_inner(tokens, graph, transaction)?;
				let node = data
					.nodes
					.get(index)
					.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EIO))?;
				let pointer = tg::graph::data::Pointer {
					graph: graph.clone(),
					index,
					kind: node.kind(),
				};
				self.artifact_from_pointer_inner(tokens, &pointer, None)
			},
			tg::graph::data::Edge::Object(id) => Ok(Self::artifact(id, tokens)),
			tg::graph::data::Edge::Pointer(pointer) => {
				self.artifact_from_pointer_inner(tokens, &pointer, None)
			},
		}
	}

	fn artifact_from_pointer_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		pointer: &tg::graph::data::Pointer,
		expected_kind: Option<tg::artifact::Kind>,
	) -> std::io::Result<ArtifactState> {
		if let Some(expected_kind) = expected_kind
			&& pointer.kind != expected_kind
		{
			tracing::error!(kind = ?pointer.kind, expected = ?expected_kind, "invalid pointer kind");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		}
		let graph = pointer.graph.clone();
		let pointer = pointer.clone();
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
		let authorization = self.authorize(tokens, &graph.clone().into())?;
		let mut artifact = Self::artifact(id, tokens);
		artifact.data = Some(data.clone());
		{
			let graph_tokens = tokens
				.lock()
				.unwrap()
				.iter()
				.filter(|token| token.body.resource == graph.clone().into())
				.cloned()
				.collect::<Vec<_>>();
			Self::insert_tokens(&mut artifact.tokens.lock().unwrap(), &graph_tokens);
		}
		self.register_data(
			Some(&artifact.children_expires_at),
			&artifact.tokens,
			&artifact.id.clone().into(),
			authorization,
			&data.into(),
		)?;
		// Retain the pointer token on the source so later traversals can reuse it.
		let incoming = artifact.tokens.lock().unwrap().clone();
		Self::insert_tokens(&mut tokens.lock().unwrap(), &incoming);
		Ok(artifact)
	}

	async fn artifact_data_inner(
		&self,
		artifact: &ArtifactState,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		let authorization = self
			.authorize_inner(&artifact.tokens, &artifact.id.clone().into())
			.await?;
		let data = artifact.data.clone();
		if let Some(data) = data {
			self.register_data(
				Some(&artifact.children_expires_at),
				&artifact.tokens,
				&artifact.id.clone().into(),
				authorization,
				&data.clone().into(),
			)?;
			return Ok(data);
		}
		let id: tg::object::Id = artifact.id.clone().into();
		let Some(data) = self
			.try_get_data_inner(Some(&artifact.children_expires_at), &artifact.tokens, &id)
			.await?
		else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(artifact = %artifact.id, "expected artifact data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	async fn directory_entries_inner(
		&self,
		directory: &ArtifactState,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let tokens = directory.tokens.clone();
			let (directory_data, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact =
							self.artifact_from_edge_inner(&tokens, edge, graph.as_ref(), None)?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = self.artifact_from_directory_edge_inner(
							&tokens,
							child.directory,
							graph.as_ref(),
							None,
						)?;
						let artifact = self.branch_child(&directory, artifact)?;
						stack.push((artifact, graph.clone()));
					}
				},
			}
		}
		Ok(entries)
	}

	async fn directory_entries_range_inner(
		&self,
		directory: &ArtifactState,
		default_graph: Option<&tg::graph::Id>,
		offset: u64,
		limit: usize,
	) -> std::io::Result<Vec<(String, ArtifactState)>> {
		let mut entries = Vec::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned(), offset)];
		while entries.len() < limit {
			let Some((directory, default_graph, offset)) = stack.pop() else {
				break;
			};
			let tokens = directory.tokens.clone();
			let (directory_data, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let offset = offset.to_usize().unwrap_or(usize::MAX);
					let limit = limit.saturating_sub(entries.len());
					for (name, edge) in leaf.entries.into_iter().skip(offset).take(limit) {
						let artifact =
							self.artifact_from_edge_inner(&tokens, edge, graph.as_ref(), None)?;
						entries.push((name, artifact));
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					let limit = limit.saturating_sub(entries.len()).to_u64().unwrap();
					let mut children = Vec::new();
					for (edge, offset) in
						Self::directory_children_range(branch.children, offset, limit)
					{
						let artifact = self.artifact_from_directory_edge_inner(
							&tokens,
							edge,
							graph.as_ref(),
							None,
						)?;
						let artifact = self.branch_child(&directory, artifact)?;
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
		directory: &ArtifactState,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
	) -> std::io::Result<Option<ArtifactState>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let tokens = directory.tokens.clone();
			let (directory_data, graph) = self.directory_node_inner(&directory).await?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact =
						self.artifact_from_edge_inner(&tokens, edge, graph.as_ref(), None)?;
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
					let artifact = self.artifact_from_directory_edge_inner(
						&tokens,
						child.directory,
						graph.as_ref(),
						None,
					)?;
					directory = self.branch_child(&directory, artifact)?;
					default_graph = graph;
				},
			}
		}
	}

	fn branch_child(
		&self,
		parent: &ArtifactState,
		child: ArtifactState,
	) -> std::io::Result<ArtifactState> {
		// Reuse the child so that the child tokens it registers persist across traversals.
		let parent_tokens = child.tokens.lock().unwrap().clone();
		let mut children = parent.branch_children.lock().unwrap();
		let entry = children
			.entry(child.id.clone())
			.or_insert_with(|| BranchChild {
				artifact: child,
				parent_tokens: parent_tokens.clone(),
			});

		// Replace the expired tokens when the parent renews its tokens for the child.
		if entry.parent_tokens != parent_tokens {
			let now = self
				.server
				.clock
				.unix_timestamp()
				.map_err(|error| Self::map_cache_sync_error(&error))?;
			let mut tokens = entry.artifact.tokens.lock().unwrap();
			tokens.retain(|token| token.body.expires_at >= now);
			Self::insert_tokens(&mut tokens, &parent_tokens);
			drop(tokens);
			entry.parent_tokens = parent_tokens;
		}

		Ok(entry.artifact.clone())
	}

	async fn directory_node_inner(
		&self,
		directory: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let tokens = directory.tokens.clone();
		let data = self.artifact_data_inner(directory).await?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&tokens, &pointer).await?;
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
		file: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let tokens = file.tokens.clone();
		let data = self.artifact_data_inner(file).await?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&tokens, &pointer).await?;
				let tg::graph::data::Node::File(node) = node else {
					tracing::error!(pointer = ?pointer, "expected file node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	async fn graph_data_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		graph: &tg::graph::Id,
	) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let Some(data) = self.try_get_data_inner(None, tokens, &id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		data.try_into().map_err(|_| {
			tracing::error!(%graph, "expected graph data");
			std::io::Error::from_raw_os_error(libc::EIO)
		})
	}

	async fn resolve_graph_node_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		pointer: &tg::graph::data::Pointer,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = pointer.graph.clone();
		let graph_data = self.graph_data_inner(tokens, &graph).await?;
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
		symlink: &ArtifactState,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let tokens = symlink.tokens.clone();
		let data = self.artifact_data_inner(symlink).await?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) = self.resolve_graph_node_inner(&tokens, &pointer).await?;
				let tg::graph::data::Node::Symlink(node) = node else {
					tracing::error!(pointer = ?pointer, "expected symlink node in the graph");
					return Err(std::io::Error::from_raw_os_error(libc::EIO));
				};
				Ok((node, Some(graph)))
			},
		}
	}

	fn artifact(
		id: tg::artifact::Id,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
	) -> ArtifactState {
		let resource = tg::Id::from(id.clone());
		let tokens = tokens
			.lock()
			.unwrap()
			.iter()
			.filter(|token| token.body.resource == resource)
			.cloned()
			.collect::<Vec<_>>();
		ArtifactState {
			branch_children: Arc::default(),
			children_expires_at: Arc::default(),
			data: None,
			id,
			tokens: Arc::new(Mutex::new(tokens)),
		}
	}

	fn register_output(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		output: &tg::object::get::Output,
	) {
		self.inherit_remote_tokens(&output.tokens);
		for child in output.children.values() {
			self.inherit_remote_tokens(&child.tokens);
		}
		let incoming = output
			.tokens
			.local_authorization()
			.iter()
			.chain(
				output
					.children
					.values()
					.flat_map(|child| child.tokens.local_authorization()),
			)
			.cloned()
			.collect::<Vec<_>>();
		Self::insert_tokens(&mut tokens.lock().unwrap(), &incoming);
	}

	fn insert_tokens(
		tokens: &mut Vec<tg::authorization::Token>,
		incoming: &[tg::authorization::Token],
	) {
		let mut entry = tg::authorization::tokens::Entry {
			authorization: std::mem::take(tokens),
		};
		let incoming = tg::authorization::tokens::Entry {
			authorization: incoming.to_vec(),
		};
		entry.inherit(&incoming);
		*tokens = entry.authorization;
	}

	fn register_data(
		&self,
		children_expires_at: Option<&Mutex<Option<i64>>>,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
		authorization: crate::authorization::Output,
		data: &tg::object::Data,
	) -> std::io::Result<()> {
		let now = self
			.server
			.clock
			.unix_timestamp()
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		if children_expires_at.is_some_and(|expires_at| {
			expires_at
				.lock()
				.unwrap()
				.is_some_and(|expires_at| expires_at >= now)
		}) {
			return Ok(());
		}
		let mut children = std::collections::BTreeSet::new();
		data.children(&mut children);
		let current = tokens.lock().unwrap();
		let resources = current
			.iter()
			.filter(|token| token.body.expires_at >= now)
			.map(|token| &token.body.resource)
			.collect::<std::collections::BTreeSet<_>>();
		let has_token = |id: &tg::object::Id| resources.contains(&tg::Id::from(id.clone()));
		if has_token(id) && children.iter().all(has_token) {
			if let Some(expires_at) = children_expires_at {
				*expires_at.lock().unwrap() =
					current.iter().map(|token| token.body.expires_at).min();
			}
			return Ok(());
		}
		let parent = (!has_token(id)).then_some(id);
		drop(current);
		let expires_at = self.token_expiration(authorization.expires_at)?;
		let session = self.session();
		let mut incoming = Vec::new();
		for id in children.iter().chain(parent) {
			if let Some(token) = session
				.create_token(
					id.clone().into(),
					authorization.permissions.iter().collect(),
					expires_at,
				)
				.map_err(|error| Self::map_cache_sync_error(&error))?
			{
				incoming.push(token);
			}
		}
		let mut tokens = tokens.lock().unwrap();
		tokens.retain(|token| token.body.expires_at >= now);
		Self::insert_tokens(&mut tokens, &incoming);
		if let Some(expires_at) = children_expires_at {
			*expires_at.lock().unwrap() = tokens.iter().map(|token| token.body.expires_at).min();
		}
		Ok(())
	}

	async fn try_get_data_inner(
		&self,
		children_expires_at: Option<&Mutex<Option<i64>>>,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
	) -> std::io::Result<Option<tg::object::Data>> {
		let authorization = self.authorize_inner(tokens, id).await?;
		if let Some(output) = self
			.server
			.try_get_object_local(id, false)
			.await
			.map_err(|error| Self::map_cache_sync_error(&error))?
		{
			let data = tg::object::Data::deserialize(id.kind(), output.bytes)
				.map_err(|error| Self::map_cache_sync_error(&error))?;
			self.register_data(children_expires_at, tokens, id, authorization, &data)?;
			return Ok(Some(data));
		}
		let mut request_tokens =
			tg::authorization::Tokens::with_authorization(tokens.lock().unwrap().clone());
		request_tokens.inherit_with_resource(
			&self.remote_tokens.lock().unwrap(),
			Some(&id.clone().into()),
		);
		let arg = tg::object::get::Arg {
			tokens: request_tokens,
			..Default::default()
		};
		let output = self
			.session()
			.try_get_object(id, arg)
			.await
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		let Some(output) = output else {
			return Ok(None);
		};
		self.register_output(tokens, &output);
		let data = tg::object::Data::deserialize(id.kind(), output.bytes)
			.map_err(|error| Self::map_cache_sync_error(&error))?;
		Ok(Some(data))
	}

	async fn blob_length_inner(
		&self,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::blob::Id,
	) -> std::io::Result<u64> {
		let id: tg::object::Id = id.clone().into();
		let arg = crate::cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		let object = self
			.server
			.cache
			.try_get_object(arg)
			.await
			.map_err(|error| {
				tracing::error!(error = %error.trace(), %id, "failed to get the object");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?
			.object;
		if let Some(object) = object {
			if let Some(length) = object.length {
				return Ok(length);
			}
			if let Some(length) = object.checkout_pointer.map(|pointer| pointer.length) {
				return Ok(length);
			}
			let Some(bytes) = object.bytes else {
				return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
			};
			let data = tg::object::Data::deserialize(id.kind(), &*bytes).map_err(|error| {
				tracing::error!(error = %error.trace(), %id, "failed to deserialize the object data");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			return Self::blob_length_from_data(&id, data);
		}
		let Some(data) = self.try_get_data_inner(None, tokens, &id).await? else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
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
		artifact: &ArtifactState,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::artifact::data::Artifact> {
		let authorization = self.authorize_sync(&artifact.tokens, &artifact.id.clone().into())?;
		let data = artifact.data.clone();
		if let Some(data) = data {
			self.register_data(
				Some(&artifact.children_expires_at),
				&artifact.tokens,
				&artifact.id.clone().into(),
				authorization,
				&data.clone().into(),
			)?;
			return Ok(data);
		}
		let id: tg::object::Id = artifact.id.clone().into();
		let output = self.try_get_data(
			Some(&artifact.children_expires_at),
			&artifact.tokens,
			&id,
			transaction,
		)?;
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
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		graph: &tg::graph::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<tg::graph::Data> {
		let id: tg::object::Id = graph.clone().into();
		let output = self.try_get_data(None, tokens, &id, transaction)?;
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
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		pointer: &tg::graph::data::Pointer,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Node, tg::graph::Id)> {
		let graph = pointer.graph.clone();
		let graph_data = self.graph_data_sync_inner(tokens, &graph, transaction)?;
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
		if let Some(length) = object.length {
			return Ok(length);
		}
		if let Some(checkout_pointer) = object.checkout_pointer {
			return Ok(checkout_pointer.length);
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
		directory: &ArtifactState,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Directory, Option<tg::graph::Id>)> {
		let tokens = directory.tokens.clone();
		let data = self.artifact_data_sync_inner(directory, transaction)?;
		let tg::artifact::data::Artifact::Directory(directory) = data else {
			tracing::error!("expected directory data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match directory {
			tg::directory::Data::Node(node) => Ok((node, None)),
			tg::directory::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&tokens, &pointer, transaction)?;
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
		tokens: &Mutex<Vec<tg::authorization::Token>>,
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
		let authorization = self.authorize_sync(tokens, &object_id)?;
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
			self.register_data(None, tokens, &object_id, authorization, &data)?;
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
							tokens,
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
		let Some(checkout_pointer) = object.checkout_pointer else {
			return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
		};
		if position >= checkout_pointer.length {
			return Ok(());
		}
		let read_length = std::cmp::min(length, checkout_pointer.length - position);
		let mut path = self
			.server
			.checkout_path()
			.join(checkout_pointer.artifact.to_string());
		if let Some(path_) = checkout_pointer.path {
			path.push(path_);
		}
		let file = match std::fs::File::open(&path) {
			Ok(file) => file,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Err(std::io::Error::from_raw_os_error(libc::ENOSYS));
			},
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open a checkout file");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			},
		};
		let file_position = checkout_pointer
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
		file: &ArtifactState,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::File, Option<tg::graph::Id>)> {
		let tokens = file.tokens.clone();
		let data = self.artifact_data_sync_inner(file, transaction)?;
		let tg::artifact::data::Artifact::File(file) = data else {
			tracing::error!("expected file data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match file {
			tg::file::Data::Node(node) => Ok((node, None)),
			tg::file::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&tokens, &pointer, transaction)?;
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
		symlink: &ArtifactState,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<(tg::graph::data::Symlink, Option<tg::graph::Id>)> {
		let tokens = symlink.tokens.clone();
		let data = self.artifact_data_sync_inner(symlink, transaction)?;
		let tg::artifact::data::Artifact::Symlink(symlink) = data else {
			tracing::error!("expected symlink data");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		match symlink {
			tg::symlink::Data::Node(node) => Ok((node, None)),
			tg::symlink::Data::Pointer(pointer) => {
				let (node, graph) =
					self.resolve_graph_node_sync_inner(&tokens, &pointer, transaction)?;
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
		directory: &ArtifactState,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<BTreeMap<String, ArtifactState>> {
		let mut entries = BTreeMap::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned())];
		while let Some((directory, default_graph)) = stack.pop() {
			let tokens = directory.tokens.clone();
			let (directory_data, graph) =
				self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					for (name, edge) in leaf.entries {
						let artifact = self.artifact_from_edge_inner(
							&tokens,
							edge,
							graph.as_ref(),
							transaction,
						)?;
						entries.insert(name, artifact);
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					for child in branch.children.into_iter().rev() {
						let artifact = self.artifact_from_directory_edge_inner(
							&tokens,
							child.directory,
							graph.as_ref(),
							transaction,
						)?;
						let artifact = self.branch_child(&directory, artifact)?;
						stack.push((artifact, graph.clone()));
					}
				},
			}
		}
		Ok(entries)
	}

	fn directory_entries_range_sync_inner(
		&self,
		directory: &ArtifactState,
		default_graph: Option<&tg::graph::Id>,
		offset: u64,
		limit: usize,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Vec<(String, ArtifactState)>> {
		let mut entries = Vec::new();
		let mut stack = vec![(directory.clone(), default_graph.cloned(), offset)];
		while entries.len() < limit {
			let Some((directory, default_graph, offset)) = stack.pop() else {
				break;
			};
			let tokens = directory.tokens.clone();
			let (directory_data, graph) =
				self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let offset = offset.to_usize().unwrap_or(usize::MAX);
					let limit = limit.saturating_sub(entries.len());
					for (name, edge) in leaf.entries.into_iter().skip(offset).take(limit) {
						let artifact = self.artifact_from_edge_inner(
							&tokens,
							edge,
							graph.as_ref(),
							transaction,
						)?;
						entries.push((name, artifact));
					}
				},
				tg::graph::data::Directory::Branch(branch) => {
					let limit = limit.saturating_sub(entries.len()).to_u64().unwrap();
					let mut children = Vec::new();
					for (edge, offset) in
						Self::directory_children_range(branch.children, offset, limit)
					{
						let artifact = self.artifact_from_directory_edge_inner(
							&tokens,
							edge,
							graph.as_ref(),
							transaction,
						)?;
						let artifact = self.branch_child(&directory, artifact)?;
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
		directory: &ArtifactState,
		name: &str,
		default_graph: Option<&tg::graph::Id>,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<ArtifactState>> {
		let mut directory = directory.clone();
		let mut default_graph = default_graph.cloned();
		loop {
			let tokens = directory.tokens.clone();
			let (directory_data, graph) =
				self.directory_node_sync_inner(&directory, transaction)?;
			let graph = graph.or(default_graph);
			match directory_data {
				tg::graph::data::Directory::Leaf(leaf) => {
					let Some(edge) = leaf.entries.get(name).cloned() else {
						return Ok(None);
					};
					let artifact =
						self.artifact_from_edge_inner(&tokens, edge, graph.as_ref(), transaction)?;
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
					let artifact = self.artifact_from_directory_edge_inner(
						&tokens,
						child.directory,
						graph.as_ref(),
						transaction,
					)?;
					directory = self.branch_child(&directory, artifact)?;
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
		artifact: Option<&ArtifactState>,
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
					Some(edge) => Some(
						self.artifact_from_edge_inner(
							&artifact.tokens,
							edge,
							graph.as_ref(),
							transaction,
						)?
						.id,
					),
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
		let Some(checkout_pointer) = object.checkout_pointer else {
			return Ok(None);
		};
		if checkout_pointer.position != 0 {
			return Ok(None);
		}
		let mut path = self
			.server
			.checkout_path()
			.join(checkout_pointer.artifact.to_string());
		if let Some(path_) = checkout_pointer.path {
			path.push(path_);
		}
		match std::fs::File::open(&path) {
			Ok(file) => Ok(Some(file.into())),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
			Err(error) => {
				tracing::error!(%error, path = %path.display(), "failed to open a checkout file");
				Err(std::io::Error::from_raw_os_error(libc::EIO))
			},
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<tangram_cache::object::Object<'static>>> {
		#[cfg(feature = "lmdb")]
		if let (crate::cache::Cache::Lmdb(cache), Some(transaction)) =
			(&self.server.cache, transaction)
		{
			let arg = crate::cache::object::get::Arg {
				bytes: true,
				id: id.clone(),
				put: None,
			};
			return cache
				.try_get_object_with_transaction(transaction, &arg)
				.map(|output| output.object)
				.map_err(|error| Self::map_cache_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		let arg = crate::cache::object::get::Arg {
			bytes: true,
			id: id.clone(),
			put: None,
		};
		self.server
			.cache
			.try_get_object_sync(&arg)
			.map(|output| output.object)
			.map_err(|error| Self::map_cache_sync_error(&error))
	}

	fn try_get_data(
		&self,
		children_expires_at: Option<&Mutex<Option<i64>>>,
		tokens: &Mutex<Vec<tg::authorization::Token>>,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		let authorization = self.authorize_sync(tokens, id)?;
		let output = self.try_get_data_local(id, transaction)?;
		if let Some((_, data)) = &output {
			self.register_data(children_expires_at, tokens, id, authorization, data)?;
		}
		Ok(output)
	}

	fn try_get_data_local(
		&self,
		id: &tg::object::Id,
		transaction: Option<&Transaction<'_>>,
	) -> std::io::Result<Option<(u64, tg::object::Data)>> {
		#[cfg(feature = "lmdb")]
		if let (crate::cache::Cache::Lmdb(cache), Some(transaction)) =
			(&self.server.cache, transaction)
		{
			return cache
				.try_get_object_data_with_transaction(transaction, id)
				.map_err(|error| Self::map_cache_sync_error(&error));
		}

		#[cfg(not(feature = "lmdb"))]
		let _ = transaction;

		self.server
			.cache
			.try_get_object_data_sync(id)
			.map_err(|error| Self::map_cache_sync_error(&error))
	}

	fn map_cache_sync_error(error: &tg::Error) -> std::io::Error {
		tracing::error!(error = %error.trace(), "failed to access local object data");
		std::io::Error::from_raw_os_error(libc::EIO)
	}

	fn attrs_from_artifact(artifact: Option<&ArtifactState>) -> Option<vfs::Attrs> {
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

	fn entry_kind_from_artifact(artifact: &ArtifactState) -> vfs::EntryKind {
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

impl ArtifactState {
	fn snapshot(&self) -> Self {
		Self {
			branch_children: Arc::default(),
			children_expires_at: Arc::default(),
			data: self.data.clone(),
			id: self.id.clone(),
			tokens: Arc::new(Mutex::new(self.tokens.lock().unwrap().clone())),
		}
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
			named: self.named.clone(),
			named_directory: self.named_directory,
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
				named: None,
				node: self.node,
			},
			DirectorySnapshotEntry {
				artifact: None,
				kind: vfs::EntryKind::Directory,
				name: "..".to_owned(),
				named: None,
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
			weight = weight.saturating_add(DIRECTORY_SNAPSHOT_ENTRY_OVERHEAD);
		}

		weight
	}
}

impl Weak {
	#[must_use]
	pub fn upgrade(&self) -> Option<Provider> {
		self.0.upgrade().map(Provider)
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
		let entry = Node {
			artifact: None,
			attrs: Some(vfs::Attrs::new(vfs::AttrsInner::Directory)),
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth: 0,
			lookup_count: u64::MAX,
			name: None,
			named: None,
			parent: vfs::ROOT_NODE_ID,
			tokens: Vec::new(),
		};
		nodes.insert(vfs::ROOT_NODE_ID, entry);
		let state = Mutex::new(State { next: 1000, nodes });
		Self { state }
	}

	fn insert_tokens(
		&self,
		session: &Session,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<()> {
		let mut state = self.state.lock().unwrap();
		let now = session.server.clock.unix_timestamp()?;
		state
			.nodes
			.get_mut(&vfs::ROOT_NODE_ID)
			.unwrap()
			.tokens
			.retain(|token| token.body.expires_at >= now);
		Provider::insert_tokens(
			&mut state.nodes.get_mut(&vfs::ROOT_NODE_ID).unwrap().tokens,
			tokens.local_authorization(),
		);
		drop(state);
		self.refresh_node_tokens(session, tokens.local_authorization())?;
		Ok(())
	}

	fn refresh_node_tokens(
		&self,
		session: &Session,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<()> {
		let now = session.server.clock.unix_timestamp()?;
		let state = self.state.lock().unwrap();
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let mut pending = Vec::new();
		for token in tokens.iter().filter(|token| token.body.authorizes(subtree)) {
			let name = token.body.resource.to_string();
			for (component, id) in state.nodes[&vfs::ROOT_NODE_ID]
				.children
				.range(name.clone()..)
			{
				if component != &name
					&& !component
						.strip_prefix(&name)
						.is_some_and(|suffix| suffix.starts_with('.'))
				{
					break;
				}
				pending.push((*id, token.clone()));
			}
		}
		while let Some((id, token)) = pending.pop() {
			let node = state.nodes.get(&id).unwrap();
			let incoming = [token.clone()];
			for dependency in &node.dependencies {
				if token.body.resource == dependency.id.clone().into() {
					let mut tokens = dependency.tokens.lock().unwrap();
					tokens.retain(|token| token.body.expires_at >= now);
					Provider::insert_tokens(&mut tokens, &incoming);
				}
			}
			let Some(artifact) = &node.artifact else {
				continue;
			};
			if token.body.resource != artifact.id.clone().into() {
				continue;
			}
			let expires_at = token.body.expires_at;
			let mut tokens = artifact.tokens.lock().unwrap();
			let improved = !tokens
				.iter()
				.any(|existing| existing.covers(&token) && existing.body.expires_at >= expires_at);
			if improved {
				Self::refresh_tokens(session, &mut tokens, expires_at)?;
			}
			Provider::insert_tokens(&mut tokens, &incoming);
			if !improved {
				continue;
			}
			for dependency in &node.dependencies {
				Self::refresh_tokens(session, &mut dependency.tokens.lock().unwrap(), expires_at)?;
				if let Some(dependency_id) = state.nodes[&vfs::ROOT_NODE_ID]
					.children
					.get(&dependency.id.to_string())
					&& *dependency_id != id
					&& let Some(token) = session.create_token(
						dependency.id.clone().into(),
						vec![subtree],
						expires_at,
					)? {
					pending.push((*dependency_id, token));
				}
			}
			for child in node.children.values() {
				let child_node = &state.nodes[child];
				if child_node.parent != id {
					continue;
				}
				let Some(artifact) = &child_node.artifact else {
					continue;
				};
				if let Some(token) =
					session.create_token(artifact.id.clone().into(), vec![subtree], expires_at)?
				{
					pending.push((*child, token));
				}
			}
		}
		Ok(())
	}

	fn refresh_tokens(
		session: &Session,
		tokens: &mut Vec<tg::authorization::Token>,
		expires_at: i64,
	) -> tg::Result<()> {
		let mut renewed = Vec::new();
		for token in tokens.iter() {
			if token.body.expires_at >= expires_at {
				renewed.push(token.clone());
				continue;
			}
			if let Some(token) = session.create_token(
				token.body.resource.clone(),
				token.body.permissions.clone(),
				expires_at,
			)? {
				renewed.push(token);
			}
		}
		*tokens = renewed;
		Ok(())
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
				let artifact = artifact.snapshot();
				let tokens = root
					.tokens
					.iter()
					.filter(|token| token.body.resource == id.clone().into())
					.cloned()
					.collect::<Vec<_>>();
				Provider::insert_tokens(&mut artifact.tokens.lock().unwrap(), &tokens);
				return artifact;
			}
		}
		let tokens = root
			.tokens
			.iter()
			.filter(|token| token.body.resource == id.clone().into())
			.cloned()
			.collect::<Vec<_>>();
		ArtifactState {
			branch_children: Arc::default(),
			children_expires_at: Arc::default(),
			data: None,
			id: id.clone(),
			tokens: Arc::new(Mutex::new(tokens)),
		}
	}

	fn insert_dependency(&self, source: u64, artifact: &ArtifactState) {
		let mut state = self.state.lock().unwrap();
		let Some(node) = state.nodes.get_mut(&source) else {
			return;
		};
		let dependency = artifact.snapshot();
		if let Some(existing) = node
			.dependencies
			.iter_mut()
			.find(|dependency| dependency.id == artifact.id)
		{
			Provider::insert_tokens(
				&mut existing.tokens.lock().unwrap(),
				&dependency.tokens.lock().unwrap(),
			);
		} else {
			node.dependencies.push(dependency);
		}
		state
			.nodes
			.get_mut(&vfs::ROOT_NODE_ID)
			.unwrap()
			.children
			.entry(artifact.id.to_string())
			.or_insert(source);
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		Ok(self.lookup_sync(parent, name))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		self.lookup_parent_sync(id)
	}

	fn lookup_sync(&self, parent: u64, name: &str) -> Option<u64> {
		let state = self.state.lock().unwrap();
		let id = *state.nodes.get(&parent)?.children.get(name)?;
		let node = state.nodes.get(&id)?;
		(node.parent == parent && node.name.as_deref() == Some(name)).then_some(id)
	}

	fn lookup_and_remember_sync(&self, parent: u64, name: &str) -> Option<u64> {
		let mut state = self.state.lock().unwrap();
		let id = *state.nodes.get(&parent)?.children.get(name)?;
		let node = state.nodes.get_mut(&id)?;
		if node.parent != parent || node.name.as_deref() != Some(name) {
			return None;
		}
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
				named: node.named.clone(),
				parent: node.parent,
			})
			.ok_or_else(|| {
				tracing::error!(%id, "node not found");
				std::io::Error::from_raw_os_error(libc::ENOENT)
			})
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
			removed.push(id);

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
			if let Some(existing) = &state.nodes[&id].artifact {
				let incoming = artifact.tokens.lock().unwrap().clone();
				Provider::insert_tokens(&mut existing.tokens.lock().unwrap(), &incoming);
			}
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
		let entry = Node {
			artifact: Some(artifact),
			attrs,
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth,
			lookup_count: u64::from(remember),
			name: Some(name.to_owned()),
			named: None,
			parent,
			tokens: Vec::new(),
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
		// Allocate a new inode when the target changes because the kernel caches symlink contents.
		if let Some(id) = state
			.nodes
			.get(&parent)
			.and_then(|node| node.children.get(name).copied())
			&& state.nodes[&id].named.as_ref().is_some_and(|existing| {
				existing.suffix == named_node.suffix
					&& existing.target.as_ref().map(|target| &target.node)
						== named_node.target.as_ref().map(|target| &target.node)
			}) {
			let node = state.nodes.get_mut(&id).unwrap();
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
			artifact: None,
			attrs: Some(attrs),
			children: BTreeMap::new(),
			dependencies: Vec::new(),
			depth,
			lookup_count: u64::from(remember),
			name: Some(name.to_owned()),
			named: Some(named_node),
			parent,
			tokens: Vec::new(),
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

fn named_node_error(error: &tg::Error) -> std::io::Error {
	tracing::error!(error = %error.trace(), "failed to access a named node");
	std::io::Error::from_raw_os_error(libc::EIO)
}

impl Deref for Provider {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
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
		super::{ArtifactState, Nodes, Provider},
		std::sync::{Arc, Mutex},
		tangram_client::prelude::*,
		tangram_vfs as vfs,
	};

	#[test]
	fn forget_releases_tokens_and_dependencies() {
		let nodes = Nodes::new();
		let source = artifact(b"source");
		let source_id = source.id.clone();
		let source_tokens = Arc::downgrade(&source.tokens);
		let root_tokens = source.tokens.lock().unwrap().clone();
		nodes
			.state
			.lock()
			.unwrap()
			.nodes
			.get_mut(&vfs::ROOT_NODE_ID)
			.unwrap()
			.tokens = root_tokens;
		let inode = nodes
			.get_or_insert_child(
				vfs::ROOT_NODE_ID,
				&source_id.to_string(),
				source,
				1,
				None,
				true,
			)
			.unwrap();
		let dependency = artifact(b"target");
		let dependency_id = dependency.id.clone();
		nodes.insert_dependency(inode, &dependency);
		drop(dependency);
		let dependency = nodes.root_artifact(&dependency_id);
		assert!(!dependency.tokens.lock().unwrap().is_empty());
		drop(dependency);
		assert_eq!(nodes.forget(inode, 1), vec![inode]);
		assert!(source_tokens.upgrade().is_none());
		assert_eq!(nodes.state.lock().unwrap().nodes.len(), 1);
		assert!(
			nodes.state.lock().unwrap().nodes[&vfs::ROOT_NODE_ID]
				.children
				.is_empty()
		);
		assert!(
			nodes
				.root_artifact(&dependency_id)
				.tokens
				.lock()
				.unwrap()
				.is_empty()
		);

		let source = nodes.root_artifact(&source_id);
		assert!(!source.tokens.lock().unwrap().is_empty());
		let inode = nodes
			.get_or_insert_child(
				vfs::ROOT_NODE_ID,
				&source_id.to_string(),
				source,
				1,
				None,
				true,
			)
			.unwrap();
		assert_eq!(nodes.forget(inode, 1), vec![inode]);
		assert_eq!(nodes.state.lock().unwrap().nodes.len(), 1);
	}

	#[test]
	fn an_accessed_dependency_survives_forgetting_its_source() {
		let nodes = Nodes::new();
		let source = artifact(b"source");
		let source_id = source.id.to_string();
		let source = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &source_id, source, 1, None, true)
			.unwrap();
		let dependency = artifact(b"target");
		let dependency_id = dependency.id.clone();
		nodes.insert_dependency(source, &dependency);
		drop(dependency);
		let dependency = nodes.root_artifact(&dependency_id);
		let tokens = Arc::downgrade(&dependency.tokens);
		let dependency = nodes
			.get_or_insert_child(
				vfs::ROOT_NODE_ID,
				&dependency_id.to_string(),
				dependency,
				1,
				None,
				true,
			)
			.unwrap();
		assert_ne!(source, dependency);
		nodes.forget(source, 1);
		assert!(tokens.upgrade().is_some());
		assert_eq!(
			nodes.lookup_sync(vfs::ROOT_NODE_ID, &dependency_id.to_string()),
			Some(dependency)
		);
		nodes.forget(dependency, 1);
		assert!(tokens.upgrade().is_none());
		assert_eq!(nodes.state.lock().unwrap().nodes.len(), 1);
	}

	#[test]
	fn snapshots_do_not_retain_loaded_inode_tokens() {
		let nodes = Nodes::new();
		let artifact = artifact(b"snapshot");
		let name = artifact.id.to_string();
		let entries = std::collections::BTreeMap::from([(name.clone(), artifact)]);
		let snapshot = Provider::create_directory_snapshot(
			vfs::ROOT_NODE_ID,
			vfs::ROOT_NODE_ID,
			0,
			Some(entries),
			true,
		);
		let artifact = snapshot.entries.as_ref().unwrap()[2]
			.artifact
			.as_ref()
			.unwrap()
			.snapshot();
		let tokens = Arc::downgrade(&artifact.tokens);
		let inode = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, artifact, 1, None, true)
			.unwrap();
		nodes.forget(inode, 1);
		assert!(tokens.upgrade().is_none());
		assert!(snapshot.entries.as_ref().unwrap()[2].artifact.is_some());
	}

	#[test]
	fn forgetting_one_source_preserves_a_shared_dependency() {
		let nodes = Nodes::new();
		let first = artifact(b"first");
		let name = first.id.to_string();
		let first = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, first, 1, None, true)
			.unwrap();
		let second = artifact(b"second");
		let name = second.id.to_string();
		let second = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, second, 1, None, true)
			.unwrap();
		let dependency = artifact(b"shared");
		let id = dependency.id.clone();
		nodes.insert_dependency(first, &dependency);
		nodes.insert_dependency(second, &dependency);
		drop(dependency);
		nodes.forget(first, 1);
		assert!(!nodes.root_artifact(&id).tokens.lock().unwrap().is_empty());
		nodes.forget(second, 1);
		assert!(nodes.root_artifact(&id).tokens.lock().unwrap().is_empty());
		assert!(
			nodes.state.lock().unwrap().nodes[&vfs::ROOT_NODE_ID]
				.children
				.is_empty()
		);
	}

	#[test]
	fn a_forgotten_dependency_is_reloaded_from_its_live_source() {
		let nodes = Nodes::new();
		let source = artifact(b"source");
		let name = source.id.to_string();
		let source = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, source, 1, None, true)
			.unwrap();
		let dependency = artifact(b"target");
		let id = dependency.id.clone();
		nodes.insert_dependency(source, &dependency);
		drop(dependency);
		let dependency = nodes.root_artifact(&id);
		let tokens = Arc::downgrade(&dependency.tokens);
		let dependency = nodes
			.get_or_insert_child(
				vfs::ROOT_NODE_ID,
				&id.to_string(),
				dependency,
				1,
				None,
				true,
			)
			.unwrap();
		nodes.forget(dependency, 1);
		assert!(tokens.upgrade().is_none());
		assert!(!nodes.root_artifact(&id).tokens.lock().unwrap().is_empty());
		nodes.forget(source, 1);
		assert_eq!(nodes.state.lock().unwrap().nodes.len(), 1);
		assert!(
			nodes.state.lock().unwrap().nodes[&vfs::ROOT_NODE_ID]
				.children
				.is_empty()
		);
	}

	#[test]
	fn existing_children_inherit_tokens() {
		let nodes = Nodes::new();
		let original = artifact(b"tokens");
		original.tokens.lock().unwrap()[0].body.expires_at = 300;
		original.tokens.lock().unwrap()[0].body.permissions =
			vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)];
		let name = original.id.to_string();
		let mut expected = tg::authorization::tokens::Entry {
			authorization: original.tokens.lock().unwrap().clone(),
		};
		let inode = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, original, 1, None, true)
			.unwrap();
		let incoming = artifact(b"tokens");
		let entry = tg::authorization::tokens::Entry {
			authorization: incoming.tokens.lock().unwrap().clone(),
		};
		expected.inherit(&entry);
		let existing = nodes
			.get_or_insert_child(vfs::ROOT_NODE_ID, &name, incoming, 1, None, true)
			.unwrap();
		assert_eq!(inode, existing);
		let artifact = nodes.get_sync(inode).unwrap().artifact.unwrap();
		assert_eq!(*artifact.tokens.lock().unwrap(), expected.authorization);
	}

	fn artifact(bytes: &[u8]) -> ArtifactState {
		let id: tg::artifact::Id = tg::file::Id::new(bytes).into();
		let token = tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at: 100,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource: id.clone().into(),
			},
			metadata: tg::authorization::Metadata {
				algorithm: tg::authorization::Algorithm::Ed25519,
				key: "test".into(),
			},
			signature: vec![0; 64],
		};
		ArtifactState {
			branch_children: Arc::default(),
			children_expires_at: Arc::default(),
			data: None,
			id,
			tokens: Arc::new(Mutex::new(vec![token])),
		}
	}
}
