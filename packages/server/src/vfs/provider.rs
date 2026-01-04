use {
	crate::Server,
	bytes::Bytes,
	dashmap::DashMap,
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::{
		collections::HashMap,
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
#[cfg(feature = "sqlite")]
use {
	crate::temp::Temp,
	indoc::{formatdoc, indoc},
	rusqlite as sqlite,
	tangram_database::{self as db, prelude::*},
};

enum Nodes {
	#[cfg_attr(feature = "sqlite", expect(dead_code))]
	Memory(MemoryNodes),
	#[cfg(feature = "sqlite")]
	Sqlite(SqliteNodes),
}

struct MemoryNodes {
	nodes: DashMap<u64, MemoryNode, fnv::FnvBuildHasher>,
}

#[derive(Clone)]
struct MemoryNode {
	parent: u64,
	artifact: Option<tg::artifact::Id>,
	depth: u64,
	children: HashMap<String, u64, fnv::FnvBuildHasher>,
}

#[cfg(feature = "sqlite")]
struct SqliteNodes {
	database: Arc<db::sqlite::Database>,
	#[expect(dead_code)]
	temp: Temp,
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
			tracing::error!(?error, "failed to get file dependencies");
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
				tracing::error!(?error, "failed to get file dependencies");
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
				tracing::error!(?error, "failed to get file's module");
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
}

impl Provider {
	pub async fn new(server: &Server, options: crate::config::Vfs) -> tg::Result<Self> {
		// Create the cache.
		let cache = moka::sync::CacheBuilder::new(options.cache_size.to_u64().unwrap())
			.time_to_idle(options.cache_ttl)
			.build_with_hasher(fnv::FnvBuildHasher::default());

		// Create the nodes.
		let nodes = Nodes::new(server, &options).await?;

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
}

impl Nodes {
	async fn new(server: &Server, options: &crate::config::Vfs) -> tg::Result<Self> {
		#[cfg(not(feature = "sqlite"))]
		{
			Ok(Self::Memory(MemoryNodes::new()))
		}
		#[cfg(feature = "sqlite")]
		{
			Ok(Self::Sqlite(SqliteNodes::new(server, options).await?))
		}
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		match self {
			Nodes::Memory(memory) => Ok(memory.lookup(parent, name)),
			#[cfg(feature = "sqlite")]
			Nodes::Sqlite(sqlite) => sqlite.lookup(parent, name).await,
		}
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		match self {
			Nodes::Memory(memory) => memory.lookup_parent(id),
			#[cfg(feature = "sqlite")]
			Nodes::Sqlite(sqlite) => sqlite.lookup_parent(id).await,
		}
	}

	async fn get(&self, id: u64) -> std::io::Result<MemoryNode> {
		match self {
			Nodes::Memory(memory) => memory.get(id),
			#[cfg(feature = "sqlite")]
			Nodes::Sqlite(sqlite) => sqlite.get(id).await,
		}
	}

	fn insert(&self, id: u64, parent: u64, name: &str, artifact: tg::artifact::Id, depth: u64) {
		match self {
			Nodes::Memory(memory) => memory.insert(id, parent, name, artifact, depth),
			#[cfg(feature = "sqlite")]
			Nodes::Sqlite(sqlite) => sqlite.insert(id, parent, name, artifact, depth),
		}
	}
}

impl MemoryNodes {
	#[cfg_attr(feature = "sqlite", expect(dead_code))]
	fn new() -> Self {
		let nodes = DashMap::default();
		nodes.insert(
			vfs::ROOT_NODE_ID,
			MemoryNode {
				parent: vfs::ROOT_NODE_ID,
				artifact: None,
				depth: 0,
				children: HashMap::default(),
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

	fn get(&self, id: u64) -> std::io::Result<MemoryNode> {
		self.nodes.get(&id).map(|n| n.clone()).ok_or_else(|| {
			tracing::error!(%id, "node not found");
			std::io::Error::from_raw_os_error(libc::ENOENT)
		})
	}

	fn insert(&self, id: u64, parent: u64, name: &str, artifact: tg::artifact::Id, depth: u64) {
		// Insert the new node.
		self.nodes.insert(
			id,
			MemoryNode {
				parent,
				artifact: Some(artifact),
				depth,
				children: HashMap::default(),
			},
		);
		// Update the parent's children map.
		if let Some(mut parent_node) = self.nodes.get_mut(&parent) {
			parent_node.children.insert(name.to_owned(), id);
		}
	}
}

#[cfg(feature = "sqlite")]
impl SqliteNodes {
	async fn new(server: &Server, options: &crate::config::Vfs) -> tg::Result<Self> {
		let temp = Temp::new(server);
		tokio::fs::create_dir_all(&temp)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the database directory"))?;
		let path = temp.path().join("vfs");
		let initialize = Arc::new(|connection: &sqlite::Connection| {
			connection.pragma_update(None, "auto_vaccum", "incremental")?;
			connection.pragma_update(None, "busy_timeout", "5000")?;
			connection.pragma_update(None, "cache_size", "-20000")?;
			connection.pragma_update(None, "foreign_keys", "on")?;
			connection.pragma_update(None, "journal_mode", "wal")?;
			connection.pragma_update(None, "mmap_size", "2147483648")?;
			connection.pragma_update(None, "recursive_triggers", "on")?;
			connection.pragma_update(None, "synchronous", "normal")?;
			connection.pragma_update(None, "temp_store", "memory")?;
			Ok(())
		});
		let database_options = db::sqlite::DatabaseOptions {
			connections: options.database_connections,
			initialize,
			path,
		};
		let database = db::sqlite::Database::new(database_options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create database"))?;
		let database = Arc::new(database);
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			r"
				create table nodes (
					id integer primary key autoincrement,
					parent integer not null,
					name text,
					artifact text,
					depth integer not null
				);

				create index node_parent_name_index on nodes (parent, name);
			"
		);
		connection
			.with(move |connection, _cache| {
				connection
					.execute_batch(statement)
					.map_err(|source| tg::error!(!source, "failed to create the database"))?;
				Ok::<_, tg::Error>(())
			})
			.await?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into nodes (id, parent, depth)
				values ({p}1, {p}1, {p}2);
			"
		);
		let params = db::params![vfs::ROOT_NODE_ID, 0];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to insert the root node"))?;
		Ok(Self { database, temp })
	}

	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database a connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		#[derive(db::row::Deserialize)]
		struct Row {
			id: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from nodes
				where parent = {p}1 and name = {p}2;
			"
		);
		let params = db::params![parent, name];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to get node data from database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		Ok(row.map(|r| r.id))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database a connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		#[derive(db::row::Deserialize)]
		struct Row {
			parent: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select parent
				from nodes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get node parent from database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		Ok(row.parent)
	}

	async fn get(&self, id: u64) -> std::io::Result<MemoryNode> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get a database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		#[derive(db::row::Deserialize)]
		struct Row {
			parent: u64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			artifact: Option<tg::artifact::Id>,
			depth: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select parent, artifact, depth
				from nodes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the node data from the database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		Ok(MemoryNode {
			parent: row.parent,
			artifact: row.artifact,
			depth: row.depth,
			children: HashMap::default(),
		})
	}

	fn insert(&self, id: u64, parent: u64, name: &str, artifact: tg::artifact::Id, depth: u64) {
		let database = self.database.clone();
		let name = name.to_owned();
		tokio::spawn(async move {
			let Ok(connection) = database.write_connection().await.map_err(|error| {
				tracing::error!(%error, "failed to get database a connection");
			}) else {
				return;
			};
			let p = connection.p();
			let statement = formatdoc!(
				"
					insert into nodes (id, parent, name, artifact, depth)
					values ({p}1, {p}2, {p}3, {p}4, {p}5)
				"
			);
			let params = db::params![id, parent, name, artifact.to_bytes(), depth];
			if let Err(error) = connection.execute(statement.into(), params).await {
				tracing::error!(%error, %id, "failed to write node to the database");
			}
		});
	}
}
