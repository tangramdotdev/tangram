use crate::{tmp::Tmp, Server};
use bytes::Bytes;
use dashmap::DashMap;
use indoc::formatdoc;
use num::ToPrimitive;
use std::{
	collections::BTreeSet,
	os::unix::ffi::OsStrExt,
	sync::atomic::{AtomicU64, Ordering},
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_vfs as vfs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub struct Provider {
	cache: moka::future::Cache<u64, Node, fnv::FnvBuildHasher>,
	counter: AtomicU64,
	database: db::sqlite::Database,
	directory_handles: DashMap<u64, DirectoryHandle, fnv::FnvBuildHasher>,
	file_handles: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
	server: Server,
	#[allow(dead_code)]
	tmp: Tmp,
}

pub struct DirectoryHandle {
	node: u64,
	directory: Option<tg::Directory>,
}

pub struct FileHandle {
	reader: tg::blob::Reader<Server>,
}

#[derive(Clone)]
struct Node {
	artifact: Option<tg::Artifact>,
	checkout: bool,
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
		}) = self.cache.get(&parent).await
		{
			let entries = object.entries(&self.server).await.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to get directory entries");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			if !entries.contains_key(name) {
				return Ok(None);
			}
		}

		// First, try to look up in the database.
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database a connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		#[derive(serde::Deserialize)]
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
			.query_optional_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to get node data from database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		drop(connection);
		if let Some(Row { id }) = row {
			return Ok(Some(id));
		}

		// If the parent is the root, then create a new node or lookup as a checkout.
		let mut checkout = false;
		let artifact = 'a: {
			if parent != vfs::ROOT_NODE_ID {
				break 'a None;
			};
			let Ok(artifact) = name.parse() else {
				return Ok(None);
			};
			let artifact = tg::Artifact::with_id(artifact);
			let exists = tokio::fs::try_exists(self.server.checkouts_path().join(name)).await;
			checkout = matches!(exists, Ok(true));
			Some(artifact)
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let artifact = 'a: {
			if let Some(artifact) = artifact {
				break 'a Some(artifact);
			}
			let Node { artifact, .. } = self.get(parent).await?;
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
			Some(artifact.clone())
		};

		// Insert the node.
		let id = self.put(parent, name, artifact.unwrap(), checkout).await?;

		Ok(Some(id))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database a connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		#[derive(serde::Deserialize)]
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
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get node parent from database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		Ok(row.parent)
	}

	async fn getattr(&self, id: u64) -> std::io::Result<vfs::Attrs> {
		match self.get(id).await? {
			Node {
				artifact: Some(tg::Artifact::File(file)),
				checkout: false,
			} => {
				let executable = file.executable(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file's executable bit");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				let size = file.size(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file's size");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				Ok(vfs::Attrs::new(vfs::FileType::File { executable, size }))
			},
			Node {
				artifact: Some(tg::Artifact::Directory(_)),
				checkout: false,
			}
			| Node { artifact: None, .. } => Ok(vfs::Attrs::new(vfs::FileType::Directory)),
			Node {
				artifact: Some(tg::Artifact::Symlink(_)),
				checkout: false,
			}
			| Node { checkout: true, .. } => Ok(vfs::Attrs::new(vfs::FileType::Symlink)),
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

		// Create the reader.
		let reader = file.reader(&self.server).await.map_err(|error| {
			tracing::error!(%error, %file, "failed to create reader");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Create the file handle.
		let file_handle = FileHandle { reader };

		// Insert the file handle.
		let id = self.counter.fetch_add(1, Ordering::SeqCst);
		self.file_handles.insert(id, file_handle);

		Ok(id)
	}

	async fn read(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		// Get the file handle.
		let Some(mut file_handle) = self.file_handles.get_mut(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};

		// Seek to the given position.
		file_handle
			.reader
			.seek(std::io::SeekFrom::Start(position))
			.await?;

		// Read the requested number of bytes.
		let mut bytes = vec![0u8; length.to_usize().unwrap()];
		let mut size = 0;
		while size < length.to_usize().unwrap() {
			let n = file_handle.reader.read(&mut bytes[size..]).await?;
			size += n;
			if n == 0 {
				break;
			}
		}

		// Truncate the bytes.
		bytes.truncate(size);

		Ok(bytes.into())
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		// Get the node.
		let Node { artifact, checkout } = self.get(id).await.map_err(|error| {
			tracing::error!(%error, "failed to lookup node");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Handle the case that it is checked out.
		if checkout {
			let id = artifact.unwrap().id(&self.server).await.map_err(|error| {
				tracing::error!(%error, "failed to get artifact id");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			let path = self
				.server
				.checkouts_path()
				.join(id.to_string())
				.as_os_str()
				.as_bytes()
				.to_vec()
				.into();
			return Ok(path);
		}

		// Ensure it is a symlink.
		let Some(tg::Artifact::Symlink(symlink)) = artifact else {
			tracing::error!(%id, "tried to readlink on an invalid file type");
			return Err(std::io::Error::other("expected a symlink"));
		};

		// Render the target.
		let mut target = tg::Path::new();
		let Ok(artifact) = symlink.artifact(&self.server).await else {
			tracing::error!("failed to get the symlink's artifact");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		let Ok(path) = symlink.path(&self.server).await else {
			tracing::error!("failed to get the symlink's path");
			return Err(std::io::Error::from_raw_os_error(libc::EIO));
		};
		if let Some(artifact) = artifact.as_ref() {
			let depth = self.depth(id).await?;
			for _ in 0..depth - 1 {
				target.push(tg::path::Component::Parent);
			}
			let Ok(artifact) = artifact.id(&self.server).await else {
				tracing::error!("failed to get the symlink's artifact id");
				return Err(std::io::Error::from_raw_os_error(libc::EIO));
			};
			target.push(tg::path::Component::Normal(artifact.to_string()));
		}
		if let Some(path) = path.as_ref() {
			target = target.join(path.clone());
		}
		let target = target.to_string().into_bytes().into();

		Ok(target)
	}

	async fn listxattrs(&self, id: u64) -> std::io::Result<Vec<String>> {
		let Node { artifact, .. } = self.get(id).await?;
		let Some(tg::Artifact::File(_)) = artifact else {
			return Ok(Vec::new());
		};
		let var_name = vec![tg::file::TANGRAM_FILE_XATTR_NAME.to_owned()];
		Ok(var_name)
	}

	async fn getxattr(&self, id: u64, name: &str) -> std::io::Result<Option<String>> {
		// Get the node.
		let Node { artifact, .. } = self.get(id).await?;

		// Ensure it is a file.
		let Some(tg::Artifact::File(file)) = artifact else {
			return Ok(None);
		};

		// Ensure the xattr name is supported.
		if name != tg::file::TANGRAM_FILE_XATTR_NAME {
			return Ok(None);
		}

		// Get the references.
		let artifacts = file.references(&self.server).await.map_err(|e| {
			tracing::error!(?e, ?file, "failed to get file references");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Create the output.
		let mut references = BTreeSet::new();
		for artifact in artifacts.iter() {
			let id = artifact.id(&self.server).await.map_err(|e| {
				tracing::error!(?e, ?artifact, "failed to get ID of artifact");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			references.insert(id);
		}
		let attributes = tg::file::Attributes { references };
		let output = serde_json::to_string(&attributes).unwrap();

		Ok(Some(output))
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
		let handle_id = self.counter.fetch_add(1, Ordering::SeqCst);
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

pub struct Options {
	pub cache_ttl: f64,
	pub cache_size: u64,
	pub connections: usize,
}

impl Provider {
	pub async fn new(server: &Server, options: Options) -> tg::Result<Self> {
		// Create the cache.
		let cache = moka::future::CacheBuilder::new(options.cache_size)
			.time_to_idle(std::time::Duration::from_secs_f64(options.cache_ttl))
			.build_with_hasher(fnv::FnvBuildHasher::default());

		// Create the database.
		let tmp = Tmp::new(server);
		tokio::fs::create_dir_all(&tmp)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the database directory"))?;
		let path = tmp.path.join("vfs");
		let database_options = db::sqlite::Options {
			path,
			connections: options.connections,
		};
		let database = db::sqlite::Database::new(database_options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create database"))?;
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		connection
			.with(|connection| {
				connection
					.pragma_update(None, "journal_mode", "wal")
					.map_err(|source| tg::error!(!source, "failed to set the journal mode"))?;
				Ok::<_, tg::Error>(())
			})
			.await?;
		let statement = formatdoc!(
			r#"
				create table nodes (
					id integer primary key autoincrement,
					parent integer not null,
					name text,
					artifact text,
					checkout integer not null
				);

				create index node_parent_name_index on nodes (id, parent);
			"#
		);
		connection
			.execute(statement, Vec::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into nodes (id, parent, checkout)
				values ({p}1, {p}1, {p}2);
			"
		);
		let params = db::params![vfs::ROOT_NODE_ID, false];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to insert the root node"))?;

		// Create the provider.
		let counter = AtomicU64::new(1000);
		let directory_handles = DashMap::default();
		let file_handles = DashMap::default();
		let server = server.clone();
		let provider = Self {
			cache,
			counter,
			database,
			directory_handles,
			file_handles,
			server,
			tmp,
		};

		Ok(provider)
	}

	async fn get(&self, id: u64) -> std::io::Result<Node> {
		// Attempt to read from the cache.
		if let Some(node) = self.cache.get(&id).await {
			return Ok(node);
		}

		// Get a database connection.
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get a database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Get the node from the database.
		#[derive(serde::Deserialize)]
		struct Row {
			artifact: Option<tg::artifact::Id>,
			checkout: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select artifact, checkout
				from nodes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get the node data from the database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;

		// Create the node.
		let artifact = row.artifact.map(tg::Artifact::with_id);
		let checkout = row.checkout;
		let node = Node { artifact, checkout };

		// Add the node to the cache.
		self.cache.insert(id, node.clone()).await;

		Ok(node)
	}

	async fn put(
		&self,
		parent: u64,
		name: &str,
		artifact: tg::Artifact,
		checkout: bool,
	) -> std::io::Result<u64> {
		// Get the artifact.
		let artifact = artifact.id(&self.server).await.map_err(|error| {
			tracing::error!(%error, "failed to get artifact id");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Get a connection.
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database a connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Insert the node.
		#[derive(serde::Deserialize)]
		struct Row {
			id: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into nodes (parent, name, artifact, checkout)
				values ({p}1, {p}2, {p}3, {p}4)
				returning id;
			"
		);
		let params = db::params![parent, name, artifact, checkout];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to put node data into database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;

		// Create the node.
		let artifact = tg::Artifact::with_id(artifact);
		let node = Node {
			artifact: Some(artifact),
			checkout,
		};

		// Add the node to the cache.
		self.cache.insert(row.id, node).await;

		Ok(row.id)
	}

	async fn depth(&self, mut node: u64) -> std::io::Result<usize> {
		// Get a database connection.
		let mut connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to create database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Create a transaction.
		let transaction = connection.transaction().await.map_err(|error| {
			tracing::error!(%error, "failed to create transaction");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// Compute the depth.
		let mut depth = 0;
		while node != vfs::ROOT_NODE_ID {
			depth += 1;
			#[derive(serde::Deserialize)]
			struct Row {
				parent: u64,
			}
			let p = transaction.p();
			let statement: String = formatdoc!(
				"
					select parent
					from nodes
					where id = {p}1;
				"
			);
			let params = db::params![node];
			let row = transaction
				.query_one_into::<Row>(statement, params)
				.await
				.map_err(|error| {
					tracing::error!(%error, %node, "failed to get the node parent from the database");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
			node = row.parent;
		}

		Ok(depth)
	}
}
