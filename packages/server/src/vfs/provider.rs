use bytes::Bytes;
use dashmap::DashMap;
use db::{Connection, Database, Query};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{
	os::unix::ffi::OsStrExt,
	sync::atomic::{AtomicU64, Ordering},
};
use tangram_client as tg;
use tangram_database as db;
use tangram_vfs as vfs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::Server;

pub struct Provider {
	server: Server,
	database: db::sqlite::Database,
	counter: AtomicU64,
	open_dirs: DashMap<u64, DirectoryHandle, fnv::FnvBuildHasher>,
	open_files: DashMap<u64, FileHandle, fnv::FnvBuildHasher>,
}

pub type FileHandle = tg::blob::Reader<Server>;
pub struct DirectoryHandle {
	node: u64,
	directory: Option<tg::Directory>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Node {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	artifact: Option<tg::artifact::Id>,
	kind: NodeKind,
}

#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NodeKind {
	Root,
	File { executable: bool, size: u64 },
	Directory,
	Symlink,
	Checkout,
}

impl vfs::Provider for Provider {
	async fn lookup(&self, parent: u64, name: &str) -> std::io::Result<Option<u64>> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		// First, try and lookup in the database.
		#[derive(serde::Deserialize)]
		struct Row {
			id: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from nodes where parent = {p}1 and name = {p}2;
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

		// If the parent is the root, create a new node or lookup as a checkout.
		let data = 'a: {
			if parent != vfs::ROOT_NODE_ID {
				break 'a None;
			};
			let Ok(artifact) = name.parse() else {
				return Ok(None);
			};
			if matches!(
				tokio::fs::try_exists(self.server.checkouts_path().join(name)).await,
				Ok(true)
			) {
				break 'a Some(Node {
					artifact: Some(artifact),
					kind: NodeKind::Checkout,
				});
			}
			let artifact = tg::Artifact::with_id(artifact);
			Some(self.create_node(&artifact).await?)
		};

		// Otherwise, get the parent artifact and attempt to lookup.
		let data = 'a: {
			if let Some(data) = data {
				break 'a Some(data);
			}
			let parent = self.get(parent).await?;
			let Some(tg::Artifact::Directory(parent)) = parent.artifact.map(tg::Artifact::with_id)
			else {
				return Ok(None);
			};
			let entries = parent.entries(&self.server).await.map_err(|error| {
				tracing::error!(%error, "failed to get directory entries");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
			let Some(artifact) = entries.get(name) else {
				return Ok(None);
			};
			Some(self.create_node(artifact).await?)
		};

		let Some(data) = data else {
			return Ok(None);
		};

		let id = self.put(parent, name, data).await?;
		Ok(Some(id))
	}

	async fn lookup_parent(&self, id: u64) -> std::io::Result<u64> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		#[derive(serde::Deserialize)]
		struct Row {
			parent: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select parent from nodes where id = {p}1;
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
		let node = self.get(id).await?;
		match node.kind {
			NodeKind::Root | NodeKind::Directory => Ok(vfs::Attrs::new(vfs::FileType::Directory)),
			NodeKind::Symlink | NodeKind::Checkout => Ok(vfs::Attrs::new(vfs::FileType::Symlink)),
			NodeKind::File { executable, size } => {
				Ok(vfs::Attrs::new(vfs::FileType::File { executable, size }))
			},
		}
	}

	async fn open(&self, id: u64) -> std::io::Result<u64> {
		let node = self.get(id).await?;
		let Some(tg::Artifact::File(file)) = node.artifact.map(tg::Artifact::with_id) else {
			tracing::error!(%id, "tried to open a non-regular file");
			return Err(std::io::Error::other("expected a file"));
		};

		let reader = file.reader(&self.server).await.map_err(|error| {
			tracing::error!(%error, %file, "failed to create reader");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		let id = self.counter.fetch_add(1, Ordering::SeqCst);
		self.open_files.insert(id, reader);
		Ok(id)
	}

	async fn read(&self, id: u64, position: u64, length: u64) -> std::io::Result<Bytes> {
		let Some(mut reader) = self.open_files.get_mut(&id) else {
			tracing::error!(%id, "tried to read from an invalid file handle");
			return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
		};
		reader.seek(std::io::SeekFrom::Start(position)).await?;
		let mut buf = vec![0u8; length.to_usize().unwrap()];
		let len = reader.read(&mut buf).await?;
		buf.truncate(len);
		Ok(buf.into())
	}

	async fn readlink(&self, id: u64) -> std::io::Result<Bytes> {
		let node = self.get(id).await?;
		if let NodeKind::Checkout = node.kind {
			let path = self
				.server
				.checkouts_path()
				.join(node.artifact.as_ref().unwrap().to_string())
				.as_os_str()
				.as_bytes()
				.to_vec()
				.into();
			return Ok(path);
		}
		let Some(tg::Artifact::Symlink(symlink)) = node.artifact.map(tg::Artifact::with_id) else {
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
			let Ok(artifact) = artifact.id(&self.server, None).await else {
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
		let node = self.get(id).await?;
		let NodeKind::File { .. } = node.kind else {
			return Ok(Vec::new());
		};
		Ok(vec![tg::file::TANGRAM_FILE_XATTR_NAME.to_owned()])
	}

	async fn getxattr(&self, id: u64, name: &str) -> Option<String> {
		let node = self.get(id).await.ok()?;
		let Some(tg::Artifact::File(file)) = node.artifact.map(tg::Artifact::with_id) else {
			tracing::warn!("called xattr on non file");
			return None;
		};

		// Compute the attribute data.
		if name != tg::file::TANGRAM_FILE_XATTR_NAME {
			tracing::warn!(?name, "name mismatch");
			return None;
		}

		let artifacts = file
			.references(&self.server)
			.await
			.inspect_err(|e| {
				tracing::error!(?e, ?file, "failed to get file references");
			})
			.ok()?;

		let mut references = Vec::with_capacity(artifacts.len());
		for artifact in artifacts.iter() {
			let id = artifact
				.id(&self.server, None)
				.await
				.inspect_err(|e| {
					tracing::error!(?e, ?artifact, "failed to get ID of artifact");
				})
				.ok()?;
			references.push(id);
		}

		let attributes = tg::file::Attributes { references };
		let result = serde_json::to_string(&attributes).unwrap();
		Some(result)
	}

	async fn opendir(&self, id: u64) -> std::io::Result<u64> {
		let node = self.get(id).await?;
		let directory = match node.artifact.map(tg::Artifact::with_id) {
			None => None,
			Some(tg::Artifact::Directory(directory)) => Some(directory),
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
		self.open_dirs.insert(handle_id, handle);
		Ok(handle_id)
	}

	async fn readdir(&self, id: u64) -> std::io::Result<Vec<(String, u64)>> {
		let Some(handle) = self.open_dirs.get(&id) else {
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
		for name in entries.keys() {
			let id = self.lookup(handle.node, name).await?.unwrap();
			result.push((name.clone(), id));
		}
		Ok(result)
	}

	async fn close(&self, id: u64) {
		if self.open_dirs.contains_key(&id) {
			self.open_dirs.remove(&id);
		}
		if self.open_files.contains_key(&id) {
			self.open_files.remove(&id);
		}
	}
}

impl Provider {
	pub async fn new(server: &Server, options: db::sqlite::Options) -> tg::Result<Self> {
		// Create the database if it doesn't exist.
		let created = tokio::fs::try_exists(&options.path).await.map_err(
			|source| tg::error!(!source, %path = options.path.display(), "failed to check if the path exists"),
		)?;

		let server = server.clone();
		let database = db::sqlite::Database::new(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to create database"))?;
		let open_dirs = DashMap::default();
		let open_files = DashMap::default();
		let provider = Self {
			server,
			database,
			counter: AtomicU64::new(1000),
			open_dirs,
			open_files,
		};
		if !created {
			provider.create_database().await?;
		}
		Ok(provider)
	}

	#[allow(clippy::unused_async)]
	async fn create_database(&self) -> tg::Result<()> {
		// Create the connection
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Initialize the database.
		let statement = formatdoc!(
			r#"
				create table nodes (
					id integer primary key autoincrement,
					parent integer not null,
					name text,
					data text not null
				);

				create index node_parent_name_index on nodes (id, parent);
			"#
		);
		connection
			.execute(statement, Vec::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;

		// Insert the root node.
		let node = Node {
			artifact: None,
			kind: NodeKind::Root,
		};
		let p = connection.p();
		let statement =
			formatdoc!("insert into nodes (id, parent, data) values ({p}1, {p}1, {p}2)");
		let params = db::params![vfs::ROOT_NODE_ID, node];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to insert the root node"))?;
		Ok(())
	}

	async fn create_node(&self, artifact: &tg::Artifact) -> std::io::Result<Node> {
		let kind = match artifact {
			tg::Artifact::Directory(_) => NodeKind::Directory,
			tg::Artifact::Symlink(_) => NodeKind::Symlink,
			tg::Artifact::File(file) => {
				let executable = file.executable(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file metadata");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				let size = file.size(&self.server).await.map_err(|error| {
					tracing::error!(%error, "failed to get file metadata");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
				NodeKind::File { executable, size }
			},
		};

		let artifact = artifact.id(&self.server, None).await.map_err(|error| {
			tracing::error!(%error, "failed to get artifact id");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		Ok(Node {
			artifact: Some(artifact),
			kind,
		})
	}

	async fn get(&self, id: u64) -> std::io::Result<Node> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		#[derive(serde::Deserialize)]
		struct Row {
			data: Node,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select data from nodes where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %id, "failed to get node data from database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;
		Ok(row.data)
	}

	async fn put(&self, parent: u64, name: &str, data: Node) -> std::io::Result<u64> {
		let connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to get database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		#[derive(serde::Deserialize)]
		struct Row {
			id: u64,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into nodes (parent, name, data)
				values ({p}1, {p}2, {p}3)
				returning id;
			"
		);
		let params = db::params![parent, name, data];
		let row = connection
			.query_one_into::<Row>(statement, params)
			.await
			.map_err(|error| {
				tracing::error!(%error, %parent, %name, "failed to put node data into database");
				std::io::Error::from_raw_os_error(libc::EIO)
			})?;

		Ok(row.id)
	}

	async fn depth(&self, mut node: u64) -> std::io::Result<usize> {
		let mut connection = self.database.connection().await.map_err(|error| {
			tracing::error!(%error, "failed to create database connection");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;
		let transaction = connection.transaction().await.map_err(|error| {
			tracing::error!(%error, "failed to create transaction");
			std::io::Error::from_raw_os_error(libc::EIO)
		})?;

		#[derive(serde::Deserialize)]
		struct Row {
			parent: u64,
		}
		let p = transaction.p();
		let statement: String = formatdoc!(
			"
				select parent from nodes where id = {p}1;
			"
		);

		let mut depth = 0;
		while node != vfs::ROOT_NODE_ID {
			depth += 1;
			let params = db::params![node];
			let row = transaction
				.query_one_into::<Row>(statement.clone(), params)
				.await
				.map_err(|error| {
					tracing::error!(%error, %node, "failed to get node parent from database");
					std::io::Error::from_raw_os_error(libc::EIO)
				})?;
			node = row.parent;
		}
		Ok(depth)
	}
}
