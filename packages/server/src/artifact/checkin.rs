use crate::{Server, blob::create::Blob, temp::Temp};
use bytes::Bytes;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
	stream::FuturesUnordered,
};
use indoc::indoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use reflink_copy::reflink;
use rusqlite as sqlite;
use std::{
	collections::{BTreeMap, HashMap},
	os::unix::fs::PermissionsExt,
	panic::AssertUnwindSafe,
	path::{Path, PathBuf},
	sync::Arc,
	time::Instant,
};
use tangram_client as tg;
use tangram_database::prelude::*;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_ignore as ignore;
use tangram_messenger::Messenger as _;
use tokio_util::task::AbortOnDropHandle;

// mod input;
// mod lockfile;
// mod object;
// mod output;
// mod unify;

struct State {
	graph: Graph,
	ignorer: Option<ignore::Matcher>,
	progress: Option<crate::progress::Handle<tg::artifact::checkin::Output>>,
}

#[derive(Clone, Debug, Default)]
pub struct Graph {
	nodes: Vec<Node>,
	paths: HashMap<PathBuf, usize, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug, Default)]
struct Node {
	blob: Option<Blob>,
	edges: Vec<Edge>,
	metadata: Option<std::fs::Metadata>,
	path: Option<Arc<PathBuf>>,
	object: Option<Object>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	node: Option<usize>,
	name: Option<String>,
}

#[derive(Clone, Debug)]
struct Object {
	bytes: Bytes,
	data: tg::object::Data,
	id: tg::object::Id,
}

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::artifact::checkin::Output>>>
		+ Send
		+ 'static,
	> {
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.check_in_artifact_inner(arg, Some(&progress)))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	// Check in the artifact.
	async fn check_in_artifact_inner(
		&self,
		mut arg: tg::artifact::checkin::Arg,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Canonicalize the path's parent.
		arg.path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = &arg.path.display(), "failed to canonicalize the path's parent"))?;
		let arg = Arc::new(arg);

		// If this is a checkin of a path in the cache directory, then retrieve the corresponding artifact.
		if let Ok(path) = arg.path.strip_prefix(self.cache_path()) {
			let id = path
				.components()
				.next()
				.map(|component| {
					let std::path::Component::Normal(name) = component else {
						return Err(tg::error!("invalid path"));
					};
					name.to_str().ok_or_else(|| tg::error!("non-utf8 path"))
				})
				.ok_or_else(|| tg::error!("cannot check in the cache directory"))??
				.parse()?;
			if path.components().count() == 1 {
				let output = tg::artifact::checkin::Output { artifact: id };
				return Ok(output);
			}
			let path = path.components().skip(1).collect::<PathBuf>();
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, path).await?;
			let id = artifact.id(self).await?;
			let output = tg::artifact::checkin::Output { artifact: id };
			return Ok(output);
		}

		// Create the ignore matcher if necessary.
		let ignorer = if arg.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Create the state.
		let mut state = State {
			graph: Graph::default(),
			ignorer,
			progress: progress.cloned(),
		};

		// Visit.
		let start = Instant::now();
		let mut state = tokio::task::spawn_blocking({
			let server = self.clone();
			let arg = arg.clone();
			move || {
				server.checkin_visit(&mut state, arg.path.clone())?;
				Ok::<_, tg::Error>(state)
			}
		})
		.await
		.unwrap()?;
		tracing::debug!(elapsed = ?start.elapsed(), "visit");

		// Remove the ignorer.
		state.ignorer.take();

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::debug!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state, 0)?;
		tracing::debug!(elapsed = ?start.elapsed(), "create objects");

		// Write the objects to the database and the store.
		let start = Instant::now();
		let state = Arc::new(state);
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let blobs_future = tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_copy_blobs(&arg, &state)
					.map_err(|source| tg::error!(!source, "failed to copy the blobs"))
					.await?;
				tracing::debug!(elapsed = ?start.elapsed(), "copy blobs");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let database_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_write_objects_to_database(&state)
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the database")
					})
					.await?;
				tracing::debug!(elapsed = ?start.elapsed(), "write objects to database");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let messenger_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_write_objects_to_messenger(&state, touched_at)
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the messenger")
					})
					.await?;
				tracing::debug!(elapsed = ?start.elapsed(), "write objects to messenger");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let store_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_write_objects_to_store(&state, touched_at)
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the store")
					})
					.await?;
				tracing::debug!(elapsed = ?start.elapsed(), "write objects to store");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		futures::try_join!(
			blobs_future,
			database_future,
			messenger_future,
			store_future
		)?;
		let state = Arc::into_inner(state).unwrap();
		tracing::debug!(elapsed = ?start.elapsed(), "write objects");

		// Get the root node's ID.
		let artifact = state.graph.nodes[0]
			.object
			.as_ref()
			.unwrap()
			.id
			.clone()
			.try_into()
			.unwrap();

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<ignore::Matcher> {
		let file_names = vec![
			".tangramignore".into(),
			".tgignore".into(),
			".gitignore".into(),
		];
		let global = indoc!(
			"
				.DS_Store
				.git
				.tangram
				tangram.lock
			"
		);
		ignore::Matcher::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	fn checkin_visit(&self, state: &mut State, path: PathBuf) -> tg::Result<Option<usize>> {
		if let Some(index) = state.graph.paths.get(&path) {
			return Ok(Some(*index));
		}
		state
			.graph
			.paths
			.insert(path.clone(), state.graph.nodes.len());

		let metadata = std::fs::symlink_metadata(&path).unwrap();

		// // Check if the path is ignored.
		// if let Some(ignore) = &mut state.ignorer {
		// 	if ignore.matches(&path, Some(metadata.is_dir())).unwrap() {
		// 		return Ok(None);
		// 	}
		// }

		let index = if metadata.is_dir() {
			self.checkin_visit_directory(state, path, metadata)?
		} else if metadata.is_file() {
			self.checkin_visit_file(state, path, metadata)?
		} else {
			return Err(tg::error!("invalid file type"));
		};

		Ok(Some(index))
	}

	fn checkin_visit_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		metadata: std::fs::Metadata,
	) -> tg::Result<usize> {
		// Insert the node.
		let index = state.graph.nodes.len();
		let node = Node {
			edges: Vec::new(),
			metadata: Some(metadata),
			path: None,
			..Default::default()
		};
		state.graph.nodes.push(node);

		// Read the entries.
		let read_dir = std::fs::read_dir(&path).unwrap();
		let mut names = Vec::new();
		for result in read_dir {
			let entry = result.unwrap();
			names.push(entry.file_name().to_str().unwrap().to_owned());
		}

		// Visit the entries and create the edges.
		let mut edges = Vec::with_capacity(names.len());
		for name in names {
			let index = self.checkin_visit(state, path.join(&name))?;
			if let Some(index) = index {
				edges.push(Edge {
					node: Some(index),
					name: Some(name),
				});
			}
		}

		// Set the edges.
		state.graph.nodes[index].edges = edges;

		// Set the path.
		state.graph.nodes[index].path = Some(Arc::new(path));

		Ok(index)
	}

	fn checkin_visit_file(
		&self,
		state: &mut State,
		path: PathBuf,
		metadata: std::fs::Metadata,
	) -> tg::Result<usize> {
		let node = Node {
			metadata: Some(metadata),
			path: Some(Arc::new(path)),
			..Default::default()
		};
		let index = state.graph.nodes.len();
		state.graph.nodes.push(node);
		Ok(index)
	}

	async fn checkin_create_blobs(&self, state: &mut State) -> tg::Result<()> {
		let blobs = state
			.graph
			.nodes
			.iter()
			.enumerate()
			.filter_map(|(index, node)| {
				node.metadata.as_ref().unwrap().is_file().then_some(
					tokio::spawn({
						let server = self.clone();
						let path = node.path.as_ref().unwrap().clone();
						async move {
							let _permit = server.file_descriptor_semaphore.acquire().await;
							let mut file = tokio::fs::File::open(path.as_ref()).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to open the file"),
							)?;
							let blob = server.create_blob_inner(&mut file, None).await.map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to create the blob"),
							)?;
							Ok::<_, tg::Error>((index, blob))
						}
					})
					.map(|result| result.unwrap()),
				)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		for (index, blob) in blobs {
			state.graph.nodes[index].blob.replace(blob);
		}
		Ok(())
	}

	async fn checkin_copy_blobs(
		&self,
		arg: &tg::artifact::checkin::Arg,
		state: &Arc<State>,
	) -> tg::Result<()> {
		let destructive = arg.destructive;
		state
			.graph
			.nodes
			.iter()
			.filter_map(|node| match (&node.path, &node.metadata, &node.blob) {
				(Some(path), Some(metadata), Some(blob)) => {
					Some((path.clone(), metadata.clone(), blob.id.clone()))
				},
				_ => None,
			})
			.map(|(path, metadata, id)| {
				let server = self.clone();
				tokio::task::spawn_blocking(move || {
					server.checkin_copy_blob(&path, &metadata, &id, destructive)
				})
				.map(|result| result.unwrap())
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<()>()
			.await?;
		Ok(())
	}

	fn checkin_copy_blob(
		&self,
		path: &Path,
		metadata: &std::fs::Metadata,
		id: &tg::blob::Id,
		destructive: bool,
	) -> tg::Result<()> {
		let src = path;
		let dst = &self.blobs_path().join(id.to_string());
		let exists = std::fs::exists(dst)
			.map_err(|source| tg::error!(!source, "failed to check if the blob path exists"))?;
		if exists {
			return Ok(());
		}
		if destructive {
			Self::checkin_copy_blob_destructive(metadata, src, dst)?;
		} else if metadata.permissions().mode() == 0o644 {
			Self::checkin_copy_blob_direct(src, dst)?;
		} else {
			self.checkin_copy_blob_temp(src, dst)?;
		}
		Ok(())
	}

	fn checkin_copy_blob_destructive(
		metadata: &std::fs::Metadata,
		src: &Path,
		dst: &Path,
	) -> tg::Result<()> {
		if metadata.permissions().mode() != 0o644 {
			let permissions = std::fs::Permissions::from_mode(0o644);
			std::fs::set_permissions(src, permissions)
				.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		}
		std::fs::rename(src, dst).map_err(|source| {
			tg::error!(!source, "failed to rename the file to the blobs directory")
		})?;
		Ok(())
	}

	fn checkin_copy_blob_direct(src: &Path, dst: &Path) -> tg::Result<()> {
		let mut copied = false;
		match reflink(src, dst) {
			Ok(()) => {
				copied = true;
			},
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
				copied = true;
			},
			Err(_) => (),
		}
		if !copied {
			std::fs::copy(src, dst)
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;
		}
		Ok(())
	}

	fn checkin_copy_blob_temp(&self, src: &Path, dst: &Path) -> tg::Result<()> {
		let mut copied = false;
		let temp = Temp::new(self);
		match reflink(src, temp.path()) {
			Ok(()) => {
				copied = true;
			},
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
				copied = true;
			},
			Err(_) => (),
		}
		if !copied {
			std::fs::copy(src, temp.path())
				.map_err(|source| tg::error!(!source, "failed to copy the file"))?;
		}
		let permissions = std::fs::Permissions::from_mode(0o644);
		std::fs::set_permissions(temp.path(), permissions)
			.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
		std::fs::rename(temp.path(), dst).map_err(|source| {
			tg::error!(!source, "failed to rename the file to the blobs directory")
		})?;
		Ok(())
	}

	fn checkin_create_objects(state: &mut State, index: usize) -> tg::Result<()> {
		let indexes = state.graph.nodes[index]
			.edges
			.iter()
			.map(|edge| edge.node.unwrap())
			.collect_vec();
		for index in indexes.iter().copied() {
			Self::checkin_create_objects(state, index)?;
		}

		let metadata = state.graph.nodes[index].metadata.as_ref().unwrap();
		let (kind, data) = if metadata.is_dir() {
			let kind = tg::object::Kind::Directory;
			let entries = state.graph.nodes[index]
				.edges
				.iter()
				.map(|edge| edge.name.as_ref().unwrap())
				.zip(indexes)
				.map(|(name, index)| {
					let name = name.clone();
					let id = state.graph.nodes[index]
						.object
						.as_ref()
						.unwrap()
						.id
						.clone()
						.try_into()
						.unwrap();
					(name, id)
				})
				.collect();
			let data = tg::directory::Data::Normal { entries };
			let data = tg::object::Data::from(data);
			(kind, data)
		} else if metadata.is_file() {
			let kind = tg::object::Kind::File;
			let contents = state.graph.nodes[index].blob.as_ref().unwrap().id.clone();
			let dependencies = BTreeMap::new();
			let executable = metadata.permissions().mode() & 0o111 != 0;
			let data = tg::file::Data::Normal {
				contents,
				dependencies,
				executable,
			};
			let data = tg::object::Data::from(data);
			(kind, data)
		} else {
			panic!();
		};

		let bytes = data.serialize().unwrap();
		let id = tg::object::Id::new(kind, &bytes);
		let object = Object { bytes, data, id };

		state.graph.nodes[index].object = Some(object);

		Ok(())
	}

	async fn checkin_write_objects_to_database(&self, state: &Arc<State>) -> tg::Result<()> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let state = state.clone();
		connection
			.unwrap_left()
			.with(move |connection| {
				let transaction = connection.transaction().unwrap();
				for node in &state.graph.nodes {
					let object = node.object.as_ref().unwrap();
					if let Some(blob) = &node.blob {
						Self::blob_create_sqlite(blob, &transaction)?;
					}
				}
				transaction.commit().unwrap();
				Ok::<_, tg::Error>(())
			})
			.await?;
		Ok(())
	}

	async fn checkin_write_objects_to_messenger(
		&self,
		state: &Arc<State>,
		touched_at: i64,
	) -> tg::Result<()> {
		for node in &state.graph.nodes {
			let object = node.object.as_ref().unwrap();

			// Create the index message.
			let message = crate::index::Message {
				children: object.data.children(),
				count: None,
				depth: None,
				id: object.id.clone(),
				size: object.bytes.len().to_u64().unwrap(),
				touched_at,
				weight: None,
			};

			// Serialize the message.
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;

			// Publish the message.
			self.messenger
				.publish("index".to_owned(), message.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		}
		Ok(())
	}

	async fn checkin_write_objects_to_store(
		&self,
		state: &State,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut objects = Vec::with_capacity(state.graph.nodes.len());
		for node in &state.graph.nodes {
			let object = node.object.as_ref().unwrap();
			objects.push((object.id.clone(), object.bytes.clone()));
			if let Some(blob) = &node.blob {
				let mut stack = vec![blob];
				while let Some(blob) = stack.pop() {
					if let Some(data) = &blob.data {
						let bytes = data.serialize()?;
						objects.push((blob.id.clone().into(), bytes));
					}
					stack.extend(&blob.children);
				}
			}
		}
		let arg = crate::store::PutBatchArg {
			objects,
			touched_at,
		};
		self.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}

	fn write_object_sqlite(
		transaction: &sqlite::Transaction<'_>,
		id: &tg::object::Id,
		bytes: &Bytes,
		data: &tg::object::Data,
		touched_at: &str,
	) -> tg::Result<()> {
		let statement = indoc!(
			"
				insert into objects (id, incomplete_children, size, touched_at)
				values (?1, ?2, ?3, ?4)
				on conflict (id) do update set touched_at = ?4;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		let size = bytes.len().to_u64().unwrap();
		let params = rusqlite::params![id.to_string(), 0, size, touched_at];
		statement
			.execute(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let statement = indoc!(
			"
				insert into object_children (object, child)
				values (?1, ?2)
				on conflict (object, child) do nothing;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		for child in data.children() {
			let params = rusqlite::params![id.to_string(), child.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.check_in_artifact(arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
