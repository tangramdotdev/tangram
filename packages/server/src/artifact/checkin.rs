use crate::{Server, blob::create::Blob};
use bytes::Bytes;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
	stream::FuturesUnordered,
};
use indoc::indoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap, ops::Not as _, os::unix::fs::PermissionsExt, panic::AssertUnwindSafe,
	path::PathBuf, sync::Arc, time::Instant,
};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::Messenger as _;
use tokio_util::task::AbortOnDropHandle;

pub(crate) mod ignore;
mod input;
mod lockfile;
mod object;
mod output;
mod unify;

struct State {
	graph: Graph,
	ignorer: Option<tangram_ignore::Ignorer>,
	progress: crate::progress::Handle<tg::artifact::checkin::Output>,
}

#[derive(Clone, Debug)]
pub struct Graph {
	nodes: Vec<Node>,
	paths: radix_trie::Trie<PathBuf, usize>,
}

#[derive(Clone, Debug)]
struct Node {
	edges: Vec<Edge>,
	#[allow(dead_code)]
	metadata: Option<std::fs::Metadata>,
	object: Option<Object>,
	path: Option<Arc<PathBuf>>,
	variant: Variant,
}

#[derive(Clone, Debug, derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
enum Variant {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug)]
struct Directory {
	entries: Vec<(String, usize)>,
}

#[derive(Clone, Debug)]
struct File {
	blob: Option<Blob>,
	executable: bool,
}

#[derive(Clone, Debug)]
struct Symlink {
	target: PathBuf,
}

#[derive(Clone, Debug)]
struct Edge {
	node: Option<usize>,
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
				let result = AssertUnwindSafe(server.check_in_artifact_inner(arg, &progress))
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
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Canonicalize the path's parent.
		arg.path = crate::util::fs::canonicalize_parent(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = &arg.path.display(), "failed to canonicalize the path's parent"))?;

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

		if arg.destructive {
			self.check_in_artifact_new(arg, progress).await
		} else {
			self.check_in_artifact_old(arg, progress).await
		}
	}

	async fn check_in_artifact_new(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Create the ignorer if necessary.
		let ignorer = if arg.ignore {
			Some(Self::checkin_create_ignorer()?)
		} else {
			None
		};

		// Create the state.
		let graph = Graph {
			nodes: Vec::new(),
			paths: radix_trie::Trie::default(),
		};
		let mut state = State {
			graph,
			ignorer,
			progress: progress.clone(),
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
		tracing::trace!(elapsed = ?start.elapsed(), "visit");

		// Remove the ignorer.
		state.ignorer.take();

		// Create blobs.
		let start = Instant::now();
		self.checkin_create_blobs(&mut state).await?;
		tracing::trace!(elapsed = ?start.elapsed(), "create blobs");

		// Create objects.
		let start = Instant::now();
		Self::checkin_create_objects(&mut state, 0)?;
		tracing::trace!(elapsed = ?start.elapsed(), "create objects");

		// Set the touch time.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get the root node's ID.
		let root = state.graph.nodes[0].object.as_ref().unwrap().id.clone();
		let root =
			tg::artifact::Id::try_from(root).map_err(|_| tg::error!("expected an artifact"))?;

		// Write the objects to the database and the store.
		let state = Arc::new(state);
		let cache_future = tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let root = root.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_cache_task(&arg, &root, touched_at)
					.map_err(|source| tg::error!(!source, "failed to copy the blobs"))
					.await?;
				tracing::trace!(elapsed = ?start.elapsed(), "copy blobs");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let messenger_future = tokio::spawn({
			let server = self.clone();
			let state = state.clone();
			let root = root.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_messenger_task(&state, &root, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the messenger")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to messenger");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		let store_future = tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			let root = root.clone();
			async move {
				let start = Instant::now();
				server
					.checkin_store_task(&arg, &state, &root, touched_at)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to write the objects to the store")
					})?;
				tracing::trace!(elapsed = ?start.elapsed(), "write objects to store");
				Ok::<_, tg::Error>(())
			}
		})
		.map(|result| result.unwrap());
		futures::try_join!(cache_future, messenger_future, store_future)?;
		let _state = Arc::into_inner(state).unwrap();

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact: root };

		Ok(output)
	}

	pub(crate) fn checkin_create_ignorer() -> tg::Result<tangram_ignore::Ignorer> {
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
		tangram_ignore::Ignorer::new(file_names, Some(global))
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
	}

	fn checkin_visit(&self, state: &mut State, path: PathBuf) -> tg::Result<usize> {
		// Check if the path has been visited.
		if let Some(index) = state.graph.paths.get(&path) {
			return Ok(*index);
		}
		state
			.graph
			.paths
			.insert(path.clone(), state.graph.nodes.len());

		// Get the metadata.
		let metadata = std::fs::symlink_metadata(&path)
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;

		// Visit the path.
		let index = if metadata.is_dir() {
			self.checkin_visit_directory(state, path, metadata)?
		} else if metadata.is_file() {
			Self::checkin_visit_file(state, path, metadata)
		} else if metadata.is_symlink() {
			Self::checkin_visit_symlink(state, path, metadata)?
		} else {
			return Err(tg::error!(?metadata, "invalid file type"));
		};

		Ok(index)
	}

	fn checkin_visit_directory(
		&self,
		state: &mut State,
		path: PathBuf,
		metadata: std::fs::Metadata,
	) -> tg::Result<usize> {
		// Insert the node.
		let index = state.graph.nodes.len();
		let variant = Variant::Directory(Directory {
			entries: Vec::new(),
		});
		let node = Node {
			edges: Vec::new(),
			metadata: Some(metadata),
			object: None,
			path: None,
			variant,
		};
		state.graph.nodes.push(node);

		// Read the entries.
		let read_dir = std::fs::read_dir(&path)
			.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
		let mut names = Vec::new();
		for result in read_dir {
			let entry = result
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?;
			let name = entry
				.file_name()
				.to_str()
				.ok_or_else(|| tg::error!("expected the entry name to be a string"))?
				.to_owned();
			names.push(name);
		}

		// Visit the entries.
		let mut edges = Vec::with_capacity(names.len());
		for name in names {
			let entry_index = self.checkin_visit(state, path.join(&name))?;
			state.graph.nodes[index]
				.variant
				.unwrap_directory_mut()
				.entries
				.push((name, entry_index));
			edges.push(Edge {
				node: Some(entry_index),
			});
		}

		// Set the edges.
		state.graph.nodes[index].edges = edges;

		// Set the path.
		state.graph.nodes[index].path = Some(Arc::new(path));

		Ok(index)
	}

	fn checkin_visit_file(state: &mut State, path: PathBuf, metadata: std::fs::Metadata) -> usize {
		let variant = Variant::File(File {
			blob: None,
			executable: metadata.permissions().mode() & 0o111 != 0,
		});
		let node = Node {
			edges: Vec::new(),
			metadata: Some(metadata),
			object: None,
			path: Some(Arc::new(path)),
			variant,
		};
		let index = state.graph.nodes.len();
		state.graph.nodes.push(node);
		index
	}

	fn checkin_visit_symlink(
		state: &mut State,
		path: PathBuf,
		metadata: std::fs::Metadata,
	) -> tg::Result<usize> {
		let target = std::fs::read_link(&path)
			.map_err(|source| tg::error!(!source, "failed to read the symlink"))?;
		let variant = Variant::Symlink(Symlink { target });
		let node = Node {
			edges: Vec::new(),
			metadata: Some(metadata),
			object: None,
			path: Some(Arc::new(path)),
			variant,
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
				node.variant.try_unwrap_file_ref().ok().map(|_| {
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
					.map(|result| result.unwrap())
				})
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		for (index, blob) in blobs {
			state.graph.nodes[index]
				.variant
				.unwrap_file_mut()
				.blob
				.replace(blob);
		}
		Ok(())
	}

	fn checkin_create_objects(state: &mut State, index: usize) -> tg::Result<()> {
		// Create object for the children.
		let indexes = state.graph.nodes[index]
			.edges
			.iter()
			.map(|edge| edge.node.unwrap())
			.collect_vec();
		for index in indexes.iter().copied() {
			Self::checkin_create_objects(state, index)?;
		}

		// Create an object for the node.
		let (kind, data) = match &state.graph.nodes[index].variant {
			Variant::Directory(directory) => {
				let kind = tg::object::Kind::Directory;
				let entries = directory
					.entries
					.iter()
					.map(|(name, index)| {
						let name = name.clone();
						let id = state.graph.nodes[*index]
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
			},
			Variant::File(file) => {
				let kind = tg::object::Kind::File;
				let contents = file.blob.as_ref().unwrap().id.clone();
				let dependencies = BTreeMap::new();
				let executable = file.executable;
				let data = tg::file::Data::Normal {
					contents,
					dependencies,
					executable,
				};
				let data = tg::object::Data::from(data);
				(kind, data)
			},
			Variant::Symlink(symlink) => {
				let kind = tg::object::Kind::Symlink;
				let target = symlink.target.clone();
				let data = tg::symlink::Data::Target { target };
				let data = tg::object::Data::from(data);
				(kind, data)
			},
		};

		// Create the object.
		let bytes = data
			.serialize()
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let id = tg::object::Id::new(kind, &bytes);
		let object = Object { bytes, data, id };
		state.graph.nodes[index].object = Some(object);

		Ok(())
	}

	async fn checkin_cache_task(
		&self,
		arg: &tg::artifact::checkin::Arg,
		root: &tg::artifact::Id,
		touched_at: i64,
	) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let path = arg.path.clone();
			move || {
				let mut stack = vec![path];
				while let Some(path) = stack.pop() {
					let metadata = std::fs::symlink_metadata(&path)
						.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
					if metadata.is_dir() {
						let read_dir = std::fs::read_dir(&path)
							.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
						for entry in read_dir {
							let entry = entry.map_err(|source| {
								tg::error!(!source, "failed to read the entry")
							})?;
							let path = entry.path();
							stack.push(path);
						}
					}
					if !metadata.is_symlink() {
						let mode = metadata.permissions().mode();
						let executable = mode & 0o111 != 0;
						let new_mode = if executable { 0o755 } else { 0o644 };
						if new_mode != mode {
							let permissions = std::fs::Permissions::from_mode(new_mode);
							std::fs::set_permissions(&path, permissions).map_err(
								|source| tg::error!(!source, %path = path.display(), "failed to set the permissions"),
							)?;
						}
					}
					let epoch =
						filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
					filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
					)?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		// Rename the path to the cache directory.
		let src = &arg.path;
		let dst = self.cache_path().join(root.to_string());
		let result = tokio::fs::rename(src, dst).await;
		match result {
			Ok(()) => (),
			Err(error)
				if matches!(
					error.kind(),
					std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::DirectoryNotEmpty,
				) => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to rename the path to the cache directory"
				));
			},
		}

		// Set the file times to the epoch.
		let path = self.cache_path().join(root.to_string());
		let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
		filetime::set_symlink_file_times(&path, epoch, epoch).map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to set the modified time"),
		)?;

		// Send a message for the cache entry.
		let message = crate::index::Message::PutCacheEntry(crate::index::PutCacheEntryMessage {
			id: root.clone(),
			touched_at,
		});
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		self.messenger
			.stream_publish("index".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	async fn checkin_messenger_task(
		&self,
		state: &Arc<State>,
		root: &tg::artifact::Id,
		touched_at: i64,
	) -> tg::Result<()> {
		for node in &state.graph.nodes {
			let object = node.object.as_ref().unwrap();
			let message = crate::index::Message::PutObject(crate::index::PutObjectMessage {
				children: object.data.children(),
				id: object.id.clone(),
				size: object.bytes.len().to_u64().unwrap(),
				touched_at,
				cache_reference: None,
			});
			let message = serde_json::to_vec(&message)
				.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
			self.messenger
				.stream_publish("index".to_owned(), message.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

			// Send messages for the blob.
			if let Variant::File(File {
				blob: Some(blob), ..
			}) = &node.variant
			{
				let mut stack = vec![blob];
				while let Some(blob) = stack.pop() {
					let children = blob
						.data
						.as_ref()
						.map(tg::blob::Data::children)
						.unwrap_or_default();
					let id = blob.id.clone().into();
					let size = blob.size;
					let message =
						crate::index::Message::PutObject(crate::index::PutObjectMessage {
							cache_reference: Some(root.clone()),
							children,
							id,
							size,
							touched_at,
						});
					let message = serde_json::to_vec(&message)
						.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
					self.messenger
						.stream_publish("index".to_owned(), message.into())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
					stack.extend(&blob.children);
				}
			}
		}
		Ok(())
	}

	async fn checkin_store_task(
		&self,
		arg: &tg::artifact::checkin::Arg,
		state: &State,
		root: &tg::artifact::Id,
		touched_at: i64,
	) -> tg::Result<()> {
		let mut objects = Vec::with_capacity(state.graph.nodes.len());
		for node in &state.graph.nodes {
			let object = node.object.as_ref().unwrap();
			objects.push((object.id.clone(), Some(object.bytes.clone()), None));
			if let Variant::File(File {
				blob: Some(blob), ..
			}) = &node.variant
			{
				let mut stack = vec![blob];
				while let Some(blob) = stack.pop() {
					let subpath = node
						.path
						.as_ref()
						.unwrap()
						.strip_prefix(&arg.path)
						.unwrap()
						.to_owned();
					let subpath = subpath
						.to_str()
						.unwrap()
						.is_empty()
						.not()
						.then_some(subpath);
					let reference = crate::store::CacheReference {
						artifact: root.clone(),
						subpath,
						position: blob.position,
						length: blob.length,
					};
					objects.push((blob.id.clone().into(), blob.bytes.clone(), Some(reference)));
					stack.extend(&blob.children);
				}
			} else {
				objects.push((object.id.clone(), Some(object.bytes.clone()), None));
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

	// Check in the artifact.
	async fn check_in_artifact_old(
		&self,
		arg: tg::artifact::checkin::Arg,
		progress: &crate::progress::Handle<tg::artifact::checkin::Output>,
	) -> tg::Result<tg::artifact::checkin::Output> {
		// Create the input graph.
		progress.log(tg::progress::Level::Info, "collecting input".into());
		progress.start(
			"input".into(),
			"files".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		let input_graph = self
			.create_input_graph(arg.clone(), progress)
			.await
			.map_err(
				|source| tg::error!(!source, %path = arg.path.display(), "failed to collect the input"),
			)?;

		// Create the unification graph and get its root node.
		progress.log(tg::progress::Level::Info, "unifying".into());
		let (unification_graph, root) = self
			.create_unification_graph(&input_graph, arg.deterministic)
			.await
			.map_err(|source| tg::error!(!source, "failed to unify dependencies"))?;

		// Create the object graph.
		progress.log(tg::progress::Level::Info, "creating objects".into());
		let object_graph = self
			.create_object_graph(&input_graph, &unification_graph, &root)
			.await
			.map_err(|source| tg::error!(!source, "failed to create objects"))?;

		// Create the output graph.
		progress.log(tg::progress::Level::Info, "collecting output".into());
		let output_graph = self
			.create_output_graph(&input_graph, &object_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to write objects"))?;

		// Cache the files.
		progress.log(tg::progress::Level::Info, "caching".into());
		self.checkin_cache_task_old(&output_graph, &input_graph)
			.await
			.map_err(|source| tg::error!(!source, "failed to copy the blobs"))?;

		// Write the output to the database and the store.
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		futures::try_join!(
			self.write_output_to_messenger(output_graph.clone(), object_graph.clone(), touched_at)
				.map_err(|source| tg::error!(!source, "failed to store the objects")),
			self.write_output_to_store(output_graph.clone(), object_graph.clone(), touched_at)
				.map_err(|source| tg::error!(!source, "failed to store the objects"))
		)?;

		// Copy or move to the cache directory.
		if arg.cache || arg.destructive {
			progress.log(
				tg::progress::Level::Info,
				"moving to cache directory".into(),
			);
			self.copy_or_move_to_cache_directory(&input_graph, &output_graph, 0, progress)
				.await
				.map_err(|source| tg::error!(!source, "failed to cache the artifact"))?;
		}

		// Get the artifact.
		let artifact = output_graph.nodes[0].id.clone();

		// If this is a non-destructive checkin, then attempt to write a lockfile.
		if arg.lockfile && !arg.destructive && artifact.is_directory() {
			progress.log(tg::progress::Level::Info, "writing lockfile".into());
			self.try_write_lockfile(&input_graph, &object_graph)
				.await
				.map_err(|source| tg::error!(!source, "failed to write lockfile"))?;
		}

		// Create the output.
		let output = tg::artifact::checkin::Output { artifact };

		Ok(output)
	}

	pub(crate) async fn ignore_matcher_for_checkin() -> tg::Result<ignore::Matcher> {
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
			.await
			.map_err(|source| tg::error!(!source, "failed to create the matcher"))
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
