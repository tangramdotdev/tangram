use crate::Server;
use either::Either;
use futures::{
	future::BoxFuture,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use indoc::formatdoc;
use itertools::Itertools;
use std::{
	collections::BTreeMap,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};
use std::{path::PathBuf, sync::Mutex};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::IntervalStream;

mod graph;

struct State {
	count: ProgressState,
	weight: ProgressState,
	visited: Mutex<BTreeMap<tg::Path, InnerOutput>>,
	graph: Mutex<graph::Graph>,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

#[derive(Clone, Debug)]
struct InnerOutput {
	artifact_id: Option<tg::artifact::Id>,
	graph_id: graph::Id,
	path: tg::path::Path,
	count: Option<u64>,
	weight: Option<u64>,
	lock: Option<Lock>,
	move_to_checkouts: bool,
	data: Option<tg::artifact::Data>,
	lock_data: Option<(tg::graph::Data, usize)>,
}

#[derive(Copy, Clone, Debug)]
enum Lock {
	File,
	Xattr,
}

#[derive(Clone)]
struct InnerInput<'a> {
	arg: tg::artifact::checkin::Arg,
	metadata: Option<std::fs::Metadata>,
	reference: tg::Reference,
	lock: Option<(tg::graph::Object, usize)>,
	state: &'a State,
}

#[derive(Copy, Clone, Debug)]
struct GraphImpl<'a>(&'a [tg::graph::data::Node]);

impl Server {
	pub async fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::artifact::checkin::Event>>> {
		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let graph = Mutex::new(graph::Graph::default());
		let visited = Mutex::new(BTreeMap::new());
		let state = Arc::new(State {
			count,
			weight,
			graph,
			visited,
		});

		// Spawn the task.
		let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
		tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let state = state.clone();
			async move {
				let result = server.check_in_artifact_task(arg, &state).await;
				result_sender.send(result).ok();
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let result = result_receiver.map(Result::unwrap).shared();
		let stream = IntervalStream::new(interval)
			.map(move |_| {
				let current = state
					.count
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.count
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let count = tg::Progress { current, total };
				let current = state
					.weight
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.weight
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let weight = tg::Progress { current, total };
				let progress = tg::artifact::checkin::Progress { count, weight };
				Ok(tg::artifact::checkin::Event::Progress(progress))
			})
			.take_until(result.clone())
			.chain(stream::once(result.map(|result| match result {
				Ok(id) => Ok(tg::artifact::checkin::Event::End(id)),
				Err(error) => Err(error),
			})));

		Ok(stream)
	}

	/// Attempt to store an artifact in the database.
	async fn check_in_artifact_task(
		&self,
		arg: tg::artifact::checkin::Arg,
		state: &State,
	) -> tg::Result<tg::artifact::Id> {
		// If this is a checkin of a path in the checkouts directory, then retrieve the corresponding artifact.
		let checkouts_path = self.checkouts_path().try_into()?;
		if let Some(path) = arg.path.diff(&checkouts_path).filter(tg::Path::is_internal) {
			let id = path
				.components()
				.get(1)
				.ok_or_else(|| tg::error!("cannot check in the checkouts directory"))?
				.try_unwrap_normal_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?
				.parse::<tg::artifact::Id>()?;
			let path = tg::Path::with_components(path.components().iter().skip(2).cloned());
			if path.components().len() == 1 {
				return Ok(id);
			}
			let artifact = tg::Artifact::with_id(id);
			let directory = artifact
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("invalid path"))?;
			let artifact = directory.get(self, &path).await?;
			let id = artifact.id(self).await?;
			return Ok(id);
		}

		// Check in the artifact.
		self.check_in_artifact_inner(arg.clone(), state).await?;

		// Get the root artifact.
		let output = state
			.visited
			.lock()
			.unwrap()
			.get(&arg.path)
			.ok_or_else(|| tg::error!("invalid object graph"))?
			.clone();

		// Get the artifact ID.
		let artifact = output
			.artifact_id
			.ok_or_else(|| tg::error!("invalid object graph"))?;

		// Move any outputs to the checkouts directory.
		// The output paths are updated in-place if they are moved.
		let mut outputs = state
			.visited
			.lock()
			.unwrap()
			.values()
			.cloned()
			.collect::<Vec<_>>();
		outputs
			.iter_mut()
			.filter_map(|output| {
				if !output.move_to_checkouts {
					return None;
				}
				let server = self.clone();
				let fut = async move { server.move_to_checkouts(output, arg.destructive).await };
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Write locks
		let graph = state.graph.lock().unwrap().clone();
		outputs
			.iter()
			.filter_map(|output| {
				let lock = output.lock?;
				let server = self.clone();
				let graph = graph.clone();
				let fut = async move { server.write_lock(output, &graph, lock).await };
				Some(fut)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Create hardlinks to files and symlinks for blobs.
		// TODO: this is an excessive amount of I/O. Do not do this.
		for output in outputs {
			let artifact_id = output
				.artifact_id
				.as_ref()
				.ok_or_else(|| tg::error!(%path = output.path, "missing artifact in graph"))?;

			// If this is a file, create a hard link to it and then create a symlink to its blob.
			if let Some(tg::artifact::Data::File(file)) = &output.data {
				// Create a hard link in the root directory.
				let src = &output.path;
				let dst = self.checkouts_path().join(artifact_id.to_string());
				tokio::fs::hard_link(src, &dst).await.ok();

				// Create a symlink in the outputs directory.
				let src = PathBuf::from("../checkouts").join(artifact_id.to_string());
				let dst = self.blobs_path().join(file.contents.to_string());
				tokio::fs::symlink(src, dst).await.ok();
			}
		}

		Ok(artifact)
	}

	async fn move_to_checkouts(
		&self,
		output: &mut InnerOutput,
		destructive: bool,
	) -> tg::Result<()> {
		// Get the artifact id.
		let artifact = output
			.artifact_id
			.as_ref()
			.ok_or_else(|| tg::error!("incomplete output"))?;

		// Get the path to the root in the checkouts directory.
		let root_path = self.checkouts_path().join(artifact.to_string());

		// If the artifact already exists, exit early.
		if tokio::fs::try_exists(&root_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if file exists"))?
		{
			return Ok(());
		}

		// Rename or move the input to the temps directory.
		if destructive {
			match tokio::fs::rename(&output.path, &root_path).await {
				Ok(()) => (),
				Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
					copy_all(output.path.as_ref(), &root_path).await.map_err(
						|source| tg::error!(!source, %path = output.path, "failed to copy file"),
					)?;
				},
				Err(source) => {
					return Err(tg::error!(!source, %path = output.path, "failed to rename file"));
				},
			}
		} else {
			copy_all(output.path.as_ref(), &root_path).await.map_err(
				|source| tg::error!(!source, %path = output.path, "failed to copy file"),
			)?;
		}

		// Update the path.
		output.path = root_path.try_into()?;
		Ok(())
	}

	async fn write_lock(
		&self,
		output: &InnerOutput,
		_graph: &graph::Graph,
		_lock: Lock,
	) -> tg::Result<()> {
		// TODO: write xattrs too.
		let Some((data, _index)) = &output.lock_data.clone() else {
			return Err(tg::error!("missing lock data"));
		};

		// Write to the database.
		tg::Graph::with_object(Arc::new(data.clone().try_into()?))
			.store(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to store lock"))?;

		let _permit = self.file_descriptor_semaphore.acquire().await;

		let metadata = tokio::fs::symlink_metadata(&output.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get file metadata"))?;

		let path = if metadata.is_dir() {
			output
				.path
				.clone()
				.join(tg::artifact::module::LOCKFILE_FILE_NAME)
		} else if metadata.is_file() {
			output
				.path
				.clone()
				.parent()
				.join(tg::artifact::module::LOCKFILE_FILE_NAME)
		} else {
			return Err(tg::error!("invalid file type"));
		};

		let bytes = serde_json::to_vec_pretty(data)
			.map_err(|source| tg::error!(!source, "failed to serialize lock file"))?;
		tokio::fs::File::options()
			.create(true)
			.write(true)
			.truncate(true)
			.append(false)
			.open(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to open file"))?
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to write lock file"))?;

		Ok(())
	}

	async fn check_in_artifact_inner(
		&self,
		arg: tg::artifact::checkin::Arg,
		state: &State,
	) -> tg::Result<()> {
		// Check if we've visited this path already.
		if state.visited.lock().unwrap().contains_key(&arg.path) {
			return Ok(());
		}

		// Create the input.
		let reference = tg::Reference::with_path(&arg.path);
		let input = InnerInput {
			arg,
			metadata: None,
			reference,
			state,
			lock: None,
		};

		// Create the graph and get the root node ID.
		let root = self.add_path_to_graph(input).await?;

		// Check if there is an existing lockfile.
		let graph = state.graph.lock().unwrap().clone();

		// Unify.
		let graph = self.unify_dependencies(graph, &root).await?;

		// Validate.
		graph.validate(self)?;

		// Create locks.
		let (assignments, mut locks) = self.create_locks(&graph, &root, state).await?;

		// Update the output locks.
		for (node, (lock, index)) in &assignments {
			let Some(path) = graph.nodes.get(node).unwrap().path.as_ref() else {
				continue;
			};
			state
				.visited
				.lock()
				.unwrap()
				.get_mut(path)
				.unwrap()
				.lock_data
				.replace((locks[*lock].clone(), *index));
		}

		// Create artifacts.
		self.create_artifacts(&graph, &root, &assignments, &mut locks, state)?;

		// Update the state.
		let visited = state.visited.lock().unwrap();
		for output in visited.values() {
			if let Some(count) = output.count {
				state.count.current.fetch_add(count, Ordering::Relaxed);
			}
			if let Some(weight) = output.weight {
				state.weight.current.fetch_add(weight, Ordering::Relaxed);
			}
		}

		Ok(())
	}

	async fn create_locks(
		&self,
		graph: &graph::Graph,
		root: &graph::Id,
		state: &State,
	) -> tg::Result<(BTreeMap<graph::Id, (usize, usize)>, Vec<tg::graph::Data>)> {
		// Create objects for every node in the graph (without dependencies).
		let objects = self.create_objects(graph, root, state);

		// Walk the graph to assign indices. This ensures the ordering of indices is stable.
		let mut stack = vec![root];
		let mut counter = 0usize;

		// Keep a bi-map of graph node ids and lock node indicies.
		let mut indices = BTreeMap::new();
		let mut ids = BTreeMap::new();
		while let Some(node) = stack.pop() {
			// Skip previously visited nodes.
			if indices.contains_key(node) {
				continue;
			};

			// Create a node index for this node.
			let index = counter;
			counter += 1;
			indices.insert(node.clone(), index);
			ids.insert(index, node.clone());
			let node = graph.nodes.get(node).unwrap();

			// Add directory children to the stack.
			if let Some(graph::Object::Directory(directory)) = &node.object {
				for neighbor in directory.values() {
					stack.push(neighbor);
				}
			}

			// Add outgoing edges to the stack.
			for neighbor in node.outgoing.values() {
				stack.push(neighbor);
			}
		}

		// Map from graph nodes to lock nodes.
		let nodes = indices
			.iter()
			.sorted_by_cached_key(|(_, v)| **v)
			.filter_map(|(id, _)| {
				let node = graph.nodes.get(id)?;
				let dependencies = node
					.outgoing
					.iter()
					.map(|(reference, id)| {
						let index = *indices.get(id).unwrap();
						(reference.clone(), Either::Left(index))
					})
					.collect();

				let object = objects.get(id).cloned();

				let node = tg::graph::data::Node {
					object,
					dependencies: Some(dependencies),
				};
				Some(node)
			})
			.collect::<Vec<_>>();

		// Split into sub-locks.
		let mut locks: Vec<tg::graph::Data> = Vec::with_capacity(nodes.len());
		let mut lock_indices = BTreeMap::new();
		let mut lock_ids = BTreeMap::new();

		let mut assignments = BTreeMap::new();

		// Create locks from the strongly connected components.
		for (lock_index, node_indices) in petgraph::algo::tarjan_scc(GraphImpl(&nodes))
			.into_iter()
			.enumerate()
		{
			// Create an empty lock object.
			let mut lock = tg::graph::Data {
				nodes: Vec::with_capacity(node_indices.len()),
			};

			// Mark all the nodes as belonging to this lock.
			for (new_index, old_index) in node_indices.iter().copied().enumerate() {
				lock_indices.insert(old_index, (lock_index, new_index));
			}

			// Create new lock nodes.
			for (new_index, old_index) in node_indices.iter().copied().enumerate() {
				// Get the node data.
				let mut node = nodes[old_index].clone();

				// Remap dependencies.
				for (_, either) in node.dependencies.iter_mut().flatten() {
					let Either::Left(old_index) = either else {
						continue;
					};
					let (lock_index_, new_index) = lock_indices.get(old_index).unwrap();

					// If the old index refers to a node in this lock, use it.
					if *lock_index_ == lock_index {
						*old_index = *new_index;
						continue;
					}

					// Otherwise create a new file.
					let lock: &tg::graph::Id = lock_ids
						.get(lock_index_)
						.ok_or_else(|| tg::error!("invalid graph"))?;
					let Some(graph::Object::File { blob, executable }) = &graph
						.nodes
						.get(ids.get(&old_index).unwrap())
						.unwrap()
						.object
					else {
						return Err(tg::error!("expected a file"));
					};
					let data = tg::file::Data {
						contents: blob.clone(),
						executable: *executable,
						metadata: None,
						dependencies: Some(tg::file::data::Dependencies::Lock(
							lock.clone(),
							*new_index,
						)),
					};
					let id = tg::file::Id::new(&data.serialize()?);
					// TODO: count/weight
					// Update the entry.
					*either = Either::Right(id.into());
				}

				// Add the node to the lock.
				lock.nodes.push(node);

				// Mark the lock and node indices for the graph node.
				let graph_id = ids.get(&old_index).unwrap().clone();
				assignments.insert(graph_id.clone(), (lock_index, new_index));
			}

			// Get the lock ID by storing it in the database.
			let id = tg::Graph::with_object(Arc::new(lock.clone().try_into()?))
				.id(self)
				.await?;

			// Add the lock to the list of locks.
			lock_ids.insert(lock_index, id);
			locks.push(lock);
		}

		Ok((assignments, locks))
	}

	fn create_objects(
		&self,
		graph: &graph::Graph,
		root: &graph::Id,
		state: &State,
	) -> BTreeMap<graph::Id, tg::object::Id> {
		let mut visited = BTreeMap::new();
		self.create_objects_inner(&graph, root, &mut visited, state);
		visited
	}

	fn create_objects_inner(
		&self,
		graph: &graph::Graph,
		node: &graph::Id,
		visited: &mut BTreeMap<graph::Id, tg::object::Id>,
		state: &State,
	) -> tg::object::Id {
		if let Some(object) = visited.get(node) {
			return object.clone();
		};
		let (object, data) = match &graph.nodes.get(node).unwrap().object.as_ref().unwrap() {
			graph::Object::Directory(entries) => {
				let entries = entries
					.iter()
					.map(|(name, node)| {
						let object = self.create_objects_inner(graph, node, visited, state);
						let artifact = match object {
							tg::object::Id::Directory(id) => tg::artifact::Id::Directory(id),
							tg::object::Id::File(id) => tg::artifact::Id::File(id),
							tg::object::Id::Symlink(id) => tg::artifact::Id::Symlink(id),
							_ => unreachable!(),
						};
						(name.clone(), artifact)
					})
					.collect();
				let data = tg::directory::Data { entries };
				let object: tg::object::Id =
					tg::directory::Id::new(&data.serialize().unwrap()).into();
				(object, data.into())
			},
			graph::Object::File { blob, executable } => {
				let data = tg::file::Data {
					contents: blob.clone(),
					dependencies: None,
					executable: *executable,
					metadata: None, // todo: metadata
				};
				let object = tg::file::Id::new(&data.serialize().unwrap()).into();
				(object, data.into())
			},
			graph::Object::Symlink(data) => {
				let object = tg::symlink::Id::new(&data.serialize().unwrap()).into();
				(object, data.clone().into())
			},
			graph::Object::Object(object) => {
				visited.insert(node.clone(), object.clone());
				return object.clone();
			},
		};
		visited.insert(node.clone(), object.clone());

		// If the node is present on the file system, mark its object data.
		if let Some(path) = graph.nodes.get(node).unwrap().path.as_ref() {
			let mut visited = state.visited.lock().unwrap();
			let output = visited.get_mut(path).unwrap();
			output.data.replace(data);
		}

		// Visit the dependencies.
		for dependency in graph.nodes.get(node).unwrap().outgoing.values() {
			self.create_objects_inner(graph, dependency, visited, state);
		}

		object
	}

	fn create_artifacts(
		&self,
		graph: &graph::Graph,
		node: &graph::Id,
		assignments: &BTreeMap<graph::Id, (usize, usize)>,
		locks: &[tg::graph::Data],
		state: &State,
	) -> tg::Result<()> {
		let (lock_index, node_index) = assignments.get(node).copied().unwrap();
		let lock_node = &locks[lock_index].nodes[node_index];
		let graph_node = graph.nodes.get(node).unwrap();
		let Some(path) = &graph_node.path else {
			return Ok(());
		};
		let mut visited = state.visited.lock().unwrap();
		let output = visited.get_mut(path).unwrap();
		if output.artifact_id.is_some() {
			return Ok(());
		};
		let Some(object) = lock_node.object.as_ref() else {
			return Ok(());
		};
		let artifact = match object {
			tg::object::Id::Directory(id) => tg::artifact::Id::Directory(id.clone()),
			tg::object::Id::File(id) => 'a: {
				if graph_node.outgoing.is_empty() {
					break 'a tg::artifact::Id::File(id.clone());
				}
				let Some(graph::Object::File { blob, executable }) = &graph_node.object else {
					unreachable!()
				};
				let lock = tg::graph::Id::new(&locks[lock_index].serialize()?);
				let node = node_index;
				let dependencies = tg::file::data::Dependencies::Lock(lock, node);
				let data = tg::file::Data {
					contents: blob.clone(),
					dependencies: Some(dependencies),
					executable: *executable,
					metadata: None, // TODO: metadata
				};
				let id = tg::file::Id::new(&data.serialize()?).into();
				output.data.replace(data.into());
				id
			},
			tg::object::Id::Symlink(id) => tg::artifact::Id::Symlink(id.clone()),
			_ => return Err(tg::error!("expected an artifact, found an object")),
		};
		output.artifact_id.replace(artifact);
		drop(visited);
		if let Some(graph::Object::Directory(children)) = &graph_node.object {
			for child in children.values() {
				self.create_artifacts(graph, child, assignments, locks, state)?;
			}
		}
		for dependency in graph_node.outgoing.values() {
			self.create_artifacts(graph, dependency, assignments, locks, state)?;
		}
		Ok(())
	}

	pub(crate) fn try_store_artifact_future(
		&self,
		id: &tg::artifact::Id,
	) -> BoxFuture<'static, tg::Result<bool>> {
		let server = self.clone();
		let id = id.clone();
		Box::pin(async move { server.try_store_artifact_inner(&id).await })
	}

	pub(crate) async fn try_store_artifact_inner(&self, id: &tg::artifact::Id) -> tg::Result<bool> {
		// Check if the artifact exists in the checkouts directory.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let path = self.checkouts_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?;
		if !exists {
			return Ok(false);
		}
		drop(permit);

		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: None,
		};
		let graph = Mutex::new(graph::Graph::default());
		let visited = Mutex::new(BTreeMap::new());
		let state = Arc::new(State {
			count,
			weight,
			graph,
			visited,
		});

		// Check in the artifact.
		let arg = tg::artifact::checkin::Arg {
			destructive: false,
			locked: true,
			path: path.try_into()?,
		};
		self.check_in_artifact_inner(arg.clone(), &state).await?;
		let artifact_id = state
			.visited
			.lock()
			.unwrap()
			.get(&arg.path)
			.ok_or_else(|| tg::error!("invalid graph"))?
			.artifact_id
			.clone()
			.ok_or_else(|| tg::error!("invalid graph"))?;

		if &artifact_id != id {
			return Err(tg::error!("corrupted internal checkout"));
		}

		// Get a database connection.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Collect the output.
		let output = state
			.visited
			.lock()
			.unwrap()
			.values()
			.cloned()
			.collect::<Vec<_>>();

		// Insert into the database.
		for output in output {
			// Validate output.
			let id = output
				.artifact_id
				.ok_or_else(|| tg::error!("invalid graph"))?;
			let data = output.data.ok_or_else(|| tg::error!("invalid graph"))?;
			let bytes = data.serialize()?;
			let count = output.count;
			let weight = output.weight;

			// Insert.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6)
					on conflict (id) do update set touched_at = {p}6;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let params = db::params![id, bytes, 1, count, weight, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_check_in_artifact_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let stream = handle.check_in_artifact(arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::artifact::checkin::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::artifact::checkin::Event::End(artifact)) => {
				let event = "end".to_owned();
				let data = serde_json::to_string(&artifact).unwrap();
				let event = tangram_http::sse::Event {
					event: Some(event),
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}

	// async fn add_output_to_tangram_directory_inner(
	// 	&self,
	// 	output: &BTreeMap<tg::Path, InnerOutput>,
	// 	from: &tg::Path,
	// 	to: Option<&tg::Path>,
	// 	visited: &Mutex<BTreeSet<tg::Path>>,
	// ) -> tg::Result<()> {
	// 	let Some(output_) = output.get(from) else {
	// 		tracing::warn!(%from, "path added to directory during checkin");
	// 		return Ok(());
	// 	};
	// 	let mut visited_ = visited.lock().unwrap();
	// 	if visited_.contains(from) {
	// 		return Ok(());
	// 	};
	// 	visited_.insert(from.clone());
	// 	drop(visited_);

	// 	// Get the artifact ID.
	// 	let artifact = output_
	// 		.artifact_id
	// 		.as_ref()
	// 		.ok_or_else(|| tg::error!("incomplete output"))?;
	// 	match artifact {
	// 		tg::artifact::Id::Directory(_artifact) => {
	// 			// Create the directory if necessary.
	// 			if let Some(to) = to {
	// 				let _permit =
	// 					self.file_descriptor_semaphore
	// 						.acquire()
	// 						.await
	// 						.map_err(|source| {
	// 							tg::error!(!source, "failed to acquire file descriptor permit")
	// 						})?;
	// 				tokio::fs::create_dir_all(&to)
	// 					.await
	// 					.map_err(
	// 						|source| tg::error!(!source, %path = to, "failed to create directory"),
	// 					)
	// 					.map_err(|source| tg::error!(!source, "failed to create directory"))?;
	// 			}

	// 			// Walk the entries.
	// 			let entries = tokio::fs::read_dir(&from).await.map_err(
	// 				|source| tg::error!(!source, %path = from, "failed to read directory"),
	// 			)?;
	// 			futures::stream::try_unfold(entries, |mut entries| async {
	// 				let Some(entry) = entries.next_entry().await.map_err(|source| {
	// 					tg::error!(!source, "failed to get next directory entry")
	// 				})?
	// 				else {
	// 					return Ok(None);
	// 				};
	// 				let name = entry
	// 					.file_name()
	// 					.into_string()
	// 					.map_err(|_| tg::error!("non utf8 file name"))?;
	// 				Ok::<_, tg::Error>(Some((name, entries)))
	// 			})
	// 			.try_collect::<Vec<_>>()
	// 			.await?
	// 			.into_iter()
	// 			.map(|name| {
	// 				let to = to.map(|to| to.clone().join(&name));
	// 				let from = from.clone().join(&name);
	// 				async move {
	// 					Box::pin(self.add_output_to_tangram_directory_inner(
	// 						output,
	// 						&from,
	// 						to.as_ref(),
	// 						visited,
	// 					))
	// 					.await
	// 				}
	// 			})
	// 			.collect::<FuturesUnordered<_>>()
	// 			.try_collect::<Vec<_>>()
	// 			.await?;
	// 		},
	// 		tg::artifact::Id::File(_artifact) => {
	// 			// Copy the file if necessary.
	// 			if let Some(to) = to {
	// 				let _permit =
	// 					self.file_descriptor_semaphore
	// 						.acquire()
	// 						.await
	// 						.map_err(|source| {
	// 							tg::error!(!source, "failed to acquire file descriptor semaphore")
	// 						})?;
	// 				tokio::fs::copy(from, to)
	// 					.await
	// 					.map_err(|source| tg::error!(!source, "failed to copy file"))?;
	// 			}

	// 			// If the file has dependencies, then add them.
	// 			let Some(data) = output_
	// 				.data
	// 				.as_ref()
	// 				.unwrap()
	// 				.try_unwrap_file_ref()
	// 				.unwrap()
	// 				.dependencies
	// 				.as_ref()
	// 			else {
	// 				return Ok(());
	// 			};
	// 			match data {
	// 				tg::file::data::Dependencies::Set(_) => todo!(),
	// 				tg::file::data::Dependencies::Map(_) => todo!(),
	// 				tg::file::data::Dependencies::Lock(id, node) => {
	// 					let lock = tg::Lock::with_id(id.clone()).object(self).await?;
	// 					let node = &lock.nodes[*node];
	// 					node.dependencies
	// 						.iter()
	// 						.flat_map(BTreeMap::keys)
	// 						.filter_map(|reference| {
	// 							let path = reference
	// 								.path()
	// 								.try_unwrap_path_ref()
	// 								.ok()
	// 								.or_else(|| reference.query()?.path.as_ref())?;
	// 							let (from, to) =
	// 								if tg::artifact::module::is_module_path(from.as_ref()) {
	// 									let from = from.clone().parent().join(path.clone());
	// 									let to =
	// 										to.map(|to| to.clone().parent().join(path.clone()));
	// 									(from, to)
	// 								} else {
	// 									let from = from.clone().join(path.clone());
	// 									let to = to.map(|to| to.clone().join(path.clone()));
	// 									(from, to)
	// 								};
	// 							let fut = async move {
	// 								Box::pin(self.add_output_to_tangram_directory_inner(
	// 									output,
	// 									&from,
	// 									to.as_ref(),
	// 									visited,
	// 								))
	// 								.await
	// 							};
	// 							Some(fut)
	// 						})
	// 						.collect::<FuturesUnordered<_>>()
	// 						.try_collect::<Vec<_>>()
	// 						.await?;
	// 				},
	// 			}
	// 		},
	// 		tg::artifact::Id::Symlink(_artifact) => {
	// 			let Some(to) = to else {
	// 				return Ok(());
	// 			};
	// 			let _permit = self
	// 				.file_descriptor_semaphore
	// 				.acquire()
	// 				.await
	// 				.map_err(|source| {
	// 					tg::error!(!source, "failed to get file descriptor permit")
	// 				})?;
	// 			let target = tokio::fs::read_link(&from)
	// 				.await
	// 				.map_err(|source| tg::error!(!source, %path = from, "failed to read link"))?;
	// 			tokio::fs::symlink(&target, &to)
	// 				.await
	// 				.map_err(|source| tg::error!(!source, "failed to create symlink"))?;
	// 		},
	// 	}
	// 	Ok(())
	// }
}

async fn copy_all(from: &std::path::Path, to: &std::path::Path) -> tg::Result<()> {
	let mut stack = vec![(from.to_owned(), to.to_owned())];
	while let Some((from, to)) = stack.pop() {
		let metadata = tokio::fs::symlink_metadata(&from).await.map_err(
			|source| tg::error!(!source, %path = from.display(), "failed to get file metadata"),
		)?;
		let file_type = metadata.file_type();
		if file_type.is_dir() {
			tokio::fs::create_dir_all(&to).await.map_err(
				|source| tg::error!(!source, %path = to.display(), "failed to create directory"),
			)?;
			let mut entries = tokio::fs::read_dir(&from).await.map_err(
				|source| tg::error!(!source, %path = from.display(), "failed to read directory"),
			)?;
			while let Some(entry) = entries
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get directory entry"))?
			{
				let from = from.join(entry.file_name());
				let to = to.join(entry.file_name());
				stack.push((from, to));
			}
		} else if file_type.is_file() {
			tokio::fs::copy(&from, &to).await.map_err(
				|source| tg::error!(!source, %from = from.display(), %to = to.display(), "failed to copy file"),
			)?;
		} else if file_type.is_symlink() {
			let target = tokio::fs::read_link(&from).await.map_err(
				|source| tg::error!(!source, %path = from.display(), "failed to read link"),
			)?;
			tokio::fs::symlink(&target, &to)
				.await
				.map_err(|source| tg::error!(!source, %src = target.display(), %dst = to.display(), "failed to create symlink"))?;
		} else {
			return Err(tg::error!(%path = from.display(), "invalid file type"))?;
		}
	}
	Ok(())
}

impl<'a> petgraph::visit::GraphBase for GraphImpl<'a> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl<'a> petgraph::visit::GraphRef for GraphImpl<'a> {}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> petgraph::visit::NodeIndexable for GraphImpl<'a> {
	fn from_index(self: &Self, i: usize) -> Self::NodeId {
		i
	}

	fn node_bound(self: &Self) -> usize {
		self.0.len()
	}

	fn to_index(self: &Self, a: Self::NodeId) -> usize {
		a
	}
}

impl<'a> petgraph::visit::IntoNeighbors for GraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		let iter = self.0[a]
			.dependencies
			.iter()
			.flat_map(BTreeMap::values)
			.filter_map(|v| v.as_ref().left().copied());
		Box::new(iter)
	}
}

impl<'a> petgraph::visit::IntoNodeIdentifiers for GraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.0.len()
	}
}
