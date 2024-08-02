use crate::Server;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt};
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};
use tangram_client as tg;

#[derive(Clone, Debug)]
struct State {
	// The current graph.
	graph: Graph,

	// The edge that we are currently following.
	edge: Edge,

	// A work queue of edges we will have to follow, in depth-first order.
	queue: im::Vector<Edge>,

	// A lazily-initialized set of packages to try.
	objects: Option<im::Vector<(tg::Tag, tg::Object)>>,
}

// A graph of packages.
#[derive(Clone, Default, Debug)]
pub(super) struct Graph {
	// A counter used to create IDs for nodes that don't have a repository ID.
	counter: usize,

	// The set of nodes in the graph.
	pub nodes: im::HashMap<Id, Node>,
}

// A node within the package graph.
#[derive(Clone, Debug)]
#[allow(clippy::struct_field_names)]
pub struct Node {
	// A unique identifier of the node within the package graph.
	pub id: Id,

	// The result of this node (None if we do not know if it is successful or not).
	pub errors: Vec<tg::Error>,

	// Direct dependencies.
	pub outgoing: BTreeMap<tg::Reference, Id>,

	// Whether to inline the object in the output.
	// TODO: inline objects
	pub _inline_object: bool,

	// The underlying object.
	pub object: Option<Object>,

	// The tag of this node, if it exists.
	pub tag: Option<tg::Tag>,

	// The path of the underlying thing on disk.
	pub path: Option<tg::Path>,
}

pub(super) type Id = Either<tg::Reference, usize>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Edge {
	// The source node of the edge (referrer)
	pub src: Id,

	// The destination of the edge (dependency)
	pub dst: Id,

	// The constraint the referrer places on the dependency.
	pub reference: tg::Reference,
}

#[derive(Clone, Debug)]
pub(super) enum Object {
	File {
		blob: tg::blob::Id,
		executable: bool,
	},
	Directory(BTreeMap<String, Id>),
	Symlink(tg::symlink::Data),
	Object(tg::object::Id),
}

impl Server {
	pub(super) async fn add_path_to_graph(
		&self,
		mut input: super::InnerInput<'_>,
	) -> tg::Result<Id> {
		// Check if we've visited this path yet.
		if let Some(output) = input
			.state
			.visited
			.lock()
			.unwrap()
			.get(&input.arg.path)
			.cloned()
		{
			return Ok(output.graph_id);
		}

		// Create the id.
		let graph_id = input.state.graph.lock().unwrap().next_id();

		// Get the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(&input.arg.path).await.map_err(
			|source| tg::error!(!source, %path = input.arg.path, "failed to get the metadata for the path"),
		)?;
		input.metadata.replace(metadata.clone());

		// Create the output.
		let output = super::InnerOutput {
			graph_id: graph_id.clone(),
			artifact_id: None,
			path: input.arg.path.clone(),
			count: None,
			weight: None,
			lock: None,
			move_to_checkouts: true,
			data: None,
			lock_data: None,
		};
		input
			.state
			.visited
			.lock()
			.unwrap()
			.insert(input.arg.path.clone(), output);

		// Call the appropriate function for the file system object at the path.
		if metadata.is_dir() {
			self.add_directory_to_graph(&graph_id, &input)
				.await
				.map_err(
					|source| tg::error!(!source, %path = input.arg.path, "failed to check in the directory"),
				)?
		} else if metadata.is_file() {
			self.add_file_to_graph(&graph_id, &input, input.arg.path.clone())
				.await
				.map_err(
					|source| tg::error!(!source, %path = input.arg.path, "failed to check in the file"),
				)?
		} else if metadata.is_symlink() {
			self.add_symlink_to_graph(&graph_id, &input).await.map_err(
				|source| tg::error!(!source, %path = input.arg.path, "failed to check in the symlink"),
			)?
		} else {
			let file_type = metadata.file_type();
			return Err(tg::error!(
				%path = input.arg.path,
				?file_type,
				"invalid file type"
			));
		};

		// Return the id.
		Ok(graph_id)
	}

	pub(super) async fn add_directory_to_graph(
		&self,
		id: &Id,
		input: &super::InnerInput<'_>,
	) -> tg::Result<()> {
		// If a root module path exists, check in as a file.
		'a: {
			let permit = self.file_descriptor_semaphore.acquire().await;
			let Ok(Some(path)) =
				tg::artifact::module::try_get_root_module_path_for_path(input.arg.path.as_ref())
					.await
			else {
				break 'a;
			};
			let path = input.arg.path.clone().join(path);
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to get the file metadata"))?;
			drop(permit);
			if !metadata.is_file() {
				return Err(tg::error!(%path, "expected a file"));
			}
			let mut input = input.clone();
			input.metadata.replace(metadata);
			return self.add_file_to_graph(id, &input, path).await;
		}

		// Read the directory.
		let names = {
			let _permit = self.file_descriptor_semaphore.acquire().await;
			let mut read_dir = tokio::fs::read_dir(&input.arg.path)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the directory"))?;
			let mut names = Vec::new();
			while let Some(entry) = read_dir
				.next_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the directory entry"))?
			{
				let name = entry
					.file_name()
					.to_str()
					.ok_or_else(|| {
						let name = entry.file_name();
						tg::error!(?name, "all file names must be valid UTF-8")
					})?
					.to_owned();
				names.push(name);
			}
			names
		};

		// Recurse into the directory's entries.
		let entries = names
			.iter()
			.map(|name| async {
				let path = input.arg.path.clone().join(name.clone());
				let mut input = input.clone();
				input.arg.path = path;
				input.reference = tg::Reference::with_path(&name.parse()?);
				let id = Box::pin(self.add_path_to_graph(input)).await?;
				Ok::<_, tg::Error>((name.clone(), id))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<BTreeMap<_, _>>()
			.await?;

		// Create a node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
			object: Some(Object::Directory(entries)),
			tag: None,
			path: Some(input.arg.path.clone()),
		};

		// Add the node to the graph.
		input
			.state
			.graph
			.lock()
			.unwrap()
			.nodes
			.insert(id.clone(), node);

		Ok(())
	}

	pub(super) async fn add_file_to_graph(
		&self,
		id: &Id,
		input: &super::InnerInput<'_>,
		path: tg::Path,
	) -> tg::Result<()> {
		// Create the blob without writing to disk/database.
		let _permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;

		// Create the (blob) output.
		let output = self
			.create_blob_inner(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the contents"))?;

		// Determine if the file is executable.
		let executable = (input.metadata.as_ref().unwrap().permissions().mode() & 0o111) != 0;

		// Get the outgoing references
		let outgoing = self
			.get_outgoing_references_for_path(id, input)
			.await
			.map_err(|source| tg::error!(!source, "failed to find outgoing references for file"))?;

		// Create a node.
		let node = Node {
			id: id.clone(),
			_inline_object: false,
			errors: Vec::new(),
			outgoing,
			object: Some(Object::File {
				blob: output.blob,
				executable,
			}),
			tag: None,
			path: Some(input.arg.path.clone()),
		};

		// Add the node to the graph.
		input
			.state
			.graph
			.lock()
			.unwrap()
			.nodes
			.insert(id.clone(), node);

		Ok(())
	}

	async fn add_object_id_to_graph(
		&self,
		state: &super::State,
		object: &tg::object::Id,
	) -> tg::Result<Id> {
		let mut graph = state.graph.lock().unwrap();

		// Create a node ID for the object.
		let id = graph.next_id();

		// Create a node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
			object: Some(Object::Object(object.clone())),
			tag: None,
			path: None,
		};

		// Update the graph
		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	async fn get_outgoing_references_for_path(
		&self,
		_id: &Id,
		input: &super::InnerInput<'_>,
	) -> tg::Result<BTreeMap<tg::Reference, Id>> {
		// Attempt to read dependencies from the xattrs, if they exist.
		if let Some(dependencies) = xattr::get(
			&input.arg.path,
			tg::file::TANGRAM_FILE_DEPENDENCIES_XATTR_NAME,
		)
		.ok()
		.flatten()
		.and_then(|bytes| serde_json::from_slice::<tg::file::data::Dependencies>(&bytes).ok())
		{
			// Mark that we need to update xattrs.
			input
				.state
				.visited
				.lock()
				.unwrap()
				.get_mut(&input.arg.path)
				.unwrap()
				.lock
				.replace(super::Lock::Xattr);

			// Walk the dependencies
			match dependencies {
				tg::file::data::Dependencies::Set(dependencies) => {
					let mut outgoing = BTreeMap::new();
					for object in dependencies {
						let id = self.add_object_id_to_graph(input.state, &object).await?;
						outgoing.insert(tg::Reference::with_object(&object), id);
					}
					return Ok(outgoing);
				},
				tg::file::data::Dependencies::Map(_) => todo!(),
				tg::file::data::Dependencies::Lock(_, _) => todo!(),
			}
		}

		// If this is not a module path, then there are no dependencies.
		if !tg::artifact::module::is_module_path(input.arg.path.as_ref()) {
			return Ok(BTreeMap::new());
		}

		// Read the file.
		let text = tokio::fs::read_to_string(&input.arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = input.arg.path, "failed to read file"))?;

		// Compile the file.
		let analysis = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

		let mut outgoing = BTreeMap::new();

		// TODO: reuse lock
		let lock = self.search_for_lock(input).await?;
		for import in analysis.imports {
			if let Some(path) = import
				.reference
				.path()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| import.reference.query()?.path.as_ref())
			{
				// Handle path dependencies.
				let path = input
					.arg
					.path
					.clone()
					.parent()
					.join(path.clone())
					.normalize();
				let mut input = input.clone();
				input.arg.path = path.clone();
				input.reference = import.reference.clone();
				input.lock = lock.clone();
				let id = Box::pin(self.add_path_to_graph(input)).await.map_err(
					|source| tg::error!(!source, %path, "failed to add dependency to graph"),
				)?;
				outgoing.insert(import.reference.clone(), id);
			} else if let Ok(object) = import.reference.path().try_unwrap_object_ref() {
				// Create a node ID for the object.
				let id = input.state.graph.lock().unwrap().next_id();

				// Add it as a dependency of the parent.
				outgoing.insert(tg::Reference::with_object(object), id.clone());

				// Create a node.
				let node = Node {
					id: id.clone(),
					errors: Vec::new(),
					outgoing: BTreeMap::new(),
					_inline_object: true,
					object: Some(Object::Object(object.clone())),
					tag: None,
					path: None,
				};

				// Update the graph
				input.state.graph.lock().unwrap().nodes.insert(id, node);
			} else {
				// Otherwise try and get a tag ID and add it.
				let id = get_id_from_reference(&import.reference)?;
				outgoing.insert(import.reference, id);
			}
		}

		// TODO clean this spaghetti up
		// Validate the outgoing edges for the lock.
		let write_lockfile = 'a: {
			let Some((lock, node)) = lock else {
				if input.arg.locked {
					return Err(tg::error!("lock is out of date"));
				}
				break 'a true;
			};
			let node = &lock.nodes[node];
			let old = node
				.dependencies
				.iter()
				.flat_map(|map| map.keys())
				.collect::<Vec<_>>();
			let new = outgoing.keys().collect::<Vec<_>>();
			if old != new {
				if input.arg.locked {
					return Err(tg::error!("lock is out of date"));
				}
				break 'a true;
			}
			false
		};
		if write_lockfile {
			// Mark that we need to update xattrs.
			input
				.state
				.visited
				.lock()
				.unwrap()
				.get_mut(&input.arg.path)
				.unwrap()
				.lock
				.replace(super::Lock::File);
		}

		Ok(outgoing)
	}

	async fn search_for_lock(
		&self,
		input: &super::InnerInput<'_>,
	) -> tg::Result<Option<(tg::lock::Object, usize)>> {
		// Return the existing lock if it is set.
		let (lock, root) = if let Some((lock, root)) = input.lock.as_ref() {
			(lock.clone(), *root)
		} else {
			// Get the parent directory
			let lock_file_path = input
				.metadata
				.as_ref()
				.unwrap()
				.is_dir()
				.then(|| input.arg.path.clone())
				.unwrap_or_else(|| input.arg.path.clone().parent().normalize())
				.join(tg::artifact::module::LOCKFILE_FILE_NAME);

			// Check if the lock exists.
			if !tokio::fs::try_exists(&lock_file_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to check if the file exists"))?
			{
				return Ok(None);
			}

			// Read the lock.
			let contents = tokio::fs::read_to_string(&lock_file_path).await.map_err(
				|source| tg::error!(!source, %path = lock_file_path, "failed to read lock file"),
			)?;

			// Deserialize
			let data = serde_json::from_str::<tg::lock::Data>(&contents)
				.map_err(|source| tg::error!(!source, "failed to deserialize lock file"))?;
			let lock = data.try_into()?;
			(lock, 0)
		};

		// Check that this path exists in the input.
		let either = lock.nodes[root]
			.dependencies
			.as_ref()
			.and_then(|dependencies| dependencies.get(&input.reference));

		let Some(either) = either else {
			if input.arg.locked {
				return Err(tg::error!(%path = input.arg.path, "lock is out of date"));
			}
			return Ok(None);
		};

		match either {
			Either::Left(node) => Ok(Some((lock.clone(), *node))),
			Either::Right(tg::Object::File(file)) => {
				let dependencies = file.dependencies(self).await?;
				let Some(tg::file::Dependencies::Lock(lock, node)) = &*dependencies else {
					if input.arg.locked {
						return Err(tg::error!(%path = input.arg.path, "lock is out of date"));
					}
					return Ok(None);
				};
				let lock = lock.object(self).await?;
				Ok(Some((lock.as_ref().clone(), *node)))
			},
			_ => {
				if input.arg.locked {
					return Err(tg::error!("lock is out of date"));
				}
				Ok(None)
			},
		}
	}

	pub(super) async fn add_symlink_to_graph(
		&self,
		id: &Id,
		input: &super::InnerInput<'_>,
	) -> tg::Result<()> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(&input.arg.path).await.map_err(
			|source| tg::error!(!source, %path = input.arg.path, r#"failed to read the symlink at path"#,),
		)?;

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		// Create the symlink.
		let data = tg::symlink::Data {
			artifact: artifact.clone(),
			path,
		};

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
			object: Some(Object::Symlink(data)),
			tag: None,
			path: Some(input.arg.path.clone()),
		};

		// Add the node to the graph.
		input
			.state
			.graph
			.lock()
			.unwrap()
			.nodes
			.insert(id.clone(), node);
		Ok(())
	}

	/// Create an initial package graph from a root module path and return the graph and id of the root node. TODO: use this when re-using lockfiles.
	// pub(super) async fn create_graph_for_lock(
	// 	&self,
	// 	lock: &tg::Lock,
	// 	root: usize,
	// ) -> tg::Result<(Graph, Id)> {
	// 	let mut graph = Graph::default();
	// 	let id = self.add_lock_to_graph(&mut graph, lock, root).await?;
	// 	Ok((graph, id))
	// }

	async fn add_lock_to_graph(
		&self,
		graph: &mut Graph,
		lock: &tg::Lock,
		root: usize,
	) -> tg::Result<Id> {
		let object = lock.object(self).await?;
		let mut visited = BTreeMap::new();
		self.add_lock_to_graph_inner(graph, object.as_ref(), root, &mut visited)
			.await
	}

	async fn add_lock_to_graph_inner(
		&self,
		graph: &mut Graph,
		lock: &tg::lock::Object,
		node: usize,
		visited: &mut BTreeMap<usize, Id>,
	) -> tg::Result<Id> {
		if let Some(id) = visited.get(&node) {
			return Ok(id.clone());
		}
		let id = graph.next_id();
		visited.insert(node, id.clone());

		let node = &lock.nodes[node];
		let mut outgoing = BTreeMap::new();
		for (reference, either) in node.dependencies.iter().flatten() {
			let id = match either {
				Either::Left(node) => {
					Box::pin(self.add_lock_to_graph_inner(graph, lock, *node, visited)).await?
				},
				Either::Right(object) => {
					Box::pin(self.add_object_to_graph(graph, object, None)).await?
				},
			};
			outgoing.insert(reference.clone(), id);
		}

		let object = if let Some(object) = node.object.as_ref() {
			let id = object.id(self).await?;
			Some(Object::Object(id))
		} else {
			None
		};
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object,
			tag: None,
			path: None,
		};
		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}

	async fn add_object_to_graph(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: Option<&tg::Tag>,
	) -> tg::Result<Id> {
		let id = if let Some(_tag) = tag {
			todo!()
		} else {
			graph.next_id()
		};
		'a: {
			let tg::Object::File(file) = object else {
				break 'a;
			};
			let Some(dependencies) = &*file.dependencies(self).await? else {
				break 'a;
			};
			match dependencies {
				tg::file::Dependencies::Set(set) => {
					let mut outgoing = BTreeMap::new();
					for object in set {
						let reference = tg::Reference::with_object(&object.id(self).await?);
						let id = Box::pin(self.add_object_to_graph(graph, object, None)).await?;
						outgoing.insert(reference, id);
					}
					let object = Object::Object(object.id(self).await?);
					let node = Node {
						id: id.clone(),
						errors: Vec::new(),
						outgoing,
						_inline_object: false,
						object: Some(object),
						tag: None,
						path: None,
					};
					graph.nodes.insert(id.clone(), node);
				},
				tg::file::Dependencies::Map(_) => todo!(),
				tg::file::Dependencies::Lock(lock, root) => {
					return self.add_lock_to_graph(graph, lock, *root).await;
				},
			}
			return Ok(id);
		}
		let object = Object::Object(object.id(self).await?);
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
			object: Some(object),
			tag: tag.cloned(),
			path: None,
		};
		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}
}

impl Graph {
	pub fn next_id(&mut self) -> Id {
		let id = Either::Right(self.counter);
		self.counter += 1;
		id
	}

	pub fn validate(&self, server: &Server) -> tg::Result<()> {
		let mut errors = Vec::new();
		for node in self.nodes.values() {
			let errors_ = node
				.errors
				.iter()
				.map(|error| tg::error!(%error, %node = node.id, "node contains error"));
			errors.extend(errors_);
			for (reference, id) in &node.outgoing {
				if !self.nodes.contains_key(id) {
					let error =
						tg::error!(%reference, %node = node.id, "failed to resolve dependency");
					errors.push(error);
				}
			}
		}
		if errors.is_empty() {
			return Ok(());
		}
		for error in errors {
			let trace = error.trace(&server.options.advanced.error_trace_options);
			tracing::error!("{trace}");
		}
		Err(tg::error!("invalid graph"))
	}

	pub fn outgoing(&self, src: Id) -> impl Iterator<Item = Edge> + '_ {
		self.nodes
			.get(&src)
			.unwrap()
			.outgoing
			.iter()
			.map(move |(dependency, dst)| Edge {
				src: src.clone(),
				dst: dst.clone(),
				reference: dependency.clone(),
			})
	}

	fn add_error(&mut self, node: &Id, error: tg::Error) {
		self.nodes.get_mut(node).unwrap().errors.push(error);
	}
}

impl Server {
	pub(super) async fn unify_dependencies(
		&self,
		mut graph: Graph,
		root: &Id,
	) -> tg::Result<Graph> {
		// Get the overrides.
		let mut overrides: BTreeMap<Id, BTreeMap<String, tg::Reference>> = BTreeMap::new();
		let root_node = graph.nodes.get_mut(root).unwrap();
		for (reference, node) in &root_node.outgoing {
			let Some(overrides_) = reference
				.query()
				.as_ref()
				.and_then(|query| query.overrides.clone())
			else {
				continue;
			};
			overrides
				.entry(node.clone())
				.or_default()
				.extend(overrides_);
		}

		let mut queue = graph
			.outgoing(root.clone())
			.filter(|edge| !graph.nodes.contains_key(&edge.dst))
			.collect::<im::Vector<_>>();

		// Get the first edge to solve.
		let Some(edge) = queue.pop_back() else {
			return Ok(graph);
		};

		// Construct the initial state.
		let packages = None;
		let mut current = State {
			graph,
			edge,
			queue,
			objects: packages,
		};

		// Create a vec of checkpoints to support backtracking.
		let mut checkpoints = Vec::new();

		// Walk the graph until we have no more edges to solve.
		loop {
			self.walk_edge(&mut checkpoints, &mut current, &overrides)
				.await;

			// Changing this to pop_back() would convert the algorithm from breadth-first to depth-first. The algorithm should be correct regardless of traversel order. However, using BFS to walk the graph allows us to propogate constraints when backtracking to shrink the search sapce.
			let Some(next) = current.queue.pop_front() else {
				break;
			};

			current.edge = next;
			current.objects.take();
		}

		Ok(current.graph)
	}

	async fn walk_edge(
		&self,
		state: &mut Vec<State>,
		current: &mut State,
		overrides: &BTreeMap<Id, BTreeMap<String, tg::Reference>>,
	) {
		// Check if an override exists.
		let reference = overrides
			.get(&current.edge.src)
			.and_then(|overrides| {
				let name = current.edge.reference.query().as_ref()?.name.as_ref()?;
				overrides.get(name)
			})
			.unwrap_or(&current.edge.reference)
			.clone();

		// If the graph doesn't contain the destination node, attempt to select a version.
		if !current.graph.nodes.contains_key(&current.edge.dst) {
			match self
				.resolve_dependency(&mut current.graph, &reference, &mut current.objects)
				.await
			{
				Ok(node) => {
					// Save the current state that we will return to later.
					state.push(current.clone());

					// Add this edge to the top of the stack.
					current.queue.push_back(current.edge.clone());

					// Add the direct dependencies to the stack and increment their counters.
					let edges = std::iter::once(current.edge.clone())
						.chain(current.graph.outgoing(node.clone()))
						.collect::<Vec<_>>();
					current.queue.extend(edges);
				},
				Err(error) => {
					current.graph.add_error(&current.edge.src, error);
				},
			};
			return;
		}

		// Check if the destination node contains errors.
		let contains_errors = !current
			.graph
			.nodes
			.get(&current.edge.dst)
			.unwrap()
			.errors
			.is_empty();

		// If the destination is an error, add an error to the referrer.
		if contains_errors {
			let error =
				tg::error!("could not solve {reference} because the dependency contains errors");
			current.graph.add_error(&current.edge.src, error);
		}

		// If there is no tag it is not a tag dependency, so return.
		if current.edge.dst.is_right() {
			return;
		}

		// Get the tag of the current node.
		let tag = current
			.graph
			.nodes
			.get(&current.edge.dst)
			.unwrap()
			.tag
			.as_ref()
			.unwrap();

		// Validate the constraint.
		match reference
			.path()
			.try_unwrap_tag_ref()
			.map(|pat| pat.matches(tag))
		{
			// If the version matches do nothing.
			Ok(true) | Err(_) => (),

			// If the version does not match, attempt to backtrack, but only if the destination contains no errors.
			Ok(false) => match try_backtrack(state, &current.edge) {
				Some(old) if !contains_errors => {
					*current = old;
				},
				_ => {
					let error = tg::error!(%reference, "package version conflict");
					current.graph.add_error(&current.edge.src, error);
				},
			},
		}
	}

	async fn resolve_dependency(
		&self,
		graph: &mut Graph,
		reference: &tg::Reference,
		objects: &mut Option<im::Vector<(tg::Tag, tg::Object)>>,
	) -> tg::Result<Id> {
		// Seed the remaining packages if necessary.
		if objects.is_none() {
			// Get the tag pattern and remote if necessary.
			let pattern = reference
				.path()
				.try_unwrap_tag_ref()
				.map_err(|_| tg::error!(%reference, "expected a tag pattern"))?
				.clone();
			let remote = reference
				.query()
				.as_ref()
				.and_then(|query| query.remote.clone());

			// List tags that match the pattern.
			let objects_: im::Vector<_> = self
				.list_tags(tg::tag::list::Arg {
					length: None,
					pattern: pattern.clone(),
					remote,
				})
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?
				.data
				.into_iter()
				.filter_map(|output| {
					let object = output.item?.right()?;
					Some((output.tag, tg::Object::with_id(object)))
				})
				.collect();

			// Update the remaining packages.
			objects.replace(objects_);
		}

		// Pop the next version off the list.
		let (tag, object) = objects
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "no solution exists"))?;

		self.add_object_to_graph(graph, &object, Some(&tag)).await
	}
}

fn try_backtrack(state: &mut Vec<State>, edge: &Edge) -> Option<State> {
	// Find the index of the state where the node was first added.
	let position = state
		.iter()
		.position(|state| state.graph.nodes.contains_key(&edge.dst))?;

	// Backtrack.
	state.truncate(position);
	let mut state = state.pop()?;

	// This bit is a little weird. TODO don't make this so jank.
	state.queue.push_front(edge.clone());

	// If the edge we failed at is still in the graph, it means that we can use the dependency as an additional heuristic to inform the next selection.
	let edge_in_graph = state
		.graph
		.nodes
		.get(&edge.src)
		.map_or(false, |src| src.outgoing.contains_key(&edge.reference));
	if edge_in_graph {
		let packages = state
			.objects
			.as_ref()
			.unwrap()
			.iter()
			.filter(|(version, _)| edge.reference.path().unwrap_tag_ref().matches(version))
			.cloned()
			.collect();
		state.objects.replace(packages);
	}

	Some(state)
}

fn get_reference_from_pattern(pattern: &tg::tag::Pattern) -> tg::Reference {
	let components = pattern.components();
	let string = match components.last() {
		Some(tg::tag::pattern::Component::Semver(_)) if components.len() >= 2 => {
			let mut components = components.clone();
			components.pop();
			let pattern = tg::tag::Pattern::with_components(components);
			format!("{pattern}/*")
		},
		_ => pattern.to_string(),
	};
	string.parse().unwrap()
}

fn try_get_id(reference: &tg::Reference) -> Option<Id> {
	reference
		.path()
		.try_unwrap_tag_ref()
		.ok()
		.map(get_reference_from_pattern)
		.map(Either::Left)
}

fn get_id_from_reference(reference: &tg::Reference) -> tg::Result<Id> {
	try_get_id(reference).ok_or_else(|| tg::error!(%reference, "invalid reference"))
}
