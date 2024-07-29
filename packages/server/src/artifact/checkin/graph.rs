use crate::Server;
use either::Either;
use futures::{stream::FuturesUnordered, TryStreamExt};
use itertools::Itertools;
use num::ToPrimitive;
use petgraph::visit::{GraphBase, GraphRef, IntoNeighbors, IntoNodeIdentifiers, NodeIndexable};
use std::{
	collections::BTreeMap,
	os::unix::fs::PermissionsExt,
	sync::{atomic::Ordering, Arc},
};
use tangram_client::{self as tg, handle::Ext};

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

	// Whether this object appears as a distinct node in the output tg::Package.
	pub is_package_node: bool,

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

#[derive(Copy, Clone, Debug)]
struct GraphImpl<'a>(&'a [tg::lock::Node]);

#[derive(Clone, Debug)]
enum Object {
	File {
		blob: tg::blob::Id,
		executable: bool,
	},
	Directory(BTreeMap<String, Id>),
	Symlink(tg::artifact::Id),
	Object(tg::object::Id),
}

impl Server {
	pub(super) async fn add_path_to_graph(
		&self,
		arg: &tg::artifact::checkin::Arg,
		state: &super::State,
	) -> tg::Result<Id> {
		// Check if we've visited this path yet.
		if let Some(output) = state.visited.lock().unwrap().get(&arg.path).cloned() {
			return Ok(output.graph_id);
		}

		// Ge the metadata for the file system object at the path.
		let metadata = tokio::fs::symlink_metadata(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, "failed to get the metadata for the path"),
		)?;

		// Create the output.
		let graph_id = state.graph.lock().unwrap().next_id();
		let output = super::InnerOutput {
			graph_id: graph_id.clone(),
			artifact_id: None,
			path: arg.path.clone(),
			data: None,
			count: None,
			weight: None,
			lock: None,
		};
		state
			.visited
			.lock()
			.unwrap()
			.insert(arg.path.clone(), output);

		// Call the appropriate function for the file system object at the path.
		if metadata.is_dir() {
			self.add_directory_to_graph(&graph_id, &arg, &metadata, state)
				.await
				.map_err(
					|source| tg::error!(!source, %path = arg.path, "failed to check in the directory"),
				)?
		} else if metadata.is_file() {
			self.add_file_to_graph(&graph_id, arg, &metadata, state)
				.await
				.map_err(
					|source| tg::error!(!source, %path = arg.path, "failed to check in the file"),
				)?
		} else if metadata.is_symlink() {
			self.add_symlink_to_graph(&graph_id, arg, &metadata, state)
				.await
				.map_err(
					|source| tg::error!(!source, %path = arg.path, "failed to check in the symlink"),
				)?
		} else {
			let file_type = metadata.file_type();
			return Err(tg::error!(
				%path = arg.path,
				?file_type,
				"invalid file type"
			));
		};

		// Update the state.
		let visited = state.visited.lock().unwrap();
		let output = visited.get(&arg.path).unwrap();
		if let Some(count) = output.count {
			state.count.current.fetch_add(count, Ordering::Relaxed);
		}
		if let Some(weight) = output.weight {
			state.weight.current.fetch_add(weight, Ordering::Relaxed);
		}

		// Return the id.
		return Ok(output.graph_id.clone());
	}

	pub(super) async fn add_directory_to_graph(
		&self,
		id: &Id,
		arg: &tg::artifact::checkin::Arg,
		_metadata: &std::fs::Metadata,
		state: &super::State,
	) -> tg::Result<()> {
		// If a root module path exists, check in as a file.
		'a: {
			let permit = self.file_descriptor_semaphore.acquire().await;
			let Ok(Some(path)) =
				tg::artifact::module::try_get_root_module_path_for_path(arg.path.as_ref()).await
			else {
				break 'a;
			};
			let path = arg.path.clone().join(path);
			let metadata = tokio::fs::metadata(&path)
				.await
				.map_err(|source| tg::error!(!source, %path, "failed to get the file metadata"))?;
			drop(permit);
			if !metadata.is_file() {
				break 'a;
			}
			let arg = tg::artifact::checkin::Arg {
				path,
				..arg.clone()
			};
			return self.add_file_to_graph(id, &arg, &metadata, state).await;
		}

		// Read the directory.
		let names = {
			let _permit = self.file_descriptor_semaphore.acquire().await;
			let mut read_dir = tokio::fs::read_dir(&arg.path)
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
				let mut arg = arg.clone();
				arg.path = arg.path.clone().join(name.clone());
				let id = Box::pin(self.add_path_to_graph(&arg, state)).await?;
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
			is_package_node: false,
			object: Some(Object::Directory(entries)),
			tag: None,
			path: Some(arg.path.clone()),
		};

		// Add the node to the graph.
		state.graph.lock().unwrap().nodes.insert(id.clone(), node);

		Ok(())
	}

	pub(super) async fn add_file_to_graph(
		&self,
		id: &Id,
		arg: &tg::artifact::checkin::Arg,
		metadata: &std::fs::Metadata,
		state: &super::State,
	) -> tg::Result<()> {
		// Create the blob without writing to disk/database.
		let _permit = self.file_descriptor_semaphore.acquire().await;
		let file = tokio::fs::File::open(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, "failed to open the file"))?;

		// Create the (blob) output.
		let output = self
			.create_blob_inner(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the contents"))?;

		// Determine if the file is executable.
		let executable = (metadata.permissions().mode() & 0o111) != 0;

		// Get the references.
		let outgoing = self.get_outgoing_references_for_path(arg, state).await?;

		// Create a node.
		let node = Node {
			id: id.clone(),
			is_package_node: true,
			errors: Vec::new(),
			outgoing,
			object: Some(Object::File {
				blob: output.blob,
				executable,
			}),
			tag: None,
			path: Some(arg.path.clone()),
		};

		// Add the node to the graph.
		state.graph.lock().unwrap().nodes.insert(id.clone(), node);

		// TODO: update count/weight
		Ok(())
	}

	async fn get_outgoing_references_for_path(
		&self,
		arg: &tg::artifact::checkin::Arg,
		state: &super::State,
	) -> tg::Result<BTreeMap<tg::Reference, Id>> {
		// Attempt to read dependencies from the xattrs, if they exist.
		if let Some(dependencies) =
			xattr::get(&arg.path, tg::file::TANGRAM_FILE_DEPENDENCIES_XATTR_NAME)
				.ok()
				.flatten()
				.and_then(|bytes| {
					serde_json::from_slice::<tg::file::data::Dependencies>(&bytes).ok()
				}) {
			// TODO: update count/weight.
			// Mark that we read a lock from xattrs.
			state
				.visited
				.lock()
				.unwrap()
				.get_mut(&arg.path)
				.ok_or_else(|| tg::error!("invalid graph"))?
				.lock
				.replace(super::Lock::Xattr);

			// Walk the dependencies
			match dependencies {
				tg::file::data::Dependencies::Set(dependencies) => {
					let mut outgoing = BTreeMap::new();
					let mut graph = state.graph.lock().unwrap();
					for object in dependencies {
						// Create a node ID for the object.
						let id = graph.next_id();

						// Add it as a dependency of the parent.
						outgoing.insert(tg::Reference::with_object(&object), id.clone());

						// Create a node.
						let node = Node {
							id: id.clone(),
							errors: Vec::new(),
							outgoing: BTreeMap::new(),
							is_package_node: false,
							object: Some(Object::Object(object)),
							tag: None,
							path: Some(arg.path.clone()),
						};

						// Update the graph
						state.graph.lock().unwrap().nodes.insert(id, node);
					}
					return Ok(outgoing);
				},
				tg::file::data::Dependencies::Map(_) => todo!(),
				tg::file::data::Dependencies::Lock(_, _) => todo!(),
			}
		}

		// If this is not a module path, then there are no dependencies.
		if !tg::artifact::module::is_module_path(arg.path.as_ref()) {
			return Ok(BTreeMap::new());
		}

		// Read the file.
		let text = tokio::fs::read_to_string(&arg.path)
			.await
			.map_err(|source| tg::error!(!source, %path = arg.path, "failed to read file"))?;

		// Compile the file.
		let analysis = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, "failed to analyze the module"))?;

		let mut outgoing = BTreeMap::new();
		for import in analysis.imports {
			if let Some(path) = import
				.reference
				.path()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| import.reference.query()?.path.as_ref())
			{
				// Handle path dependencies.
				// TODO: use import kind.
				let path = arg.path.clone().parent().join(path.clone()).normalize();
				let arg = tg::artifact::checkin::Arg {
					path: path.clone(),
					..arg.clone()
				};
				let id = Box::pin(self.add_path_to_graph(&arg, state))
					.await
					.map_err(
						|source| tg::error!(!source, %path, "failed to add dependency to graph"),
					)?;
				outgoing.insert(import.reference.clone(), id);
			} else if let Ok(object) = import.reference.path().try_unwrap_object_ref() {
				// Create a node ID for the object.
				let id = state.graph.lock().unwrap().next_id();

				// Add it as a dependency of the parent.
				outgoing.insert(tg::Reference::with_object(object), id.clone());

				// Create a node.
				let node = Node {
					id: id.clone(),
					errors: Vec::new(),
					outgoing: BTreeMap::new(),
					is_package_node: false,
					object: Some(Object::Object(object.clone())),
					tag: None,
					path: None,
				};

				// Update the graph
				state.graph.lock().unwrap().nodes.insert(id, node);
			} else {
				// Otherwise try and get a tag ID and add it.
				let id = get_id_from_reference(&import.reference)?;
				outgoing.insert(import.reference, id);
			}
		}

		Ok(outgoing)
	}

	pub(super) async fn add_symlink_to_graph(
		&self,
		id: &Id,
		arg: &tg::artifact::checkin::Arg,
		metadata: &std::fs::Metadata,
		state: &super::State,
	) -> tg::Result<()> {
		// Read the target from the symlink.
		let target = tokio::fs::read_link(&arg.path).await.map_err(
			|source| tg::error!(!source, %path = arg.path, r#"failed to read the symlink at path"#,),
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
		let bytes = data.serialize()?;
		let symlink_id = tg::artifact::Id::from(tg::symlink::Id::new(&bytes));
		let data = Arc::new(data.into());

		let count = if let Some(artifact) = &artifact {
			let metadata = self.get_object_metadata(&artifact.clone().into()).await?;
			metadata.count.map(|count| 1 + count)
		} else {
			Some(1)
		};
		let weight = if let Some(artifact) = &artifact {
			let metadata = self.get_object_metadata(&artifact.clone().into()).await?;
			metadata
				.weight
				.map(|weight| bytes.len().to_u64().unwrap() + weight)
		} else {
			Some(bytes.len().to_u64().unwrap())
		};

		// Get the output.
		let mut visited = state.visited.lock().unwrap();
		let output = visited
			.get_mut(&arg.path)
			.ok_or_else(|| tg::error!("invalid check in state"))?;

		// Update the state.
		output.artifact_id.replace(symlink_id.clone());
		output.data.replace(data);
		output.count = count;
		output.weight = weight;

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			is_package_node: false,
			object: Some(Object::Symlink(symlink_id)),
			tag: None,
			path: None,
		};

		// Add the node to the graph.
		state.graph.lock().unwrap().nodes.insert(id.clone(), node);
		Ok(())
	}

	/// Create an initial package graph from a root module path and return the graph and id of the root node.
	pub(super) async fn create_graph_for_lockfile(
		&self,
		package: &tg::Lock,
	) -> tg::Result<(Graph, Id)> {
		todo!()
		// let mut graph = Graph::default();
		// let root = self
		// 	.add_object_to_graph(&mut graph, false, package.clone().into(), None, None)
		// 	.await
		// 	.map_err(|source| tg::error!(!source, "failed to create graph for lockfile"))?;
		// Ok((graph, root))
	}

	pub(super) async fn create_graph_for_path(
		&self,
		path: &tg::Path,
		locked: bool,
		state: &super::State,
	) -> tg::Result<Id> {
		todo!()
	}

	async fn infer_import_kind_from_path(&self, path: &tg::Path) -> tg::Result<tg::import::Kind> {
		let metadata = tokio::fs::metadata(path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to get file metadata"))?;
		if metadata.is_file() {
			if path.as_str().to_lowercase().ends_with(".js") {
				Ok(tg::import::Kind::Js)
			} else if path.as_str().to_lowercase().ends_with(".ts") {
				Ok(tg::import::Kind::Ts)
			} else {
				Ok(tg::import::Kind::File)
			}
		} else if metadata.is_dir() {
			let Some(root_module_path) =
				tg::artifact::module::try_get_root_module_path_for_path(path.as_ref())
					.await
					.map_err(|source| tg::error!(!source, "failed to get root module path"))?
			else {
				return Ok(tg::import::Kind::Directory);
			};
			if root_module_path.as_str().to_lowercase().ends_with(".js") {
				Ok(tg::import::Kind::Js)
			} else if root_module_path.as_str().to_lowercase().ends_with(".ts") {
				Ok(tg::import::Kind::Ts)
			} else {
				Err(tg::error!(%path = root_module_path, "unknown root module file"))
			}
		} else {
			return Err(tg::error!(%path, "invalid file type"));
		}
	}

	async fn add_object_to_graph(
		&self,
		graph: &mut Graph,
		unify: bool,
		object: tg::Object,
		reference: Option<&tg::Reference>,
		tag: Option<tg::Tag>,
	) -> tg::Result<Id> {
		if let tg::Object::File(file) = &object {
			let mut outgoing = BTreeMap::new();
			if let Some(dependencies) = &*file.dependencies(self).await? {
				match dependencies {
					tg::file::Dependencies::Set(dependencies) => {
						for object in dependencies {
							let reference = tg::Reference::with_object(&object.id(self).await?);
							let id = Box::pin(self.add_object_to_graph(
								graph,
								unify,
								object.clone(),
								None,
								None,
							))
							.await?;
							outgoing.insert(reference, id);
						}
					},
					tg::file::Dependencies::Map(_) => todo!(),
					tg::file::Dependencies::Lock(lock, node) => {
						todo!()
					},
				}
			}
		}

		// Assign an ID.
		let id = reference.and_then(try_get_id).unwrap_or_else(|| {
			let id = Either::Right(graph.counter);
			graph.counter += 1;
			id
		});

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			is_package_node: false,
			object: Some(Object::Object(object.id(self).await?)),
			tag,
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

		// Add to the graph.
		self.add_object_to_graph(graph, true, object, Some(reference), Some(tag))
			.await
	}

	pub(super) async fn create_lock(
		&self,
		graph: &Graph,
		root: &Id,
	) -> tg::Result<super::lock::Output> {
		// Walk the graph to assign indices. This ensures the ordering of indices is stable.
		let mut stack = vec![root];
		let mut counter = 0usize;
		let mut indices = BTreeMap::new();
		let mut stripped = BTreeMap::new();
		while let Some(node) = stack.pop() {
			if indices.contains_key(node) {
				continue;
			};
			let index = counter;
			counter += 1;
			indices.insert(node.clone(), index);
			let node = graph.nodes.get(node).unwrap();
			for (reference, neighbor) in &node.outgoing {
				let node = graph.nodes.get(neighbor).unwrap();
				if node.is_package_node {
					stack.push(neighbor);
				} else if neighbor.is_right()
					&& reference
						.path()
						.try_unwrap_path_ref()
						.ok()
						.or_else(|| reference.query()?.path.as_ref())
						.is_some()
				{
					let mut node = node.clone();
					node.object.take();
					stripped.insert(node.id.clone(), node);
				}
			}
		}

		// Get the root index.
		let root = *indices.get(root).unwrap();

		// Map from graph nodes to lock nodes.
		let nodes = indices
			.iter()
			.sorted_by_cached_key(|(_, v)| **v)
			.filter_map(|(id, _)| {
				let node = graph.nodes.get(id)?;
				self.try_create_lock_node(graph, node, &indices)
			})
			.collect::<Vec<_>>();

		// Split into sub-locks.
		let mut locks: Vec<tg::Lock> = vec![];
		let mut lock_indices = BTreeMap::new();

		// Create locks from the strongly connected components.
		for (lock_index, node_indices) in petgraph::algo::tarjan_scc(GraphImpl(&nodes))
			.into_iter()
			.enumerate()
		{
			// Create an empty lock object.
			let mut lock = tg::lock::Object {
				nodes: Vec::with_capacity(node_indices.len()),
			};

			// Mark all the nodes as belonging to this lock.
			for (new_index, old_index) in node_indices.iter().copied().enumerate() {
				lock_indices.insert(old_index, (lock_index, new_index));
			}

			// Create new lock nodes.
			for (_, old_index) in node_indices.iter().copied().enumerate() {
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

					// Otherwise create a new file object.
					let mut file = node
						.object
						.as_ref()
						.ok_or_else(|| tg::error!("missing object in graph"))?
						.clone()
						.try_unwrap_file()
						.map_err(|_| tg::error!("expected a file"))?
						.object(self)
						.await?
						.as_ref()
						.clone();
					let lock = locks[*lock_index_].clone();
					let dependencies = tg::file::Dependencies::Lock(lock, *new_index);
					file.dependencies.replace(dependencies);

					// Update the entry.
					*either = Either::Right(tg::File::with_object(file).into());
				}

				// Add the node to the lock.
				lock.nodes.push(node);
			}

			// Add the lock to the list of locks.
			locks.push(tg::Lock::with_object(lock));
		}

		// Get the root lock.
		let lock = locks[root].clone();
		Ok(super::lock::Output {
			stripped: todo!(),
			non_stripped: lock,
			root_node_index: root,
		})
	}

	fn try_create_lock_node(
		&self,
		graph: &Graph,
		node: &Node,
		indices: &BTreeMap<Id, usize>,
	) -> Option<tg::lock::Node> {
		todo!()
		// let object = tg::Object::with_id(node.object.clone());
		// let dependencies = node
		// 	.outgoing
		// 	.iter()
		// 	.map(|(reference, id)| {
		// 		let object = tg::Object::with_id(graph.nodes.get(id).unwrap().object.clone());
		// 		let either = indices
		// 			.get(&node.id)
		// 			.copied()
		// 			.map_or_else(|| Either::Right(object), Either::Left);
		// 		(reference.clone(), either)
		// 	})
		// 	.collect();
		// let node = tg::lock::Node {
		// 	object: Some(object),
		// 	dependencies: Some(dependencies),
		// };
		// Some(node)
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

impl<'a> GraphBase for GraphImpl<'a> {
	type EdgeId = (usize, usize);
	type NodeId = usize;
}

impl<'a> GraphRef for GraphImpl<'a> {}

#[allow(clippy::needless_arbitrary_self_type)]
impl<'a> NodeIndexable for GraphImpl<'a> {
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

impl<'a> IntoNeighbors for GraphImpl<'a> {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;
	fn neighbors(self, a: Self::NodeId) -> Self::Neighbors {
		let iter = self.0[a]
			.dependencies
			.iter()
			.flatten()
			.filter_map(|(_, v)| v.as_ref().left().copied());
		Box::new(iter)
	}
}

impl<'a> IntoNodeIdentifiers for GraphImpl<'a> {
	type NodeIdentifiers = std::ops::Range<usize>;
	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.node_bound()
	}
}
