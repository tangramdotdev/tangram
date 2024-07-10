use crate::Server;
use either::Either;
use std::collections::BTreeMap;
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
	pub object: tg::Object,

	// Object metadata.
	pub metadata: BTreeMap<String, tg::Value>,

	// The tag of this node, if it exists.
	pub tag: Option<tg::Tag>,
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

impl Server {
	/// Create an initial package graph from a root module path and return the graph and id of the root node.
	pub(super) async fn create_graph_for_lockfile(
		&self,
		package: &tg::Package,
	) -> tg::Result<(Graph, Id)> {
		let mut graph = Graph::default();
		let object = package.clone().into();
		let root = graph
			.create_node_from_object(self, false, object, None, None)
			.await?;
		Ok((graph, root))
	}

	/// Create an initial package graph from a root module path and return the graph and id of the root node.
	pub(super) async fn create_graph_for_path(
		&self,
		path: &tg::Path,
		root_module_path: &tg::Path,
	) -> tg::Result<(Graph, Id)> {
		let referrer = path.clone().join(root_module_path.clone());
		let reference = tg::Reference::with_path(root_module_path);
		let import_kind = if root_module_path.as_str().ends_with(".js") {
			tg::import::Kind::Js
		} else if root_module_path.as_str().ends_with(".ts") {
			tg::import::Kind::Ts
		} else {
			tg::import::Kind::Artifact
		};
		let mut nodes = BTreeMap::new();
		let mut visited = BTreeMap::new();

		let root = self
			.create_graph_node(&referrer, &reference, import_kind, &mut nodes, &mut visited)
			.await
			.map_err(
				|source| tg::error!(!source, %path = root_module_path, "failed to create package graph"),
			)?;
		let counter = nodes.len();
		let nodes = nodes.into();
		let graph = Graph { counter, nodes };
		Ok((graph, root))
	}

	// Create a single package graph node from a reference and import kind.
	async fn create_graph_node(
		&self,
		referrer: &tg::Path,
		reference: &tg::Reference,
		import_kind: tg::import::Kind,
		nodes: &mut BTreeMap<Id, Node>,
		visited: &mut BTreeMap<tg::Path, Id>,
	) -> tg::Result<Id> {
		if let Some(reference) = reference
			.path()
			.try_unwrap_path_ref()
			.ok()
			.or_else(|| reference.query()?.path.as_ref())
		{
			let path = referrer
				.clone()
				.parent()
				.join(reference.clone())
				.normalize();
			return self
				.create_graph_node_at_path(&path, import_kind, nodes, visited)
				.await;
		}
		match reference.path() {
			tg::reference::Path::Build(_) => {
				Err(tg::error!("cannot create a graph node for a build"))
			},
			tg::reference::Path::Object(object) => {
				let id = Either::Right(nodes.len());
				let node = Node {
					id: id.clone(),
					errors: Vec::new(),
					outgoing: BTreeMap::new(),
					is_package_node: false,
					object: tg::Object::with_id(object.clone()),
					metadata: BTreeMap::new(),
					tag: None,
				};
				nodes.insert(id.clone(), node);
				Ok(id)
			},
			tg::reference::Path::Path(_) => unreachable!(),
			tg::reference::Path::Tag(pattern) => {
				Ok(Either::Left(get_reference_from_pattern(pattern)))
			},
		}
	}

	async fn create_graph_node_at_path(
		&self,
		path: &tg::Path,
		import_kind: tg::import::Kind,
		nodes: &mut BTreeMap<Id, Node>,
		visited: &mut BTreeMap<tg::Path, Id>,
	) -> tg::Result<Id> {
		// Canonicalize the path.
		let path = tokio::fs::canonicalize(path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to canonicalize path"))?
			.try_into()
			.map_err(|source| tg::error!(!source, "failed to convert path"))?;
		if let Some(id) = visited.get(&path) {
			return Ok(id.clone());
		};

		// Get the object for this node.
		let arg = tg::artifact::checkin::Arg {
			path: path.clone(),
			destructive: false,
		};
		let object = tg::Artifact::check_in(self, arg)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to check in the artifact"))?;

		// Create the metadata and get the outgoing edges.
		let mut errors = Vec::new();
		let mut metadata = BTreeMap::new();
		let (outgoing, is_package_node) = match import_kind {
			tg::import::Kind::Ts | tg::import::Kind::Js => 'a: {
				if path.as_str().ends_with(".js") {
					metadata.insert("kind".to_owned(), "js".to_owned().into());
				} else if path.as_str().ends_with(".ts") {
					metadata.insert("kind".to_owned(), "js".to_owned().into());
				}
				let Ok(file) = object.try_unwrap_file_ref() else {
					errors.push(tg::error!(%path, "expected a file"));
					break 'a (BTreeMap::new(), true);
				};
				let Ok(text) = file.text(self).await else {
					errors.push(tg::error!(%path, "failed to read file"));
					break 'a (BTreeMap::new(), true);
				};
				let Ok(analysis) = crate::compiler::Compiler::analyze_module(text) else {
					errors.push(tg::error!(%path, "failed to analyze module"));
					break 'a (BTreeMap::new(), true);
				};

				// Add the metadata.
				metadata.extend(
					analysis
						.metadata
						.into_iter()
						.flatten()
						.map(|(k, v)| (k, v.try_into().unwrap())),
				);

				// Recurse.
				let mut outgoing = BTreeMap::new();
				for import in analysis.imports {
					let result = Box::pin(self.create_graph_node(
						&path,
						&import.reference,
						import.kind.unwrap_or(tg::import::Kind::Js),
						nodes,
						visited,
					))
					.await;
					match result {
						Ok(id) => {
							outgoing.insert(import.reference, id);
						},
						Err(source) => errors.push(
							tg::error!(!source, %reference = import.reference, "failed to resolve dependency"),
						),
					}
				}
				(outgoing, true)
			},
			_ => {
				if object.try_unwrap_directory_ref().is_ok() {
					metadata.insert("kind".to_owned(), "directory".to_owned().into());
				} else {
					metadata.insert("kind".to_owned(), "file".to_owned().into());
				}
				(BTreeMap::new(), false)
			},
		};

		// Create the ID for this node.
		let id = Either::Right(nodes.len());

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors,
			outgoing,
			is_package_node,
			object: object.into(),
			metadata,
			tag: None,
		};

		nodes.insert(id.clone(), node);
		visited.insert(path, id.clone());
		Ok(id)
	}
}

impl Graph {
	/// Convert the graph into a tg::package::Object
	pub fn into_package_object(&self, root: &Id) -> tg::package::Object {
		// Assign an index to every node that isn't a bare package.
		let indices = self
			.nodes
			.values()
			.filter(|node| node.is_package_node)
			.enumerate()
			.map(|(index, node)| (node.id.clone(), index))
			.collect::<BTreeMap<_, _>>();

		// Get the root index.
		let root = *indices.get(root).unwrap();

		// Map from graph nodes to package nodes.
		let nodes = self
			.nodes
			.values()
			.filter_map(|node| {
				if !node.is_package_node {
					return None;
				}
				let object = Some(node.object.clone());
				let metadata = node
					.metadata
					.iter()
					.map(|(key, data)| (key.clone(), data.clone().try_into().unwrap()))
					.collect();
				let dependencies = node
					.outgoing
					.iter()
					.map(|(reference, id)| {
						let node = self.nodes.get(id).unwrap();
						let package = indices
							.get(&node.id)
							.copied()
							.map(Either::Left)
							.unwrap_or_else(|| Either::Right(node.object.clone()));
						(reference.clone(), Some(package))
					})
					.collect();
				Some(tg::package::Node {
					dependencies,
					object,
					metadata,
				})
			})
			.collect();

		// Create the object.
		tg::package::Object { root, nodes }
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

	async fn create_node_from_object(
		&mut self,
		server: &Server,
		unify: bool,
		object: tg::Object,
		reference: Option<&tg::Reference>,
		tag: Option<tg::Tag>,
	) -> tg::Result<Id> {
		if let tg::Object::Package(package) = object {
			return self
				.create_node_from_package(server, unify, package, reference, tag)
				.await;
		}
		let id = reference.and_then(try_get_id).unwrap_or_else(|| {
			let id = self.counter;
			self.counter += 1;
			Either::Right(id)
		});
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			is_package_node: false,
			object,
			metadata: BTreeMap::new(),
			tag,
		};
		self.nodes.insert(id.clone(), node);
		return Ok(id);
	}

	async fn create_node_from_package(
		&mut self,
		server: &Server,
		unify: bool,
		package: tg::Package,
		reference: Option<&tg::Reference>,
		tag: Option<tg::Tag>,
	) -> tg::Result<Id> {
		let mut visited = BTreeMap::new();
		let package = package.object(server).await?;
		let index = package.root;
		let id = self
			.create_node_from_package_inner(server, unify, &package, index, reference, &mut visited)
			.await?;
		self.nodes.get_mut(&id).unwrap().tag = tag;
		Ok(id)
	}

	async fn create_node_from_package_inner(
		&mut self,
		server: &Server,
		unify: bool,
		package: &tg::package::Object,
		index: usize,
		reference: Option<&tg::Reference>,
		visited: &mut BTreeMap<usize, Id>,
	) -> tg::Result<Id> {
		if let Some(id) = visited.get(&index) {
			return Ok(id.clone());
		}
		let node = &package.nodes[index];
		let mut errors = Vec::new();
		let mut outgoing = BTreeMap::new();
		for (reference, dependency) in &node.dependencies {
			// If the package is incomplete, log an error.
			let Some(dependency) = dependency else {
				errors.push(tg::error!(%reference, "missing dependency"));
				continue;
			};

			let id = match (try_get_id(reference), dependency) {
				// If we can get an ID for the reference and unify is true, use it.
				(Some(id), _) if unify => id,

				// If the dependency is in the package object, recurse.
				(_, Either::Left(index)) => {
					let Ok(id) = Box::pin(self.create_node_from_package_inner(
						server,
						unify,
						package,
						*index,
						Some(reference),
						visited,
					))
					.await
					.inspect_err(|source| {
						let source = source.clone();
						errors.push(tg::error!(!source, "failed to create package node"));
					}) else {
						continue;
					};
					id
				},

				// If the dependency is an object, create a new node for it.
				(_, Either::Right(object)) => {
					let Ok(id) = Box::pin(self.create_node_from_object(
						server,
						unify,
						object.clone(),
						None,
						None,
					))
					.await
					.inspect_err(|source| {
						let source = source.clone();
						errors.push(tg::error!(!source, "failed to create package node"));
					}) else {
						continue;
					};
					id
				},
			};
			outgoing.insert(reference.clone(), id);
		}

		// Get the object and its metadata.
		let object = node
			.object
			.clone()
			.ok_or_else(|| tg::error!("missing object"))?;
		let metadata = node.metadata.clone();

		// Create the node.
		let id = reference.and_then(try_get_id).unwrap_or_else(|| {
			let id = self.counter;
			self.counter += 1;
			Either::Right(id)
		});
		let node = Node {
			id: id.clone(),
			errors,
			outgoing,
			is_package_node: true,
			object,
			metadata,
			tag: None,
		};
		self.nodes.insert(id.clone(), node);
		visited.insert(index, id.clone());
		Ok(id)
	}

	fn add_error(&mut self, node: &Id, error: tg::Error) {
		self.nodes.get_mut(node).unwrap().errors.push(error);
	}
}

impl Server {
	pub(super) async fn walk_package_graph(
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
		let Some(tag) = current
			.graph
			.nodes
			.get(&current.edge.dst)
			.unwrap()
			.tag
			.as_ref()
		else {
			return;
		};

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
					return;
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
			let objects_ = self
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
		graph
			.create_node_from_object(self, true, object, Some(reference), Some(tag))
			.await
	}
}

fn try_backtrack(state: &mut Vec<State>, edge: &Edge) -> Option<State> {
	// Find the index of the state where the node was first added.
	let position = state.len()
		- state
			.iter()
			.rev()
			.position(|state| !state.graph.nodes.contains_key(&edge.dst))?;

	// Backtrack.
	state.truncate(position);
	let mut state = state.pop()?;

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
