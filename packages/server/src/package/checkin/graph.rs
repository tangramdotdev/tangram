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
		let root = self
			.add_object_to_graph(&mut graph, false, package.clone().into(), None, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create graph for lockfile"))?;
		Ok((graph, root))
	}

	pub(super) async fn create_graph_for_path(&self, path: &tg::Path) -> tg::Result<(Graph, Id)> {
		let mut graph = Graph::default();
		let root = self
			.add_path_to_graph(&mut graph, path.clone())
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to create graph for package"))?;
		Ok((graph, root))
	}

	async fn add_path_to_graph(&self, graph: &mut Graph, path: tg::Path) -> tg::Result<Id> {
		// Get the root module path, if it exists.
		let root_module_path = tg::package::try_get_root_module_path_for_path(path.as_ref())
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to get root module path"))?;

		// Compute the path and infer the import kind.
		let (path, kind) = if let Some(root_module_path) = root_module_path {
			let path = path.join(root_module_path);
			let kind = if path.as_str().ends_with(".js") {
				tg::import::Kind::Js
			} else if path.as_str().ends_with(".ts") {
				tg::import::Kind::Ts
			} else {
				return Err(tg::error!(%path, "unknown root module file extension"));
			};
			(path, kind)
		} else {
			let kind = tg::import::Kind::Artifact;
			(path, kind)
		};

		// Recursively walk the imports starting from the root.
		let mut visited = BTreeMap::new();
		self.add_path_to_graph_inner(graph, path, kind, &mut visited)
			.await
	}

	async fn add_path_to_graph_inner(
		&self,
		graph: &mut Graph,
		path: tg::Path,
		kind: tg::import::Kind,
		visited: &mut BTreeMap<tg::Path, Id>,
	) -> tg::Result<Id> {
		// Canonicalize the path.
		let path = tokio::fs::canonicalize(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to canonicalize the path"))?
			.try_into()
			.map_err(|source| tg::error!(!source, "invalid path"))?;

		// Check if we've already visited this path.
		if let Some(id) = visited.get(&path) {
			return Ok(id.clone());
		};

		// Check in the path to get the object.
		let arg = tg::artifact::checkin::Arg {
			path: path.clone(),
			destructive: false,
		};
		let artifact = tg::Artifact::check_in(self, arg)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to check in the artifact"))?;

		if matches!(
			(&artifact, kind),
			(
				tg::Artifact::File(_),
				tg::import::Kind::Artifact | tg::import::Kind::File
			) | (
				tg::Artifact::Directory(_),
				tg::import::Kind::Artifact | tg::import::Kind::Directory
			)
		) {
			// If the import kind is an artifact, file, or directory, add it like any other object to the graph.
			let id = self
				.add_object_to_graph(graph, false, artifact.into(), None, None)
				.await?;

			// Objects imported by path must appear as package nodes in the final output, so that they may be stripped from the package before writing a lockfile.
			graph.nodes.get_mut(&id).unwrap().is_package_node = true;
			visited.insert(path.clone(), id.clone());
			return Ok(id);
		}

		// Otherwise attempt to analyze as a typescript/javascript import.
		let file = artifact
			.try_unwrap_file()
			.map_err(|_| tg::error!(%path, "expected a file"))?;
		let text = file
			.text(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file contents"))?;
		let analysis = crate::compiler::Compiler::analyze_module(text)
			.map_err(|source| tg::error!(!source, %path, "failed to analyze module"))?;

		// Assign an ID.
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		visited.insert(path.clone(), id.clone());

		let mut errors = Vec::new();
		let mut outgoing = BTreeMap::new();
		for import in analysis.imports {
			// If there is a path, attempt to resolve it.
			if let Some(path_) = import
				.reference
				.path()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| import.reference.query()?.path.as_ref())
			{
				// Infer the kind if it is not available.
				let kind = import.kind.or_else(|| {
					if path_.as_str().ends_with(".js") {
						Some(tg::import::Kind::Js)
					} else if path_.as_str().ends_with(".ts") {
						Some(tg::import::Kind::Ts)
					} else {
						None
					}
				});
				let path_ = path.clone().parent().join(path_.clone()).normalize();
				let result = match kind {
					None | Some(tg::import::Kind::Package) => {
						Box::pin(self.add_path_to_graph(graph, path_)).await
					},
					Some(kind) => {
						Box::pin(self.add_path_to_graph_inner(graph, path_, kind, visited)).await
					},
				};
				match result {
					Ok(id) => {
						outgoing.insert(import.reference, id);
					},
					Err(source) => {
						let error = tg::error!(!source, %referrer = path, %reference = import.reference, "failed to resolve dependency");
						errors.push(error);
					},
				}
				continue;
			}

			match import.reference.path() {
				tg::reference::Path::Build(_) => {
					let error = tg::error!(%reference = import.reference, "invalid reference");
					errors.push(error);
				},
				tg::reference::Path::Object(object) => {
					let object = tg::Object::with_id(object.clone());
					match self
						.add_object_to_graph(graph, true, object, None, None)
						.await
					{
						Ok(id) => {
							outgoing.insert(import.reference, id);
						},
						Err(source) => {
							let error = tg::error!(!source, %reference = import.reference, "failed to resolve dependency");
							errors.push(error);
						},
					}
				},
				tg::reference::Path::Path(_) => unreachable!(),
				tg::reference::Path::Tag(pattern) => {
					let id = Either::Left(get_reference_from_pattern(pattern));
					outgoing.insert(import.reference, id);
				},
			}
		}

		let mut metadata = BTreeMap::new();
		for (key, data) in analysis.metadata.into_iter().flatten() {
			match data.try_into() {
				Ok(value) => {
					metadata.insert(key, value);
				},
				Err(source) => {
					let error = tg::error!(!source, %key, "failed to convert metadata");
					errors.push(error);
				},
			}
		}
		metadata.insert("kind".to_owned(), kind.to_string().into());
		let node = Node {
			id: id.clone(),
			errors,
			outgoing,
			is_package_node: true,
			object: file.into(),
			metadata,
			tag: None,
		};
		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	async fn add_object_to_graph(
		&self,
		graph: &mut Graph,
		unify: bool,
		object: tg::Object,
		reference: Option<&tg::Reference>,
		tag: Option<tg::Tag>,
	) -> tg::Result<Id> {
		if let tg::Object::Package(package) = object {
			let package = package.object(self).await?;
			let mut visited = BTreeMap::new();
			let root = self
				.add_package_to_graph_inner(
					graph,
					unify,
					&package,
					reference,
					package.root,
					&mut visited,
				)
				.await?;
			graph.nodes.get_mut(&root).unwrap().tag = tag;
			return Ok(root);
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
			object,
			metadata: BTreeMap::new(),
			tag,
		};

		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}

	async fn add_package_to_graph_inner(
		&self,
		graph: &mut Graph,
		unify: bool,
		package: &tg::package::Object,
		reference: Option<&tg::Reference>,
		node: usize,
		visited: &mut BTreeMap<usize, Id>,
	) -> tg::Result<Id> {
		if let Some(id) = visited.get(&node) {
			return Ok(id.clone());
		};

		// Assign an ID.
		let id = reference.and_then(try_get_id).unwrap_or_else(|| {
			let id = Either::Right(graph.counter);
			graph.counter += 1;
			id
		});
		visited.insert(node, id.clone());

		let mut errors = Vec::new();
		let mut outgoing = BTreeMap::new();
		for (reference, either) in &package.nodes[node].dependencies {
			if let Some(reference) = reference
				.path()
				.try_unwrap_tag_ref()
				.ok()
				.and_then(|pattern| unify.then(|| get_reference_from_pattern(pattern)))
			{
				outgoing.insert(reference.clone(), Either::Left(reference));
				continue;
			}
			let result =
				match either {
					Either::Left(node) => {
						Box::pin(self.add_package_to_graph_inner(
							graph, unify, package, None, *node, visited,
						))
						.await
					},
					Either::Right(object) => {
						Box::pin(self.add_object_to_graph(graph, unify, object.clone(), None, None))
							.await
					},
				};
			match result {
				Ok(id) => {
					outgoing.insert(reference.clone(), id);
				},
				Err(source) => {
					let error = tg::error!(!source, %reference, "failed to resolve dependency");
					errors.push(error);
				},
			}
		}

		// Get the object and metadata.
		let object = package.nodes[node]
			.object
			.clone()
			.ok_or_else(|| tg::error!("incomplete package"))?;
		let metadata = package.nodes[node].metadata.clone();

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors,
			outgoing,
			is_package_node: true,
			object,
			metadata,
			tag: None,
		};
		graph.nodes.insert(id.clone(), node);
		Ok(id)
	}
}

impl Graph {
	/// Convert the graph into a tg::package::Object
	pub fn into_package_object(&self, root: &Id) -> tg::package::Object {
		// Walk the graph to assign indices. This ensures the ordering of indices is stable.
		let mut stack = vec![root];
		let mut counter = 0usize;
		let mut indices = BTreeMap::new();
		while let Some(node) = stack.pop() {
			if indices.contains_key(node) {
				continue;
			};
			let index = counter;
			counter += 1;
			indices.insert(node.clone(), index);
			let node = self.nodes.get(node).unwrap();
			for neighbor in node.outgoing.values() {
				let node = self.nodes.get(neighbor).unwrap();
				if node.is_package_node {
					stack.push(neighbor);
				}
			}
		}

		// Get the root index.
		let root = *indices.get(root).unwrap();

		// Map from graph nodes to package nodes.
		let nodes = self
			.nodes
			.values()
			.filter_map(|node| {
				let index = indices.get(&node.id)?;
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
						let either = indices
							.get(&node.id)
							.copied()
							.map(Either::Left)
							.unwrap_or_else(|| Either::Right(node.object.clone()));
						(reference.clone(), either)
					})
					.collect();

				let node = tg::package::Node {
					dependencies,
					object,
					metadata,
				};
				Some((*index, node))
			})
			.collect::<BTreeMap<_, _>>()
			.into_iter()
			.map(|(_, node)| node)
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
		self.add_object_to_graph(graph, true, object, Some(reference), Some(tag))
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
