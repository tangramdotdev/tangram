use super::input;
use crate::Server;
use itertools::Itertools as _;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tangram_client as tg;
use tangram_either::Either;

// A graph of packages.
#[derive(Clone, Default, Debug)]
pub struct Graph {
	// A counter used to create IDs for nodes that don't have a repository ID.
	pub counter: usize,

	// The set of nodes in the graph.
	pub nodes: im::HashMap<Id, Node>,

	// The set of paths in the graph.
	pub paths: im::HashMap<PathBuf, Id>,
}

// A node within the package graph.
#[derive(Clone, Debug)]
pub struct Node {
	// Direct dependencies.
	pub edges: BTreeMap<tg::Reference, Edge>,

	// The result of this node (None if we do not know if it is successful or not).
	pub errors: Vec<tg::Error>,

	// The underlying object.
	pub object: Either<usize, tg::object::Id>,

	// The tag of this node, if it exists.
	pub tag: Option<tg::Tag>,
}

pub type Id = Either<tg::Reference, usize>;

// An unresolved reference in the graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Unresolved {
	// The source node of the edge.
	pub src: Id,

	// The destination of the edge.
	pub dst: Id,

	// The constraint the referrer places on the dependency.
	pub reference: tg::Reference,
}

// An edge in the graph.
#[derive(Clone, Debug)]
pub struct Edge {
	pub referent: Id,
	pub kind: Option<tg::module::Kind>,
	pub path: Option<PathBuf>,
	pub subpath: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct State {
	// The current graph.
	graph: Graph,

	// The edge that we are currently following.
	edge: Unresolved,

	// A work queue of edges we will have to follow, in depth-first order.
	queue: im::Vector<Unresolved>,

	// A lazily-initialized set of packages to try.
	objects: Option<im::Vector<(tg::Tag, tg::Object)>>,

	// A list of visited edges.
	visited: im::HashSet<Unresolved>,
}

impl Server {
	pub async fn create_unification_graph(
		&self,
		input: &input::Graph,
		deterministic: bool,
	) -> tg::Result<(Graph, Id)> {
		let mut graph: Graph = Graph::default();
		let mut visited_graph_nodes = BTreeMap::new();

		let root = self
			.create_unification_node_from_input(input, 0, &mut graph, &mut visited_graph_nodes)
			.await?;

		// Unify.
		if !deterministic {
			graph = self
				.unify_dependencies(graph.clone(), &root)
				.await
				.map_err(|source| tg::error!(!source, "failed to unify the object graph"))?;

			graph = self
				.fix_unification_subpaths(input, graph.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to get unification subpaths"))?;
		}

		// Validate.
		graph.validate(input).await?;

		Ok((graph, root))
	}

	async fn create_unification_node_from_input(
		&self,
		input: &input::Graph,
		node: usize,
		graph: &mut Graph,
		visited_graph_nodes: &mut BTreeMap<(tg::graph::Id, usize), Id>,
	) -> tg::Result<Id> {
		let input_node = &input.nodes[node];
		if let Some(id) = graph.paths.get(&input_node.arg.path).cloned() {
			return Ok(id);
		}
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		graph.paths.insert(input_node.arg.path.clone(), id.clone());

		// Get the outgoing edges.
		let mut edges = BTreeMap::new();

		// Add dependencies.
		let input_edges = input_node.edges.clone();
		'outer: for input_edge in input_edges {
			// If there is an input node at the edge, convert it to a unification node and continue.
			if let Some(node) = input_edge.node {
				let id = Box::pin(self.create_unification_node_from_input(
					input,
					node,
					graph,
					visited_graph_nodes,
				))
				.await?;
				let reference = input_edge.reference.clone();
				let unify_edge = Edge {
					kind: input_edge.kind,
					referent: id,
					path: input_edge.path.clone(),
					subpath: input_edge.subpath.clone(),
				};
				edges.insert(reference, unify_edge);
				continue;
			}

			if let Some(id) = &input_edge.object {
				// If the item at the edge is an object, create a unification node for the object and continue.
				let id = self
					.create_unification_node_from_tagged_object(
						graph,
						&tg::Object::with_id(id.clone()),
						input_edge.tag.clone(),
						true,
					)
					.await?;
				let reference = input_edge.reference.clone();
				let edge = Edge {
					kind: input_edge.kind,
					referent: id,
					path: input_edge.path.clone(),
					subpath: input_edge.subpath.clone(),
				};
				edges.insert(reference, edge);
				continue;
			}

			// Check if there is a solution in the lock file.
			let lockfile = input_node.lockfile.clone();
			'a: {
				// If there is no lockfile, break.
				let Some(lockfile) = lockfile else {
					break 'a;
				};

				// If there is no resolution in the lockfile, break.
				let Ok(Some(referent)) =
					lockfile.try_resolve_dependency(&input_node.arg.path, &input_edge.reference)
				else {
					// If we didn't find a resolution and the --locked arg was passed, it means the lockfile was out of date and we need to error.
					if input_node.arg.locked {
						return Err(tg::error!("lockfile is out of date"));
					};
					break 'a;
				};

				// Get the object ID from the lockfile.
				let object = match referent.item.as_ref() {
					// If the item in the lockfile is accessible at a file system path, we assume its presence in the input graph and can break.
					Some(Either::Left(_path)) => break 'a,

					// If the item in the lockfile is accessible by ID, we get its ID and break.
					Some(Either::Right(object)) => object.clone(),

					// If the item in the lockfile is not on disk, and we don't know its ID, we attempt to resolve it by tag. Note: this happens _pre_ unification, to ensure that the tag dependencies from the lockfile are re-used.
					None => {
						// If the referent has no tag, break.
						let Some(tag) = &referent.tag else {
							break 'a;
						};

						// Parse the tag pattern from the tag in the lockfile.
						let pattern = tag
							.as_str()
							.parse()
							.map_err(|_| tg::error!("invalid tag"))?;

						// Get the item referred to by tag.
						let Some(output) = self.try_get_tag(&pattern).await? else {
							if input_node.arg.locked {
								// Fail early.
								return Err(
									tg::error!(%tag, "the lockfile is up to date, but the tag could not be resolved"),
								);
							}
							break 'a;
						};
						output
							.item
							.right()
							.ok_or_else(|| tg::error!(%tag, "expected an object"))?
					},
				};

				// Given the ID of the item, and its tag, attempt to create a unification node.
				let id = self
					.create_unification_node_from_tagged_object(
						graph,
						&tg::Object::with_id(object.clone()),
						referent.tag.clone(),
						true,
					)
					.await?;

				// Update the edges and continue the loop.
				let reference = input_edge.reference.clone();
				let edge = Edge {
					kind: input_edge.kind,
					referent: id,
					path: input_edge.path.clone(),
					subpath: input_edge.subpath.clone(),
				};
				edges.insert(reference, edge);
				continue 'outer;
			}

			// Otherwise, create partial nodes.
			match input_edge.reference.item() {
				tg::reference::Item::Process(_) => {
					return Err(tg::error!(%reference = input_edge.reference, "invalid reference"));
				},
				tg::reference::Item::Object(object) => {
					let id = self
						.create_unification_node_from_tagged_object(
							graph,
							&tg::Object::with_id(object.clone()),
							None,
							true,
						)
						.await?;
					let reference = input_edge.reference.clone();
					let edge = Edge {
						kind: input_edge.kind,
						referent: id,
						path: input_edge.path.clone(),
						subpath: input_edge.subpath.clone(),
					};
					edges.insert(reference, edge);
				},
				tg::reference::Item::Tag(pattern) => {
					let id = Either::Left(get_reference_from_pattern(pattern));
					let reference = input_edge.reference.clone();
					let edge = Edge {
						kind: input_edge.kind,
						referent: id,
						path: input_edge.path.clone(),
						subpath: input_edge.subpath.clone(),
					};
					edges.insert(reference, edge);
				},
				tg::reference::Item::Path(_) => {
					if !input_node.metadata.is_symlink() {
						return Err(tg::error!("invalid input graph"));
					}
				},
			}
		}

		// Create the node.
		let node = Node {
			edges,
			errors: Vec::new(),
			object: Either::Left(node),
			tag: None,
		};

		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}
}

fn get_reference_from_tag(tag: &tg::Tag) -> tg::Reference {
	let mut components = tag
		.components()
		.iter()
		.cloned()
		.map_into::<tg::tag::pattern::Component>()
		.collect_vec();
	if components.last().is_some_and(|component| {
		matches!(
			component,
			tg::tag::pattern::Component::Normal(tg::tag::Component::Version(_))
		)
	}) {
		*components.last_mut().unwrap() = tg::tag::pattern::Component::Wildcard;
	}
	let pattern = tg::tag::Pattern::with_components(components);
	tg::Reference::with_tag(&pattern)
}

fn get_reference_from_pattern(pattern: &tg::tag::Pattern) -> tg::Reference {
	if pattern.components().last().unwrap().is_version() {
		let mut components = pattern.components().clone();
		*components.last_mut().unwrap() = tg::tag::pattern::Component::Wildcard;
		let pattern = tg::tag::Pattern::with_components(components);
		tg::Reference::with_tag(&pattern)
	} else {
		tg::Reference::with_tag(pattern)
	}
}

impl Server {
	pub async fn unify_dependencies(&self, mut graph: Graph, root: &Id) -> tg::Result<Graph> {
		// Get the overrides.
		let mut overrides: BTreeMap<Id, BTreeMap<String, tg::Reference>> = BTreeMap::new();
		let root_node = graph.nodes.get_mut(root).unwrap();
		for (reference, edge) in &root_node.edges {
			let Some(overrides_) = reference
				.options()
				.as_ref()
				.and_then(|query| query.overrides.clone())
			else {
				continue;
			};
			overrides
				.entry(edge.referent.clone())
				.or_default()
				.extend(overrides_);
		}

		// Get the list of unsolved edges from the root.
		let mut queue = graph
			.edges(root.clone())
			.filter(|edge| {
				let Some(node) = graph.nodes.get(&edge.dst) else {
					return false;
				};

				let solved = node
					.edges
					.values()
					.all(|edge| graph.nodes.contains_key(&edge.referent));

				!solved
			})
			.collect::<im::Vector<_>>();

		// Get the first edge to solve.
		let Some(edge) = queue.pop_back() else {
			return Ok(graph);
		};

		// Construct the initial state.
		let objects = None;
		let visited = im::HashSet::new();
		let mut current = State {
			graph,
			edge,
			queue,
			objects,
			visited,
		};

		// Create a vec of checkpoints to support backtracking.
		let mut checkpoints = Vec::new();

		// Walk the graph until we have no more edges to solve.
		loop {
			self.walk_edge(&mut checkpoints, &mut current, &overrides)
				.await;

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
		// Check if this edge has already been visited.
		if current.visited.contains(&current.edge) {
			return;
		}
		current.visited.insert(current.edge.clone());

		// Check if an override exists.
		let reference = overrides
			.get(&current.edge.src)
			.and_then(|overrides| {
				let name = current.edge.reference.options().as_ref()?.name.as_ref()?;
				overrides.get(name)
			})
			.unwrap_or(&current.edge.reference)
			.clone();

		// If the graph doesn't contain the destination node, attempt to select a version.
		if !current.graph.nodes.contains_key(&current.edge.dst) {
			// Save the current state that we will return to later.
			state.push(current.clone());

			// Attempt to resolve a dependency.
			match self
				.resolve_dependency(&mut current.graph, &reference, &mut current.objects)
				.await
			{
				Ok(dst) => {
					// Add the direct dependencies to the queue.
					let edges = current.graph.edges(dst.clone());
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
			// Add any un-walked edges.
			let edges = current
				.graph
				.edges(current.edge.dst.clone())
				.filter(|edge| !current.visited.contains(edge));
			current.queue.extend(edges);
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
			.item()
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
				.item()
				.try_unwrap_tag_ref()
				.map_err(|_| tg::error!(%reference, "expected a tag pattern"))?
				.clone();
			let remote = reference
				.options()
				.as_ref()
				.and_then(|query| query.remote.clone());

			// List tags that match the pattern.
			let objects_: im::Vector<_> = self
				.list_tags(tg::tag::list::Arg {
					length: None,
					pattern: pattern.clone(),
					remote,
					reverse: false,
				})
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?
				.data
				.into_iter()
				.filter_map(|output| {
					let object = output.item.right()?;
					Some((output.tag, tg::Object::with_id(object)))
				})
				.collect();

			if objects_.is_empty() {
				return Err(tg::error!(%reference, "no tagged items match the reference"));
			}

			// Update the remaining packages.
			objects.replace(objects_);
		}

		// Pop the next version off the list.
		let (tag, object) = objects
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "no solution exists"))?;

		let unify = true;
		self.create_unification_node_from_tagged_object(graph, &object, Some(tag), unify)
			.await
	}

	async fn create_unification_node_from_tagged_object(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		unify: bool,
	) -> tg::Result<Id> {
		let mut visited = BTreeMap::new();
		self.create_unification_node_from_tagged_object_inner(
			graph,
			object,
			tag,
			unify,
			&mut visited,
		)
		.await
	}

	async fn create_unification_node_from_tagged_object_inner(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		unify: bool,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<Id> {
		let object_id = object.id(self).await?;
		if let Some(id) = visited.get(&object_id) {
			return Ok(id.clone());
		}

		let id = tag.as_ref().map_or_else(
			|| {
				let id = Either::Right(graph.counter);
				graph.counter += 1;
				id
			},
			|tag| Either::Left(get_reference_from_tag(tag)),
		);

		visited.insert(object_id.clone(), id.clone());
		let edges = match object {
			tg::Object::Directory(directory) if unify => {
				self.get_unify_directory_edges(graph, directory, visited)
					.await?
			},
			tg::Object::File(file) if unify => {
				self.get_unify_file_edges(graph, file, visited).await?
			},
			tg::Object::Symlink(symlink) if unify => {
				self.get_unify_symlink_edges(graph, symlink, visited)
					.await?
			},
			_ => BTreeMap::new(),
		};

		let node = Node {
			errors: Vec::new(),
			edges,
			object: Either::Right(object_id),
			tag,
		};
		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	async fn get_unify_directory_edges(
		&self,
		graph: &mut Graph,
		directory: &tg::Directory,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<BTreeMap<tg::Reference, Edge>> {
		let mut edges = BTreeMap::new();
		for (name, object) in directory.entries(self).await? {
			let reference = tg::Reference::with_path(name);
			let id = Box::pin(self.create_unification_node_from_tagged_object_inner(
				graph,
				&object.into(),
				None,
				true,
				visited,
			))
			.await?;
			let edge = Edge {
				kind: None,
				referent: id,
				path: None,
				subpath: None,
			};
			edges.insert(reference, edge);
		}
		Ok(edges)
	}

	async fn get_unify_file_edges(
		&self,
		graph: &mut Graph,
		file: &tg::File,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<BTreeMap<tg::Reference, Edge>> {
		let mut edges = BTreeMap::new();
		for (reference, referent) in file.dependencies(self).await? {
			if let Ok(pat) = reference.item().try_unwrap_tag_ref() {
				let id = Either::Left(get_reference_from_pattern(pat));
				let edge = Edge {
					kind: None,
					referent: id,
					path: None,
					subpath: referent.subpath.clone(),
				};
				edges.insert(reference, edge);
			} else {
				let id = Box::pin(self.create_unification_node_from_tagged_object_inner(
					graph,
					&referent.item,
					referent.tag,
					true,
					visited,
				))
				.await?;
				let edge = Edge {
					kind: None,
					referent: id,
					path: None,
					subpath: referent.subpath,
				};
				edges.insert(reference, edge);
			}
		}
		Ok(edges)
	}

	async fn get_unify_symlink_edges(
		&self,
		graph: &mut Graph,
		symlink: &tg::Symlink,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<BTreeMap<tg::Reference, Edge>> {
		let mut edges = BTreeMap::new();
		let subpath = symlink.subpath(self).await?;
		if let Some(artifact) = symlink.artifact(self).await? {
			let reference = tg::Reference::with_object(&artifact.id(self).await?.into());
			let id = Box::pin(self.create_unification_node_from_tagged_object_inner(
				graph,
				&artifact.into(),
				None,
				true,
				visited,
			))
			.await?;
			let edge = Edge {
				kind: None,
				referent: id,
				path: None,
				subpath,
			};
			edges.insert(reference, edge);
		}
		Ok(edges)
	}
}

impl Server {
	async fn fix_unification_subpaths(
		&self,
		input: &input::Graph,
		mut graph: Graph,
	) -> tg::Result<Graph> {
		for node in graph.nodes.keys().cloned().collect::<Vec<_>>() {
			// Skip nodes that don't refer to input nodes.
			let Some(input_node) = graph
				.nodes
				.get(&node)
				.unwrap()
				.object
				.as_ref()
				.left()
				.copied()
			else {
				continue;
			};

			// Skip nodes that are not modules.
			let path = &input.nodes[input_node].arg.path;
			if !tg::package::is_module_path(path) {
				continue;
			}

			// Collect the list of tag dependencies of this node that are missing subpaths.
			let references_missing_subpaths = graph
				.nodes
				.get(&node)
				.unwrap()
				.edges
				.iter()
				.filter_map(|(reference, edge)| {
					let is_missing_subpath = graph.nodes.get(&edge.referent)?.tag.is_some()
						&& graph
							.nodes
							.get(&edge.referent)
							.unwrap()
							.object
							.as_ref()
							.right()
							.is_some_and(tg::object::Id::is_directory)
						&& edge.subpath.is_none()
						&& matches!(
							edge.kind,
							None | Some(tg::module::Kind::Ts | tg::module::Kind::Js)
						);
					is_missing_subpath.then(|| (reference.clone(), edge.clone()))
				})
				.collect::<Vec<_>>();

			for (reference, edge) in references_missing_subpaths {
				let subpath = self
					.try_get_root_module_name_for_node(input, &graph, &edge.referent)
					.await;
				graph
					.nodes
					.get_mut(&node)
					.unwrap()
					.edges
					.get_mut(&reference)
					.unwrap()
					.subpath = subpath;
			}
		}
		Ok(graph)
	}

	async fn try_get_root_module_name_for_node(
		&self,
		input: &input::Graph,
		graph: &Graph,
		node: &Id,
	) -> Option<PathBuf> {
		let node = graph.nodes.get(node)?;
		match &node.object {
			Either::Left(index) => input
				.nodes
				.get(*index)?
				.metadata
				.is_dir()
				.then(|| {
					node.edges.keys().find_map(|reference| {
						let path = reference.item().try_unwrap_path_ref().ok()?;
						tg::package::is_root_module_path(path)
							.then(|| path.file_name().unwrap().into())
					})
				})
				.flatten(),
			Either::Right(object) => {
				if !object.is_directory() {
					return None;
				}

				let object = tg::Object::with_id(object.clone());
				if let Ok(path) =
					tg::package::try_get_root_module_file_name(self, Either::Left(&object)).await
				{
					return path.map(PathBuf::from);
				}

				node.edges.keys().find_map(|reference| {
					let path = reference.item().try_unwrap_path_ref().ok()?;
					tg::package::is_root_module_path(path).then(|| path.file_name().unwrap().into())
				})
			},
		}
	}
}

fn try_backtrack(state: &mut Vec<State>, edge: &Unresolved) -> Option<State> {
	// Find the index of the state where the node was first added.
	let position = state
		.iter()
		.position(|state| state.graph.nodes.contains_key(&edge.dst))?;

	// Backtrack.
	state.truncate(position);
	let mut state = state.pop()?;

	// Make sure to retry the edge.
	state.queue.push_front(state.edge.clone());

	Some(state)
}

impl Graph {
	pub async fn validate(&self, input: &input::Graph) -> tg::Result<()> {
		let mut errors = Vec::new();
		for node in self.nodes.values() {
			let errors_ = node.errors.iter().cloned();
			errors.extend(errors_);
			for (reference, edge) in &node.edges {
				if !self.nodes.contains_key(&edge.referent) {
					let referrer = match &node.object {
						Either::Left(index) => input.nodes[*index].arg.path.display().to_string(),
						Either::Right(object) => object.to_string(),
					};
					let error = tg::error!(%reference, %referrer, "failed to resolve dependency");
					errors.push(error);
				}
			}
		}
		if errors.is_empty() {
			return Ok(());
		}

		let mut last_error = errors.pop();
		while let Some(mut error) = errors.pop() {
			error.source.replace(Arc::new(last_error.take().unwrap()));
			last_error.replace(error);
		}
		Err(last_error.unwrap())
	}

	pub fn edges(&self, src: Id) -> impl Iterator<Item = Unresolved> + '_ {
		self.nodes
			.get(&src)
			.unwrap()
			.edges
			.iter()
			.map(move |(reference, edge)| Unresolved {
				src: src.clone(),
				dst: edge.referent.clone(),
				reference: reference.clone(),
			})
	}

	fn add_error(&mut self, node: &Id, error: tg::Error) {
		self.nodes.get_mut(node).unwrap().errors.push(error);
	}
}

impl std::fmt::Display for Edge {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.referent {
			Either::Left(reference) => write!(f, "(reference: {reference}")?,
			Either::Right(index) => write!(f, "(index: {index}")?,
		};
		if let Some(path) = &self.path {
			write!(f, ", path: {}", path.display())?;
		}
		if let Some(subpath) = &self.subpath {
			write!(f, ", subpath: {}", subpath.display())?;
		}
		write!(f, ")")?;
		Ok(())
	}
}
