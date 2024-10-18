use super::input;
use crate::Server;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tangram_client as tg;
use tangram_either::Either;

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

// A graph of packages.
#[derive(Clone, Default, Debug)]
pub(super) struct Graph {
	// A counter used to create IDs for nodes that don't have a repository ID.
	counter: usize,

	// The set of nodes in the graph.
	pub nodes: im::HashMap<Id, Node>,

	// The set of paths in the graph.
	pub paths: im::HashMap<PathBuf, Id>,
}

// A node within the package graph.
#[derive(Clone, Debug)]
pub struct Node {
	// The result of this node (None if we do not know if it is successful or not).
	pub errors: Vec<tg::Error>,

	// Direct dependencies.
	pub edges: BTreeMap<tg::Reference, Edge>,

	// The underlying object.
	pub object: Either<Arc<tokio::sync::RwLock<input::Graph>>, tg::object::Id>,

	// The tag of this node, if it exists.
	pub tag: Option<tg::Tag>,
}

pub(super) type Id = Either<tg::Reference, usize>;

// An un-resolved reference in the graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Unresolved {
	// The source node of the edge.
	pub src: Id,

	// The destination of the edge.
	pub dst: Id,

	// The constraint the referrer places on the dependency.
	pub reference: tg::Reference,
}

// An edge in the graph.
#[derive(Clone, Debug)]
pub(super) struct Edge {
	pub referent: Id,
	pub subpath: Option<PathBuf>,
}

impl Server {
	pub(super) async fn create_unification_graph(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
	) -> tg::Result<(Graph, Id)> {
		let mut graph: Graph = Graph::default();
		let mut visited_graph_nodes = BTreeMap::new();

		let root = self
			.create_unification_graph_from_input(input, &mut graph, &mut visited_graph_nodes)
			.await?;
		Ok((graph, root))
	}

	async fn create_unification_graph_from_input(
		&self,
		input: Arc<tokio::sync::RwLock<input::Graph>>,
		graph: &mut Graph,
		visited_graph_nodes: &mut BTreeMap<(tg::graph::Id, usize), Id>,
	) -> tg::Result<Id> {
		if let Some(id) = graph.paths.get(&input.read().await.arg.path).cloned() {
			return Ok(id);
		}
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		graph
			.paths
			.insert(input.read().await.arg.path.clone(), id.clone());

		// Get the outgoing edges.
		let mut edges = BTreeMap::new();

		// Add dependencies.
		let input_edges = input.read().await.edges.clone();
		'outer: for edge in input_edges {
			if let Some(input) = edge.node() {
				let id = Box::pin(self.create_unification_graph_from_input(
					input.clone(),
					graph,
					visited_graph_nodes,
				))
				.await?;
				let reference = edge.reference.clone();
				let edge = Edge {
					referent: id,
					subpath: edge.subpath,
				};
				edges.insert(reference, edge);
				continue;
			}

			if let Some(id) = &edge.object {
				let id = if let Some(tag) = &edge.tag {
					let unify = false;
					self.create_unification_node_from_tagged_object(
						graph,
						&tg::Object::with_id(id.clone()),
						tag.clone(),
						unify,
					)
					.await?
				} else {
					self.create_unification_node_from_object(graph, id.clone())
						.await?
				};
				let reference = edge.reference.clone();
				let edge = Edge {
					referent: id,
					subpath: edge.subpath.clone(),
				};
				edges.insert(reference, edge);
				continue;
			}

			// Check if there is a solution in the lock file.
			let lockfile = input.read().await.lockfile.clone();
			'a: {
				let Some((lockfile, node)) = lockfile else {
					break 'a;
				};
				let tg::lockfile::Node::File { dependencies, .. } = &lockfile.nodes[node] else {
					break 'a;
				};
				let Some(referrent) = dependencies.get(&edge.reference) else {
					if input.read().await.arg.locked {
						return Err(tg::error!("lockfile is out of date"))?;
					};
					break 'a;
				};
				let Either::Right(object) = &referrent.item else {
					break 'a;
				};

				let id = if let Some(tag) = &referrent.tag {
					let unify = true;
					self.create_unification_node_from_tagged_object(
						graph,
						&tg::Object::with_id(object.clone()),
						tag.clone(),
						unify,
					)
					.await?
				} else {
					self.create_unification_node_from_object(graph, object.clone())
						.await?
				};
				let reference = edge.reference.clone();
				let edge = Edge {
					referent: id,
					subpath: edge.subpath,
				};
				edges.insert(reference, edge);
				continue 'outer;
			}

			// Otherwise, create partial nodes.
			match edge.reference.path() {
				tg::reference::Path::Build(_) => {
					return Err(tg::error!(%reference = edge.reference, "invalid reference"))
				},
				tg::reference::Path::Object(object) => {
					let id = self
						.create_unification_node_from_object(graph, object.clone())
						.await?;
					let reference = edge.reference.clone();
					let edge = Edge {
						referent: id,
						subpath: edge.subpath,
					};
					edges.insert(reference, edge);
				},
				tg::reference::Path::Tag(pattern) => {
					let id = Either::Left(get_reference_from_pattern(pattern));
					let reference = edge.reference.clone();
					let edge = Edge {
						referent: id,
						subpath: edge.subpath,
					};
					edges.insert(reference, edge);
				},
				tg::reference::Path::Path(_) => return Err(tg::error!("unimplemented")),
			}
		}

		// Create the node.
		let node = Node {
			errors: Vec::new(),
			edges,
			object: Either::Left(input.clone()),
			tag: None,
		};

		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	#[allow(clippy::unused_async)]
	async fn create_unification_node_from_object(
		&self,
		graph: &mut Graph,
		object: tg::object::Id,
	) -> tg::Result<Id> {
		// Get an ID.
		let id = Either::Right(graph.counter);
		graph.counter += 1;

		// Create a node.
		let node = Node {
			errors: Vec::new(),
			edges: BTreeMap::new(),
			object: Either::Right(object),
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
		.map(|component| {
			component
				.as_str()
				.parse::<tg::tag::pattern::Component>()
				.unwrap()
		})
		.collect::<Vec<_>>();

	let is_semver = components.last().map_or(false, |component| {
		component
			.to_string()
			.parse::<tangram_version::Version>()
			.is_ok()
	});

	if is_semver {
		*components.last_mut().unwrap() = tg::tag::pattern::Component::Glob;
	}

	let pattern = tg::tag::Pattern::with_components(components);
	tg::Reference::with_tag(&pattern).unwrap()
}

fn get_reference_from_pattern(pattern: &tg::tag::Pattern) -> tg::Reference {
	let components = pattern.components();
	if matches!(
		components.last(),
		Some(tg::tag::pattern::Component::Version(_))
	) {
		let mut components = components.clone();
		let last = components.last_mut().unwrap();
		*last = tg::tag::pattern::Component::Glob;
		let pattern = tg::tag::Pattern::with_components(components);
		tg::Reference::with_tag(&pattern).unwrap()
	} else {
		tg::Reference::with_tag(pattern).unwrap()
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
		for (reference, edge) in &root_node.edges {
			let Some(overrides_) = reference
				.query()
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
				let name = current.edge.reference.query().as_ref()?.name.as_ref()?;
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
		self.create_unification_node_from_tagged_object(graph, &object, tag, unify)
			.await
	}

	async fn create_unification_node_from_tagged_object(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: tg::Tag,
		unify: bool,
	) -> tg::Result<Id> {
		let mut visited = BTreeMap::new();
		self.create_unification_node_from_tagged_object_inner(
			graph,
			object,
			Some(tag),
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
				referent: id,
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
			if let Ok(pat) = reference.path().try_unwrap_tag_ref() {
				let id = Either::Left(get_reference_from_pattern(pat));
				let edge = Edge {
					referent: id,
					subpath: None,
				};
				edges.insert(reference, edge);
			} else {
				let object = match (&referent.item, &referent.subpath) {
					(tg::Object::Directory(directory), Some(subpath)) => {
						directory.get(self, subpath).await?.into()
					},
					(_, Some(_)) => return Err(tg::error!("unexpected subpath")),
					(object, None) => object.clone(),
				};
				let id = Box::pin(self.create_unification_node_from_tagged_object_inner(
					graph,
					&object,
					referent.tag,
					true,
					visited,
				))
				.await?;
				let edge = Edge {
					referent: id,
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
				referent: id,
				subpath: None,
			};
			edges.insert(reference, edge);
		}
		Ok(edges)
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
	pub async fn validate(&self) -> tg::Result<()> {
		let mut errors = Vec::new();
		for node in self.nodes.values() {
			let errors_ = node.errors.iter().cloned();
			errors.extend(errors_);
			for (reference, edge) in &node.edges {
				if !self.nodes.contains_key(&edge.referent) {
					let referrer = match &node.object {
						Either::Left(input) => input.read().await.arg.path.display().to_string(),
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
