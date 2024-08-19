use crate::Server;
use either::Either;
use itertools::Itertools;
use std::{
	collections::BTreeMap,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_semver::Version;

use super::input::Input;

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

	// The set of paths in the graph.
	pub paths: im::HashMap<tg::Path, Id>,
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
	pub object: Either<Arc<RwLock<Input>>, tg::object::Id>,

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
	pub(super) async fn create_unification_graph(
		&self,
		input: Arc<RwLock<Input>>,
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
		input: Arc<RwLock<Input>>,
		graph: &mut Graph,
		visited_graph_nodes: &mut BTreeMap<(tg::graph::Id, usize), Id>,
	) -> tg::Result<Id> {
		if let Some(id) = graph.paths.get(&input.read().unwrap().arg.path).cloned() {
			return Ok(id);
		}
		let id = Either::Right(graph.counter);
		graph.counter += 1;
		graph
			.paths
			.insert(input.read().unwrap().arg.path.clone(), id.clone());

		// Get the outgoing edges.
		let mut outgoing = BTreeMap::new();

		// Add dependencies.
		let dependencies = input.read().unwrap().dependencies.clone();
		for (dependency, child) in dependencies {
			// Recurse on existing input.
			if let Some(input) = child {
				let id = Box::pin(self.create_unification_graph_from_input(
					input,
					graph,
					visited_graph_nodes,
				))
				.await?;
				outgoing.insert(dependency.clone(), id);
				continue;
			}

			// Check if there is a solution in the lock file.
			let lockfile = input.read().unwrap().lockfile.clone();
			if let Some((lockfile, _)) = lockfile {
				let node = *lockfile.paths.get(&input.read().unwrap().arg.path).unwrap();
				let node = &lockfile.nodes[node];
				if let tg::graph::data::Node::File(file) = node {
					if let Some(entry) = file
						.dependencies
						.as_ref()
						.and_then(|map| map.get(&dependency))
					{
						match entry {
							Either::Left(_index) => (), // if referenced by index it will be in the input. todo: verify this.
							Either::Right(object) => {
								let id = self
									.create_unification_node_from_object(graph, object.clone())
									.await?;
								outgoing.insert(dependency.clone(), id);
								continue;
							},
						}
					}
				}
			}

			// Otherwise, create partial nodes.
			match dependency.path() {
				tg::reference::Path::Build(_) => {
					return Err(tg::error!(%dependency, "invalid reference"))
				},
				tg::reference::Path::Object(object) => {
					let id = self
						.create_unification_node_from_object(graph, object.clone())
						.await?;
					outgoing.insert(dependency.clone(), id);
				},
				tg::reference::Path::Tag(pattern) => {
					let id = get_reference_from_pattern(pattern);
					outgoing.insert(dependency.clone(), Either::Left(id));
				},
				tg::reference::Path::Path(_) => return Err(tg::error!("unimplemented")),
			}
		}

		// Create the node.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: Either::Left(input.clone()),
			tag: None,
		};

		graph.nodes.insert(id.clone(), node);

		Ok(id)
	}

	async fn create_unification_node_from_graph_node(
		&self,
		input_graph: &mut Graph,
		output_graph: &tg::Graph,
		node: usize,
		visited: &mut BTreeMap<(tg::graph::Id, usize), Id>,
	) -> tg::Result<Id> {
		let key = (output_graph.id(self).await?, node);
		if let Some(id) = visited.get(&key) {
			return Ok(id.clone());
		}
		let id = Either::Right(input_graph.counter);
		input_graph.counter += 1;
		visited.insert(key, id.clone());

		let object = output_graph.object(self).await?;
		let output_graph_node = &object.nodes[node];

		let mut outgoing = BTreeMap::new();
		if let tg::graph::Node::File(file) = &object.nodes[node] {
			for (dependency, either) in file.dependencies.iter().flatten() {
				match either {
					Either::Left(node) => {
						let id = Box::pin(self.create_unification_node_from_graph_node(
							input_graph,
							output_graph,
							*node,
							visited,
						))
						.await?;
						outgoing.insert(dependency.clone(), id);
					},
					// TODO: unify
					// Either::Right(tg::Object::File(file)) => {
					// },
					Either::Right(object) => {
						let id = object.id(self).await?;
						let id = self
							.create_unification_node_from_object(input_graph, id)
							.await?;
						outgoing.insert(dependency.clone(), id);
					},
				}
			}
		}

		// Create the underlying object.
		let object: tg::Object = match output_graph_node.kind() {
			tg::artifact::Kind::Directory => {
				tg::Directory::with_object(Arc::new(tg::directory::Object::Graph {
					graph: output_graph.clone(),
					node,
				}))
				.into()
			},
			tg::artifact::Kind::File => tg::File::with_object(Arc::new(tg::file::Object::Graph {
				graph: output_graph.clone(),
				node,
			}))
			.into(),
			tg::artifact::Kind::Symlink => {
				tg::Symlink::with_object(Arc::new(tg::symlink::Object::Graph {
					graph: output_graph.clone(),
					node,
				}))
				.into()
			},
		};

		// Create thenode.
		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: Either::Right(object.id(self).await?),
			tag: None,
		};

		input_graph.nodes.insert(id.clone(), node);
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
			id: id.clone(),
			errors: Vec::new(),
			outgoing: BTreeMap::new(),
			_inline_object: true,
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
		.map(|component| component.as_str().parse().unwrap())
		.collect::<Vec<_>>();
	let is_semver = components.last().map_or(false, |component| {
		matches!(component, tg::tag::pattern::Component::Semver(_))
	});
	if is_semver {
		let last = components.last_mut().unwrap();
		*last = tg::tag::pattern::Component::Glob;
	}
	let pattern = tg::tag::Pattern::with_components(components);
	tg::Reference::with_tag(&pattern).unwrap()
}

fn get_reference_from_pattern(pattern: &tg::tag::Pattern) -> tg::Reference {
	let components = pattern.components();
	if matches!(
		components.last(),
		Some(tg::tag::pattern::Component::Semver(_))
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

fn try_get_id(reference: &tg::Reference) -> Option<Id> {
	reference
		.path()
		.try_unwrap_tag_ref()
		.ok()
		.map(get_reference_from_pattern)
		.map(Either::Left)
}

impl Server {
	pub async fn unify_dependencies(&self, mut graph: Graph, root: &Id) -> tg::Result<Graph> {
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

		self.create_unification_node_from_tagged_object(graph, &object, tag)
			.await
	}

	async fn create_unification_node_from_tagged_object(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: tg::Tag,
	) -> tg::Result<Id> {
		let mut visited = BTreeMap::new();
		self.create_unification_node_from_tagged_object_inner(
			graph,
			object,
			Some(tag),
			&mut visited,
		)
		.await
	}

	async fn create_unification_node_from_tagged_object_inner(
		&self,
		graph: &mut Graph,
		object: &tg::Object,
		tag: Option<tg::Tag>,
		visited: &mut BTreeMap<tg::object::Id, Id>,
	) -> tg::Result<Id> {
		let object_id = object.id(self).await?;
		if let Some(id) = visited.get(&object_id) {
			return Ok(id.clone());
		}

		let id = tag
			.as_ref()
			.map(|tag| Either::Left(get_reference_from_tag(tag)))
			.unwrap_or_else(|| {
				let id = Either::Right(graph.counter);
				graph.counter += 1;
				id
			});

		visited.insert(object_id.clone(), id.clone());

		// If this is a file, get the outgoing edges.
		let mut outgoing = BTreeMap::new();
		if let tg::Object::File(file) = object {
			let dependencies = file.dependencies(self).await?;
			if let Some(Either::Right(dependencies)) = dependencies {
				for (reference, object) in dependencies {
					if let Ok(pattern) = reference.path().try_unwrap_tag_ref() {
						let id = get_reference_from_pattern(pattern);
						outgoing.insert(reference, Either::Left(id));
					} else {
						let id = Box::pin(self.create_unification_node_from_tagged_object_inner(
							graph, &object, None, visited,
						))
						.await?;
						outgoing.insert(reference, id);
					}
				}
			}
		}

		let node = Node {
			id: id.clone(),
			errors: Vec::new(),
			outgoing,
			_inline_object: false,
			object: Either::Right(object_id),
			tag,
		};
		graph.nodes.insert(id.clone(), node);

		Ok(id)
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

impl Graph {
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