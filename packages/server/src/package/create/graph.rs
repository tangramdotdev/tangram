use crate::Server;
use derive_more::FromStr;
use either::Either;
use futures::{future, stream::FuturesOrdered, FutureExt, TryStreamExt};
use itertools::Itertools;
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
	packages: Option<im::Vector<(tg::Tag, tg::Package)>>,
}

// A graph of packages.
#[derive(Clone, Default, Debug)]
pub(super) struct Graph {
	// A counter used to create IDs for nodes that don't have a repository ID.
	counter: u32,

	// The set of nodes in the graph.
	nodes: im::HashMap<Id, Node>,
}

// A node within the package graph.
#[derive(Clone, Debug)]
struct Node {
	// A unique identifier of the node within the package graph.
	id: Id,

	// The result of this node (None if we do not know if it is successful or not).
	errors: Vec<tg::Error>,

	// Direct dependencies.
	outgoing: BTreeMap<tg::Reference, Id>,

	// The underlying package.
	package: Either<tg::package::Node, tg::Package>,

	// The tag of this package version, if it exists.
	tag: Option<tg::Tag>,
}

pub(super) type Id = Either<tg::Tag, u32>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Edge {
	// The source node of the edge (referrer)
	pub src: Id,

	// The destination of the edge (dependency)
	pub dst: Id,

	// The constraint the referrer places on the dependency.
	pub reference: tg::Reference,
}

impl Graph {
	/// Convert the graph into a tg::package::Object
	pub fn into_package_object(&self, root: &Id) -> tg::package::Object {
		// Assign an index to every node that isn't a bare package.
		let nodes = self
			.nodes
			.values()
			.enumerate()
			.filter_map(|(index, node)| {
				node.package
					.is_left()
					.then_some((node.id.clone(), (index, node)))
			})
			.collect::<BTreeMap<_, _>>();

		// Get the root index.
		let root = nodes.get(root).unwrap().0;

		// Map from graph nodes to package nodes.
		let nodes = self
			.nodes
			.values()
			.filter_map(|node| {
				let package = node.package.as_ref().left()?;
				let dependencies = node
					.outgoing
					.iter()
					.map(|(reference, id)| {
						let node = self.nodes.get(id).unwrap();
						let package = match &node.package {
							Either::Left(_) => Either::Left(nodes.get(&node.id).unwrap().0),
							Either::Right(package) => Either::Right(package.clone()),
						};
						(reference.clone(), Some(package))
					})
					.collect();
				Some(tg::package::Node {
					dependencies,
					..package.clone()
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

	async fn insert(
		&mut self,
		server: &Server,
		package: &tg::Package,
		reference: &tg::Reference,
	) -> tg::Result<Id> {
		// Get the object.
		let object = package
			.object(server)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the package object"))?;

		// Add the root and its direct dependencies contained within the package to the graph.
		let index = object.root;
		let mut visited = BTreeMap::new();
		let id = self.insert_inner(
			&Either::Left(index),
			object.as_ref(),
			reference,
			&mut visited,
		);
		Ok(id)
	}

	fn insert_inner(
		&mut self,
		package: &Either<usize, tg::Package>,
		object: &tg::package::Object,
		reference: &tg::Reference,
		visited: &mut BTreeMap<usize, Id>,
	) -> Id {
		// Get the tag if it exists.
		let tag = get_tag_from_reference(reference);

		// Get the index or short circuit.
		let index = match package {
			Either::Left(index) => *index,
			Either::Right(package) => {
				// Compute the ID.
				let src = tag.clone().map(Either::Left).unwrap_or_else(|| {
					let id = Either::Right(self.counter);
					self.counter += 1;
					id
				});

				// Create the node.
				let node = Node {
					id: src.clone(),
					errors: Vec::new(),
					outgoing: BTreeMap::new(),
					package: Either::Right(package.clone()),
					tag,
				};

				// Insert and return the id.
				self.nodes.insert(src.clone(), node);
				return src;
			},
		};

		// Check if we've visited this node in the package.
		if let Some(id) = visited.get(&index) {
			return id.clone();
		}

		// Compute the ID.
		let src = tag.clone().map(Either::Left).unwrap_or_else(|| {
			let id = Either::Right(self.counter);
			self.counter += 1;
			id
		});

		// Update the visited table.
		visited.insert(index, src.clone());

		// Get the node.
		let node = &object.nodes[index];

		// Get the outgoing edges.
		let mut errors = Vec::new();
		let mut outgoing = BTreeMap::new();
		for (reference, package) in node.dependencies.iter() {
			let tag = get_tag_from_reference(reference);

			// Get the destination ID.
			let dst = if let Some(tag) = tag {
				Either::Left(tag)
			} else if let Some(package) = package {
				self.insert_inner(package, object, reference, visited)
			} else {
				errors.push(tg::error!(%reference, "invalid package"));
				continue;
			};

			// Add the destination as an outgoing node.
			outgoing.insert(reference.clone(), dst);
		}

		let package = Either::Left(object.nodes[index].clone());
		let node = Node {
			id: src.clone(),
			errors,
			outgoing,
			package,
			tag,
		};

		// Insert the node and return the id.
		self.nodes.insert(src.clone(), node);
		src
	}

	fn add_error(&mut self, node: &Id, error: tg::Error) {
		self.nodes.get_mut(node).unwrap().errors.push(error);
	}
}

impl Server {
	pub(super) async fn create_package_graph(
		&self,
		package: &tg::Package,
		reference: &tg::Reference,
	) -> tg::Result<(Id, Graph)> {
		// Create an empty graph.
		let mut graph = Graph::default();

		// Add the root package to the solution and get its outgoing edges.
		let root = graph.insert(self, package, reference).await?;

		// Get the overrides.
		let mut overrides: BTreeMap<Id, BTreeMap<String, tg::Reference>> = BTreeMap::new();
		let root_node = graph.nodes.get_mut(&root).unwrap();
		for (reference, node) in &root_node.outgoing {
			let Some(overrides_) = reference
				.query
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
			return Ok((root, graph));
		};

		// Construct the initial state.
		let packages = None;
		let mut current = State {
			graph,
			edge,
			queue,
			packages,
		};

		// Create a vec of checkpoints to support backtracking.
		let mut checkpoints = Vec::new();

		// Walk the graph until we have no more edges to solve.
		loop {
			self.follow_edge(&mut checkpoints, &mut current, &overrides)
				.await;

			// Changing this to pop_back() would convert the algorithm from breadth-first to depth-first. The algorithm should be correct regardless of traversel order. However, using BFS to walk the graph allows us to propogate constraints when backtracking to shrink the search sapce.
			let Some(next) = current.queue.pop_front() else {
				break;
			};

			current.edge = next;
		}

		Ok((root, current.graph))
	}

	async fn follow_edge(
		&self,
		state: &mut Vec<State>,
		current: &mut State,
		overrides: &BTreeMap<Id, BTreeMap<String, tg::Reference>>,
	) {
		// Check if an override exists.
		let reference = overrides
			.get(&current.edge.src)
			.and_then(|overrides| {
				let name = current.edge.reference.query.as_ref()?.name.as_ref()?;
				overrides.get(name)
			})
			.unwrap_or(&current.edge.reference)
			.clone();

		// If the graph doesn't contain the destination node, attempt to select a version.
		if !current.graph.nodes.contains_key(&current.edge.dst) {
			match self
				.resolve_dependency(&mut current.graph, &reference, &mut current.packages)
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

		// If there is no tag it is not a tag dependency, so return.
		if current.edge.dst.is_right() {
			return;
		}

		// Validate the constraint.
		match reference
			.path
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
		packages: &mut Option<im::Vector<(tg::Tag, tg::Package)>>,
	) -> tg::Result<Id> {
		// Seed the remaining packages if necessary.
		if packages.is_none() {
			// Get the tag pattern and remote if necessary.
			let pattern = reference
				.path
				.try_unwrap_tag_ref()
				.map_err(|_| tg::error!(%reference, "expected a tag pattern"))?
				.clone();
			let remote = reference
				.query
				.as_ref()
				.and_then(|query| query.remote.clone());

			// List tags that match the pattern.
			let output = self
				.list_tags(tg::tag::list::Arg {
					pattern: pattern.clone(),
					remote,
				})
				.await
				.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?;

			// Convert the tag objects into packages.
			let packages_ = output
				.data
				.into_iter()
				.filter_map(|output| {
					let object = output.item.right()?;
					let root = Either::Right(tg::Object::with_id(object));
					Some(
						self.create_package_with_path_dependencies(root)
							.then(|result| future::ready(result.map(|p| (output.tag, p)))),
					)
				})
				.collect::<FuturesOrdered<_>>()
				.try_collect()
				.await?;

			// Update the remaining packages.
			packages.replace(packages_);
		}

		// Pop the next version off the list.
		let package = packages
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| tg::error!(%reference, "no solution exists"))?
			.1;

		// insert.
		graph.insert(self, &package, reference).await
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
			.packages
			.as_ref()
			.unwrap()
			.iter()
			.filter(|(version, _)| edge.reference.path.unwrap_tag_ref().matches(version))
			.cloned()
			.collect();
		state.packages.replace(packages);
	}

	Some(state)
}

fn get_tag_from_reference(reference: &tg::Reference) -> Option<tg::Tag> {
	let pattern = reference.path.try_unwrap_tag_ref().ok()?;
	let components = pattern.components();
	if components.len() < 2
		|| components
			.last()
			.map_or(false, |c| c.try_unwrap_normal_ref().is_ok())
	{
		return None;
	}

	let mut components = components.clone();
	components.pop();
	let string = components.iter().map(|c| c.to_string()).join("/");
	tg::Tag::from_str(&string).ok()
}
