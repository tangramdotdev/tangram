use {
	crate::{
		Server,
		checkin::graph::{Contents, Directory, File, Graph, Node, Symlink, Variant},
	},
	smallvec::SmallVec,
	std::{
		fmt::Write as _,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

struct State {
	checkpoints: Vec<Checkpoint>,
	root: PathBuf,
	unsolved_dependencies: bool,
	updates: Vec<tg::tag::Pattern>,
}

#[derive(Clone)]
struct Checkpoint {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	graphs: im::HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	graph_nodes: im::HashMap<(tg::graph::Id, usize), usize, fnv::FnvBuildHasher>,
	listed: bool,
	queue: im::Vector<Item>,
	lock: Option<Arc<tg::graph::Data>>,
	solutions: Solutions,
	visited: im::HashSet<Item, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
pub struct Candidate {
	node: Option<usize>,
	object: tg::object::Id,
	tag: tg::Tag,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Item {
	node: usize,
	variant: ItemVariant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
enum ItemVariant {
	DirectoryEntry(String),
	FileDependency(tg::Reference),
	SymlinkArtifact,
}

pub type Solutions = im::HashMap<tg::tag::Pattern, Solution, fnv::FnvBuildHasher>;

#[derive(Clone)]
pub struct Solution {
	pub referent: Option<tg::Referent<usize>>,
	pub referrers: Vec<Referrer>,
}

enum TagInnerOutput {
	Solved(tg::Referent<usize>),
	Conflicted,
	Poisoned,
}

#[derive(Clone)]
pub struct Referrer {
	pub node: usize,
	pub pattern: Option<tg::tag::Pattern>,
}

impl Server {
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn checkin_solve(
		&self,
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		next: usize,
		lock: Option<Arc<tg::graph::Data>>,
		solutions: &mut Solutions,
		root: &Path,
		progress: &crate::progress::Handle<tg::checkin::Output>,
	) -> tg::Result<()> {
		progress.spinner("solving", "solving");
		if solutions.is_empty() {
			// If solutions is empty, then just solve.
			self.checkin_solve_inner(arg, graph, next, lock, solutions, root)
				.await?;
		} else if !arg.updates.is_empty() {
			// If there are updates, then unsolve and clean the graph, clear the solutions, and solve from the beginning.
			graph.unsolve();
			graph.clean(root);
			solutions.clear();
			self.checkin_solve_inner(arg, graph, next, lock, solutions, root)
				.await?;
		} else {
			// Otherwise, attempt to solve.
			let result = self
				.checkin_solve_inner(arg, graph, next, lock.clone(), solutions, root)
				.await;
			if result.is_ok() {
				return Ok(());
			}

			// Unsolve and clean the graph, clear the solutions, and solve from the beginning.
			graph.unsolve();
			graph.clean(root);
			solutions.clear();
			self.checkin_solve_inner(arg, graph, next, lock, solutions, root)
				.await?;
		}
		progress.finish("solving");
		Ok(())
	}

	async fn checkin_solve_inner(
		&self,
		arg: &tg::checkin::Arg,
		graph: &mut Graph,
		next: usize,
		lock: Option<Arc<tg::graph::Data>>,
		solutions: &mut Solutions,
		root: &Path,
	) -> tg::Result<()> {
		// Create the state
		let mut state = State {
			checkpoints: Vec::new(),
			root: root.to_owned(),
			unsolved_dependencies: arg.options.unsolved_dependencies,
			updates: arg.updates.clone(),
		};

		// Create the first checkpoint.
		let index = graph.paths.get(root).unwrap();
		let mut checkpoint = Checkpoint {
			candidates: None,
			graph: graph.clone(),
			graphs: im::HashMap::default(),
			graph_nodes: im::HashMap::default(),
			listed: false,
			lock: lock.clone(),
			queue: im::Vector::new(),
			solutions: solutions.clone(),
			visited: im::HashSet::default(),
		};
		Self::checkin_solve_enqueue_items_for_node(&mut checkpoint, *index);

		// Solve.
		while let Some(item) = checkpoint.queue.pop_front() {
			self.checkin_solve_visit_item(&mut state, &mut checkpoint, item)
				.await?;
		}

		// Mark all new nodes as solved.
		for index in next..checkpoint.graph.next {
			let node = checkpoint.graph.nodes.get_mut(&index).unwrap();
			node.solved = true;
		}

		// Set the checkpoint and solutions.
		*graph = checkpoint.graph;
		*solutions = checkpoint.solutions;

		Ok(())
	}

	async fn checkin_solve_visit_item(
		&self,
		state: &mut State,
		checkpoint: &mut Checkpoint,
		item: Item,
	) -> tg::Result<()> {
		// If the item has been visited, then return.
		if checkpoint.visited.insert(item.clone()).is_some() {
			return Ok(());
		}

		// If the item is solved, then add its destination's items to the queue and return.
		if let Some(destination) = Self::checkin_solve_get_destination_for_item(checkpoint, &item) {
			let destination = match destination {
				tg::graph::data::Edge::Reference(reference) => {
					if let Some(graph_id) = &reference.graph {
						let index = self
							.checkin_solve_add_graph_node(
								checkpoint,
								&item,
								graph_id,
								reference.index,
							)
							.await?;
						let kind = checkpoint.graph.nodes.get(&index).unwrap().variant.kind();
						let node = checkpoint.graph.nodes.get_mut(&item.node).unwrap();
						match &item.variant {
							ItemVariant::DirectoryEntry(name) => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								*node
									.variant
									.unwrap_directory_mut()
									.entries
									.get_mut(name)
									.unwrap() = edge;
							},
							ItemVariant::FileDependency(reference) => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								node.variant
									.unwrap_file_mut()
									.dependencies
									.get_mut(reference)
									.unwrap()
									.get_or_insert_with(|| tg::Referent::with_item(edge.clone()))
									.item = edge.clone();
							},
							ItemVariant::SymlinkArtifact => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								*node.variant.unwrap_symlink_mut().artifact.as_mut().unwrap() =
									edge;
							},
						}
						checkpoint
							.graph
							.nodes
							.get_mut(&index)
							.unwrap()
							.referrers
							.push(item.node);
						Some(index)
					} else {
						Some(reference.index)
					}
				},
				tg::graph::data::Edge::Object(id) => {
					let index = if let Ok(artifact_id) = tg::artifact::Id::try_from(id) {
						Some(
							self.checkin_solve_add_node(checkpoint, &item, &artifact_id)
								.await?,
						)
					} else {
						None
					};
					if let Some(index) = index {
						let kind = checkpoint.graph.nodes.get(&index).unwrap().variant.kind();
						let node = checkpoint.graph.nodes.get_mut(&item.node).unwrap();
						match &item.variant {
							ItemVariant::DirectoryEntry(name) => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								*node
									.variant
									.unwrap_directory_mut()
									.entries
									.get_mut(name)
									.unwrap() = edge;
							},
							ItemVariant::FileDependency(reference) => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								node.variant
									.unwrap_file_mut()
									.dependencies
									.get_mut(reference)
									.unwrap()
									.get_or_insert_with(|| tg::Referent::with_item(edge.clone()))
									.item = edge.clone();
							},
							ItemVariant::SymlinkArtifact => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										index,
										kind,
									});
								*node.variant.unwrap_symlink_mut().artifact.as_mut().unwrap() =
									edge;
							},
						}
						checkpoint
							.graph
							.nodes
							.get_mut(&index)
							.unwrap()
							.referrers
							.push(item.node);
						Some(index)
					} else {
						None
					}
				},
			};
			if let Some(destination) = destination {
				Self::checkin_solve_enqueue_items_for_node(checkpoint, destination);
			}
			return Ok(());
		}

		// Get the reference.
		let reference = item
			.variant
			.try_unwrap_file_dependency_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a file dependency"))?
			.clone();

		let tg::reference::Item::Tag(pattern) = reference.item() else {
			if state.unsolved_dependencies {
				return Ok(());
			}
			return Err(tg::error!(%reference, "expected reference to be a tag"));
		};

		self.checkin_solve_visit_item_with_tag(
			state,
			checkpoint,
			item,
			reference.clone(),
			pattern.clone(),
		)
		.await
	}

	async fn checkin_solve_visit_item_with_tag(
		&self,
		state: &mut State,
		checkpoint: &mut Checkpoint,
		item: Item,
		reference: tg::Reference,
		pattern: tg::tag::Pattern,
	) -> tg::Result<()> {
		// Get the key.
		let key = if pattern
			.components()
			.last()
			.is_some_and(|component| component.contains(['=', '>', '<', '^']))
		{
			let mut key = pattern.parent().unwrap();
			key.push("*");
			key
		} else {
			pattern.clone()
		};

		// Solve the item.
		let output = self
			.checkin_solve_visit_item_with_tag_inner(state, checkpoint, &item, &key, &pattern)
			.await?;

		// Get the referrer.
		let referrer = Referrer {
			node: item.node,
			pattern: Some(pattern),
		};

		// Handle the output.
		match output {
			TagInnerOutput::Solved(referent) => {
				// Checkpoint.
				state.checkpoints.push(checkpoint.clone());

				// Create the edge.
				let kind = checkpoint
					.graph
					.nodes
					.get(&referent.item)
					.unwrap()
					.variant
					.kind();
				checkpoint
					.graph
					.nodes
					.get_mut(&item.node)
					.unwrap()
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(r, referent)| (r == &reference).then_some(referent))
					.unwrap()
					.replace(referent.clone().map(|item| {
						tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							graph: None,
							index: item,
							kind,
						})
					}));
				checkpoint
					.graph
					.nodes
					.get_mut(&referent.item)
					.unwrap()
					.referrers
					.push(item.node);

				// Add the referrer to the solution.
				checkpoint
					.solutions
					.get_mut(&key)
					.unwrap()
					.referrers
					.push(referrer);

				// Enqueue the node's items.
				Self::checkin_solve_enqueue_items_for_node(checkpoint, referent.item);
			},

			TagInnerOutput::Conflicted => {
				// Try to backtrack.
				if let Some(result) = Self::checkin_solve_backtrack(state, &key) {
					*checkpoint = result;
					return Ok(());
				}

				// Get the solution
				let Checkpoint {
					graph, solutions, ..
				} = checkpoint;
				let solution = solutions.get_mut(&key).unwrap();

				// Add the new referrer.
				solution.referrers.push(referrer);

				// If unsolved dependencies is false, then error.
				if !state.unsolved_dependencies {
					let error = Self::checkin_solve_backtrack_error(state, checkpoint, &key);
					return Err(error);
				}

				// Otherwise, remove the edges from the referrers and poison the solution.
				'outer: for referrer in &solution.referrers {
					let node = graph.nodes.get_mut(&referrer.node).unwrap();
					let Variant::File(file) = &mut node.variant else {
						continue;
					};
					for referent in file.dependencies.values_mut() {
						if referent
							.as_ref()
							.and_then(|r| r.tag())
							.is_some_and(|tag| key.matches(tag))
						{
							referent.take();
							continue 'outer;
						}
					}
				}
				solution.referent.take();
			},

			TagInnerOutput::Poisoned => {
				// Add the referrer to the solution.
				checkpoint
					.solutions
					.get_mut(&key)
					.unwrap()
					.referrers
					.push(referrer);
			},
		}
		// Remove the candidates.
		checkpoint.candidates.take();

		Ok(())
	}

	async fn checkin_solve_visit_item_with_tag_inner(
		&self,
		state: &State,
		checkpoint: &mut Checkpoint,
		item: &Item,
		key: &tg::tag::Pattern,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<TagInnerOutput> {
		// Check if the solution exists.
		if let Some(solution) = checkpoint.solutions.get(key) {
			let Some(referent) = &solution.referent else {
				return Ok(TagInnerOutput::Poisoned);
			};
			if !pattern.matches(referent.tag().unwrap()) {
				return Ok(TagInnerOutput::Conflicted);
			}
			return Ok(TagInnerOutput::Solved(referent.clone()));
		}

		// Get the lock candidate if necessary.
		if checkpoint.candidates.is_none() {
			let candidate = Self::checkin_solve_get_lock_candidate(state, checkpoint, item);
			let candidates = candidate.into_iter().collect();
			checkpoint.candidates.replace(candidates);
			checkpoint.listed = false;
		}

		// If there are no candidates left and tags have not been listed yet, then list them.
		if checkpoint.candidates.as_ref().unwrap().is_empty() && !checkpoint.listed {
			let candidates = self
				.checkin_solve_get_tag_candidates(pattern)
				.await
				.map_err(|error| tg::error!(!error, %pattern, "failed to list tags"))?;
			checkpoint.candidates.replace(candidates);
			checkpoint.listed = true;
		}

		// Get the next candidate.
		let candidate = checkpoint
			.candidates
			.as_mut()
			.unwrap()
			.pop_back()
			.ok_or_else(|| {
				tg::error!(
					referrer = %Self::checkin_solve_get_referrer(state, &checkpoint.graph, item.node),
					%pattern,
					"no matching tags were found",
				)
			})?;

		// Try to reuse a node if it exists. Otherwise, create a new node.
		let node = if let Some(node) = candidate.node {
			node
		} else {
			let id = candidate
				.object
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			self.checkin_solve_add_node(checkpoint, item, &id).await?
		};

		// Create the referent.
		let options = tg::referent::Options {
			id: Some(candidate.object),
			tag: Some(candidate.tag),
			..Default::default()
		};
		let referent = tg::Referent::new(node, options);

		let solution = Solution {
			referent: Some(referent.clone()),
			referrers: vec![],
		};

		// Add the solution.
		checkpoint.solutions.insert(key.clone(), solution);

		Ok(TagInnerOutput::Solved(referent))
	}

	fn checkin_solve_get_lock_candidate(
		state: &State,
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<Candidate> {
		let lock_index = checkpoint.graph.nodes.get(&item.node).unwrap().lock_node?;
		let candidate = Self::checkin_solve_get_lock_candidate_inner(checkpoint, item, lock_index)?;
		if state
			.updates
			.iter()
			.any(|pattern| pattern.matches(&candidate.tag))
		{
			return None;
		}
		Some(candidate)
	}

	fn checkin_solve_get_lock_candidate_inner(
		checkpoint: &Checkpoint,
		item: &Item,
		lock_index: usize,
	) -> Option<Candidate> {
		let lock_node = &checkpoint.lock.as_ref().unwrap().nodes[lock_index];
		let referent = if let ItemVariant::FileDependency(reference) = &item.variant {
			lock_node
				.try_unwrap_file_ref()
				.ok()?
				.dependencies
				.get(reference)?
				.as_ref()?
		} else {
			return None;
		};
		let node = if let Some(artifact) = referent.artifact() {
			checkpoint.graph.artifacts.get(artifact).copied()
		} else {
			None
		};
		let object = referent.id().cloned()?;
		let tag = referent.tag().cloned()?;
		let candidate = Candidate { node, object, tag };
		Some(candidate)
	}

	async fn checkin_solve_get_tag_candidates(
		&self,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<im::Vector<Candidate>> {
		let output = self
			.list_tags(tg::tag::list::Arg {
				length: None,
				pattern: pattern.clone(),
				recursive: false,
				remote: None,
				reverse: false,
			})
			.await
			.map_err(|source| tg::error!(!source, %pattern, "failed to list tags"))?;
		let candidates = output
			.data
			.into_iter()
			.filter_map(|output| {
				let object = output.item?.left()?;
				let node = None;
				let tag = output.tag;
				let candidate = Candidate { node, object, tag };
				Some(candidate)
			})
			.collect::<im::Vector<_>>();
		Ok(candidates)
	}

	async fn checkin_solve_add_node(
		&self,
		checkpoint: &mut Checkpoint,
		item: &Item,
		id: &tg::artifact::Id,
	) -> tg::Result<usize> {
		// Load the object and deserialize it.
		let output = self
			.get_object(&id.clone().into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let data = tg::artifact::Data::deserialize(id.kind(), output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

		// Create the checkin graph node.
		let variant = match data {
			tg::artifact::Data::Directory(tg::directory::Data::Reference(reference))
			| tg::artifact::Data::File(tg::file::Data::Reference(reference))
			| tg::artifact::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
				let Some(graph) = reference.graph else {
					return Err(tg::error!("invalid artifact"));
				};
				return self
					.checkin_solve_add_graph_node(checkpoint, item, &graph, reference.index)
					.await;
			},
			tg::artifact::Data::Directory(tg::directory::Data::Node(directory)) => {
				Variant::Directory(Directory {
					entries: directory.entries,
				})
			},
			tg::artifact::Data::File(tg::file::Data::Node(file)) => {
				let contents = if let Some(id) = file.contents {
					let (complete, metadata) = self
						.try_get_object_complete_and_metadata(&id.clone().into())
						.await
						.ok()
						.flatten()
						.unwrap_or((false, tg::object::Metadata::default()));
					Some(Contents::Id {
						id,
						complete,
						metadata: Some(metadata),
					})
				} else {
					None
				};
				let dependencies = file
					.dependencies
					.into_iter()
					.map(|(reference, referent)| {
						if reference.is_solvable() {
							(reference, None)
						} else {
							(reference, referent)
						}
					})
					.collect();
				let executable = file.executable;
				Variant::File(File {
					contents,
					dependencies,
					executable,
				})
			},
			tg::artifact::Data::Symlink(tg::symlink::Data::Node(symlink)) => {
				Variant::Symlink(Symlink {
					artifact: symlink.artifact,
					path: symlink.path,
				})
			},
		};
		let lock_node = Self::checkin_solve_get_lock_node(checkpoint, item);
		let node = Node {
			artifact: None,
			complete: false,
			edge: None,
			id: None,
			lock_node,
			metadata: None,
			path: None,
			path_metadata: None,
			referrers: SmallVec::new(),
			solvable: true,
			solved: false,
			variant,
		};

		// Insert the node into the graph.
		let index = checkpoint.graph.next;
		checkpoint.graph.next += 1;
		checkpoint.graph.nodes.insert(index, Box::new(node));

		Ok(index)
	}

	async fn checkin_solve_add_graph_node(
		&self,
		checkpoint: &mut Checkpoint,
		item: &Item,
		graph_id: &tg::graph::Id,
		node_index: usize,
	) -> tg::Result<usize> {
		// Check if this graph node has already been added.
		let key = (graph_id.clone(), node_index);
		if let Some(index) = checkpoint.graph_nodes.get(&key).copied() {
			return Ok(index);
		}

		// Load the graph data from the cache or fetch it.
		let graph_data = if let Some(cached) = checkpoint.graphs.get(graph_id) {
			cached
		} else {
			let graph = tg::Graph::with_id(graph_id.clone());
			let data = graph
				.data(self)
				.await
				.map_err(|source| tg::error!(!source, "failed to get graph data"))?;
			checkpoint.graphs.insert(graph_id.clone(), data);
			checkpoint.graphs.get(graph_id).unwrap()
		};

		// Get the node.
		let graph_node = graph_data
			.nodes
			.get(node_index)
			.ok_or_else(|| tg::error!("graph node index out of bounds"))?;

		// Create the checkin graph node.
		let variant = match graph_node {
			tg::graph::data::Node::Directory(directory) => {
				let mut entries = std::collections::BTreeMap::new();
				for (name, edge) in &directory.entries {
					let edge = match edge {
						tg::graph::data::Edge::Reference(reference) => {
							let graph = reference.graph.clone().or_else(|| Some(graph_id.clone()));
							tg::graph::data::Edge::Reference(tg::graph::data::Reference {
								graph,
								index: reference.index,
								kind: reference.kind,
							})
						},
						tg::graph::data::Edge::Object(id) => {
							tg::graph::data::Edge::Object(id.clone())
						},
					};
					entries.insert(name.clone(), edge);
				}
				Variant::Directory(Directory { entries })
			},

			tg::graph::data::Node::File(file) => {
				let contents = if let Some(id) = file.contents.clone() {
					let (complete, metadata) = self
						.try_get_object_complete_and_metadata(&id.clone().into())
						.await
						.ok()
						.flatten()
						.unwrap_or((false, tg::object::Metadata::default()));
					Some(Contents::Id {
						id,
						complete,
						metadata: Some(metadata),
					})
				} else {
					None
				};
				let mut dependencies = std::collections::BTreeMap::new();
				for (reference, referent) in &file.dependencies {
					let Some(referent) = referent else {
						if !reference.is_solvable() {
							return Err(tg::error!(%reference, "unsolvable unsolved dependency"));
						}
						dependencies.insert(reference.clone(), None);
						continue;
					};
					if referent.tag().is_some() {
						dependencies.insert(reference.clone(), None);
					} else {
						let referent = referent.clone().map(|item| match item {
							tg::graph::data::Edge::Reference(reference) => {
								let graph =
									reference.graph.clone().or_else(|| Some(graph_id.clone()));
								tg::graph::data::Edge::Reference(tg::graph::data::Reference {
									graph,
									index: reference.index,
									kind: reference.kind,
								})
							},
							tg::graph::data::Edge::Object(id) => {
								tg::graph::data::Edge::Object(id.clone())
							},
						});
						dependencies.insert(reference.clone(), Some(referent));
					}
				}
				Variant::File(File {
					contents,
					dependencies,
					executable: file.executable,
				})
			},

			tg::graph::data::Node::Symlink(symlink) => {
				let artifact = symlink.artifact.as_ref().map(|edge| match edge {
					tg::graph::data::Edge::Reference(reference) => {
						let graph = reference.graph.clone().or_else(|| Some(graph_id.clone()));
						tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							graph,
							index: reference.index,
							kind: reference.kind,
						})
					},
					tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.clone()),
				});
				Variant::Symlink(Symlink {
					artifact,
					path: symlink.path.clone(),
				})
			},
		};
		let lock_node = Self::checkin_solve_get_lock_node(checkpoint, item);
		let node = Node {
			artifact: None,
			complete: false,
			edge: None,
			id: None,
			lock_node,
			metadata: None,
			path: None,
			path_metadata: None,
			referrers: SmallVec::new(),
			solvable: true,
			solved: false,
			variant,
		};

		// Add the node to the checkin graph.
		let index = checkpoint.graph.next;
		checkpoint.graph.next += 1;
		checkpoint.graph.nodes.insert(index, Box::new(node));

		// Cache the mapping.
		checkpoint.graph_nodes.insert(key, index);

		Ok(index)
	}

	fn checkin_solve_get_lock_node(checkpoint: &Checkpoint, item: &Item) -> Option<usize> {
		let Some(lock) = &checkpoint.lock else {
			return None;
		};
		let parent_index = checkpoint.graph.nodes.get(&item.node).unwrap().lock_node?;
		let parent_node = lock.nodes.get(parent_index).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(name)?
					.try_unwrap_reference_ref()
					.ok()?
					.index,
			),
			ItemVariant::FileDependency(reference) => Some(
				parent_node
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(reference)?
					.as_ref()?
					.item()
					.try_unwrap_reference_ref()
					.ok()?
					.index,
			),
			ItemVariant::SymlinkArtifact => Some(
				parent_node
					.try_unwrap_symlink_ref()
					.ok()?
					.artifact
					.as_ref()?
					.try_unwrap_reference_ref()
					.ok()?
					.index,
			),
		}
	}

	fn checkin_solve_enqueue_items_for_node(checkpoint: &mut Checkpoint, index: usize) {
		// Get the node.
		let node = checkpoint.graph.nodes.get(&index).unwrap();

		// If the node is solved, then do not enqueue any of its items.
		if node.solved {
			return;
		}

		// Enqueue the node's items.
		match &node.variant {
			Variant::Directory(directory) => {
				let items = directory.entries.keys().map(|name| Item {
					node: index,
					variant: ItemVariant::DirectoryEntry(name.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::File(file) => {
				let items = file.dependencies.keys().map(|reference| Item {
					node: index,
					variant: ItemVariant::FileDependency(reference.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::Symlink(symlink) => {
				let items = symlink.artifact.iter().map(|_| Item {
					node: index,
					variant: ItemVariant::SymlinkArtifact,
				});
				checkpoint.queue.extend(items);
			},
		}
	}

	fn checkin_solve_get_destination_for_item(
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<tg::graph::data::Edge<tg::object::Id>> {
		let node = checkpoint.graph.nodes.get(&item.node).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => {
				let directory = node.variant.unwrap_directory_ref();
				directory.entries.get(name).cloned().map(|edge| match edge {
					tg::graph::data::Edge::Reference(reference) => {
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.into()),
				})
			},
			ItemVariant::FileDependency(reference) => {
				let file = node.variant.unwrap_file_ref();
				file.dependencies
					.get(reference)
					.cloned()
					.unwrap()
					.map(|referent| referent.item)
			},
			ItemVariant::SymlinkArtifact => {
				let symlink = node.variant.unwrap_symlink_ref();
				symlink.artifact.clone().map(|edge| match edge {
					tg::graph::data::Edge::Reference(reference) => {
						tg::graph::data::Edge::Reference(reference)
					},
					tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.into()),
				})
			},
		}
	}

	fn checkin_solve_backtrack(state: &mut State, key: &tg::tag::Pattern) -> Option<Checkpoint> {
		let position = state
			.checkpoints
			.iter()
			.position(|checkpoint| checkpoint.solutions.contains_key(key))?;
		if state.checkpoints[position]
			.candidates
			.as_ref()
			.unwrap()
			.is_empty()
		{
			return None;
		}
		state.checkpoints.truncate(position);
		let mut checkpoint = state.checkpoints.pop()?;
		checkpoint.solutions.remove(key);
		Some(checkpoint)
	}

	fn checkin_solve_backtrack_error(
		state: &State,
		checkpoint: &Checkpoint,
		key: &tg::tag::Pattern,
	) -> tg::Error {
		let mut message = format!("failed to solve {key}");
		if let Some(solution) = checkpoint.solutions.get(key) {
			for referrer in &solution.referrers {
				let reference =
					Self::checkin_solve_get_referrer(state, &checkpoint.graph, referrer.node);
				write!(message, "\ndepended on by {reference}").unwrap();
				if let Some(pattern) = &referrer.pattern {
					write!(message, " with pattern {pattern}").unwrap();
				}
			}
		}
		tg::Error {
			message: Some(message),
			..Default::default()
		}
	}

	fn checkin_solve_get_referrer(state: &State, graph: &Graph, node: usize) -> String {
		let mut tag = None;
		let mut id = None;
		let mut components = vec![];
		let mut current = node;
		while tag.is_none() && id.is_none() {
			let Some(parent) = graph
				.nodes
				.get(&current)
				.and_then(|node| node.referrers.first().copied())
			else {
				break;
			};
			match &graph.nodes.get(&parent).unwrap().variant {
				Variant::Directory(directory) => {
					let name = directory
						.entries
						.iter()
						.find_map(|(name, edge)| {
							let reference = edge.try_unwrap_reference_ref().ok()?;
							if reference.graph.is_some() {
								return None;
							}
							(reference.index == current).then_some(name.clone())
						})
						.unwrap();
					components.push(name);
				},
				Variant::File(file) => {
					let referent = file
						.dependencies
						.values()
						.flatten()
						.find_map(|referent| {
							let reference = referent.item.try_unwrap_reference_ref().ok()?;
							if reference.graph.is_some() {
								return None;
							}
							(reference.index == current).then_some(referent)
						})
						.unwrap();

					if let Some(path) = referent.path() {
						components.push(path.display().to_string());
					}
					if let Some(tag_) = referent.tag() {
						tag.replace(tag_.clone());
					}

					if let Some(id_) = referent.id() {
						id.replace(id_.clone());
					}
				},
				Variant::Symlink(symlink) => {
					let Some(path) = &symlink.path else {
						break;
					};
					components.push(path.display().to_string());
				},
			}
			current = parent;
		}
		components.reverse();
		let path = components.join("/");
		let path = if path.is_empty() { None } else { Some(path) };

		if let Some(tag) = tag {
			let mut reference = tag.to_string();
			if let Some(path) = path {
				write!(reference, "?path={path}").unwrap();
			}
			reference
		} else if let Some(id) = id {
			let mut reference = id.to_string();
			if let Some(path) = path {
				write!(reference, "?path={path}").unwrap();
			}
			reference
		} else {
			let mut reference = state.root.clone();
			if let Some(path) = path {
				reference.push(path);
			}
			reference.to_string_lossy().into_owned()
		}
	}
}
