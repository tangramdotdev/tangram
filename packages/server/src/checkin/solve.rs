use {
	crate::{
		Server,
		checkin::graph::{Contents, Directory, File, Graph, Node, Symlink, Variant},
	},
	dashmap::DashMap,
	smallvec::SmallVec,
	std::{
		fmt::Write as _,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

const PREFETCH_CONCURRENCY: usize = 16;

struct State<'a> {
	arg: &'a tg::checkin::Arg,
	checkpoints: Vec<Checkpoint>,
	prefetch: Prefetch,
	root: PathBuf,
}

type Objects = Arc<DashMap<tg::object::Id, tg::object::get::Output, tg::id::BuildHasher>>;

type ObjectTasks = tangram_futures::task::Map<
	tg::object::Id,
	tg::Result<tg::object::get::Output>,
	(),
	tg::id::BuildHasher,
>;

type Tags = Arc<DashMap<tg::tag::Pattern, tg::tag::list::Output, fnv::FnvBuildHasher>>;

type TagTasks = tangram_futures::task::Map<
	tg::tag::Pattern,
	tg::Result<tg::tag::list::Output>,
	(),
	fnv::FnvBuildHasher,
>;

#[derive(Clone)]
struct Prefetch {
	object_tasks: ObjectTasks,
	objects: Objects,
	semaphore: Arc<tokio::sync::Semaphore>,
	tag_tasks: TagTasks,
	tags: Tags,
}

#[derive(Clone)]
struct Checkpoint {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	graphs: Graphs,
	graph_nodes: GraphNodes,
	listed: bool,
	queue: im::Vector<Item>,
	lock: Option<Arc<tg::graph::Data>>,
	solutions: Solutions,
	visited: im::HashSet<Item, fnv::FnvBuildHasher>,
}

type Graphs = im::HashMap<
	tg::graph::Id,
	(tg::graph::Data, Option<tg::object::Metadata>),
	tg::id::BuildHasher,
>;

type GraphNodes = im::HashMap<(tg::graph::Id, usize), usize, fnv::FnvBuildHasher>;

#[derive(Clone, Debug)]
struct Candidate {
	index: Option<usize>,
	object: tg::object::Id,
	tag: tg::Tag,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Item {
	referent: tg::Referent<usize>,
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

#[derive(Clone, Default)]
#[expect(clippy::struct_field_names)]
pub struct Solutions {
	solutions: im::HashMap<tg::tag::Pattern, Solution, fnv::FnvBuildHasher>,
	referents:
		im::HashMap<usize, im::HashSet<tg::tag::Pattern, fnv::FnvBuildHasher>, fnv::FnvBuildHasher>,
	referrers:
		im::HashMap<usize, im::HashSet<tg::tag::Pattern, fnv::FnvBuildHasher>, fnv::FnvBuildHasher>,
}

#[derive(Clone)]
pub struct Solution {
	pub referent: Option<tg::Referent<usize>>,
	pub referrers: Vec<Referrer>,
}

enum TagInnerOutput {
	Solved(tg::Referent<usize>),
	Conflicted,
	Unsolved,
}

#[derive(Clone)]
pub struct Referrer {
	pub index: usize,
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
		progress: &crate::progress::Handle<super::TaskOutput>,
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
			arg,
			checkpoints: Vec::new(),
			prefetch: Prefetch {
				object_tasks: tangram_futures::task::Map::default(),
				objects: Arc::new(DashMap::default()),
				semaphore: Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY)),
				tag_tasks: tangram_futures::task::Map::default(),
				tags: Arc::new(DashMap::default()),
			},
			root: root.to_owned(),
		};

		// Prefetch from the lock.
		if let Some(lock) = &lock {
			self.checkin_solve_prefetch_from_lock(&state.prefetch, lock);
		}

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
		let referent = tg::Referent::with_item(*index);
		Self::checkin_solve_enqueue_items_for_node(&mut checkpoint, &referent);

		// Solve.
		while let Some(item) = checkpoint.queue.pop_front() {
			self.checkin_solve_item(&mut state, &mut checkpoint, item)
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

	async fn checkin_solve_item(
		&self,
		state: &mut State<'_>,
		checkpoint: &mut Checkpoint,
		item: Item,
	) -> tg::Result<()> {
		// If the item has been visited, then return.
		if checkpoint.visited.insert(item.clone()).is_some() {
			return Ok(());
		}

		// If the item is solved, then add the node if necessary, create the edge, and enqueue the node's items.
		if let Some(edge) = Self::checkin_solve_get_solved_edge_for_item(checkpoint, &item) {
			let index = match edge {
				tg::graph::data::Edge::Pointer(pointer) => {
					if let Some(graph_id) = &pointer.graph {
						let index = self
							.checkin_solve_add_graph_node(
								state,
								checkpoint,
								&item,
								graph_id,
								pointer.index,
							)
							.await?;
						Self::checkin_create_edge_for_item(checkpoint, &item, index);
						Some(index)
					} else {
						Some(pointer.index)
					}
				},
				tg::graph::data::Edge::Object(id) => {
					let index = if let Ok(id) = tg::artifact::Id::try_from(id) {
						Some(
							self.checkin_solve_add_node(state, checkpoint, &item, &id)
								.await?,
						)
					} else {
						None
					};
					if let Some(index) = index {
						Self::checkin_create_edge_for_item(checkpoint, &item, index);
					}
					index
				},
			};
			if let Some(index) = index {
				let referent = tg::Referent::with_item(index);
				Self::checkin_solve_enqueue_items_for_node(checkpoint, &referent);
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
			if state.arg.options.unsolved_dependencies {
				return Ok(());
			}
			return Err(tg::error!(%reference, "expected reference to be a tag"));
		};

		self.checkin_solve_item_with_tag(
			state,
			checkpoint,
			item,
			reference.clone(),
			pattern.clone(),
		)
		.await
	}

	fn checkin_create_edge_for_item(checkpoint: &mut Checkpoint, item: &Item, index: usize) {
		let kind = checkpoint.graph.nodes.get(&index).unwrap().variant.kind();
		let node = checkpoint.graph.nodes.get_mut(&item.referent.item).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => {
				let edge = tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
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
				let edge = tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				});
				node.variant
					.unwrap_file_mut()
					.dependencies
					.get_mut(reference)
					.unwrap()
					.get_or_insert_with(|| {
						tg::graph::data::Dependency(tg::Referent::with_item(Some(edge.clone())))
					})
					.0
					.item = Some(edge.clone());
			},
			ItemVariant::SymlinkArtifact => {
				let edge = tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
					graph: None,
					index,
					kind,
				});
				*node.variant.unwrap_symlink_mut().artifact.as_mut().unwrap() = edge;
			},
		}
		checkpoint
			.graph
			.nodes
			.get_mut(&index)
			.unwrap()
			.referrers
			.push(item.referent.item);
	}

	async fn checkin_solve_item_with_tag(
		&self,
		state: &mut State<'_>,
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
			.checkin_solve_item_with_tag_inner(state, checkpoint, &item, &key, &pattern)
			.await?;

		// Get the referrer.
		let referrer = Referrer {
			index: item.referent.item,
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
					.get_mut(&item.referent.item)
					.unwrap()
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(r, option)| (r == &reference).then_some(option))
					.unwrap()
					.replace(tg::graph::data::Dependency(referent.clone().map(|item| {
						Some(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
							graph: None,
							index: item,
							kind,
						}))
					})));
				checkpoint
					.graph
					.nodes
					.get_mut(&referent.item)
					.unwrap()
					.referrers
					.push(item.referent.item);

				// Add the referrer to the solution.
				checkpoint.solutions.add_referrer(&key, referrer);

				// Enqueue the node's items.
				Self::checkin_solve_enqueue_items_for_node(checkpoint, &referent);
			},

			TagInnerOutput::Conflicted => {
				// Try to backtrack.
				if let Some(result) = Self::checkin_solve_backtrack(state, &key) {
					*checkpoint = result;
					return Ok(());
				}

				// Add the new referrer.
				checkpoint.solutions.add_referrer(&key, referrer);

				// If unsolved dependencies is false, then error.
				if !state.arg.options.unsolved_dependencies {
					let error = Self::checkin_solve_backtrack_error(state, checkpoint, &key);
					return Err(error);
				}

				// Otherwise, remove the edges from the referrers and remove the solution's referent.
				let referrers = checkpoint.solutions.get(&key).unwrap().referrers.clone();
				'outer: for referrer in &referrers {
					let node = checkpoint.graph.nodes.get_mut(&referrer.index).unwrap();
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
				checkpoint.solutions.clear_referent(&key);
			},

			TagInnerOutput::Unsolved => {
				// Add the referrer to the solution.
				checkpoint.solutions.add_referrer(&key, referrer);
			},
		}
		// Remove the candidates.
		checkpoint.candidates.take();

		Ok(())
	}

	async fn checkin_solve_item_with_tag_inner(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		item: &Item,
		key: &tg::tag::Pattern,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<TagInnerOutput> {
		// Check if a solution exists for the key.
		if let Some(solution) = checkpoint.solutions.get(key) {
			let Some(referent) = &solution.referent else {
				return Ok(TagInnerOutput::Unsolved);
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
				.checkin_solve_get_tag_candidates(state, pattern)
				.await
				.map_err(|error| tg::error!(!error, %pattern, "failed to list tags"))?;
			checkpoint.candidates.replace(candidates);
			checkpoint.listed = true;
		}

		// Get the next candidate.
		let Some(candidate) = checkpoint.candidates.as_mut().unwrap().pop_back() else {
			if state.arg.options.unsolved_dependencies {
				let solution = Solution {
					referent: None,
					referrers: vec![],
				};
				checkpoint.solutions.insert(key.clone(), solution);
				return Ok(TagInnerOutput::Unsolved);
			}
			return Err(tg::error!(
				referrer = %Self::checkin_solve_get_referrer(state, &checkpoint.graph, item.referent.item),
				%pattern,
				"no matching tags were found",
			));
		};

		// Try to reuse a node if it exists. Otherwise, create a new node.
		let node = if let Some(node) = candidate.index {
			node
		} else {
			let id = candidate
				.object
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			self.checkin_solve_add_node(state, checkpoint, item, &id)
				.await?
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
		state: &State<'_>,
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<Candidate> {
		let lock_index = checkpoint
			.graph
			.nodes
			.get(&item.referent.item)
			.unwrap()
			.lock_node?;
		let candidate = Self::checkin_solve_get_lock_candidate_inner(checkpoint, item, lock_index)?;
		if state
			.arg
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
		let index = if let Some(artifact) = referent.artifact() {
			checkpoint.graph.artifacts.get(artifact).copied()
		} else {
			None
		};
		let object = referent.id().cloned()?;
		let tag = referent.tag().cloned()?;
		let candidate = Candidate { index, object, tag };
		Some(candidate)
	}

	async fn checkin_solve_get_tag_candidates(
		&self,
		state: &State<'_>,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<im::Vector<Candidate>> {
		let output = self
			.checkin_solve_list_tags(&state.prefetch, pattern)
			.await?;

		let candidates = output
			.data
			.into_iter()
			.filter_map(|output| {
				let object = output.item?.left()?;
				let index = None;
				let tag = output.tag;
				let candidate = Candidate { index, object, tag };
				Some(candidate)
			})
			.collect::<im::Vector<_>>();

		Ok(candidates)
	}

	async fn checkin_solve_add_node(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		item: &Item,
		id: &tg::artifact::Id,
	) -> tg::Result<usize> {
		// Get the object.
		let output = self
			.checkin_solve_get_object(&state.prefetch, &id.clone().into())
			.await?;
		let data = tg::artifact::Data::deserialize(id.kind(), output.bytes.clone())
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

		// Create the checkin graph node.
		let variant = match data {
			tg::artifact::Data::Directory(tg::directory::Data::Pointer(pointer))
			| tg::artifact::Data::File(tg::file::Data::Pointer(pointer))
			| tg::artifact::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => {
				let Some(graph) = pointer.graph else {
					return Err(tg::error!("invalid artifact"));
				};
				return self
					.checkin_solve_add_graph_node(state, checkpoint, item, &graph, pointer.index)
					.await;
			},
			tg::artifact::Data::Directory(tg::directory::Data::Node(directory)) => {
				Variant::Directory(Directory {
					entries: directory.entries,
				})
			},
			tg::artifact::Data::File(tg::file::Data::Node(file)) => {
				let contents = if let Some(id) = file.contents {
					let (stored, metadata) = self
						.try_get_object_stored_and_metadata(&id.clone().into())
						.await
						.ok()
						.flatten()
						.unwrap_or((
							crate::object::stored::Output::default(),
							tg::object::Metadata::default(),
						));
					Some(Contents::Id {
						id,
						stored: stored.clone(),
						metadata: Some(metadata),
					})
				} else {
					None
				};
				let dependencies = file
					.dependencies
					.into_iter()
					.map(|(reference, option)| {
						if reference.is_solvable() {
							(reference, None)
						} else {
							(reference, option)
						}
					})
					.collect();
				let executable = file.executable;
				let module = file.module;
				Variant::File(File {
					contents,
					dependencies,
					executable,
					module,
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
			edge: None,
			id: None,
			lock_node,
			metadata: None,
			path: None,
			path_metadata: None,
			referrers: SmallVec::new(),
			solvable: output
				.metadata
				.as_ref()
				.and_then(|metadata| metadata.subtree.solvable)
				.unwrap_or(true),
			solved: false,
			stored: crate::object::stored::Output::default(),
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
		state: &State<'_>,
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
		let (graph_data, graph_metadata) = if let Some(cached) = checkpoint.graphs.get(graph_id) {
			cached
		} else {
			let output = self
				.checkin_solve_get_object(&state.prefetch, &graph_id.clone().into())
				.await?;
			let data = tg::graph::Data::deserialize(output.bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
			checkpoint
				.graphs
				.insert(graph_id.clone(), (data, output.metadata));
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
						tg::graph::data::Edge::Pointer(pointer) => {
							let graph = pointer.graph.clone().or_else(|| Some(graph_id.clone()));
							tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
								graph,
								index: pointer.index,
								kind: pointer.kind,
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
					let (stored, metadata) = self
						.try_get_object_stored_and_metadata(&id.clone().into())
						.await
						.ok()
						.flatten()
						.unwrap_or((
							crate::object::stored::Output::default(),
							tg::object::Metadata::default(),
						));
					Some(Contents::Id {
						id,
						stored: stored.clone(),
						metadata: Some(metadata),
					})
				} else {
					None
				};
				let mut dependencies = std::collections::BTreeMap::new();
				for (reference, option) in &file.dependencies {
					let Some(dependency) = option else {
						if !reference.is_solvable() {
							return Err(tg::error!(%reference, "unsolvable unsolved dependency"));
						}
						dependencies.insert(reference.clone(), None);
						continue;
					};
					if dependency.tag().is_some() {
						dependencies.insert(reference.clone(), None);
					} else {
						let referent = dependency.0.clone().map(|item| match item {
							Some(tg::graph::data::Edge::Pointer(pointer)) => {
								let graph =
									pointer.graph.clone().or_else(|| Some(graph_id.clone()));
								Some(tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
									graph,
									index: pointer.index,
									kind: pointer.kind,
								}))
							},
							Some(tg::graph::data::Edge::Object(id)) => {
								Some(tg::graph::data::Edge::Object(id.clone()))
							},
							None => None,
						});
						dependencies.insert(
							reference.clone(),
							Some(tg::graph::data::Dependency(referent)),
						);
					}
				}
				Variant::File(File {
					contents,
					dependencies,
					executable: file.executable,
					module: file.module,
				})
			},

			tg::graph::data::Node::Symlink(symlink) => {
				let artifact = symlink.artifact.as_ref().map(|edge| match edge {
					tg::graph::data::Edge::Pointer(pointer) => {
						let graph = pointer.graph.clone().or_else(|| Some(graph_id.clone()));
						tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
							graph,
							index: pointer.index,
							kind: pointer.kind,
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
			edge: None,
			id: None,
			lock_node,
			metadata: None,
			path: None,
			path_metadata: None,
			referrers: SmallVec::new(),
			solvable: graph_metadata
				.as_ref()
				.and_then(|metadata| metadata.subtree.solvable)
				.unwrap_or(true),
			solved: false,
			stored: crate::object::stored::Output::default(),
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
		let parent_index = checkpoint
			.graph
			.nodes
			.get(&item.referent.item)
			.unwrap()
			.lock_node?;
		let parent_node = lock.nodes.get(parent_index).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(name)?
					.try_unwrap_pointer_ref()
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
					.as_ref()?
					.try_unwrap_pointer_ref()
					.ok()?
					.index,
			),
			ItemVariant::SymlinkArtifact => Some(
				parent_node
					.try_unwrap_symlink_ref()
					.ok()?
					.artifact
					.as_ref()?
					.try_unwrap_pointer_ref()
					.ok()?
					.index,
			),
		}
	}

	fn checkin_solve_enqueue_items_for_node(
		checkpoint: &mut Checkpoint,
		referent: &tg::Referent<usize>,
	) {
		// Get the node.
		let node = checkpoint.graph.nodes.get(&referent.item).unwrap();

		// If the node is not solvable or is solved, then do not enqueue any of its items.
		if !node.solvable || node.solved {
			return;
		}

		// Enqueue the node's items.
		match &node.variant {
			Variant::Directory(directory) => {
				let items = directory.entries.keys().map(|name| Item {
					referent: referent.clone(),
					variant: ItemVariant::DirectoryEntry(name.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::File(file) => {
				let items = file.dependencies.keys().map(|reference| Item {
					referent: referent.clone(),
					variant: ItemVariant::FileDependency(reference.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::Symlink(symlink) => {
				let items = symlink.artifact.iter().map(|_| Item {
					referent: referent.clone(),
					variant: ItemVariant::SymlinkArtifact,
				});
				checkpoint.queue.extend(items);
			},
		}
	}

	fn checkin_solve_get_solved_edge_for_item(
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<tg::graph::data::Edge<tg::object::Id>> {
		let node = checkpoint.graph.nodes.get(&item.referent.item).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => {
				let directory = node.variant.unwrap_directory_ref();
				directory.entries.get(name).cloned().map(|edge| match edge {
					tg::graph::data::Edge::Pointer(pointer) => {
						tg::graph::data::Edge::Pointer(pointer)
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
					.and_then(|dependency| dependency.0.item)
			},
			ItemVariant::SymlinkArtifact => {
				let symlink = node.variant.unwrap_symlink_ref();
				symlink.artifact.clone().map(|edge| match edge {
					tg::graph::data::Edge::Pointer(pointer) => {
						tg::graph::data::Edge::Pointer(pointer)
					},
					tg::graph::data::Edge::Object(id) => tg::graph::data::Edge::Object(id.into()),
				})
			},
		}
	}

	fn checkin_solve_backtrack(
		state: &mut State<'_>,
		key: &tg::tag::Pattern,
	) -> Option<Checkpoint> {
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
					Self::checkin_solve_get_referrer(state, &checkpoint.graph, referrer.index);
				write!(message, "\ndepended on by {reference}").unwrap();
				if let Some(pattern) = &referrer.pattern {
					write!(message, " with pattern {pattern}").unwrap();
				}
			}
		}
		tg::Error::with_object(tg::error::Object {
			message: Some(message),
			..Default::default()
		})
	}

	fn checkin_solve_get_referrer(state: &State<'_>, graph: &Graph, node: usize) -> String {
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
							let pointer = edge.try_unwrap_pointer_ref().ok()?;
							if pointer.graph.is_some() {
								return None;
							}
							(pointer.index == current).then_some(name.clone())
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
							let pointer = referent.item.as_ref()?.try_unwrap_pointer_ref().ok()?;
							if pointer.graph.is_some() {
								return None;
							}
							(pointer.index == current).then_some(referent)
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

	fn checkin_solve_prefetch_from_lock(&self, prefetch: &Prefetch, lock: &tg::graph::Data) {
		for node in &lock.nodes {
			self.checkin_solve_prefetch_from_graph_node(prefetch, node);
			if let tg::graph::data::Node::File(file) = node {
				for dependency in file.dependencies.values().flatten() {
					if let Some(id) = dependency.id() {
						self.checkin_solve_get_or_spawn_object_task(prefetch, id);
					}
				}
			}
		}
	}

	async fn checkin_solve_get_object(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
	) -> tg::Result<tg::object::get::Output> {
		if let Some(output) = prefetch.objects.get(id).map(|value| value.clone()) {
			return Ok(output);
		}
		let task = self.checkin_solve_get_or_spawn_object_task(prefetch, id);
		let output = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the object task panicked"))??;
		Ok(output)
	}

	fn checkin_solve_get_or_spawn_object_task(
		&self,
		prefetch: &Prefetch,
		id: &tg::object::Id,
	) -> tangram_futures::task::Shared<tg::Result<tg::object::get::Output>, ()> {
		prefetch.object_tasks.get_or_spawn(id.clone(), {
			let server = self.clone();
			let id = id.clone();
			let prefetch = prefetch.clone();
			move |_| async move {
				// Acquire a permit to limit concurrent requests.
				let permit = prefetch.semaphore.acquire().await;

				// Get the object.
				let arg = tg::object::get::Arg {
					metadata: true,
					..Default::default()
				};
				let output = server
					.get_object(&id, arg)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
				let data = tg::object::Data::deserialize(id.kind(), output.bytes.clone())
					.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;

				// Drop the permit.
				drop(permit);

				// If the object is solvable, then spawn tasks to prefetch its descendant objects and tags.
				let solvable = output
					.metadata
					.as_ref()
					.and_then(|metadata| metadata.subtree.solvable)
					.unwrap_or(true);
				if solvable {
					match &data {
						tg::object::Data::Directory(tg::directory::Data::Pointer(pointer))
						| tg::object::Data::File(tg::file::Data::Pointer(pointer))
						| tg::object::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => {
							if let Some(graph_id) = &pointer.graph {
								server.checkin_solve_get_or_spawn_object_task(
									&prefetch,
									&graph_id.clone().into(),
								);
							}
						},

						tg::object::Data::Directory(tg::directory::Data::Node(directory)) => {
							let node = tg::graph::data::Node::Directory(directory.clone());
							server.checkin_solve_prefetch_from_graph_node(&prefetch, &node);
						},
						tg::object::Data::File(tg::file::Data::Node(file)) => {
							let node = tg::graph::data::Node::File(file.clone());
							server.checkin_solve_prefetch_from_graph_node(&prefetch, &node);
						},
						tg::object::Data::Symlink(tg::symlink::Data::Node(symlink)) => {
							let node = tg::graph::data::Node::Symlink(symlink.clone());
							server.checkin_solve_prefetch_from_graph_node(&prefetch, &node);
						},

						tg::object::Data::Graph(graph) => {
							for node in &graph.nodes {
								server.checkin_solve_prefetch_from_graph_node(&prefetch, node);
							}
						},

						_ => {},
					}
				}

				prefetch.objects.insert(id, output.clone());

				Ok(output)
			}
		})
	}

	fn checkin_solve_prefetch_from_graph_node(
		&self,
		prefetch: &Prefetch,
		node: &tg::graph::data::Node,
	) {
		match node {
			tg::graph::data::Node::Directory(directory) => {
				for edge in directory.entries.values() {
					self.checkin_solve_prefetch_from_artifact_edge(prefetch, edge);
				}
			},
			tg::graph::data::Node::File(file) => {
				for reference in file.dependencies.keys() {
					if let tg::reference::Item::Tag(pattern) = reference.item() {
						self.checkin_solve_get_or_spawn_tag_task(prefetch, pattern);
					}
				}
				for dependency in file.dependencies.values() {
					if let Some(dependency) = dependency
						&& let Some(edge) = &dependency.0.item
					{
						self.checkin_solve_prefetch_from_object_edge(prefetch, edge);
					}
				}
			},
			tg::graph::data::Node::Symlink(symlink) => {
				if let Some(edge) = &symlink.artifact {
					self.checkin_solve_prefetch_from_artifact_edge(prefetch, edge);
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_artifact_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, &id.clone().into());
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(prefetch, &graph_id.clone().into());
				}
			},
		}
	}

	fn checkin_solve_prefetch_from_object_edge(
		&self,
		prefetch: &Prefetch,
		edge: &tg::graph::data::Edge<tg::object::Id>,
	) {
		match edge {
			tg::graph::data::Edge::Object(id) => {
				self.checkin_solve_get_or_spawn_object_task(prefetch, id);
			},
			tg::graph::data::Edge::Pointer(pointer) => {
				if let Some(graph_id) = &pointer.graph {
					self.checkin_solve_get_or_spawn_object_task(prefetch, &graph_id.clone().into());
				}
			},
		}
	}

	async fn checkin_solve_list_tags(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<tg::tag::list::Output> {
		if let Some(output) = prefetch.tags.get(pattern).map(|value| value.clone()) {
			return Ok(output);
		}
		let task = self.checkin_solve_get_or_spawn_tag_task(prefetch, pattern);
		let output = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the tag task panicked"))??;
		Ok(output)
	}

	fn checkin_solve_get_or_spawn_tag_task(
		&self,
		prefetch: &Prefetch,
		pattern: &tg::tag::Pattern,
	) -> tangram_futures::task::Shared<tg::Result<tg::tag::list::Output>, ()> {
		prefetch.tag_tasks.get_or_spawn(pattern.clone(), {
			let server = self.clone();
			let pattern = pattern.clone();
			let prefetch = prefetch.clone();
			move |_| async move {
				// Acquire a permit to limit concurrent requests.
				let permit = prefetch.semaphore.acquire().await;

				// List tags.
				let output = server
					.list_tags(tg::tag::list::Arg {
						length: None,
						local: None,
						pattern: pattern.clone(),
						recursive: false,
						remotes: None,
						reverse: false,
					})
					.await
					.map_err(|source| tg::error!(!source, %pattern, "failed to list tags"))?;

				// Drop the permit.
				drop(permit);

				// Prefetch the first candidate's object.
				if let Some(output) = output.data.last()
					&& let Some(id) = output.item.as_ref().and_then(|item| item.as_ref().left())
				{
					server.checkin_solve_get_or_spawn_object_task(&prefetch, id);
				}

				prefetch.tags.insert(pattern, output.clone());

				Ok(output)
			}
		})
	}
}

impl Solutions {
	pub fn is_empty(&self) -> bool {
		self.solutions.is_empty()
	}

	pub fn get(&self, key: &tg::tag::Pattern) -> Option<&Solution> {
		self.solutions.get(key)
	}

	pub fn contains_key(&self, key: &tg::tag::Pattern) -> bool {
		self.solutions.contains_key(key)
	}

	pub fn insert(&mut self, key: tg::tag::Pattern, solution: Solution) {
		if let Some(existing) = self.solutions.get(&key)
			&& let Some(referent) = &existing.referent
			&& let Some(patterns) = self.referents.get_mut(&referent.item)
		{
			patterns.remove(&key);
			if patterns.is_empty() {
				self.referents.remove(&referent.item);
			}
		}
		if let Some(referent) = &solution.referent {
			self.referents
				.entry(referent.item)
				.or_default()
				.insert(key.clone());
		}
		self.solutions.insert(key, solution);
	}

	pub fn remove(&mut self, key: &tg::tag::Pattern) -> Option<Solution> {
		let solution = self.solutions.remove(key)?;
		if let Some(referent) = &solution.referent
			&& let Some(patterns) = self.referents.get_mut(&referent.item)
		{
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(&referent.item);
			}
		}
		for referrer in &solution.referrers {
			if let Some(patterns) = self.referrers.get_mut(&referrer.index) {
				patterns.remove(key);
				if patterns.is_empty() {
					self.referrers.remove(&referrer.index);
				}
			}
		}
		Some(solution)
	}

	pub fn clear(&mut self) {
		self.solutions.clear();
		self.referents.clear();
		self.referrers.clear();
	}

	pub fn remove_by_node(&mut self, node: usize) {
		if let Some(patterns) = self.referents.remove(&node) {
			for pattern in &patterns {
				if let Some(solution) = self.solutions.remove(pattern) {
					for referrer in &solution.referrers {
						if let Some(referrer_patterns) = self.referrers.get_mut(&referrer.index) {
							referrer_patterns.remove(pattern);
							if referrer_patterns.is_empty() {
								self.referrers.remove(&referrer.index);
							}
						}
					}
				}
			}
		}

		if let Some(patterns) = self.referrers.remove(&node) {
			let mut to_remove = Vec::new();
			for pattern in patterns {
				if let Some(solution) = self.solutions.get_mut(&pattern) {
					solution.referrers.retain(|r| r.index != node);
					if solution.referrers.is_empty() {
						to_remove.push(pattern);
					}
				}
			}
			for pattern in to_remove {
				if let Some(solution) = self.solutions.remove(&pattern)
					&& let Some(referent) = &solution.referent
					&& let Some(referent_patterns) = self.referents.get_mut(&referent.item)
				{
					referent_patterns.remove(&pattern);
					if referent_patterns.is_empty() {
						self.referents.remove(&referent.item);
					}
				}
			}
		}
	}

	pub fn clear_referent(&mut self, key: &tg::tag::Pattern) {
		if let Some(solution) = self.solutions.get_mut(key)
			&& let Some(referent) = solution.referent.take()
			&& let Some(patterns) = self.referents.get_mut(&referent.item)
		{
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(&referent.item);
			}
		}
	}

	pub fn add_referrer(&mut self, key: &tg::tag::Pattern, referrer: Referrer) {
		self.referrers
			.entry(referrer.index)
			.or_default()
			.insert(key.clone());
		if let Some(solution) = self.solutions.get_mut(key) {
			solution.referrers.push(referrer);
		}
	}
}
