use {
	self::prefetch::{ObjectOptions, Prefetch},
	crate::{
		Session,
		checkin::graph::{Contents, Directory, File, Graph, Node, Symlink, Variant},
	},
	smallvec::SmallVec,
	std::{
		fmt::Write as _,
		path::{Path, PathBuf},
		sync::Arc,
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

mod prefetch;

struct State<'a> {
	arg: &'a tg::checkin::Arg,
	checkpoints: Vec<SavedCheckpoint>,
	prefetch: Prefetch,
	root: PathBuf,
}

struct SavedCheckpoint {
	checkpoint: Checkpoint,
	key: tg::specifier::Pattern,
}

#[derive(Clone)]
struct Checkpoint {
	candidates: Option<im::Vector<Candidate>>,
	directory_options: DirectoryOptions,
	graph: Graph,
	graphs: Graphs,
	graph_pointers: GraphPointers,
	listed: bool,
	lock: Option<Arc<tg::graph::Data>>,
	next: usize,
	observed_graph_nodes: ObservedGraphNodes,
	queue: im::Vector<Item>,
	solutions: Solutions,
	visited: im::HashSet<ItemKey, fnv::FnvBuildHasher>,
}

type Graphs = im::HashMap<
	tg::graph::Id,
	(Arc<tg::graph::Data>, Option<tg::object::Metadata>),
	tg::id::BuildHasher,
>;

type GraphPointers =
	im::HashMap<(tg::graph::Id, usize), tg::graph::data::Pointer, fnv::FnvBuildHasher>;

type ObservedGraphNodes = im::HashSet<(tg::graph::Id, usize), fnv::FnvBuildHasher>;

type DirectoryOptions = im::HashMap<(usize, String), ObjectOptions, fnv::FnvBuildHasher>;

struct CollectedDirectory {
	entries: std::collections::BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
	options: std::collections::BTreeMap<String, ObjectOptions>,
}

struct GraphObservation {
	data: Arc<tg::graph::Data>,
	id: tg::graph::Id,
	metadata: Option<tg::object::Metadata>,
	node_index: usize,
}

#[derive(Clone, Debug)]
struct Candidate {
	index: Option<usize>,
	location: Option<tg::Location>,
	object: tg::object::Id,
	tag: tg::Specifier,
	tokens: tg::authorization::Tokens,
}

#[derive(Clone, Debug)]
struct Item {
	options: ObjectOptions,
	referent: tg::Referent<usize>,
	variant: ItemVariant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ItemKey {
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

#[derive(Clone, Default)]
pub struct Solutions {
	map: im::HashMap<tg::specifier::Pattern, Solution, fnv::FnvBuildHasher>,
	referents: im::HashMap<
		usize,
		im::HashSet<tg::specifier::Pattern, fnv::FnvBuildHasher>,
		fnv::FnvBuildHasher,
	>,
	referrers: im::HashMap<
		usize,
		im::HashSet<tg::specifier::Pattern, fnv::FnvBuildHasher>,
		fnv::FnvBuildHasher,
	>,
}

#[derive(Clone)]
pub struct Solution {
	candidate_options: ObjectOptions,
	pub referent: Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>,
	pub referrers: Vec<Referrer>,
}

#[derive(derive_more::IsVariant)]
enum TagInnerOutput {
	Conflicted,
	Reused(
		tg::Referent<tg::graph::data::Edge<tg::object::Id>>,
		ObjectOptions,
	),
	Selected(
		tg::Referent<tg::graph::data::Edge<tg::object::Id>>,
		ObjectOptions,
	),
	Unsolved,
}

#[derive(Clone)]
pub struct Referrer {
	pub index: usize,
	pub pattern: Option<tg::specifier::Pattern>,
}

pub(super) struct CheckinSolveArg<'a> {
	pub arg: &'a tg::checkin::Arg,
	pub graph: &'a mut Graph,
	pub lock: Option<Arc<tg::graph::Data>>,
	pub next: usize,
	pub progress: &'a crate::progress::Handle<super::TaskOutput>,
	pub root: &'a Path,
	pub solutions: &'a mut Solutions,
}

struct CheckinSolveInnerArg<'a> {
	arg: &'a tg::checkin::Arg,
	graph: &'a mut Graph,
	lock: Option<Arc<tg::graph::Data>>,
	next: usize,
	root: &'a Path,
	solutions: &'a mut Solutions,
}

impl Session {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_solve(&self, arg: CheckinSolveArg<'_>) -> tg::Result<()> {
		let CheckinSolveArg {
			arg,
			graph,
			lock,
			next,
			progress,
			root,
			solutions,
		} = arg;
		progress.spinner("solving", "solving");
		if solutions.is_empty() {
			// If solutions is empty, then just solve.
			let inner_arg = CheckinSolveInnerArg {
				arg,
				graph,
				lock,
				next,
				root,
				solutions,
			};
			self.checkin_solve_inner(inner_arg).await?;
		} else {
			// Otherwise, attempt to solve.
			let inner_arg = CheckinSolveInnerArg {
				arg,
				graph,
				lock: lock.clone(),
				next,
				root,
				solutions,
			};
			let result = self.checkin_solve_inner(inner_arg).await;
			if result.is_ok() {
				return Ok(());
			}

			// Unsolve and clean the graph, clear the solutions, and solve from the beginning.
			graph.unsolve();
			graph.clean(root);
			solutions.clear();
			let inner_arg = CheckinSolveInnerArg {
				arg,
				graph,
				lock,
				next,
				root,
				solutions,
			};
			self.checkin_solve_inner(inner_arg).await?;
		}
		progress.finish("solving");
		Ok(())
	}

	async fn checkin_solve_inner(&self, arg: CheckinSolveInnerArg<'_>) -> tg::Result<()> {
		let CheckinSolveInnerArg {
			arg,
			graph,
			lock,
			next,
			root,
			solutions,
		} = arg;
		// Create the prefetcher.
		let prefetch = Prefetch::new(arg.clone());
		let _prefetch_abort_guard = scopeguard::guard(prefetch.clone(), |prefetch| {
			prefetch.abort();
		});

		// Create the state
		let mut state = State {
			arg,
			checkpoints: Vec::new(),
			prefetch,
			root: root.to_owned(),
		};

		// Prefetch from the lock.
		if let Some(lock) = &lock {
			self.checkin_solve_prefetch_from_lock(&state.prefetch, graph, lock, next);
		}

		// Create the first checkpoint.
		let index = graph.paths.get(root).unwrap();
		let mut checkpoint = Checkpoint {
			candidates: None,
			directory_options: im::HashMap::default(),
			graph: graph.clone(),
			graphs: im::HashMap::default(),
			graph_pointers: im::HashMap::default(),
			listed: false,
			lock: lock.clone(),
			next,
			observed_graph_nodes: im::HashSet::default(),
			queue: im::Vector::new(),
			solutions: solutions.clone(),
			visited: im::HashSet::default(),
		};
		let referent = tg::Referent::with_node(*index);
		let options = ObjectOptions::default();
		Self::checkin_solve_enqueue_items_for_node(&mut checkpoint, &referent, &options);

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
		let key = ItemKey {
			node: item.referent.node,
			variant: item.variant.clone(),
		};
		if checkpoint.visited.insert(key).is_some() {
			return Ok(());
		}

		// Check if the edge is already solved.
		if let Some(edge) = Self::checkin_solve_get_solved_edge_for_item(checkpoint, &item) {
			let mut options = Self::checkin_solve_get_object_options_for_item(checkpoint, &item);
			let permission_only = Self::checkin_solve_item_is_permission_only(checkpoint, &item);

			// Try to remap the edge if necessary.
			let edge = match edge {
				// If this is a pointer to within a graph, create a new graph node pointer.
				tg::graph::data::Edge::Pointer(pointer) if pointer.graph.is_some() => {
					let pointer = self
						.checkin_solve_create_graph_pointer(
							state,
							checkpoint,
							&item,
							&pointer,
							&mut options,
							permission_only,
						)
						.await?;
					tg::graph::data::Edge::Pointer(pointer)
				},

				// If this is an artifact edge, try to create a new edge pointing to it.
				tg::graph::data::Edge::Object(id) if id.is_artifact() => {
					let id = id.try_into().unwrap();
					self.checkin_solve_create_edge_for_artifact(
						state,
						checkpoint,
						&item,
						&id,
						&mut options,
						permission_only,
					)
					.await?
				},

				// If this is another object edge, record the permissions returned by the get.
				tg::graph::data::Edge::Object(id) => {
					self.checkin_solve_get_object_with_options(
						&state.prefetch,
						&mut checkpoint.graph,
						checkpoint.next,
						&id,
						&mut options,
					)
					.await?;
					tg::graph::data::Edge::Object(id)
				},

				// Otherwise, reuse the existing edge.
				edge @ tg::graph::data::Edge::Pointer(_) => edge,
			};

			// Add the edge to the item.
			Self::checkin_add_edge_for_item(checkpoint, &item, edge.clone());

			// If the edge is a pointer into the checkin graph, enqueue its outgoing edges.
			if let Some(pointer) = edge.try_unwrap_pointer_ref().ok()
				&& pointer.graph.is_none()
			{
				let referent = tg::Referent::with_node(pointer.index);
				Self::checkin_solve_enqueue_items_for_node(checkpoint, &referent, &options);
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

		let tg::reference::Node::Specifier(tag) = reference.node() else {
			if state.arg.options.unsolved_dependencies {
				return Ok(());
			}
			return Err(
				tg::error!(node = %reference.node(), "expected reference to be a specifier"),
			);
		};

		self.checkin_solve_item_with_tag(state, checkpoint, item, reference.clone(), tag.clone())
			.await
	}

	fn checkin_solve_get_object_options_for_item(
		checkpoint: &Checkpoint,
		item: &Item,
	) -> ObjectOptions {
		let mut options = match &item.variant {
			ItemVariant::FileDependency(reference) => checkpoint
				.graph
				.nodes
				.get(&item.referent.node)
				.unwrap()
				.variant
				.unwrap_file_ref()
				.dependencies
				.get(reference)
				.and_then(Option::as_ref)
				.map_or_else(
					|| ObjectOptions::from_reference(reference),
					|dependency| ObjectOptions::from_dependency(reference, dependency),
				),
			ItemVariant::DirectoryEntry(_) | ItemVariant::SymlinkArtifact => {
				ObjectOptions::default()
			},
		};
		options.inherit(&item.options);

		options
	}

	fn checkin_solve_item_is_permission_only(checkpoint: &Checkpoint, item: &Item) -> bool {
		let ItemVariant::FileDependency(reference) = &item.variant else {
			return false;
		};
		!reference.is_solvable()
			&& item.referent.node >= checkpoint.next
			&& checkpoint
				.graph
				.nodes
				.get(&item.referent.node)
				.unwrap()
				.path
				.is_some()
	}

	fn checkin_add_edge_for_item(
		checkpoint: &mut Checkpoint,
		item: &Item,
		edge: tg::graph::data::Edge<tg::object::Id>,
	) {
		if let Ok(pointer) = edge.try_unwrap_pointer_ref()
			&& pointer.graph.is_none()
		{
			checkpoint
				.graph
				.nodes
				.get_mut(&pointer.index)
				.unwrap()
				.referrers
				.push(item.referent.node);
		}
		let node = checkpoint.graph.nodes.get_mut(&item.referent.node).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => {
				let edge = match edge {
					tg::graph::data::Edge::Object(object) => {
						let artifact = object.try_into().unwrap();
						tg::graph::data::Edge::Object(artifact)
					},
					tg::graph::data::Edge::Pointer(pointer) => {
						tg::graph::data::Edge::Pointer(pointer)
					},
				};
				*node
					.variant
					.unwrap_directory_mut()
					.entries
					.get_mut(name)
					.unwrap() = edge;
			},
			ItemVariant::FileDependency(reference) => {
				node.variant
					.unwrap_file_mut()
					.dependencies
					.get_mut(reference)
					.unwrap()
					.get_or_insert_with(|| {
						tg::graph::data::Dependency(tg::Referent::with_node(Some(edge.clone())))
					})
					.0
					.node = Some(edge.clone());
			},
			ItemVariant::SymlinkArtifact => {
				let edge = match edge {
					tg::graph::data::Edge::Object(object) => {
						let artifact = object.try_into().unwrap();
						tg::graph::data::Edge::Object(artifact)
					},
					tg::graph::data::Edge::Pointer(pointer) => {
						tg::graph::data::Edge::Pointer(pointer)
					},
				};
				*node.variant.unwrap_symlink_mut().artifact.as_mut().unwrap() = edge;
			},
		}
	}

	async fn checkin_solve_item_with_tag(
		&self,
		state: &mut State<'_>,
		checkpoint: &mut Checkpoint,
		item: Item,
		reference: tg::Reference,
		tag: tg::specifier::Pattern,
	) -> tg::Result<()> {
		// Get the key.
		let key = if tag.contains_operators() {
			tg::specifier::Pattern::any_in_parent(tag.parent.clone())
		} else {
			tag.clone()
		};

		// Solve the item.
		let output = self
			.checkin_solve_item_with_tag_inner(state, checkpoint, &item, &key, &tag)
			.await?;

		// Get the referrer.
		let referrer = Referrer {
			index: item.referent.node,
			pattern: Some(tag),
		};
		let selected = output.is_selected();

		// Handle the output.
		match output {
			TagInnerOutput::Reused(referent, options)
			| TagInnerOutput::Selected(referent, options) => {
				// Checkpoint.
				if selected {
					let saved_checkpoint = SavedCheckpoint {
						checkpoint: checkpoint.clone(),
						key: key.clone(),
					};
					state.checkpoints.push(saved_checkpoint);
				}

				// Add the edge.
				checkpoint
					.graph
					.nodes
					.get_mut(&item.referent.node)
					.unwrap()
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(r, option)| (r == &reference).then_some(option))
					.unwrap()
					.replace(tg::graph::data::Dependency(referent.clone().map(Some)));

				// Add the referrer to the solution.
				checkpoint.solutions.add_referrer(&key, referrer);

				// Add the referrer to the target node and enqueue its items.
				if let Ok(pointer) = referent.node().try_unwrap_pointer_ref()
					&& pointer.graph.is_none()
				{
					checkpoint
						.graph
						.nodes
						.get_mut(&pointer.index)
						.unwrap()
						.referrers
						.push(item.referent.node);
					let referent = referent.clone().map(|_| pointer.index);
					Self::checkin_solve_enqueue_items_for_node(checkpoint, &referent, &options);
				}
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
							.is_some_and(|tag| key.matches_specifier_for_list(tag))
						{
							referent.take();
							continue 'outer;
						}
					}
				}
				checkpoint.solutions.clear_referent(&key);
			},

			TagInnerOutput::Unsolved => checkpoint.solutions.add_referrer(&key, referrer),
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
		key: &tg::specifier::Pattern,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<TagInnerOutput> {
		// Check if a solution exists for the key.
		if let Some(solution) = checkpoint.solutions.get(key) {
			let Some(referent) = solution.referent.clone() else {
				return Ok(TagInnerOutput::Unsolved);
			};
			if !pattern.matches_specifier_for_list(referent.tag().unwrap()) {
				return Ok(TagInnerOutput::Conflicted);
			}
			let candidate_options = solution.candidate_options.clone();
			let mut options = Self::checkin_solve_get_object_options_for_item(checkpoint, item);
			options.inherit(&candidate_options);
			let id = referent
				.id()
				.cloned()
				.ok_or_else(|| tg::error!("expected the solution object id"))?
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			self.checkin_solve_observe_artifact(state, checkpoint, &id, &mut options)
				.await?;

			return Ok(TagInnerOutput::Reused(referent, options));
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
		let Some(candidate) = checkpoint.candidates.as_mut().unwrap().pop_front() else {
			if state.arg.options.unsolved_dependencies {
				let solution = Solution {
					candidate_options: ObjectOptions::default(),
					referent: None,
					referrers: vec![],
				};
				checkpoint.solutions.insert(key.clone(), solution);
				return Ok(TagInnerOutput::Unsolved);
			}
			return Err(tg::error!(
				referrer = %Self::checkin_solve_get_referrer(state, &checkpoint.graph, item.referent.node),
				%pattern,
				"no matching tags were found",
			));
		};

		// Create the traversal options.
		let candidate_options = ObjectOptions::with_location_and_tokens(
			candidate.location.clone(),
			candidate.tokens.clone(),
		);
		let mut options = Self::checkin_solve_get_object_options_for_item(checkpoint, item);
		options.inherit(&candidate_options);

		// Try to reuse a node if it exists. Otherwise, create a new edge.
		let edge = if let Some(index) = candidate.index {
			let id = candidate
				.object
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			self.checkin_solve_observe_artifact(state, checkpoint, &id, &mut options)
				.await?;
			tg::graph::data::Edge::Pointer(tg::graph::data::Pointer {
				graph: None,
				index,
				kind: checkpoint.graph.nodes[&index].variant.kind(),
			})
		} else {
			let id = candidate
				.object
				.clone()
				.try_into()
				.map_err(|_| tg::error!("expected an artifact"))?;
			self.checkin_solve_create_edge_for_artifact(
				state,
				checkpoint,
				item,
				&id,
				&mut options,
				false,
			)
			.await?
		};

		let get = item
			.variant
			.try_unwrap_file_dependency_ref()
			.ok()
			.and_then(|reference| reference.options().get.clone());
		let referent_options = tg::referent::Options {
			id: Some(candidate.object),
			path: get,
			tag: Some(candidate.tag.clone()),
			..Default::default()
		};
		let referent = tg::Referent::new(edge, referent_options);

		let solution = Solution {
			candidate_options,
			referent: Some(referent.clone()),
			referrers: vec![],
		};

		// Add the solution.
		checkpoint.solutions.insert(key.clone(), solution);

		Ok(TagInnerOutput::Selected(referent, options))
	}

	fn checkin_solve_get_lock_candidate(
		state: &State<'_>,
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<Candidate> {
		// If source_dependencies is true and the reference has a source option, do not use the lock candidate.
		if let ItemVariant::FileDependency(reference) = &item.variant
			&& state.arg.options.source_dependencies
			&& reference.options().source.is_some()
		{
			return None;
		}
		let lock_index = checkpoint
			.graph
			.nodes
			.get(&item.referent.node)
			.unwrap()
			.lock_index?;
		let candidate = Self::checkin_solve_get_lock_candidate_inner(checkpoint, item, lock_index)?;
		if state
			.arg
			.updates
			.iter()
			.any(|pattern| pattern_matches_specifier_or_ancestor(pattern, &candidate.tag))
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
		let location = referent.options.location.clone();
		let tokens = referent.options.tokens.clone();
		let candidate = Candidate {
			index,
			location,
			object: object.clone(),
			tag,
			tokens,
		};
		Some(candidate)
	}

	async fn checkin_solve_get_tag_candidates(
		&self,
		state: &State<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<im::Vector<Candidate>> {
		let output = self
			.checkin_solve_list_tag_entries(&state.prefetch, pattern)
			.await?;

		let candidates = output
			.data
			.into_iter()
			.filter_map(|output| {
				if output.kind() != tg::id::Kind::Tag {
					return None;
				}
				let tag = output.specifier;
				let target = output.target?;
				let location = target.options.location;
				let object = target.node.left()?;
				let tokens = target.options.tokens;
				let index = None;
				let candidate = Candidate {
					index,
					location,
					object,
					tag,
					tokens,
				};
				Some(candidate)
			})
			.collect();

		Ok(candidates)
	}

	async fn checkin_solve_collect_directory_entries(
		&self,
		prefetch: &Prefetch,
		checkpoint: &mut Checkpoint,
		directory: &tg::graph::data::Directory,
		options: &ObjectOptions,
	) -> tg::Result<CollectedDirectory> {
		match directory {
			tg::graph::data::Directory::Leaf(leaf) => {
				let entries = leaf.entries.clone();
				let options = entries
					.keys()
					.map(|name| (name.clone(), options.clone()))
					.collect();
				Ok(CollectedDirectory { entries, options })
			},
			tg::graph::data::Directory::Branch(branch) => {
				let mut entries = std::collections::BTreeMap::new();
				let mut entry_options = std::collections::BTreeMap::new();
				for child in &branch.children {
					let mut child_options = options.clone();
					let child_directory = match &child.directory {
						tg::graph::data::Edge::Object(id) => {
							let object_id = tg::object::Id::from(id.clone());
							let output = self
								.checkin_solve_get_object_with_options(
									prefetch,
									&mut checkpoint.graph,
									checkpoint.next,
									&object_id,
									&mut child_options,
								)
								.await?;
							let data = tg::directory::Data::deserialize(output.output.bytes)
								.map_err(|error| {
									tg::error!(!error, "failed to deserialize directory data")
								})?;
							match data {
								tg::directory::Data::Node(directory) => directory,
								tg::directory::Data::Pointer(pointer) => {
									let graph_id = pointer.graph.as_ref().ok_or_else(|| {
										tg::error!("expected graph in standalone directory pointer")
									})?;
									self.checkin_solve_get_directory_from_pointer(
										prefetch,
										checkpoint,
										&pointer,
										graph_id,
										&mut child_options,
									)
									.await?
								},
							}
						},
						tg::graph::data::Edge::Pointer(pointer) => {
							let graph_id = pointer.graph.as_ref().ok_or_else(|| {
								tg::error!("expected graph in standalone directory pointer")
							})?;
							self.checkin_solve_get_directory_from_pointer(
								prefetch,
								checkpoint,
								pointer,
								graph_id,
								&mut child_options,
							)
							.await?
						},
					};
					let child = Box::pin(self.checkin_solve_collect_directory_entries(
						prefetch,
						checkpoint,
						&child_directory,
						&child_options,
					))
					.await?;
					entries.extend(child.entries);
					entry_options.extend(child.options);
				}
				let options = entry_options;
				Ok(CollectedDirectory { entries, options })
			},
		}
	}

	async fn checkin_solve_collect_graph_directory_entries(
		&self,
		prefetch: &Prefetch,
		checkpoint: &mut Checkpoint,
		directory: &tg::graph::data::Directory,
		graph_id: &tg::graph::Id,
		options: &ObjectOptions,
	) -> tg::Result<CollectedDirectory> {
		match directory {
			tg::graph::data::Directory::Leaf(leaf) => {
				let mut entries = std::collections::BTreeMap::new();
				for (name, edge) in &leaf.entries {
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
				let options = entries
					.keys()
					.map(|name| (name.clone(), options.clone()))
					.collect();
				Ok(CollectedDirectory { entries, options })
			},
			tg::graph::data::Directory::Branch(branch) => {
				let mut entries = std::collections::BTreeMap::new();
				let mut entry_options = std::collections::BTreeMap::new();
				for child in &branch.children {
					let mut child_options = options.clone();
					let child_directory = match &child.directory {
						tg::graph::data::Edge::Object(id) => {
							let object_id = tg::object::Id::from(id.clone());
							let output = self
								.checkin_solve_get_object_with_options(
									prefetch,
									&mut checkpoint.graph,
									checkpoint.next,
									&object_id,
									&mut child_options,
								)
								.await?;
							let data = tg::directory::Data::deserialize(output.output.bytes)
								.map_err(|error| {
									tg::error!(!error, "failed to deserialize directory data")
								})?;
							match data {
								tg::directory::Data::Node(directory) => directory,
								tg::directory::Data::Pointer(pointer) => {
									self.checkin_solve_get_directory_from_pointer(
										prefetch,
										checkpoint,
										&pointer,
										graph_id,
										&mut child_options,
									)
									.await?
								},
							}
						},
						tg::graph::data::Edge::Pointer(pointer) => {
							self.checkin_solve_get_directory_from_pointer(
								prefetch,
								checkpoint,
								pointer,
								graph_id,
								&mut child_options,
							)
							.await?
						},
					};
					let child = Box::pin(self.checkin_solve_collect_graph_directory_entries(
						prefetch,
						checkpoint,
						&child_directory,
						graph_id,
						&child_options,
					))
					.await?;
					entries.extend(child.entries);
					entry_options.extend(child.options);
				}
				let options = entry_options;
				Ok(CollectedDirectory { entries, options })
			},
		}
	}

	async fn checkin_solve_get_directory_from_pointer(
		&self,
		prefetch: &Prefetch,
		checkpoint: &mut Checkpoint,
		pointer: &tg::graph::data::Pointer,
		graph_id: &tg::graph::Id,
		options: &mut ObjectOptions,
	) -> tg::Result<tg::graph::data::Directory> {
		let child_graph_id = pointer.graph.clone().unwrap_or_else(|| graph_id.clone());
		let output = self
			.checkin_solve_get_object_with_options(
				prefetch,
				&mut checkpoint.graph,
				checkpoint.next,
				&child_graph_id.clone().into(),
				options,
			)
			.await?;
		let graph_data = if let Some((data, _)) = checkpoint.graphs.get(&child_graph_id) {
			data.clone()
		} else {
			let data = tg::graph::Data::deserialize(output.output.bytes)
				.map(Arc::new)
				.map_err(|error| tg::error!(!error, "failed to deserialize graph data"))?;
			checkpoint.graphs.insert(
				child_graph_id.clone(),
				(data.clone(), output.output.metadata),
			);
			data
		};
		let directory = graph_data
			.nodes
			.get(pointer.index)
			.ok_or_else(|| tg::error!("graph node index out of bounds"))?
			.try_unwrap_directory_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected directory node in branch child"))?;
		let key = (child_graph_id.clone(), pointer.index);
		if checkpoint.observed_graph_nodes.insert(key).is_none() {
			let id = tg::object::Id::from(child_graph_id);
			Self::checkin_record_object_children(
				&mut checkpoint.graph,
				checkpoint.next,
				&id,
				|children| {
					directory.children(children);
				},
			);
		}
		Ok(directory.clone())
	}

	async fn checkin_solve_observe_artifact(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		id: &tg::artifact::Id,
		options: &mut ObjectOptions,
	) -> tg::Result<()> {
		// Get the artifact.
		let object_id = tg::object::Id::from(id.clone());
		let output = self
			.checkin_solve_get_object_with_options(
				&state.prefetch,
				&mut checkpoint.graph,
				checkpoint.next,
				&object_id,
				options,
			)
			.await?;

		// Observe the graph node when the artifact is a pointer.
		let pointer = match output.data.as_ref() {
			tg::object::Data::Directory(tg::directory::Data::Pointer(pointer))
			| tg::object::Data::File(tg::file::Data::Pointer(pointer))
			| tg::object::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => Some(pointer),
			_ => None,
		};
		if let Some(pointer) = pointer {
			self.checkin_solve_observe_graph_pointer(state, checkpoint, pointer, options)
				.await?;
		}

		Ok(())
	}

	async fn checkin_solve_create_edge_for_artifact(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		item: &Item,
		id: &tg::artifact::Id,
		options: &mut ObjectOptions,
		permission_only: bool,
	) -> tg::Result<tg::graph::data::Edge<tg::object::Id>> {
		// Get the object.
		let output = self
			.checkin_solve_get_object_with_options(
				&state.prefetch,
				&mut checkpoint.graph,
				checkpoint.next,
				&id.clone().into(),
				options,
			)
			.await?;
		if permission_only
			&& !Self::checkin_solve_metadata_requires_solving(output.output.metadata.as_ref())
		{
			return Ok(tg::graph::data::Edge::Object(id.clone().into()));
		}
		let data = tg::artifact::Data::deserialize(id.kind(), output.output.bytes.clone())
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))?;
		let kind = data.kind();

		// Try to create a checkin graph node.
		let mut directory_options = std::collections::BTreeMap::new();
		let variant = match data {
			tg::artifact::Data::Directory(tg::directory::Data::Pointer(pointer))
			| tg::artifact::Data::File(tg::file::Data::Pointer(pointer))
			| tg::artifact::Data::Symlink(tg::symlink::Data::Pointer(pointer)) => {
				// Cannot add nodes that are missing their graph.
				if pointer.graph.is_none() {
					return Err(tg::error!("invalid artifact"));
				}

				// Get a pointer to the graph node.
				let pointer = self
					.checkin_solve_create_graph_pointer(
						state,
						checkpoint,
						item,
						&pointer,
						options,
						permission_only,
					)
					.await?;

				// Otherwise return a pointer to the original graph.
				return Ok(tg::graph::data::Edge::Pointer(pointer));
			},
			tg::artifact::Data::Directory(tg::directory::Data::Node(directory)) => {
				let collected = self
					.checkin_solve_collect_directory_entries(
						&state.prefetch,
						checkpoint,
						&directory,
						options,
					)
					.await?;
				let entries = collected.entries;
				directory_options = collected.options;
				Variant::Directory(Directory { entries })
			},
			tg::artifact::Data::File(tg::file::Data::Node(file)) => {
				let contents = if let Some(id) = file.contents {
					let object = self
						.server
						.index
						.try_get_object(&id.clone().into())
						.await
						.ok()
						.flatten();
					let (storage, metadata) = object
						.map(|object| (object.storage, object.metadata))
						.unwrap_or_default();
					Some(Contents::Id {
						id,
						metadata: Some(metadata),
						storage,
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
		let lock_index = Self::checkin_solve_get_lock_index(checkpoint, item);

		let node = Node {
			artifact: None,
			edge: None,
			id: None,
			lock_index,
			metadata: None,
			object_children: im::HashSet::default(),
			object_complete: false,
			path: None,
			path_metadata: None,
			permissions: tg::authorization::permission::object::Set::empty(),
			referrers: SmallVec::new(),
			solvable: output
				.output
				.metadata
				.as_ref()
				.and_then(|metadata| metadata.subtree.solvable)
				.unwrap_or(true),
			solved: false,
			storage: tangram_index::object::Storage::default(),
			variant,
		};

		// Insert the node into the graph.
		let index = checkpoint.graph.next;
		checkpoint.graph.next += 1;
		checkpoint.graph.nodes.insert(index, Box::new(node));
		checkpoint.directory_options.extend(
			directory_options
				.into_iter()
				.map(|(name, options)| ((index, name), options)),
		);

		let pointer = tg::graph::data::Pointer {
			graph: None,
			index,
			kind,
		};
		let edge = tg::graph::data::Edge::Pointer(pointer);

		Ok(edge)
	}

	async fn checkin_solve_create_graph_pointer(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		item: &Item,
		pointer: &tg::graph::data::Pointer,
		options: &mut ObjectOptions,
		permission_only: bool,
	) -> tg::Result<tg::graph::data::Pointer> {
		let observation = self
			.checkin_solve_observe_graph_pointer(state, checkpoint, pointer, options)
			.await?;
		let GraphObservation {
			data: graph_data,
			id: graph_id,
			metadata: graph_metadata,
			node_index,
		} = observation;
		let graph_node = graph_data
			.nodes
			.get(node_index)
			.ok_or_else(|| tg::error!("graph node index out of bounds"))?;

		// Retain an opaque pointer when the graph does not require solving.
		let create = if permission_only {
			Self::checkin_solve_metadata_requires_solving(graph_metadata.as_ref())
		} else {
			graph_metadata
				.as_ref()
				.is_none_or(|metadata| metadata.node.solvable)
		};
		if !create {
			let pointer = tg::graph::data::Pointer {
				graph: Some(graph_id),
				index: node_index,
				kind: graph_node.kind(),
			};
			return Ok(pointer);
		}

		// Check if this graph node has already been added.
		let key = (graph_id.clone(), node_index);
		if let Some(pointer) = checkpoint.graph_pointers.get(&key).cloned() {
			return Ok(pointer);
		}
		self.checkin_solve_prefetch_from_graph_node(&state.prefetch, graph_node, options);

		// Create the checkin graph node.
		let mut directory_options = std::collections::BTreeMap::new();
		let variant = match graph_node {
			tg::graph::data::Node::Directory(directory) => {
				let collected = self
					.checkin_solve_collect_graph_directory_entries(
						&state.prefetch,
						checkpoint,
						directory,
						&graph_id,
						options,
					)
					.await?;
				let entries = collected.entries;
				directory_options = collected.options;
				Variant::Directory(Directory { entries })
			},

			tg::graph::data::Node::File(file) => {
				let contents = if let Some(id) = file.contents.clone() {
					let object = self
						.server
						.index
						.try_get_object(&id.clone().into())
						.await
						.ok()
						.flatten();
					let (storage, metadata) = object
						.map(|object| (object.storage, object.metadata))
						.unwrap_or_default();
					Some(Contents::Id {
						id,
						metadata: Some(metadata),
						storage,
					})
				} else {
					None
				};
				let mut dependencies = std::collections::BTreeMap::new();
				for (reference, option) in &file.dependencies {
					let Some(dependency) = option else {
						if !reference.is_solvable() {
							return Err(
								tg::error!(node = %reference.node(), "unsolvable unsolved dependency"),
							);
						}
						dependencies.insert(reference.clone(), None);
						continue;
					};
					if dependency.tag().is_some() {
						dependencies.insert(reference.clone(), None);
					} else {
						let referent = dependency.0.clone().map(|node| match node {
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
		let lock_index = Self::checkin_solve_get_lock_index(checkpoint, item);

		let node = Node {
			artifact: None,
			edge: None,
			id: None,
			lock_index,
			metadata: None,
			object_children: im::HashSet::default(),
			object_complete: false,
			path: None,
			path_metadata: None,
			permissions: tg::authorization::permission::object::Set::empty(),
			referrers: SmallVec::new(),
			solvable: true,
			solved: false,
			storage: tangram_index::object::Storage::default(),
			variant,
		};

		// Add the node to the checkin graph.
		let index = checkpoint.graph.next;
		checkpoint.graph.next += 1;
		checkpoint.graph.nodes.insert(index, Box::new(node));
		checkpoint.directory_options.extend(
			directory_options
				.into_iter()
				.map(|(name, options)| ((index, name), options)),
		);

		// Create the pointer.
		let pointer = tg::graph::data::Pointer {
			graph: None,
			index,
			kind: graph_node.kind(),
		};

		// Cache the mapping.
		checkpoint.graph_pointers.insert(key, pointer.clone());

		Ok(pointer)
	}

	async fn checkin_solve_observe_graph_pointer(
		&self,
		state: &State<'_>,
		checkpoint: &mut Checkpoint,
		pointer: &tg::graph::data::Pointer,
		options: &mut ObjectOptions,
	) -> tg::Result<GraphObservation> {
		// Get the graph.
		let graph_id = pointer
			.graph
			.as_ref()
			.ok_or_else(|| tg::error!("expected a graph pointer"))?;
		let output = self
			.checkin_solve_get_object_with_options(
				&state.prefetch,
				&mut checkpoint.graph,
				checkpoint.next,
				&graph_id.clone().into(),
				options,
			)
			.await?;

		// Load the graph data and merge its metadata.
		let cached = checkpoint.graphs.get(graph_id).cloned();
		let (data, mut metadata) = if let Some((data, metadata)) = cached {
			(data, metadata)
		} else {
			let data = tg::graph::Data::deserialize(output.output.bytes.clone())
				.map(Arc::new)
				.map_err(|error| tg::error!(!error, "failed to deserialize the data"))?;
			(data, None)
		};
		if let Some(output_metadata) = output.output.metadata {
			if output_metadata.node == tg::object::metadata::Node::default() {
				match &mut metadata {
					Some(metadata) => metadata.merge(&output_metadata),
					None => metadata = Some(output_metadata),
				}
			} else {
				metadata = Some(output_metadata);
			}
		}
		checkpoint
			.graphs
			.insert(graph_id.clone(), (data.clone(), metadata.clone()));

		// Record the graph node's children once.
		let node = data
			.nodes
			.get(pointer.index)
			.ok_or_else(|| tg::error!("graph node index out of bounds"))?;
		let key = (graph_id.clone(), pointer.index);
		if checkpoint.observed_graph_nodes.insert(key).is_none() {
			let id = tg::object::Id::from(graph_id.clone());
			Self::checkin_record_object_children(
				&mut checkpoint.graph,
				checkpoint.next,
				&id,
				|children| match node {
					tg::graph::data::Node::Directory(directory) => directory.children(children),
					tg::graph::data::Node::File(file) => file.children(children),
					tg::graph::data::Node::Symlink(symlink) => symlink.children(children),
				},
			);
		}

		// Create the observation.
		let observation = GraphObservation {
			data,
			id: graph_id.clone(),
			metadata,
			node_index: pointer.index,
		};

		Ok(observation)
	}

	fn checkin_solve_metadata_requires_solving(metadata: Option<&tg::object::Metadata>) -> bool {
		metadata.is_none_or(|metadata| {
			metadata.subtree.solvable != Some(false) && metadata.subtree.solved != Some(true)
		})
	}

	fn checkin_solve_get_lock_index(checkpoint: &Checkpoint, item: &Item) -> Option<usize> {
		let Some(lock) = &checkpoint.lock else {
			return None;
		};
		let parent_lock_index = checkpoint
			.graph
			.nodes
			.get(&item.referent.node)
			.unwrap()
			.lock_index?;
		let parent_node = lock.nodes.get(parent_lock_index).unwrap();
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.try_unwrap_leaf_ref()
					.expect("lock directories must be leaves")
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
					.node()
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
		options: &ObjectOptions,
	) {
		// Get the node.
		let node = checkpoint.graph.nodes.get(&referent.node).unwrap();

		// Traverse unsolved nodes and new input nodes with explicit dependencies.
		let solve = node.solvable && !node.solved;
		let observe = referent.node >= checkpoint.next && node.path.is_some();
		if !solve && !observe {
			return;
		}

		// Enqueue the node's items.
		match &node.variant {
			Variant::Directory(directory) => {
				let items = directory.entries.keys().map(|name| {
					let mut item_options = checkpoint
						.directory_options
						.get(&(referent.node, name.clone()))
						.cloned()
						.unwrap_or_default();
					item_options.inherit(options);
					Item {
						options: item_options,
						referent: referent.clone(),
						variant: ItemVariant::DirectoryEntry(name.clone()),
					}
				});
				checkpoint.queue.extend(items);
			},
			Variant::File(file) => {
				let items = file
					.dependencies
					.iter()
					.filter(|(_, dependency)| solve || dependency.is_some())
					.map(|(reference, _)| Item {
						options: options.clone(),
						referent: referent.clone(),
						variant: ItemVariant::FileDependency(reference.clone()),
					});
				checkpoint.queue.extend(items);
			},
			Variant::Object => {},
			Variant::Symlink(symlink) => {
				let items = symlink.artifact.iter().map(|_| Item {
					options: options.clone(),
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
		let node = checkpoint.graph.nodes.get(&item.referent.node).unwrap();
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
					.and_then(|dependency| dependency.0.node)
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
		key: &tg::specifier::Pattern,
	) -> Option<Checkpoint> {
		let position = state
			.checkpoints
			.iter()
			.rposition(|checkpoint| &checkpoint.key == key)?;
		let candidates = state.checkpoints[position].checkpoint.candidates.as_ref()?;
		if candidates.is_empty() {
			return None;
		}
		state.checkpoints.truncate(position + 1);
		let mut checkpoint = state.checkpoints.pop()?.checkpoint;
		checkpoint.solutions.remove(key);
		Some(checkpoint)
	}

	fn checkin_solve_backtrack_error(
		state: &State,
		checkpoint: &Checkpoint,
		key: &tg::specifier::Pattern,
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

	fn checkin_solve_get_referrer(state: &State<'_>, graph: &Graph, index: usize) -> String {
		let mut tag = None;
		let mut id = None;
		let mut components = vec![];
		let mut current = index;
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
							let pointer = referent.node.as_ref()?.try_unwrap_pointer_ref().ok()?;
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
				Variant::Object => unreachable!(),
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
		let get = components.join("/");
		let get = if get.is_empty() { None } else { Some(get) };

		if let Some(tag) = tag {
			let mut reference = tag.to_string();
			if let Some(get) = get {
				write!(reference, "?get={get}").unwrap();
			}
			reference
		} else if let Some(id) = id {
			let mut reference = id.to_string();
			if let Some(get) = get {
				write!(reference, "?get={get}").unwrap();
			}
			reference
		} else {
			let mut reference = state.root.clone();
			if let Some(get) = get {
				reference.push(get);
			}
			reference.to_string_lossy().into_owned()
		}
	}
}

fn pattern_matches_specifier_or_ancestor(
	pattern: &tg::specifier::Pattern,
	specifier: &tg::Specifier,
) -> bool {
	for ancestor in specifier.prefixes() {
		if pattern.matches_specifier(&ancestor) {
			return true;
		}
	}
	false
}

impl Solutions {
	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	pub fn get(&self, key: &tg::specifier::Pattern) -> Option<&Solution> {
		self.map.get(key)
	}

	pub fn insert(&mut self, key: tg::specifier::Pattern, solution: Solution) {
		if let Some(existing) = self.map.get(&key)
			&& let Some(referent) = &existing.referent
			&& let Some(pointer) = referent.node().try_unwrap_pointer_ref().ok()
			&& pointer.graph.is_none()
			&& let Some(patterns) = self.referents.get_mut(&pointer.index)
		{
			patterns.remove(&key);
			if patterns.is_empty() {
				self.referents.remove(&pointer.index);
			}
		}
		if let Some(referent) = &solution.referent
			&& let Some(pointer) = referent.node().try_unwrap_pointer_ref().ok()
			&& pointer.graph.is_none()
		{
			self.referents
				.entry(pointer.index)
				.or_default()
				.insert(key.clone());
		}
		self.map.insert(key, solution);
	}

	pub fn remove(&mut self, key: &tg::specifier::Pattern) -> Option<Solution> {
		let solution = self.map.remove(key)?;
		let Some(referent) = &solution.referent else {
			return Some(solution);
		};
		let Some(pointer) = referent.node().try_unwrap_pointer_ref().ok() else {
			return Some(solution);
		};
		if pointer.graph.is_some() {
			return Some(solution);
		}
		if let Some(patterns) = self.referents.get_mut(&pointer.index) {
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(&pointer.index);
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
		self.map.clear();
		self.referents.clear();
		self.referrers.clear();
	}

	pub fn remove_by_node(&mut self, node: usize) {
		if let Some(patterns) = self.referents.remove(&node) {
			for pattern in &patterns {
				if let Some(solution) = self.map.remove(pattern) {
					for referrer in solution.referrers {
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
				if let Some(solution) = self.map.get_mut(&pattern) {
					solution.referrers.retain(|r| r.index != node);
					if solution.referrers.is_empty() {
						to_remove.push(pattern);
					}
				}
			}
			for pattern in to_remove {
				if let Some(solution) = self.map.remove(&pattern)
					&& let Some(referent) = &solution.referent
					&& let Some(pointer) = referent.node().try_unwrap_pointer_ref().ok()
					&& pointer.graph.is_none()
					&& let Some(referent_patterns) = self.referents.get_mut(&pointer.index)
				{
					referent_patterns.remove(&pattern);
					if referent_patterns.is_empty() {
						self.referents.remove(&pointer.index);
					}
				}
			}
		}
	}

	pub fn clear_referent(&mut self, key: &tg::specifier::Pattern) {
		if let Some(solution) = self.map.get_mut(key)
			&& let Some(referent) = solution.referent.take()
			&& let Some(pointer) = referent.node().try_unwrap_pointer_ref().ok()
			&& pointer.graph.is_none()
			&& let Some(patterns) = self.referents.get_mut(&pointer.index)
		{
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(&pointer.index);
			}
		}
	}

	pub fn add_referrer(&mut self, key: &tg::specifier::Pattern, referrer: Referrer) {
		self.referrers
			.entry(referrer.index)
			.or_default()
			.insert(key.clone());
		if let Some(solution) = self.map.get_mut(key) {
			solution.referrers.push(referrer);
		}
	}
}
