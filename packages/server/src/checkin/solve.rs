use {
	super::state::{Graph, State, Variant},
	crate::Server,
	tangram_client as tg,
};

#[derive(Clone)]
struct Checkpoint {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	queue: im::Vector<Edge>,
	tags: im::HashMap<tg::Tag, tg::Referent<usize>, fnv::FnvBuildHasher>,
	visited: im::HashSet<Edge, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
pub struct Candidate {
	object: tg::object::Id,
	tag: tg::Tag,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Edge {
	node: usize,
	variant: EdgeVariant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
enum EdgeVariant {
	DirectoryEntry(String),
	FileDependency(tg::Reference),
	SymlinkArtifact,
}

impl Server {
	pub(super) async fn checkin_solve(&self, state: &mut State) -> tg::Result<()> {
		// Create the checkpoints.
		let mut checkpoints = Vec::new();

		// Create the first checkpoint.
		let graph = state.graph.clone();
		let mut checkpoint = Checkpoint {
			candidates: None,
			graph,
			queue: im::Vector::new(),
			tags: im::HashMap::default(),
			visited: im::HashSet::default(),
		};
		Self::checkin_solve_enqueue_edges_for_node(&mut checkpoint, 0);

		// Solve.
		while let Some(edge) = checkpoint.queue.pop_front() {
			self.checkin_solve_visit_edge(state, &mut checkpoints, &mut checkpoint, edge)
				.await?;
		}

		// Use the graph from the checkpoint.
		state.graph = checkpoint.graph;

		Ok(())
	}

	async fn checkin_solve_visit_edge(
		&self,
		state: &State,
		checkpoints: &mut Vec<Checkpoint>,
		checkpoint: &mut Checkpoint,
		edge: Edge,
	) -> tg::Result<()> {
		// If the edge has been visited, then return.
		if checkpoint.visited.insert(edge.clone()).is_some() {
			return Ok(());
		}

		// If the edge is solved, then add its referent's edges to the queue and return.
		if let Some(referent) = Self::checkin_solve_get_referent_for_edge(checkpoint, &edge) {
			Self::checkin_solve_enqueue_edges_for_node(checkpoint, referent);
			return Ok(());
		}

		// Get the reference.
		let reference = edge
			.variant
			.try_unwrap_file_dependency_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a file dependency"))?
			.clone();

		// Get the pattern.
		let pattern = reference
			.item()
			.try_unwrap_tag_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a tag"))?
			.clone();

		// Validate the pattern.
		let valid = Self::checkin_solve_validate_pattern(&pattern);
		if !valid {
			return Err(tg::error!("invalid tag"));
		}

		// Get the tag.
		let tag = if matches!(
			pattern.components().last(),
			Some(tg::tag::pattern::Component::Version(_) | tg::tag::pattern::Component::Wildcard)
		) {
			pattern.parent().unwrap().try_into().unwrap()
		} else {
			pattern.clone().try_into().unwrap()
		};

		// Solve the edge.
		let result = self
			.checkin_solve_edge(state, checkpoints, checkpoint, &edge, &tag, &pattern)
			.await?;

		// Handle the result.
		match result {
			Ok(referent) => {
				// Checkpoint.
				checkpoints.push(checkpoint.clone());

				// Set the tag.
				checkpoint.tags.insert(tag, referent.clone());

				// Enqueue the node's edges.
				Self::checkin_solve_enqueue_edges_for_node(checkpoint, referent.item);
			},

			Err(()) => {
				// Backtrack.
				*checkpoint = Self::checkin_solve_backtrack(checkpoints, &tag)
					.ok_or_else(|| tg::error!("backtracking failed"))?;
			},
		}

		// Remove the candidates.
		checkpoint.candidates.take();

		Ok(())
	}

	async fn checkin_solve_edge(
		&self,
		state: &State,
		checkpoints: &mut Vec<Checkpoint>,
		checkpoint: &mut Checkpoint,
		edge: &Edge,
		tag: &tg::Tag,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Result<tg::Referent<usize>, ()>> {
		// Check if the tag is already set.
		if let Some(referent) = checkpoint.tags.get(tag) {
			return Ok(Ok(referent.clone()));
		}

		// Get the candidates if necessary.
		if checkpoint.candidates.is_none() {
			let candidates = self.checkin_solve_get_candidates(pattern).await?;
			checkpoint.candidates.replace(candidates);
		}

		// Get the next candidate.
		let Some(candidate) = checkpoint.candidates.as_mut().unwrap().pop_back() else {
			return Ok(Err(()));
		};

		let item = todo!();
		let options = tg::referent::Options {
			path: None,
			tag: Some(candidate.tag),
		};
		let referent = tg::Referent::new(item, options);

		Ok(Ok(referent))
	}

	async fn checkin_solve_get_candidates(
		&self,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<im::Vector<Candidate>> {
		let output = self
			.list_tags(tg::tag::list::Arg {
				length: None,
				pattern: pattern.clone(),
				remote: None,
				reverse: false,
			})
			.await
			.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?;
		let candidates = output
			.data
			.into_iter()
			.filter_map(|output| {
				let object = output.item.right()?;
				let tag = output.tag;
				let candidate = Candidate { object, tag };
				Some(candidate)
			})
			.collect();
		Ok(candidates)
	}

	fn checkin_solve_enqueue_edges_for_node(checkpoint: &mut Checkpoint, node: usize) {
		match &checkpoint.graph.nodes[node].variant {
			Some(Variant::Directory(directory)) => {
				let edges = directory.entries.keys().map(|name| Edge {
					node,
					variant: EdgeVariant::DirectoryEntry(name.clone()),
				});
				checkpoint.queue.extend(edges);
			},
			Some(Variant::File(file)) => {
				let edges = file.dependencies.keys().map(|reference| Edge {
					node,
					variant: EdgeVariant::FileDependency(reference.clone()),
				});
				checkpoint.queue.extend(edges);
			},
			Some(Variant::Symlink(symlink)) => {
				let edges = symlink.artifact.iter().map(|_| Edge {
					node,
					variant: EdgeVariant::SymlinkArtifact,
				});
				checkpoint.queue.extend(edges);
			},
			None => (),
		}
	}

	fn checkin_solve_get_referent_for_edge(checkpoint: &Checkpoint, edge: &Edge) -> Option<usize> {
		let node = &checkpoint.graph.nodes[edge.node];
		let variant = node.variant.as_ref().unwrap();
		match &edge.variant {
			EdgeVariant::DirectoryEntry(name) => {
				let directory = variant.unwrap_directory_ref();
				directory.entries.get(name).copied()
			},
			EdgeVariant::FileDependency(reference) => {
				let file = variant.unwrap_file_ref();
				file.dependencies
					.get(reference)
					.cloned()
					.unwrap()
					.map(|referent| referent.item)
			},
			EdgeVariant::SymlinkArtifact => {
				let symlink = variant.unwrap_symlink_ref();
				symlink.artifact.clone()
			},
		}
	}

	fn checkin_solve_backtrack(
		checkpoints: &mut Vec<Checkpoint>,
		tag: &tg::Tag,
	) -> Option<Checkpoint> {
		let position = checkpoints
			.iter()
			.position(|checkpoint| checkpoint.tags.contains_key(tag))?;
		checkpoints.truncate(position + 1);
		checkpoints.pop()
	}

	fn checkin_solve_validate_pattern(pattern: &tg::tag::Pattern) -> bool {
		let all_components_normal = pattern
			.components()
			.iter()
			.all(tg::tag::pattern::Component::is_normal);
		let mut components_reverse_iter = pattern.components().iter().rev();
		let last_component_version_or_wildcard = matches!(
			components_reverse_iter.next(),
			Some(tg::tag::pattern::Component::Version(_) | tg::tag::pattern::Component::Wildcard)
		);
		let all_components_except_last_normal =
			components_reverse_iter.all(tg::tag::pattern::Component::is_normal);
		let all_components_normal_except_last =
			last_component_version_or_wildcard && all_components_except_last_normal;
		all_components_normal || all_components_normal_except_last
	}
}
