use {
	crate::{
		Server,
		checkin::state::{Directory, File, Graph, Node, State, Symlink, Variant},
	},
	smallvec::SmallVec,
	tangram_client::{self as tg, prelude::*},
	tangram_either::Either,
};

#[derive(Clone)]
struct Checkpoint {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	graphs: im::HashMap<tg::graph::Id, tg::graph::Data, fnv::FnvBuildHasher>,
	queue: im::Vector<Item>,
	tags: im::HashMap<tg::Tag, tg::Referent<usize>, fnv::FnvBuildHasher>,
	visited: im::HashSet<Item, fnv::FnvBuildHasher>,
}

#[derive(Clone, Debug)]
pub struct Candidate {
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

impl Server {
	pub(super) async fn checkin_solve(&self, state: &mut State) -> tg::Result<()> {
		// Create the checkpoints.
		let mut checkpoints = Vec::new();

		// Create the first checkpoint.
		let graph = state.graph.clone();
		let mut checkpoint = Checkpoint {
			candidates: None,
			graph,
			graphs: im::HashMap::default(),
			queue: im::Vector::new(),
			tags: im::HashMap::default(),
			visited: im::HashSet::default(),
		};
		let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
			graph: None,
			node: 0,
		});
		self.checkin_solve_enqueue_items_for_node(&mut checkpoint, &edge)
			.await?;

		// Solve.
		while let Some(item) = checkpoint.queue.pop_front() {
			self.checkin_solve_visit_item(&mut checkpoints, &mut checkpoint, item)
				.await?;
		}

		// Use the graph from the checkpoint.
		state.graph = checkpoint.graph;

		Ok(())
	}

	async fn checkin_solve_visit_item(
		&self,
		checkpoints: &mut Vec<Checkpoint>,
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
					Some(tg::graph::data::Edge::Reference(reference))
				},
				tg::graph::data::Edge::Object(id) => {
					id.try_into().ok().map(tg::graph::data::Edge::Object)
				},
			};
			if let Some(destination) = destination {
				self.checkin_solve_enqueue_items_for_node(checkpoint, &destination)
					.await?;
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

		// Solve the item.
		let result = self.checkin_solve_item(checkpoint, &tag, &pattern).await?;

		// Handle the result.
		match result {
			Ok(referent) => {
				// Set the tag.
				checkpoint.tags.insert(tag, referent.clone());

				// Checkpoint.
				checkpoints.push(checkpoint.clone());

				// Create the edge.
				let index = referent.item;
				checkpoint.graph.nodes[item.node]
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(reference, referent)| {
						(reference == item.variant.unwrap_file_dependency_ref()).then_some(referent)
					})
					.unwrap()
					.replace(referent.map(|item| {
						tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							graph: None,
							node: item,
						})
					}));
				let node = &mut checkpoint.graph.nodes[index];
				if !node.parents.contains(&item.node) {
					node.parents.push(item.node);
				}

				// Enqueue the node's items.
				let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
					graph: None,
					node: index,
				});
				self.checkin_solve_enqueue_items_for_node(checkpoint, &edge)
					.await?;
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

	async fn checkin_solve_item(
		&self,
		checkpoint: &mut Checkpoint,
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

		// Add the node.
		let id = candidate
			.object
			.try_into()
			.map_err(|_| tg::error!("expected an artifact"))?;
		let edge = tg::graph::data::Edge::Object(id);
		let node = self.checkin_solve_add_node(checkpoint, &edge).await?;

		// Create the referent.
		let item = node;
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

	async fn checkin_solve_add_node(
		&self,
		checkpoint: &mut Checkpoint,
		node: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<usize> {
		if let Some(node) = node
			.try_unwrap_reference_ref()
			.ok()
			.filter(|reference| reference.graph.is_none())
			.map(|reference| reference.node)
		{
			return Ok(node);
		}
		let (_, data, _) = self.checkin_solve_get_node(checkpoint, node).await?;
		let variant = match data {
			tg::graph::data::Node::Directory(directory) => Variant::Directory(Directory {
				entries: directory.entries,
			}),
			tg::graph::data::Node::File(file) => Variant::File(File {
				contents: file.contents.map(Either::Right),
				dependencies: file
					.dependencies
					.into_keys()
					.map(|reference| (reference, None))
					.collect(),
				executable: file.executable,
			}),
			tg::graph::data::Node::Symlink(symlink) => Variant::Symlink(Symlink {
				artifact: symlink.artifact,
				path: symlink.path,
			}),
		};
		let node = Node {
			lock_node: None,
			object_id: None,
			parents: SmallVec::new(),
			path: None,
			path_metadata: None,
			variant,
		};
		let index = checkpoint.graph.nodes.len();
		checkpoint.graph.nodes.push_back(node);
		Ok(index)
	}

	async fn checkin_solve_get_node(
		&self,
		checkpoint: &mut Checkpoint,
		edge: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<(
		tg::artifact::Id,
		tg::graph::data::Node,
		Option<tg::graph::Id>,
	)> {
		match edge {
			// If this is a reference, then load the graph and return the node.
			tg::graph::data::Edge::Reference(reference) => {
				// Get the graph.
				let graph = reference
					.graph
					.as_ref()
					.ok_or_else(|| tg::error!("missing graph"))?;

				// Ensure the graph is cached.
				if !checkpoint.graphs.contains_key(graph) {
					let output = self
						.get_object(&graph.clone().into())
						.await
						.map_err(|source| tg::error!(!source, "failed to get the object"))?;
					let data = tg::graph::Data::deserialize(output.bytes).map_err(|source| {
						tg::error!(!source, "failed to deserialize the object")
					})?;
					checkpoint.graphs.insert(graph.clone(), data);
				}

				// Get the node.
				let node = checkpoint
					.graphs
					.get(graph)
					.unwrap()
					.nodes
					.get(reference.node)
					.ok_or_else(|| tg::error!("invalid graph node"))?
					.clone();

				// Create the data.
				let data: tg::artifact::data::Artifact = match node.kind() {
					tg::artifact::Kind::Directory => {
						tg::directory::Data::Reference(reference.clone()).into()
					},
					tg::artifact::Kind::File => tg::file::Data::Reference(reference.clone()).into(),
					tg::artifact::Kind::Symlink => {
						tg::symlink::Data::Reference(reference.clone()).into()
					},
				};

				// Create the ID.
				let id = tg::artifact::Id::new(node.kind(), &data.serialize()?);

				Ok((id, node, Some(graph.clone())))
			},
			tg::graph::data::Edge::Object(id) => {
				let output = self
					.get_object(&id.clone().into())
					.await
					.map_err(|source| tg::error!(!source, "failed to get the object"))?;
				let data = tg::artifact::Data::deserialize(id.kind(), output.bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
				match data {
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Reference(
						reference,
					))
					| tg::artifact::data::Artifact::File(tg::file::Data::Reference(reference))
					| tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Reference(
						reference,
					)) => {
						let (_, node, graph) = Box::pin(self.checkin_solve_get_node(
							checkpoint,
							&tg::graph::data::Edge::Reference(reference),
						))
						.await?;
						Ok((id.clone(), node, graph))
					},
					tg::artifact::data::Artifact::Directory(tg::directory::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::Directory(node), None))
					},
					tg::artifact::data::Artifact::File(tg::file::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::File(node), None))
					},
					tg::artifact::data::Artifact::Symlink(tg::symlink::Data::Node(node)) => {
						Ok((id.clone(), tg::graph::data::Node::Symlink(node), None))
					},
				}
			},
		}
	}

	async fn checkin_solve_enqueue_items_for_node(
		&self,
		checkpoint: &mut Checkpoint,
		node: &tg::graph::data::Edge<tg::artifact::Id>,
	) -> tg::Result<()> {
		let node = self.checkin_solve_add_node(checkpoint, node).await?;
		match &checkpoint.graph.nodes[node].variant {
			Variant::Directory(directory) => {
				let items = directory.entries.keys().map(|name| Item {
					node,
					variant: ItemVariant::DirectoryEntry(name.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::File(file) => {
				let items = file.dependencies.keys().map(|reference| Item {
					node,
					variant: ItemVariant::FileDependency(reference.clone()),
				});
				checkpoint.queue.extend(items);
			},
			Variant::Symlink(symlink) => {
				let items = symlink.artifact.iter().map(|_| Item {
					node,
					variant: ItemVariant::SymlinkArtifact,
				});
				checkpoint.queue.extend(items);
			},
		}
		Ok(())
	}

	fn checkin_solve_get_destination_for_item(
		checkpoint: &Checkpoint,
		item: &Item,
	) -> Option<tg::graph::data::Edge<tg::object::Id>> {
		let node = &checkpoint.graph.nodes[item.node];
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

	fn checkin_solve_backtrack(
		checkpoints: &mut Vec<Checkpoint>,
		tag: &tg::Tag,
	) -> Option<Checkpoint> {
		let position = checkpoints
			.iter()
			.position(|checkpoint| checkpoint.tags.contains_key(tag))?;
		checkpoints.truncate(position + 1);
		let mut checkpoint = checkpoints.pop()?;
		checkpoint.tags.remove(tag);
		Some(checkpoint)
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
