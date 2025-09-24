use {
	crate::{
		Server,
		checkin::state::{Directory, File, Graph, Node, State, Symlink, Variant},
	},
	tangram_client::{self as tg, prelude::*},
	tangram_either::Either,
};

#[derive(Clone)]
struct Checkpoint<'a> {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	graphs: im::HashMap<tg::graph::Id, usize, fnv::FnvBuildHasher>,
	ids: im::HashMap<tg::artifact::Id, usize, fnv::FnvBuildHasher>,
	queue: im::Vector<Item>,
	state: &'a State,
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
			ids: im::HashMap::default(),
			queue: im::Vector::new(),
			state,
			tags: im::HashMap::default(),
			visited: im::HashSet::default(),
		};
		Self::checkin_solve_enqueue_items_for_node(&mut checkpoint, 0);

		// Solve.
		while let Some(item) = checkpoint.queue.pop_front() {
			self.checkin_solve_visit_item(&mut checkpoints, &mut checkpoint, item)
				.await?;
		}

		// Use the graph from the checkpoint.
		state.graph = checkpoint.graph;

		Ok(())
	}

	async fn checkin_solve_visit_item<'a>(
		&self,
		checkpoints: &mut Vec<Checkpoint<'a>>,
		checkpoint: &mut Checkpoint<'a>,
		item: Item,
	) -> tg::Result<()> {
		// If the item has been visited, then return.
		if checkpoint.visited.insert(item.clone()).is_some() {
			return Ok(());
		}

		// If the item is solved, then add its destination's items to the queue and return.
		if let Some(destination) = Self::checkin_solve_get_destination_for_item(checkpoint, &item) {
			let destination = match destination {
				tg::graph::data::Edge::Reference(reference) => Some(reference.node),
				tg::graph::data::Edge::Object(id) => {
					if let Ok(id) = tg::artifact::Id::try_from(id) {
						let index = self.checkin_solve_add_node(checkpoint, &item, &id).await?;
						let node = &mut checkpoint.graph.nodes[item.node];
						match &item.variant {
							ItemVariant::DirectoryEntry(name) => {
								let edge =
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph: None,
										node: index,
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
										node: index,
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
										node: index,
									});
								*node.variant.unwrap_symlink_mut().artifact.as_mut().unwrap() =
									edge;
							},
						}
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

		// Handle different reference item types.
		match reference.item() {
			tg::reference::Item::Object(id) => {
				self.checkin_solve_visit_item_with_object(
					checkpoint,
					item,
					reference.clone(),
					id.clone(),
				)
				.await
			},

			tg::reference::Item::Tag(pattern) => {
				self.checkin_solve_visit_item_with_tag(
					checkpoints,
					checkpoint,
					item,
					reference.clone(),
					pattern.clone(),
				)
				.await
			},

			_ => Err(tg::error!("unsupported reference item type")),
		}
	}

	async fn checkin_solve_visit_item_with_object(
		&self,
		checkpoint: &mut Checkpoint<'_>,
		item: Item,
		reference: tg::Reference,
		id: tg::object::Id,
	) -> tg::Result<()> {
		let Ok(id) = tg::artifact::Id::try_from(id) else {
			return Ok(());
		};

		// Check if there is already a node for the object.
		let index = if let Some(index) = checkpoint.ids.get(&id).copied() {
			index
		} else {
			let index = self.checkin_solve_add_node(checkpoint, &item, &id).await?;
			checkpoint.ids.insert(id.clone(), index);
			index
		};

		// Create the edge.
		let edge = tg::graph::data::Edge::Reference(tg::graph::data::Reference {
			graph: None,
			node: index,
		});
		checkpoint.graph.nodes[item.node]
			.variant
			.unwrap_file_mut()
			.dependencies
			.get_mut(&reference)
			.unwrap()
			.get_or_insert_with(|| tg::Referent::with_item(edge.clone()))
			.item = edge.clone();

		// Enqueue the node's items.
		Self::checkin_solve_enqueue_items_for_node(checkpoint, index);

		Ok(())
	}

	async fn checkin_solve_visit_item_with_tag<'a>(
		&self,
		checkpoints: &mut Vec<Checkpoint<'a>>,
		checkpoint: &mut Checkpoint<'a>,
		item: Item,
		reference: tg::Reference,
		pattern: tg::tag::Pattern,
	) -> tg::Result<()> {
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
		let result = self
			.checkin_solve_visit_item_with_tag_inner(checkpoint, &item, &tag, &pattern)
			.await?;

		// Handle the result.
		match result {
			Ok(referent) => {
				// Set the tag.
				checkpoint.tags.insert(tag, referent.clone());

				// Checkpoint.
				checkpoints.push(checkpoint.clone());

				// Create the edge.
				checkpoint.graph.nodes[item.node]
					.variant
					.unwrap_file_mut()
					.dependencies
					.iter_mut()
					.find_map(|(r, referent)| (r == &reference).then_some(referent))
					.unwrap()
					.replace(referent.clone().map(|item| {
						tg::graph::data::Edge::Reference(tg::graph::data::Reference {
							graph: None,
							node: item,
						})
					}));

				// Enqueue the node's items.
				Self::checkin_solve_enqueue_items_for_node(checkpoint, referent.item);
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

	async fn checkin_solve_visit_item_with_tag_inner(
		&self,
		checkpoint: &mut Checkpoint<'_>,
		item: &Item,
		tag: &tg::Tag,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Result<tg::Referent<usize>, ()>> {
		// Check if the tag is already set.
		if let Some(referent) = checkpoint.tags.get(tag) {
			return Ok(Ok(referent.clone()));
		}

		// Get the candidates if necessary.
		if checkpoint.candidates.is_none() {
			let candidates = self
				.checkin_solve_get_candidates(checkpoint, item, pattern)
				.await?;
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
		let node = self.checkin_solve_add_node(checkpoint, item, &id).await?;

		// Create the referent.
		let item = node;
		let options = tg::referent::Options {
			id: Some(id.into()),
			tag: Some(candidate.tag),
			..Default::default()
		};
		let referent = tg::Referent::new(item, options);

		Ok(Ok(referent))
	}

	async fn checkin_solve_get_candidates(
		&self,
		checkpoint: &Checkpoint<'_>,
		item: &Item,
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
		let mut candidates = output
			.data
			.into_iter()
			.filter_map(|output| {
				let object = output.item?.right()?;
				let tag = output.tag;
				let candidate = Candidate { object, tag };
				Some(candidate)
			})
			.collect::<im::Vector<_>>();
		if let Some(lock_index) = checkpoint.graph.nodes[item.node].lock_node
			&& let Some(candidate) =
				Self::checkin_solve_get_candidate_from_lock(checkpoint, item, lock_index)
		{
			candidates.push_back(candidate);
		}
		Ok(candidates)
	}

	fn checkin_solve_get_candidate_from_lock(
		checkpoint: &Checkpoint<'_>,
		item: &Item,
		lock_index: usize,
	) -> Option<Candidate> {
		let lock_node = &checkpoint.state.lock.as_ref().unwrap().nodes[lock_index];
		let options = if let ItemVariant::FileDependency(reference) = &item.variant {
			lock_node
				.try_unwrap_file_ref()
				.ok()?
				.dependencies
				.get(reference)?
				.options()
		} else {
			return None;
		};
		let object = options.id.clone()?;
		let tag = options.tag.clone()?;
		let candidate = Candidate { object, tag };
		Some(candidate)
	}

	async fn checkin_solve_add_node(
		&self,
		checkpoint: &mut Checkpoint<'_>,
		item: &Item,
		id: &tg::artifact::Id,
	) -> tg::Result<usize> {
		let output = self
			.get_object(&id.clone().into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let data = tg::artifact::Data::deserialize(id.kind(), output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
		let variant = match data {
			tg::artifact::Data::Directory(tg::directory::Data::Reference(reference))
			| tg::artifact::Data::File(tg::file::Data::Reference(reference))
			| tg::artifact::Data::Symlink(tg::symlink::Data::Reference(reference)) => {
				let graph = reference
					.graph
					.ok_or_else(|| tg::error!("expected the graph to be set"))?;
				let graph = self.checkin_solve_add_graph(checkpoint, &graph).await?;
				let node = graph + reference.node;
				return Ok(node);
			},
			tg::artifact::Data::Directory(tg::directory::Data::Node(directory)) => {
				Variant::Directory(Directory {
					entries: directory.entries,
				})
			},
			tg::artifact::Data::File(tg::file::Data::Node(file)) => {
				let contents = file.contents.map(Either::Right);
				let dependencies = file
					.dependencies
					.into_iter()
					.map(|(reference, referent)| {
						if reference.item().is_tag() {
							(reference, None)
						} else {
							(reference, Some(referent))
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
			lock_node,
			object_id: None,
			parents: None,
			path: None,
			path_metadata: None,
			variant,
		};
		let index = checkpoint.graph.nodes.len();
		checkpoint.graph.nodes.push_back(node);
		Ok(index)
	}

	async fn checkin_solve_add_graph(
		&self,
		checkpoint: &mut Checkpoint<'_>,
		id: &tg::graph::Id,
	) -> tg::Result<usize> {
		if let Some(index) = checkpoint.graphs.get(id).copied() {
			return Ok(index);
		}
		let index = checkpoint.graph.nodes.len();
		let output = self
			.get_object(&id.clone().into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let data = tg::graph::Data::deserialize(output.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the object"))?;
		for node in data.nodes {
			let variant = match node {
				tg::graph::data::Node::Directory(mut directory) => {
					for mut edge in directory.entries.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = &mut edge {
							reference.node += index;
						}
					}
					Variant::Directory(Directory {
						entries: directory.entries,
					})
				},
				tg::graph::data::Node::File(mut file) => {
					for referent in file.dependencies.values_mut() {
						if let tg::graph::data::Edge::Reference(reference) = &mut referent.item {
							reference.node += index;
						}
					}
					let contents = file.contents.map(Either::Right);
					let dependencies = file
						.dependencies
						.into_iter()
						.map(|(reference, referent)| {
							if reference.item().is_tag() {
								(reference, None)
							} else {
								(reference, Some(referent))
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
				tg::graph::data::Node::Symlink(mut symlink) => {
					if let Some(tg::graph::data::Edge::Reference(reference)) = &mut symlink.artifact
					{
						reference.node += index;
					}
					Variant::Symlink(Symlink {
						artifact: symlink.artifact,
						path: symlink.path,
					})
				},
			};
			let node = Node {
				lock_node: None,
				object_id: None,
				parents: None,
				path: None,
				path_metadata: None,
				variant,
			};
			checkpoint.graph.nodes.push_back(node);
		}
		checkpoint.graphs.insert(id.clone(), index);
		Ok(index)
	}

	fn checkin_solve_get_lock_node(checkpoint: &Checkpoint, item: &Item) -> Option<usize> {
		let Some(lock) = &checkpoint.state.lock else {
			return None;
		};
		let parent_index = checkpoint.graph.nodes[item.node].lock_node?;
		let parent_node = &lock.nodes[parent_index];
		match &item.variant {
			ItemVariant::DirectoryEntry(name) => Some(
				parent_node
					.try_unwrap_directory_ref()
					.ok()?
					.entries
					.get(name)?
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
			ItemVariant::FileDependency(reference) => Some(
				parent_node
					.try_unwrap_file_ref()
					.ok()?
					.dependencies
					.get(reference)?
					.item()
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
			ItemVariant::SymlinkArtifact => Some(
				parent_node
					.try_unwrap_symlink_ref()
					.ok()?
					.artifact
					.as_ref()?
					.try_unwrap_reference_ref()
					.ok()?
					.node,
			),
		}
	}

	fn checkin_solve_enqueue_items_for_node(checkpoint: &mut Checkpoint, node: usize) {
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

	fn checkin_solve_backtrack<'a>(
		checkpoints: &mut Vec<Checkpoint<'a>>,
		tag: &tg::Tag,
	) -> Option<Checkpoint<'a>> {
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
