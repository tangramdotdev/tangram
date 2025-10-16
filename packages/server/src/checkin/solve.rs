use {
	crate::{
		Server,
		checkin::state::{Directory, File, Graph, Node, State, Symlink, Variant},
	},
	smallvec::SmallVec,
	std::collections::HashMap,
	tangram_client::{self as tg, handle::Ext as _},
	tangram_either::Either,
};

struct Context<'a> {
	checkpoints: Vec<Checkpoint<'a>>,
	tags: HashMap<tg::Tag, Vec<(usize, tg::tag::Pattern)>, fnv::FnvBuildHasher>,
	updates: &'a [tg::tag::Pattern],
}

#[derive(Clone)]
struct Checkpoint<'a> {
	candidates: Option<im::Vector<Candidate>>,
	graph: Graph,
	graphs: im::HashMap<tg::graph::Id, tg::graph::Data, tg::id::BuildHasher>,
	graph_nodes: im::HashMap<(tg::graph::Id, usize), usize, fnv::FnvBuildHasher>,
	ids: im::HashMap<tg::artifact::Id, usize, tg::id::BuildHasher>,
	queue: im::Vector<Item>,
	lock: Option<&'a tg::graph::Data>,
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
		let State {
			arg, graph, lock, ..
		} = state;

		// Create the context
		let mut context = Context {
			checkpoints: Vec::new(),
			tags: HashMap::default(),
			updates: &arg.updates,
		};

		// Create the first checkpoint.
		let graph = graph.clone();
		let mut checkpoint = Checkpoint {
			candidates: None,
			graph,
			graphs: im::HashMap::default(),
			graph_nodes: im::HashMap::default(),
			ids: im::HashMap::default(),
			lock: lock.as_ref(),
			queue: im::Vector::new(),
			tags: im::HashMap::default(),
			visited: im::HashSet::default(),
		};
		Self::checkin_solve_enqueue_items_for_node(&mut checkpoint, 0);

		// Solve.
		while let Some(item) = checkpoint.queue.pop_front() {
			self.checkin_solve_visit_item(&mut context, &mut checkpoint, item)
				.await?;
		}

		// Use the graph from the checkpoint.
		state.graph = checkpoint.graph;

		Ok(())
	}

	async fn checkin_solve_visit_item<'a>(
		&self,
		context: &mut Context<'a>,
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
				tg::graph::data::Edge::Reference(reference) => {
					if let Some(graph_id) = &reference.graph {
						let index = self
							.checkin_solve_add_graph_node(
								checkpoint,
								&item,
								graph_id,
								reference.node,
							)
							.await?;
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
						checkpoint.graph.nodes[index].referrers.push(item.node);
						Some(index)
					} else {
						Some(reference.node)
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
						checkpoint.graph.nodes[index].referrers.push(item.node);
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
					context,
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
		checkpoint.graph.nodes[index].referrers.push(item.node);

		// Enqueue the node's items.
		Self::checkin_solve_enqueue_items_for_node(checkpoint, index);

		Ok(())
	}

	async fn checkin_solve_visit_item_with_tag<'a>(
		&self,
		context: &mut Context<'a>,
		checkpoint: &mut Checkpoint<'a>,
		item: Item,
		reference: tg::Reference,
		pattern: tg::tag::Pattern,
	) -> tg::Result<()> {
		// Get the tag.
		let tag: tg::Tag = if pattern
			.components()
			.last()
			.is_some_and(|component| component.contains(['=', '>', '<', '^']))
		{
			pattern.parent().unwrap().try_into().unwrap()
		} else {
			pattern.clone().try_into().unwrap()
		};

		// Insert the tag.
		context
			.tags
			.entry(tag.clone())
			.or_default()
			.push((item.node, pattern.clone()));

		// Solve the item.
		let result = self
			.checkin_solve_visit_item_with_tag_inner(context, checkpoint, &item, &tag, &pattern)
			.await?;

		// Handle the result.
		if let Ok(referent) = result {
			// Checkpoint.
			context.checkpoints.push(checkpoint.clone());

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
			checkpoint.graph.nodes[referent.item]
				.referrers
				.push(item.node);

			// Enqueue the node's items.
			Self::checkin_solve_enqueue_items_for_node(checkpoint, referent.item);
		} else {
			// Backtrack.
			*checkpoint = Self::checkin_solve_backtrack(context, &tag)
				.ok_or_else(|| explain(context, &checkpoint.graph, &tag))?;

			return Ok(());
		}

		// Remove the candidates.
		checkpoint.candidates.take();

		Ok(())
	}

	async fn checkin_solve_visit_item_with_tag_inner(
		&self,
		context: &Context<'_>,
		checkpoint: &mut Checkpoint<'_>,
		item: &Item,
		tag: &tg::Tag,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Result<tg::Referent<usize>, ()>> {
		// Check if the tag is already set.
		if let Some(referent) = checkpoint.tags.get(tag) {
			if !pattern.matches(referent.tag().unwrap()) {
				return Ok(Err(()));
			}
			// Return the referent.
			return Ok(Ok(referent.clone()));
		}

		// Get the candidates if necessary.
		if checkpoint.candidates.is_none() {
			// Try to get an initial list of candidates.
			let candidates = self
				.checkin_solve_get_candidates(context, checkpoint, item, pattern)
				.await
				.inspect_err(|e| {
					tracing::error!(?e, "failed to get matching tags for {pattern}");
				})?;

			// If the list was empty, return an error that the tag is bogus.
			if candidates.is_empty() {
				return Err(tg::error!(
					"{} but no matching tags were found",
					format_dependency(&checkpoint.graph, item.node, pattern)
				));
			}

			// Update the list of candidates.
			checkpoint.candidates.replace(candidates);
		}

		// Get the next candidate.
		let candidate = checkpoint.candidates.as_mut().unwrap().pop_back().unwrap();

		// Add the node.
		let id = candidate
			.object
			.try_into()
			.map_err(|_| tg::error!("expected an artifact"))?;
		let node = self.checkin_solve_add_node(checkpoint, item, &id).await?;

		// Create the referent.
		let new_item = node;
		let options = tg::referent::Options {
			id: Some(id.into()),
			tag: Some(candidate.tag),
			..Default::default()
		};
		let referent = tg::Referent::new(new_item, options);

		// Update the tags.
		checkpoint.tags.insert(tag.clone(), referent.clone());

		Ok(Ok(referent))
	}

	async fn checkin_solve_get_candidates(
		&self,
		context: &Context<'_>,
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
			&& !context
				.updates
				.iter()
				.any(|pattern| pattern.matches(&candidate.tag))
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
		let lock_node = &checkpoint.lock.as_ref().unwrap().nodes[lock_index];
		let options = if let ItemVariant::FileDependency(reference) = &item.variant {
			lock_node
				.try_unwrap_file_ref()
				.ok()?
				.dependencies
				.get(reference)?
				.as_ref()?
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
					.checkin_solve_add_graph_node(checkpoint, item, &graph, reference.node)
					.await;
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
			lock_node,
			object_id: None,
			referrers: SmallVec::new(),
			path: None,
			path_metadata: None,
			variant,
		};

		// Insert the node into the graph.
		let index = checkpoint.graph.nodes.len();
		checkpoint.graph.nodes.push_back(node);

		Ok(index)
	}

	async fn checkin_solve_add_graph_node(
		&self,
		checkpoint: &mut Checkpoint<'_>,
		item: &Item,
		graph_id: &tg::graph::Id,
		node_index: usize,
	) -> tg::Result<usize> {
		// Check if this graph node has already been added.
		let key = (graph_id.clone(), node_index);
		if let Some(&index) = checkpoint.graph_nodes.get(&key) {
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
								node: reference.node,
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
				let contents = file.contents.as_ref().map(|id| Either::Right(id.clone()));
				let mut dependencies = std::collections::BTreeMap::new();
				for (reference, referent) in &file.dependencies {
					let Some(referent) = referent else {
						if !reference.item().is_tag() {
							return Err(
								tg::error!(%reference, "unresolved reference in file dependencies"),
							);
						}
						dependencies.insert(reference.clone(), None);
						continue;
					};
					if referent.options.tag.is_some() {
						dependencies.insert(reference.clone(), None);
					} else {
						let referent = tg::Referent {
							item: match &referent.item {
								tg::graph::data::Edge::Reference(reference) => {
									let graph =
										reference.graph.clone().or_else(|| Some(graph_id.clone()));
									tg::graph::data::Edge::Reference(tg::graph::data::Reference {
										graph,
										node: reference.node,
									})
								},
								tg::graph::data::Edge::Object(id) => {
									tg::graph::data::Edge::Object(id.clone())
								},
							},
							options: referent.options.clone(),
						};
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
							node: reference.node,
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
			lock_node,
			object_id: None,
			referrers: SmallVec::new(),
			path: None,
			path_metadata: None,
			variant,
		};

		// Add the node to the checkin graph.
		let index = checkpoint.graph.nodes.len();
		checkpoint.graph.nodes.push_back(node);

		// Cache the mapping.
		checkpoint.graph_nodes.insert(key, index);

		Ok(index)
	}

	fn checkin_solve_get_lock_node(checkpoint: &Checkpoint, item: &Item) -> Option<usize> {
		let Some(lock) = &checkpoint.lock else {
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
					.as_ref()?
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
		context: &mut Context<'a>,
		tag: &tg::Tag,
	) -> Option<Checkpoint<'a>> {
		let position = context
			.checkpoints
			.iter()
			.position(|checkpoint| checkpoint.tags.contains_key(tag))?;
		if context.checkpoints[position]
			.candidates
			.as_ref()
			.unwrap()
			.is_empty()
		{
			return None;
		}
		context.checkpoints.truncate(position);
		let mut checkpoint = context.checkpoints.pop()?;
		checkpoint.tags.remove(tag);
		context.tags.remove(tag);
		Some(checkpoint)
	}
}

fn explain(context: &Context, graph: &Graph, tag: &tg::Tag) -> tg::Error {
	let mut source = None;
	let referrers = context.tags.get(tag).unwrap();
	for (node, pattern) in referrers {
		let mut error = tg::error!("{}", format_dependency(graph, *node, pattern));
		if let Some(source) = source.take() {
			error.source.replace(tg::Referent::with_item(source));
		}
		source.replace(Box::new(error));
	}
	tg::error!(
		source = source.unwrap(),
		"failed to find a matching tag for '{tag}'"
	)
}

fn format_dependency(graph: &Graph, node: usize, pattern: &tg::tag::Pattern) -> String {
	let referrer = &graph.nodes[node];
	if let Some(path) = &referrer.path {
		return format!("{} requires '{pattern}'", path.display());
	}
	if let Some(id) = &graph.nodes[node].object_id {
		return format!("{id} requires '{pattern}'");
	}
	let mut tag = None;
	let mut id = None;
	let mut components = vec![];
	let mut current = node;
	while tag.is_none() && id.is_none() {
		let parent = *graph.nodes[current].referrers.first().unwrap();
		match &graph.nodes[parent].variant {
			Variant::Directory(directory) => {
				let name = directory
					.entries
					.iter()
					.find_map(|(name, edge)| {
						let reference = edge.try_unwrap_reference_ref().ok()?;
						if reference.graph.is_some() {
							return None;
						}
						(reference.node == current).then_some(name.clone())
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
						(reference.node == current).then_some(referent)
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
	let name = if let Some(tag) = tag {
		format!("{tag}:{path}")
	} else if let Some(id) = id {
		format!("{id}:{path}")
	} else {
		path
	};
	format!("{name} requires '{pattern}'")
}
