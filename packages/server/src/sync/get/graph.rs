use {
	indexmap::{IndexMap, IndexSet},
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashSet},
	tangram_client::prelude::*,
	tangram_either::Either,
	tangram_util::iter::Ext as _,
};

#[derive(Default)]
pub struct Graph {
	pub nodes: IndexMap<Id, Node, fnv::FnvBuildHasher>,
	pub roots: IndexSet<Id, fnv::FnvBuildHasher>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Id {
	Object(tg::object::Id),
	Process(tg::process::Id),
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Node {
	Object(ObjectNode),
	Process(ProcessNode),
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	pub marked: bool,
	pub metadata: Option<tg::object::Metadata>,
	pub parents: SmallVec<[usize; 1]>,
	pub requested: Option<Requested>,
	pub stored: Option<crate::object::stored::Output>,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub marked: bool,
	pub metadata: Option<tg::process::Metadata>,
	pub objects: Option<Vec<(usize, crate::index::message::ProcessObjectKind)>>,
	pub parents: SmallVec<[usize; 1]>,
	pub requested: Option<Requested>,
	pub stored: Option<crate::process::stored::Output>,
}

#[derive(Clone, Debug, Default)]
pub struct Requested {
	pub eager: bool,
}

impl Graph {
	pub fn new(roots: &[Either<tg::object::Id, tg::process::Id>]) -> Self {
		let roots = roots
			.iter()
			.map(|id| match id {
				Either::Left(id) => Id::Object(id.clone()),
				Either::Right(id) => Id::Process(id.clone()),
			})
			.collect();
		Graph {
			nodes: IndexMap::default(),
			roots,
		}
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		data: Option<&tg::object::Data>,
		stored: Option<crate::object::stored::Output>,
		metadata: Option<tg::object::Metadata>,
		marked: Option<bool>,
		requested: Option<Requested>,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		let children = if let Some(data) = data {
			let mut children = BTreeSet::new();
			data.children(&mut children);
			let children: Vec<usize> = children
				.into_iter()
				.map(|child| {
					let child_entry = self.nodes.entry(child.into());
					let child_index = child_entry.index();
					let child_node =
						child_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
					child_node.unwrap_object_mut().parents.push(index);
					child_index
				})
				.collect();
			Some(children)
		} else {
			None
		};

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();
		let node_old_stored = node.stored.clone();
		if let Some(children) = children {
			let subtree_stored = children.iter().all(|child| {
				self.nodes
					.get_index(*child)
					.unwrap()
					.1
					.unwrap_object_ref()
					.stored
					.as_ref()
					.is_some_and(|stored| stored.subtree)
			});
			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_object_mut();
			node.children = Some(children);
			node.stored = Some(crate::object::stored::Output {
				subtree: subtree_stored,
			});
		}

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_object_mut();
		if let Some(stored) = stored {
			node.stored = Some(stored);
		}
		if let Some(new_metadata) = metadata {
			if let Some(existing) = &mut node.metadata {
				existing.node.size = new_metadata.node.size;
				if new_metadata.subtree.count.is_some() {
					existing.subtree.count = new_metadata.subtree.count;
				}
				if new_metadata.subtree.depth.is_some() {
					existing.subtree.depth = new_metadata.subtree.depth;
				}
				if new_metadata.subtree.size.is_some() {
					existing.subtree.size = new_metadata.subtree.size;
				}
				if new_metadata.subtree.solvable.is_some() {
					existing.subtree.solvable = new_metadata.subtree.solvable;
				}
				if new_metadata.subtree.solved.is_some() {
					existing.subtree.solved = new_metadata.subtree.solved;
				}
			} else {
				node.metadata = Some(new_metadata);
			}
		}
		if let Some(marked) = marked {
			node.marked = marked;
		}
		if let Some(requested) = requested {
			node.requested = Some(requested);
		}

		let old_stored = node_old_stored
			.as_ref()
			.is_some_and(|stored| stored.subtree);
		let new_stored = node.stored.as_ref().is_some_and(|stored| stored.subtree);
		// Propagate subtree stored.
		if !old_stored && new_stored {
			// Check if this node is a root that just completed.
			let mut stack: Vec<usize> = node.parents.iter().copied().collect();
			while let Some(parent_index) = stack.pop() {
				// Get parent info, cloning what we need so we can release the borrow.
				let Some((parent_id, children, parent_parents)) =
					self.nodes.get_index(parent_index).and_then(|(id, node)| {
						let node = node.try_unwrap_object_ref().ok()?;
						if node.stored.as_ref().is_some_and(|s| s.subtree) {
							return None;
						}
						let children = node.children.as_ref()?.clone();
						Some((id.clone(), children, node.parents.clone()))
					})
				else {
					continue;
				};

				// Check if all children are now stored.
				let all_children_stored = children.iter().all(|child_index| {
					self.nodes
						.get_index(*child_index)
						.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.stored.as_ref())
						.is_some_and(|s| s.subtree)
				});

				if all_children_stored {
					// Update the parent's stored status.
					if let Some((_, node)) = self.nodes.get_index_mut(parent_index)
						&& let Ok(obj) = node.try_unwrap_object_mut()
					{
						obj.stored = Some(crate::object::stored::Output { subtree: true });
					}

					// Check if this parent is a root that just completed.
					if self.roots.contains(&parent_id) {
						tracing::trace!(%parent_id, "root is now complete");
					}

					// Add grandparents to the stack.
					stack.extend(parent_parents);
				}
			}
		}
	}

	pub fn update_process(
		&mut self,
		id: &tg::process::Id,
		data: Option<&tg::process::Data>,
		stored: Option<crate::process::stored::Output>,
		metadata: Option<tg::process::Metadata>,
		marked: Option<bool>,
		requested: Option<Requested>,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Process(ProcessNode::default()));

		let children = if let Some(data) = data {
			data.children.as_ref().map(|children| {
				children
					.iter()
					.map(|child| {
						let child_entry = self.nodes.entry(child.item.clone().into());
						let child_index = child_entry.index();
						let child_node =
							child_entry.or_insert_with(|| Node::Process(ProcessNode::default()));
						child_node.unwrap_process_mut().parents.push(index);
						child_index
					})
					.collect()
			})
		} else {
			None
		};

		let objects = if let Some(data) = data {
			let mut objects = Vec::new();

			let command_id: tg::object::Id = data.command.clone().into();
			let command_entry = self.nodes.entry(command_id.into());
			let command_index = command_entry.index();
			let command_node = command_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
			command_node.unwrap_object_mut().parents.push(index);
			objects.push((
				command_index,
				crate::index::message::ProcessObjectKind::Command,
			));

			if let Some(output) = &data.output {
				let mut output_children = BTreeSet::new();
				output.children(&mut output_children);
				for object_id in output_children {
					let object_entry = self.nodes.entry(object_id.into());
					let object_index = object_entry.index();
					let object_node =
						object_entry.or_insert_with(|| Node::Object(ObjectNode::default()));
					object_node.unwrap_object_mut().parents.push(index);
					objects.push((
						object_index,
						crate::index::message::ProcessObjectKind::Output,
					));
				}
			}

			Some(objects)
		} else {
			None
		};

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();
		let node_old_stored = node.stored.clone();

		// Compute stored status based on children and objects.
		if children.is_some() || objects.is_some() {
			let children = children
				.as_ref()
				.or(node.children.as_ref())
				.cloned()
				.unwrap_or_default();
			let objects = objects
				.as_ref()
				.or(node.objects.as_ref())
				.cloned()
				.unwrap_or_default();

			let mut new_stored = crate::process::stored::Output {
				subtree: true,
				subtree_command: true,
				subtree_output: true,
				node_command: true,
				node_output: true,
			};

			// Check child processes.
			for child_index in &children {
				let child_stored = self
					.nodes
					.get_index(*child_index)
					.and_then(|(_, node)| node.try_unwrap_process_ref().ok()?.stored.as_ref());
				if let Some(child_stored) = child_stored {
					new_stored.subtree = new_stored.subtree && child_stored.subtree;
					new_stored.subtree_command =
						new_stored.subtree_command && child_stored.subtree_command;
					new_stored.subtree_output =
						new_stored.subtree_output && child_stored.subtree_output;
				} else {
					new_stored.subtree = false;
					new_stored.subtree_command = false;
					new_stored.subtree_output = false;
				}
			}

			// Check objects (command and outputs).
			for (object_index, object_kind) in &objects {
				let object_stored = self
					.nodes
					.get_index(*object_index)
					.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.stored.as_ref())
					.is_some_and(|s| s.subtree);
				match object_kind {
					crate::index::message::ProcessObjectKind::Command => {
						new_stored.node_command = new_stored.node_command && object_stored;
						new_stored.subtree_command = new_stored.subtree_command && object_stored;
					},
					crate::index::message::ProcessObjectKind::Output => {
						new_stored.node_output = new_stored.node_output && object_stored;
						new_stored.subtree_output = new_stored.subtree_output && object_stored;
					},
					_ => {},
				}
			}

			let node = self
				.nodes
				.get_index_mut(index)
				.unwrap()
				.1
				.unwrap_process_mut();
			node.stored = Some(new_stored);
		}

		let node = self
			.nodes
			.get_index_mut(index)
			.unwrap()
			.1
			.unwrap_process_mut();
		if let Some(children) = children {
			node.children = Some(children);
		}
		if let Some(stored) = stored {
			node.stored = Some(stored);
		}
		if let Some(metadata) = metadata {
			node.metadata = Some(metadata);
		}
		if let Some(objects) = objects {
			node.objects = Some(objects);
		}
		if let Some(marked) = marked {
			node.marked = marked;
		}
		if let Some(requested) = requested {
			node.requested = Some(requested);
		}

		// Propagate subtree stored.
		let should_propagate = |old: &Option<crate::process::stored::Output>,
		                        new: &Option<crate::process::stored::Output>|
		 -> bool {
			let improved = |old: bool, new: bool| !old && new;
			let Some(old) = old else {
				return new.is_some();
			};
			let Some(new) = new else {
				return false;
			};
			improved(old.subtree, new.subtree)
				|| improved(old.subtree_command, new.subtree_command)
				|| improved(old.subtree_output, new.subtree_output)
		};

		if should_propagate(&node_old_stored, &node.stored) {
			let mut stack: Vec<usize> = node.parents.iter().copied().collect();
			while let Some(parent_index) = stack.pop() {
				// Get parent info, cloning what we need so we can release the borrow.
				let Some((_, parent_old_stored, children, objects, parent_parents)) =
					self.nodes.get_index(parent_index).and_then(|(id, node)| {
						let node = node.try_unwrap_process_ref().ok()?;
						let children = node.children.as_ref()?.clone();
						let objects = node.objects.as_ref()?.clone();
						Some((
							id.clone(),
							node.stored.clone(),
							children,
							objects,
							node.parents.clone(),
						))
					})
				else {
					continue;
				};

				// Compute the new stored status for the parent.
				let mut new_stored = crate::process::stored::Output {
					subtree: true,
					subtree_command: true,
					subtree_output: true,
					node_command: true,
					node_output: true,
				};

				// Check child processes.
				for child_index in &children {
					let Some(child_stored) = self
						.nodes
						.get_index(*child_index)
						.and_then(|(_, node)| node.try_unwrap_process_ref().ok()?.stored.as_ref())
					else {
						new_stored.subtree = false;
						new_stored.subtree_command = false;
						new_stored.subtree_output = false;
						break;
					};
					new_stored.subtree = new_stored.subtree && child_stored.subtree;
					new_stored.subtree_command =
						new_stored.subtree_command && child_stored.subtree_command;
					new_stored.subtree_output =
						new_stored.subtree_output && child_stored.subtree_output;
				}

				// Check objects (command and outputs).
				for (object_index, object_kind) in &objects {
					let object_stored = self
						.nodes
						.get_index(*object_index)
						.and_then(|(_, node)| node.try_unwrap_object_ref().ok()?.stored.as_ref())
						.is_some_and(|s| s.subtree);
					match object_kind {
						crate::index::message::ProcessObjectKind::Command => {
							new_stored.node_command = new_stored.node_command && object_stored;
							new_stored.subtree_command =
								new_stored.subtree_command && object_stored;
						},
						crate::index::message::ProcessObjectKind::Output => {
							new_stored.node_output = new_stored.node_output && object_stored;
							new_stored.subtree_output = new_stored.subtree_output && object_stored;
						},
						_ => {},
					}
				}

				// Check if the new stored status is an improvement.
				if should_propagate(&parent_old_stored, &Some(new_stored.clone())) {
					// Update the parent's stored status.
					if let Some((_, node)) = self.nodes.get_index_mut(parent_index)
						&& let Ok(process) = node.try_unwrap_process_mut()
					{
						process.stored = Some(new_stored);
					}

					// Add grandparents to the stack.
					stack.extend(parent_parents);
				}
			}
		}
	}

	pub fn get_roots_stored(&self, arg: &tg::sync::Arg) -> bool {
		// Iterate each root and determine if it is stored.
		self.roots.iter().all(|root| {
			let node = self.nodes.get(root).unwrap();
			let stored = match node {
				Node::Object(node) => node.stored.as_ref().is_some_and(|stored| stored.subtree),
				Node::Process(node) => node.stored.as_ref().is_some_and(|stored| {
					if arg.recursive {
						stored.subtree
							&& (!arg.commands || stored.subtree_command)
							&& (!arg.outputs || stored.subtree_output)
					} else {
						(!arg.commands || stored.node_command)
							&& (!arg.outputs || stored.node_output)
					}
				}),
			};
			stored
		})
	}

	pub fn get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> Option<&crate::process::stored::Output> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().stored.as_ref())
	}

	pub fn get_object_requested(&self, id: &tg::object::Id) -> Option<Requested> {
		self.nodes
			.get(&Id::Object(id.clone()))
			.and_then(|node| node.unwrap_object_ref().requested.clone())
	}

	pub fn get_process_requested(&self, id: &tg::process::Id) -> Option<Requested> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().requested.clone())
	}
}

impl Node {
	pub fn parents(&self) -> &SmallVec<[usize; 1]> {
		match self {
			Node::Object(node) => &node.parents,
			Node::Process(node) => &node.parents,
		}
	}

	pub fn marked(&self) -> bool {
		match self {
			Node::Object(node) => node.marked,
			Node::Process(node) => node.marked,
		}
	}
}

impl petgraph::visit::GraphBase for Graph {
	type EdgeId = (usize, usize);

	type NodeId = usize;
}

impl petgraph::visit::IntoNodeIdentifiers for &Graph {
	type NodeIdentifiers = std::ops::Range<Self::NodeId>;

	fn node_identifiers(self) -> Self::NodeIdentifiers {
		0..self.nodes.len()
	}
}

impl petgraph::visit::NodeIndexable for Graph {
	fn node_bound(&self) -> usize {
		self.nodes.len()
	}

	fn to_index(&self, id: Self::NodeId) -> usize {
		id
	}

	fn from_index(&self, index: usize) -> Self::NodeId {
		index
	}
}

impl<'a> petgraph::visit::IntoNeighbors for &'a Graph {
	type Neighbors = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let (_, node) = self.nodes.get_index(id).unwrap();
		match &node {
			Node::Object(node) => node.children.iter().flatten().copied().boxed(),
			Node::Process(node) => std::iter::empty()
				.chain(node.children.iter().flatten())
				.chain(node.objects.iter().flatten().map(|(id, _)| id))
				.copied()
				.boxed(),
		}
	}
}

impl<'a> petgraph::visit::IntoNeighborsDirected for &'a Graph {
	type NeighborsDirected = Box<dyn Iterator<Item = usize> + 'a>;

	fn neighbors_directed(
		self,
		id: Self::NodeId,
		direction: petgraph::Direction,
	) -> Self::NeighborsDirected {
		match direction {
			petgraph::Direction::Outgoing => self.neighbors(id),
			petgraph::Direction::Incoming => {
				let (_, node) = self.nodes.get_index(id).unwrap();
				match node {
					Node::Object(node) => node.parents.iter().copied().boxed(),
					Node::Process(node) => node.parents.iter().copied().boxed(),
				}
			},
		}
	}
}

impl petgraph::visit::Visitable for Graph {
	type Map = HashSet<Self::NodeId>;

	fn visit_map(&self) -> Self::Map {
		HashSet::with_capacity(self.nodes.len())
	}

	fn reset_map(&self, map: &mut Self::Map) {
		map.clear();
		map.reserve(self.nodes.len());
	}
}
