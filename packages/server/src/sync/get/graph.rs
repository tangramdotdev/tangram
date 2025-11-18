use {
	indexmap::IndexMap,
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashSet},
	tangram_client::prelude::*,
	tangram_util::iter::Ext as _,
};

#[derive(Default)]
pub struct Graph {
	pub nodes: IndexMap<Id, Node, fnv::FnvBuildHasher>,
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
	Process(tg::process::Id),
	Object(tg::object::Id),
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Node {
	Process(ProcessNode),
	Object(ObjectNode),
}

#[derive(Clone, Debug, Default)]
pub struct ProcessNode {
	pub children: Option<Vec<usize>>,
	pub complete: Option<crate::process::complete::Output>,
	pub metadata: Option<tg::process::Metadata>,
	pub objects: Option<Vec<(usize, crate::index::message::ProcessObjectKind)>>,
	pub parents: SmallVec<[usize; 1]>,
	pub stored: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectNode {
	pub children: Option<Vec<usize>>,
	pub complete: Option<bool>,
	pub metadata: Option<tg::object::Metadata>,
	pub parents: SmallVec<[usize; 1]>,
	pub size: Option<u64>,
	pub stored: bool,
}

impl Graph {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn update_process(
		&mut self,
		id: &tg::process::Id,
		data: Option<&tg::process::Data>,
		complete: Option<crate::process::complete::Output>,
		metadata: Option<tg::process::Metadata>,
		stored: Option<bool>,
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
		if let Some(children) = children {
			node.children = Some(children);
		}
		if let Some(complete) = complete {
			node.complete = Some(complete);
		}
		if let Some(metadata) = metadata {
			node.metadata = Some(metadata);
		}
		if let Some(objects) = objects {
			node.objects = Some(objects);
		}
		if let Some(stored) = stored {
			node.stored = stored;
		}
	}

	pub fn get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> Option<&crate::process::complete::Output> {
		self.nodes
			.get(&Id::Process(id.clone()))
			.and_then(|node| node.unwrap_process_ref().complete.as_ref())
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		data: Option<&tg::object::Data>,
		complete: Option<bool>,
		metadata: Option<tg::object::Metadata>,
		size: Option<u64>,
		stored: Option<bool>,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node::Object(ObjectNode::default()));

		let children = if let Some(data) = data {
			let mut children = BTreeSet::new();
			data.children(&mut children);
			let children = children
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
		if let Some(children) = children {
			node.children = Some(children);
		}
		if let Some(complete) = complete {
			node.complete = Some(complete);
		}
		if let Some(metadata) = metadata {
			node.metadata = Some(metadata);
		}
		if let Some(size) = size {
			node.size = Some(size);
		}
		if let Some(stored) = stored {
			node.stored = stored;
		}
	}
}

impl Node {
	pub fn parents(&self) -> &SmallVec<[usize; 1]> {
		match self {
			Node::Process(node) => &node.parents,
			Node::Object(node) => &node.parents,
		}
	}

	pub fn stored(&self) -> bool {
		match self {
			Node::Process(node) => node.stored,
			Node::Object(node) => node.stored,
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
			Node::Process(node) => std::iter::empty()
				.chain(node.children.iter().flatten())
				.chain(node.objects.iter().flatten().map(|(id, _)| id))
				.copied()
				.boxed(),
			Node::Object(node) => node.children.iter().flatten().copied().boxed(),
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
					Node::Process(node) => node.parents.iter().copied().boxed(),
					Node::Object(node) => node.parents.iter().copied().boxed(),
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
