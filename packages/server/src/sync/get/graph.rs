use {
	indexmap::IndexMap, petgraph::visit::IntoNeighbors as _, smallvec::SmallVec,
	std::collections::HashSet, tangram_client::prelude::*, tangram_util::iter::Ext as _,
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

	// pub fn update_process(
	// 	&mut self,
	// 	id: &tg::process::Id,
	// 	data: Option<&tg::process::Data>,
	// 	complete: Option<crate::process::complete::Output>,
	// 	metadata: Option<tg::process::Metadata>,
	// ) {
	// 	let entry = self.nodes.entry(id.clone().into());
	// 	let index = entry.index();
	// 	entry.or_default();
	// 	let children = data
	// 		.children
	// 		.as_ref()
	// 		.unwrap()
	// 		.iter()
	// 		.map(|child| child.item.clone().into());
	// 	let children_indices = children
	// 		.map(|child| {
	// 			let child_entry = self.nodes.entry(child);
	// 			let child_index = child_entry.index();
	// 			let child_entry = child_entry.or_default();
	// 			child_entry.parents.push(index);
	// 			child_index
	// 		})
	// 		.collect();
	// 	let command =
	// 		std::iter::once(data.command.clone().into()).map(|id| (id, ProcessObjectKind::Command));
	// 	let mut outputs = BTreeSet::new();
	// 	if let Some(output) = &data.output {
	// 		output.children(&mut outputs);
	// 	}
	// 	let outputs = outputs
	// 		.into_iter()
	// 		.map(|object| (object, ProcessObjectKind::Output));
	// 	let objects = std::iter::empty().chain(command).chain(outputs);
	// 	let object_indices = objects
	// 		.map(|(object, kind)| {
	// 			let object_entry = self.nodes.entry(object.into());
	// 			let object_index = object_entry.index();
	// 			let object_entry = object_entry.or_default();
	// 			object_entry.parents.push(index);
	// 			(object_index, kind)
	// 		})
	// 		.collect();
	// 	self.nodes.entry(id.clone().into()).and_modify(|node| {
	// 		node.inner.replace(NodeInner::Process(ProcessNode {
	// 			children: children_indices,
	// 			complete,
	// 			metadata,
	// 			objects: object_indices,
	// 		}));
	// 	});
	// }

	pub fn set_process_stored(&mut self, id: &tg::process::Id) {
		self.nodes
			.entry(id.clone().into())
			.or_insert_with(|| Node::Process(ProcessNode::default()))
			.unwrap_process_mut()
			.stored = true;
	}

	// pub fn update_object(
	// 	&mut self,
	// 	id: &tg::object::Id,
	// 	data: Option<&tg::object::Data>,
	// 	complete: Option<bool>,
	// 	metadata: Option<tg::object::Metadata>,
	// ) {
	// 	let entry = self.nodes.entry(id.clone().into());
	// 	let index = entry.index();
	// 	entry.or_default();
	// 	let mut children = BTreeSet::new();
	// 	data.children(&mut children);
	// 	let children_indices = children
	// 		.into_iter()
	// 		.map(|child| {
	// 			let child_entry = self.nodes.entry(child.into());
	// 			let child_index = child_entry.index();
	// 			let child_entry = child_entry.or_default();
	// 			child_entry.parents.push(index);
	// 			child_index
	// 		})
	// 		.collect();
	// 	self.nodes.entry(id.clone().into()).and_modify(|node| {
	// 		let size = data.serialize().unwrap().len().to_u64().unwrap();
	// 		node.inner.replace(NodeInner::Object(ObjectNode {
	// 			children: children_indices,
	// 			complete,
	// 			metadata,
	// 			size,
	// 		}));
	// 	});
	// }

	pub fn set_object_stored(&mut self, id: &tg::object::Id) {
		self.nodes
			.entry(id.clone().into())
			.or_insert_with(|| Node::Object(ObjectNode::default()))
			.unwrap_object_mut()
			.stored = true;
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
