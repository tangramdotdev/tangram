use {
	crate::index::message::ProcessObjectKind,
	indexmap::IndexMap,
	num::ToPrimitive as _,
	petgraph::visit::IntoNeighbors as _,
	smallvec::SmallVec,
	std::collections::{BTreeSet, HashSet},
	tangram_client as tg,
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

#[derive(Debug, Default)]
pub struct Node {
	pub parents: SmallVec<[usize; 1]>,
	pub inner: Option<NodeInner>,
	pub stored: bool,
}

#[derive(Debug, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum NodeInner {
	Process(ProcessNode),
	Object(ObjectNode),
}

#[derive(Debug)]
pub struct ProcessNode {
	pub children: Vec<usize>,
	pub complete: crate::process::complete::Output,
	pub metadata: tg::process::Metadata,
	pub objects: Vec<(usize, crate::index::message::ProcessObjectKind)>,
}

#[derive(Debug)]
pub struct ObjectNode {
	pub children: Vec<usize>,
	pub complete: bool,
	pub metadata: tg::object::Metadata,
	pub size: u64,
}

impl Graph {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn update_process(
		&mut self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		complete: crate::process::complete::Output,
		metadata: tg::process::Metadata,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_default();
		let children = data
			.children
			.as_ref()
			.unwrap()
			.iter()
			.map(|child| child.item.clone().into());
		let children_indices = children
			.map(|child| {
				let child_entry = self.nodes.entry(child);
				let child_index = child_entry.index();
				let child_entry = child_entry.or_default();
				child_entry.parents.push(index);
				child_index
			})
			.collect();
		let command =
			std::iter::once(data.command.clone().into()).map(|id| (id, ProcessObjectKind::Command));
		let mut outputs = BTreeSet::new();
		if let Some(output) = &data.output {
			output.children(&mut outputs);
		}
		let outputs = outputs
			.into_iter()
			.map(|object| (object, ProcessObjectKind::Output));
		let objects = std::iter::empty().chain(command).chain(outputs);
		let object_indices = objects
			.map(|(object, kind)| {
				let object_entry = self.nodes.entry(object.into());
				let object_index = object_entry.index();
				let object_entry = object_entry.or_default();
				object_entry.parents.push(index);
				(object_index, kind)
			})
			.collect();
		self.nodes.entry(id.clone().into()).and_modify(|node| {
			node.inner.replace(NodeInner::Process(ProcessNode {
				children: children_indices,
				complete,
				metadata,
				objects: object_indices,
			}));
		});
	}

	pub fn set_process_stored(&mut self, id: &tg::process::Id) {
		self.nodes.entry(id.clone().into()).or_default().stored = true;
	}

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		data: &tg::object::Data,
		complete: bool,
		metadata: tg::object::Metadata,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_default();
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let children_indices = children
			.into_iter()
			.map(|child| {
				let child_entry = self.nodes.entry(child.into());
				let child_index = child_entry.index();
				let child_entry = child_entry.or_default();
				child_entry.parents.push(index);
				child_index
			})
			.collect();
		self.nodes.entry(id.clone().into()).and_modify(|node| {
			let size = data.serialize().unwrap().len().to_u64().unwrap();
			node.inner.replace(NodeInner::Object(ObjectNode {
				children: children_indices,
				complete,
				metadata,
				size,
			}));
		});
	}

	pub fn set_object_stored(&mut self, id: &tg::object::Id) {
		self.nodes.entry(id.clone().into()).or_default().stored = true;
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

impl petgraph::visit::IntoNeighbors for &Graph {
	type Neighbors = std::vec::IntoIter<Self::NodeId>;

	fn neighbors(self, id: Self::NodeId) -> Self::Neighbors {
		let (_, node) = self.nodes.get_index(id).unwrap();
		match &node.inner {
			Some(NodeInner::Process(node)) => std::iter::empty()
				.chain(&node.children)
				.chain(node.objects.iter().map(|(id, _)| id))
				.copied()
				.collect::<Vec<_>>()
				.into_iter(),
			Some(NodeInner::Object(node)) => node.children.clone().into_iter(),
			None => vec![].into_iter(),
		}
	}
}

impl petgraph::visit::IntoNeighborsDirected for &Graph {
	type NeighborsDirected = std::vec::IntoIter<Self::NodeId>;

	fn neighbors_directed(
		self,
		id: Self::NodeId,
		direction: petgraph::Direction,
	) -> Self::NeighborsDirected {
		match direction {
			petgraph::Direction::Outgoing => self.neighbors(id),
			petgraph::Direction::Incoming => {
				let (_, node) = self.nodes.get_index(id).unwrap();
				node.parents.clone().into_vec().into_iter()
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
