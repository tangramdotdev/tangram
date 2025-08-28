use crate::index::message::ProcessObjectKind;
use indexmap::IndexMap;
use num::ToPrimitive;
use petgraph::visit::IntoNeighbors as _;
use smallvec::SmallVec;
use std::collections::{BTreeSet, HashSet};
use tangram_client as tg;

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

pub struct Node {
	pub parents: SmallVec<[usize; 1]>,
	pub inner: Option<NodeInner>,
}

#[derive(derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum NodeInner {
	Process(ProcessNode),
	Object(ObjectNode),
}

pub struct ProcessNode {
	pub children: Vec<usize>,
	pub complete: crate::process::complete::Output,
	pub metadata: tg::process::Metadata,
	pub objects: Vec<(usize, crate::index::message::ProcessObjectKind)>,
}

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

	pub fn update_object_with_parent(&mut self, id: tg::object::Id, parent: usize) {
		let entry = self.nodes.entry(id.into()).or_insert_with(|| Node {
			parents: SmallVec::from([parent]),
			inner: None,
		});
		entry.parents.push(parent);
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
		entry.or_insert_with(|| Node {
			parents: SmallVec::new(),
			inner: None,
		});
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
				let child_entry = child_entry.or_insert_with(|| Node {
					parents: SmallVec::new(),
					inner: None,
				});
				child_entry.parents.push(index);
				child_index
			})
			.collect();
		let command =
			std::iter::once(data.command.clone().into()).map(|id| (id, ProcessObjectKind::Command));
		let error = data
			.error
			.as_ref()
			.map(tg::error::Data::children)
			.into_iter()
			.flatten()
			.map(|id| (id, ProcessObjectKind::Error));
		let logs = data
			.log
			.iter()
			.cloned()
			.map(Into::into)
			.map(|id| (id, ProcessObjectKind::Log));
		let output = data
			.output
			.as_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten()
			.map(|id| (id, ProcessObjectKind::Output));
		let objects = std::iter::empty()
			.chain(command)
			.chain(error)
			.chain(logs)
			.chain(output);
		let object_indices = objects
			.map(|(object, kind)| {
				let object_entry = self.nodes.entry(object.into());
				let object_index = object_entry.index();
				let object_entry = object_entry.or_insert_with(|| Node {
					parents: SmallVec::new(),
					inner: None,
				});
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

	pub fn update_object(
		&mut self,
		id: &tg::object::Id,
		data: &tg::object::Data,
		complete: bool,
		metadata: tg::object::Metadata,
	) {
		let entry = self.nodes.entry(id.clone().into());
		let index = entry.index();
		entry.or_insert_with(|| Node {
			parents: SmallVec::new(),
			inner: None,
		});
		let children = data.children().map(Into::into).collect::<BTreeSet<_>>();
		let children_indices = children
			.into_iter()
			.map(|child| {
				let child_entry = self.nodes.entry(child);
				let child_index = child_entry.index();
				let child_entry = child_entry.or_insert_with(|| Node {
					parents: SmallVec::new(),
					inner: None,
				});
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
