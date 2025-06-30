use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum Directory {
	Graph(Graph),
	Node(Node),
}

#[derive(Clone, Debug)]
pub struct Graph {
	pub graph: tg::Graph,
	pub node: usize,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub entries: BTreeMap<String, tg::Artifact>,
}

impl Directory {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph(graph) => std::iter::once(graph.graph.clone()).map_into().collect(),
			Self::Node(node) => node.entries.values().cloned().map(Into::into).collect(),
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph(graph) => {
				let id = graph.graph.id();
				let node = graph.node;
				Data::Graph(tg::directory::data::Graph { graph: id, node })
			},
			Self::Node(node) => {
				let entries = node
					.entries
					.iter()
					.map(|(name, artifact)| (name.clone(), artifact.id()))
					.collect();
				Data::Node(tg::directory::data::Node { entries })
			},
		}
	}
}

impl TryFrom<Data> for Directory {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		match data {
			Data::Graph(data) => {
				let graph = tg::Graph::with_id(data.graph);
				let node = data.node;
				Ok(Self::Graph(Graph { graph, node }))
			},
			Data::Node(data) => {
				let entries = data
					.entries
					.into_iter()
					.map(|(name, id)| (name, tg::Artifact::with_id(id)))
					.collect();
				Ok(Self::Node(Node { entries }))
			},
		}
	}
}
