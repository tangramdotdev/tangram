use super::Data;
use crate as tg;

#[derive(Clone, Debug)]
pub enum Directory {
	Graph(tg::graph::object::Ref),
	Node(Node),
}

pub type Node = tg::graph::object::Directory;

impl Directory {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph(graph) => vec![graph.graph.clone().unwrap().into()],
			Self::Node(node) => node
				.entries
				.values()
				.cloned()
				.filter_map(|edge| match edge {
					tg::graph::object::Edge::Graph(edge) => edge.graph.map(tg::Object::from),
					tg::graph::object::Edge::Object(edge) => Some(edge.into()),
				})
				.collect(),
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph(graph) => {
				let id = graph.graph.as_ref().unwrap().id();
				let node = graph.node;
				Data::Graph(tg::graph::data::Ref {
					graph: Some(id),
					node,
				})
			},
			Self::Node(node) => {
				let entries = node
					.entries
					.clone()
					.into_iter()
					.map(|(name, edge)| (name, edge.into()))
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
				let graph = tg::Graph::with_id(data.graph.unwrap());
				let node = data.node;
				Ok(Self::Graph(tg::graph::object::Ref {
					graph: Some(graph),
					node,
				}))
			},
			Data::Node(data) => {
				let entries = data
					.entries
					.into_iter()
					.map(|(name, edge)| (name, edge.into()))
					.collect();
				Ok(Self::Node(Node { entries }))
			},
		}
	}
}
