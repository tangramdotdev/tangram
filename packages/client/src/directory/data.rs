use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Directory {
	Graph(tg::graph::data::Ref),
	Node(Node),
}

pub type Node = tg::graph::data::Directory;

impl Directory {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		serde_json::from_reader(bytes.into().as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		match self {
			Self::Graph(graph) => std::iter::once(graph.graph.clone().unwrap())
				.map_into()
				.left_iterator(),
			Self::Node(node) => node
				.entries
				.values()
				.cloned()
				.filter_map(|edge| match edge {
					tg::graph::data::Edge::Graph(edge) => edge.graph.map(tg::object::Id::from),
					tg::graph::data::Edge::Object(edge) => Some(edge.into()),
				})
				.right_iterator(),
		}
	}
}
