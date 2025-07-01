use crate as tg;
use bytes::Bytes;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Symlink {
	Graph(tg::graph::data::Ref),
	Node(Node),
}

pub type Node = tg::graph::data::Symlink;

impl Symlink {
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
			Self::Graph(graph) => std::iter::once(graph.graph.clone().unwrap().into()).boxed(),
			Self::Node(node) => node
				.artifact
				.clone()
				.and_then(|edge| match edge {
					tg::graph::data::Edge::Graph(edge) => edge.graph.map(tg::object::Id::from),
					tg::graph::data::Edge::Object(edge) => Some(edge.into()),
				})
				.into_iter()
				.boxed(),
		}
	}
}
