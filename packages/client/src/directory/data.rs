use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use std::collections::BTreeMap;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Directory {
	Graph(Graph),
	Normal(Normal),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Graph {
	pub graph: tg::graph::Id,
	pub node: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Normal {
	pub entries: BTreeMap<String, tg::artifact::Id>,
}

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
			Self::Graph(graph) => std::iter::once(graph.graph.clone())
				.map_into()
				.left_iterator(),
			Self::Normal(normal) => normal
				.entries
				.values()
				.cloned()
				.map(Into::into)
				.right_iterator(),
		}
	}
}
