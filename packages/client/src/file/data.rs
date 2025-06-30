use crate::{self as tg, util::serde::is_false};
use bytes::Bytes;
use itertools::Itertools as _;
use std::collections::BTreeMap;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum File {
	Graph(Graph),
	Node(Node),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Graph {
	pub graph: tg::graph::Id,
	pub node: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Node {
	pub contents: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<tg::object::Id>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

impl File {
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
			Self::Node(node) => {
				let contents = node.contents.clone().map(tg::object::Id::from);
				let dependencies = node
					.dependencies
					.values()
					.map(|dependency| dependency.item.clone());
				contents.into_iter().chain(dependencies).right_iterator()
			},
		}
	}
}
