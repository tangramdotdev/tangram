use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Directory {
	Graph {
		graph: tg::graph::Id,
		node: usize,
	},
	Normal {
		entries: BTreeMap<String, tg::artifact::Id>,
	},
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

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Graph { graph, .. } => std::iter::once(graph.clone()).map_into().collect(),
			Self::Normal { entries } => entries.values().cloned().map(Into::into).collect(),
		}
	}
}
