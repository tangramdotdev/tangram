use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Directory {
	Normal {
		entries: BTreeMap<String, tg::artifact::Id>,
	},
	Graph {
		graph: tg::graph::Id,
		node: usize,
	},
}

impl Directory {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize(bytes: &Bytes) -> tg::Result<Self> {
		serde_json::from_reader(bytes.as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Normal { entries } => entries.values().cloned().map(Into::into).collect(),
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
		}
	}
}
