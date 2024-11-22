use crate as tg;
use bytes::Bytes;
use itertools::Itertools as _;
use std::{collections::BTreeSet, path::PathBuf};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Symlink {
	Graph {
		graph: tg::graph::Id,
		node: usize,
	},

	Target {
		target: PathBuf,
	},

	Artifact {
		artifact: tg::artifact::Id,

		#[serde(default, skip_serializing_if = "Option::is_none")]
		subpath: Option<PathBuf>,
	},
}

impl Symlink {
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
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
			Self::Target { .. } => BTreeSet::new(),
			Self::Artifact { artifact, .. } => [artifact.clone()].into_iter().map_into().collect(),
		}
	}
}
