use crate as tg;
use bytes::Bytes;
use std::path::PathBuf;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Symlink {
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
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

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
			Self::Graph(graph) => std::iter::once(graph.graph.clone().into()).boxed(),
			Self::Normal(normal) => {
				if let Some(artifact) = &normal.artifact {
					std::iter::once(artifact.clone().into()).boxed()
				} else {
					std::iter::empty().boxed()
				}
			},
		}
	}
}
