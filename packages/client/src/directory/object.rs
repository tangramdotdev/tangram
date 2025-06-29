use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum Directory {
	Graph {
		graph: tg::Graph,
		node: usize,
	},
	Normal {
		entries: BTreeMap<String, tg::Artifact>,
	},
}

impl Directory {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph { graph, .. } => std::iter::once(graph.clone()).map_into().collect(),
			Self::Normal { entries } => entries.values().cloned().map(Into::into).collect(),
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph { graph, node } => {
				let graph = graph.id();
				let node = *node;
				Data::Graph(tg::directory::data::Graph { graph, node })
			},
			Self::Normal { entries } => {
				let entries = entries
					.iter()
					.map(|(name, artifact)| (name.clone(), artifact.id()))
					.collect();
				Data::Normal(tg::directory::data::Normal { entries })
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
				Ok(Self::Graph { graph, node })
			},
			Data::Normal(data) => {
				let entries = data
					.entries
					.into_iter()
					.map(|(name, id)| (name, tg::Artifact::with_id(id)))
					.collect();
				Ok(Self::Normal { entries })
			},
		}
	}
}
