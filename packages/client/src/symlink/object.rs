use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub enum Symlink {
	Normal {
		artifact: Option<tg::Artifact>,
		subpath: Option<PathBuf>,
	},
	Graph {
		graph: tg::Graph,
		node: usize,
	},
}

impl Symlink {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Normal { artifact, .. } => artifact.clone().into_iter().map_into().collect(),
			Self::Graph { graph, .. } => [graph.clone()].into_iter().map_into().collect(),
		}
	}
}

impl TryFrom<Data> for Symlink {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		match data {
			Data::Normal { artifact, subpath } => {
				let artifact = artifact.map(tg::Artifact::with_id);
				Ok(Self::Normal { artifact, subpath })
			},
			Data::Graph { graph, node } => {
				let graph = tg::Graph::with_id(graph);
				Ok(Self::Graph { graph, node })
			},
		}
	}
}
