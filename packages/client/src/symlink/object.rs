use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub enum Symlink {
	Graph {
		graph: tg::Graph,
		node: usize,
	},
	Target {
		target: PathBuf,
	},
	Artifact {
		artifact: tg::Artifact,
		subpath: Option<PathBuf>,
	},
}

impl Symlink {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph { graph, .. } => std::iter::once(graph.clone()).map_into().collect(),
			Self::Target { .. } => vec![],
			Self::Artifact { artifact, .. } => {
				std::iter::once(artifact.clone()).map_into().collect()
			},
		}
	}
}

impl TryFrom<Data> for Symlink {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		match data {
			Data::Graph { graph, node } => {
				let graph = tg::Graph::with_id(graph);
				Ok(Self::Graph { graph, node })
			},
			Data::Target { target } => Ok(Self::Target { target }),
			Data::Artifact { artifact, subpath } => {
				let artifact = tg::Artifact::with_id(artifact);
				Ok(Self::Artifact { artifact, subpath })
			},
		}
	}
}
