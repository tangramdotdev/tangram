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
	Normal {
		artifact: Option<tg::Artifact>,
		path: Option<PathBuf>,
	},
}

impl Symlink {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph { graph, .. } => std::iter::once(graph.clone()).map_into().collect(),
			Self::Normal { artifact, .. } => {
				if let Some(artifact) = artifact {
					std::iter::once(artifact.clone()).map_into().collect()
				} else {
					vec![]
				}
			},
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph { graph, node } => {
				let graph = graph.id();
				let node = *node;
				Data::Graph(tg::symlink::data::Graph { graph, node })
			},
			Self::Normal { artifact, path } => {
				let artifact = artifact.as_ref().map(tg::Artifact::id);
				let path = path.clone();
				Data::Normal(tg::symlink::data::Normal { artifact, path })
			},
		}
	}
}

impl TryFrom<Data> for Symlink {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		match data {
			Data::Graph(data) => {
				let graph = tg::Graph::with_id(data.graph);
				let node = data.node;
				Ok(Self::Graph { graph, node })
			},
			Data::Normal(data) => {
				let artifact = data.artifact.map(tg::Artifact::with_id);
				let path = data.path;
				Ok(Self::Normal { artifact, path })
			},
		}
	}
}
