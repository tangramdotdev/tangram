use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub enum Symlink {
	Graph(Graph),
	Normal(Normal),
}

#[derive(Clone, Debug)]
pub struct Graph {
	pub graph: tg::Graph,
	pub node: usize,
}

#[derive(Clone, Debug)]
pub struct Normal {
	pub artifact: Option<tg::Artifact>,
	pub path: Option<PathBuf>,
}

impl Symlink {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph(graph) => std::iter::once(graph.graph.clone()).map_into().collect(),
			Self::Normal(normal) => {
				if let Some(artifact) = &normal.artifact {
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
			Self::Graph(graph) => {
				let graph_id = graph.graph.id();
				let node = graph.node;
				Data::Graph(tg::symlink::data::Graph {
					graph: graph_id,
					node,
				})
			},
			Self::Normal(normal) => {
				let artifact = normal.artifact.as_ref().map(tg::Artifact::id);
				let path = normal.path.clone();
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
				Ok(Self::Graph(Graph { graph, node }))
			},
			Data::Normal(data) => {
				let artifact = data.artifact.map(tg::Artifact::with_id);
				let path = data.path;
				Ok(Self::Normal(Normal { artifact, path }))
			},
		}
	}
}
