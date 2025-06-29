use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum File {
	Graph {
		graph: tg::Graph,
		node: usize,
	},
	Normal {
		contents: tg::Blob,
		dependencies: BTreeMap<tg::Reference, tg::Referent<tg::Object>>,
		executable: bool,
	},
}

impl File {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph { graph, .. } => std::iter::once(graph.clone()).map_into().collect(),
			Self::Normal {
				contents,
				dependencies,
				..
			} => {
				let contents = contents.clone().into();
				let dependencies = dependencies
					.values()
					.map(|dependency| dependency.item.clone());
				std::iter::once(contents).chain(dependencies).collect()
			},
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph { graph, node } => {
				let graph = graph.id();
				let node = *node;
				Data::Graph(tg::file::data::Graph { graph, node })
			},
			Self::Normal {
				contents,
				dependencies,
				executable,
			} => {
				let contents = contents.id();
				let dependencies = dependencies
					.iter()
					.map(|(reference, referent)| {
						let object = referent.item.id();
						let dependency = tg::Referent {
							item: object,
							path: referent.path.clone(),
							tag: referent.tag.clone(),
						};
						(reference.clone(), dependency)
					})
					.collect();
				let executable = *executable;
				Data::Normal(tg::file::data::Normal {
					contents,
					dependencies,
					executable,
				})
			},
		}
	}
}

impl TryFrom<Data> for File {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		match data {
			Data::Graph(data) => {
				let graph = tg::Graph::with_id(data.graph);
				let node = data.node;
				Ok(Self::Graph { graph, node })
			},
			Data::Normal(data) => {
				let contents = tg::Blob::with_id(data.contents);
				let dependencies = data
					.dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let referent = referent.map(tg::Object::with_id);
						(reference, referent)
					})
					.collect();
				Ok(Self::Normal {
					contents,
					dependencies,
					executable: data.executable,
				})
			},
		}
	}
}
