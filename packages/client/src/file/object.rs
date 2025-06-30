use super::Data;
use crate as tg;
use itertools::Itertools as _;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum File {
	Graph(Graph),
	Node(Node),
}

#[derive(Clone, Debug)]
pub struct Graph {
	pub graph: tg::Graph,
	pub node: usize,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub contents: tg::Blob,
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<tg::Object>>,
	pub executable: bool,
}

impl File {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Graph(graph) => std::iter::once(graph.graph.clone()).map_into().collect(),
			Self::Node(node) => {
				let contents = node.contents.clone().into();
				let dependencies = node
					.dependencies
					.values()
					.map(|dependency| dependency.item.clone());
				std::iter::once(contents).chain(dependencies).collect()
			},
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Graph(graph) => {
				let id = graph.graph.id();
				let node = graph.node;
				Data::Graph(tg::file::data::Graph { graph: id, node })
			},
			Self::Node(node) => {
				let contents = node.contents.id();
				let dependencies = node
					.dependencies
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
				let executable = node.executable;
				Data::Node(tg::file::data::Node {
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
				Ok(Self::Graph(Graph { graph, node }))
			},
			Data::Node(data) => {
				let contents = tg::Blob::with_id(data.contents);
				let dependencies = data
					.dependencies
					.into_iter()
					.map(|(reference, referent)| {
						let referent = referent.map(tg::Object::with_id);
						(reference, referent)
					})
					.collect();
				Ok(Self::Node(Node {
					contents,
					dependencies,
					executable: data.executable,
				}))
			},
		}
	}
}
