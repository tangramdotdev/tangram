use crate::{self as tg, util::serde::is_false};
use bytes::Bytes;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Graph {
	pub nodes: Vec<tg::graph::data::Node>,
}

#[derive(
	Clone, Debug, serde::Deserialize, serde::Serialize, derive_more::TryUnwrap, derive_more::Unwrap,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Node {
	Directory(Directory),
	File(File),
	Symlink(Symlink),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Directory {
	pub entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct File {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub contents: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<tg::graph::data::Edge<tg::object::Id>>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Symlink {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(
	Clone, Debug, derive_more::TryUnwrap, derive_more::Unwrap, serde::Deserialize, serde::Serialize,
)]
#[serde(untagged)]
pub enum Edge<T> {
	Graph(Ref),
	Object(T),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(from = "ReferenceSerde", into = "ReferenceSerde")]
pub struct Ref {
	pub graph: Option<tg::graph::Id>,
	pub node: usize,
}

impl Graph {
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
		self.nodes.iter().flat_map(|node| match node {
			tg::graph::data::Node::Directory(tg::graph::data::Directory { entries }) => entries
				.values()
				.filter_map(|edge| match edge {
					Edge::Graph(graph) => graph.graph.clone().map(tg::object::Id::from),
					Edge::Object(object) => Some(object.clone().into()),
				})
				.boxed(),
			tg::graph::data::Node::File(tg::graph::data::File {
				contents,
				dependencies,
				..
			}) => contents
				.clone()
				.map(tg::object::Id::from)
				.into_iter()
				.chain(
					dependencies
						.values()
						.filter_map(|referent| match &referent.item {
							Edge::Graph(graph) => graph.graph.clone().map(tg::object::Id::from),
							Edge::Object(object) => Some(object.clone()),
						}),
				)
				.boxed(),
			tg::graph::data::Node::Symlink(symlink) => {
				let artifact = symlink.artifact.as_ref().and_then(|edge| match edge {
					Edge::Graph(graph) => graph.graph.clone().map(tg::object::Id::from),
					Edge::Object(object) => Some(object.clone().into()),
				});
				artifact.into_iter().boxed()
			},
		})
	}
}

impl Node {
	#[must_use]
	pub fn kind(&self) -> tg::artifact::Kind {
		match self {
			Self::Directory(_) => tg::artifact::Kind::Directory,
			Self::File(_) => tg::artifact::Kind::File,
			Self::Symlink(_) => tg::artifact::Kind::Symlink,
		}
	}
}

impl<T> std::str::FromStr for Edge<T>
where
	T: std::str::FromStr + std::fmt::Display,
{
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(edge) = s.parse() {
			return Ok(Self::Graph(edge));
		}
		if let Ok(edge) = s.parse() {
			return Ok(Self::Object(edge));
		}
		Err(tg::error!("expected a graph or object edge"))
	}
}

impl std::fmt::Display for Ref {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "?")?;
		if let Some(graph) = &self.graph {
			write!(f, "graph={graph},")?;
		}
		write!(f, "node={}", self.node)?;
		Ok(())
	}
}

impl std::str::FromStr for Ref {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let value = serde_urlencoded::from_str::<BTreeMap<String, String>>(s)
			.map_err(|_| tg::error!("failed to deserialize edge"))?;
		let graph = value.get("graph").map(|s| s.parse()).transpose()?;
		let node = value
			.get("node")
			.ok_or_else(|| tg::error!("missing node"))?
			.parse()
			.map_err(|_| tg::error!("expected a number"))?;
		Ok(Self { graph, node })
	}
}

impl From<tg::graph::object::Edge<tg::Object>> for Edge<tg::object::Id> {
	fn from(value: tg::graph::object::Edge<tg::Object>) -> Self {
		match value {
			tg::graph::object::Edge::Graph(data) => Self::Graph(Ref {
				graph: data.graph.map(|data| data.id()),
				node: data.node,
			}),
			tg::graph::object::Edge::Object(data) => Self::Object(data.id()),
		}
	}
}

impl From<tg::graph::object::Edge<tg::Artifact>> for Edge<tg::artifact::Id> {
	fn from(value: tg::graph::object::Edge<tg::Artifact>) -> Self {
		match value {
			tg::graph::object::Edge::Graph(data) => Self::Graph(Ref {
				graph: data.graph.map(|data| data.id()),
				node: data.node,
			}),
			tg::graph::object::Edge::Object(data) => Self::Object(data.id()),
		}
	}
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum ReferenceSerde {
	Number(usize),
	Object {
		graph: Option<tg::graph::Id>,
		node: usize,
	},
}

impl From<ReferenceSerde> for Ref {
	fn from(value: ReferenceSerde) -> Self {
		match value {
			ReferenceSerde::Object { graph, node } => Ref { graph, node },
			ReferenceSerde::Number(node) => Ref { graph: None, node },
		}
	}
}

impl From<Ref> for ReferenceSerde {
	fn from(value: Ref) -> Self {
		match value.graph {
			None => Self::Number(value.node),
			Some(graph) => Self::Object {
				graph: Some(graph),
				node: value.node,
			},
		}
	}
}
