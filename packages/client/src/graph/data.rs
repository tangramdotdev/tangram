use crate::{self as tg, util::serde::is_false};
use bytes::Bytes;
use serde_with::{DisplayFromStr, PickFirst, serde_as};
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

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Directory {
	#[serde_as(as = "BTreeMap<_, PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct File {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub contents: Option<tg::blob::Id>,

	#[serde_as(as = "BTreeMap<_, PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Reference, tg::Referent<tg::graph::data::Edge<tg::object::Id>>>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Symlink {
	#[serde_as(as = "Option<PickFirst<(_, DisplayFromStr)>>")]
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
	Reference(Reference),
	Object(T),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Reference {
	#[serde(default, skip_serializing_if = "Option::is_none")]
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
			tg::graph::data::Node::Directory(node) => node.children().boxed(),
			tg::graph::data::Node::File(file) => file.children().boxed(),
			tg::graph::data::Node::Symlink(symlink) => symlink.children().boxed(),
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

impl Directory {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		self.entries.values().flat_map(Edge::children)
	}
}

impl File {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		let contents = self.contents.clone().map(Into::into);
		let dependencies = self
			.dependencies
			.values()
			.flat_map(|referent| referent.item.children());
		std::iter::empty().chain(contents).chain(dependencies)
	}
}

impl Symlink {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		self.artifact.iter().flat_map(Edge::children)
	}
}

impl<T> Edge<T>
where
	T: Into<tg::object::Id> + Clone,
{
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		match self {
			Self::Reference(reference) => reference.children().left_iterator(),
			Self::Object(object) => std::iter::once(object.clone().into()).right_iterator(),
		}
	}
}

impl Reference {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		self.graph.clone().into_iter().map(Into::into)
	}
}

impl<T> std::fmt::Display for Edge<T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Reference(reference) => write!(f, "{reference}"),
			Self::Object(object) => write!(f, "{object}"),
		}
	}
}

impl<T> std::str::FromStr for Edge<T>
where
	T: std::str::FromStr,
{
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(reference) = s.parse() {
			Ok(Self::Reference(reference))
		} else if let Ok(object) = s.parse() {
			Ok(Self::Object(object))
		} else {
			Err(tg::error!("expected an edge"))
		}
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(graph) = &self.graph {
			write!(f, "graph={graph}&")?;
		}
		write!(f, "node={}", self.node)?;
		Ok(())
	}
}

impl std::str::FromStr for Reference {
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
			tg::graph::object::Edge::Reference(data) => Self::Reference(Reference {
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
			tg::graph::object::Edge::Reference(data) => Self::Reference(Reference {
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
	String(String),
	Object {
		graph: Option<tg::graph::Id>,
		node: usize,
	},
}

impl TryFrom<ReferenceSerde> for Reference {
	type Error = tg::Error;

	fn try_from(value: ReferenceSerde) -> Result<Self, Self::Error> {
		match value {
			ReferenceSerde::String(string) => string.parse(),
			ReferenceSerde::Object { graph, node } => Ok(Self { graph, node }),
		}
	}
}

impl From<Reference> for ReferenceSerde {
	fn from(value: Reference) -> Self {
		Self::String(value.to_string())
	}
}
