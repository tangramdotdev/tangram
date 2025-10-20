use {
	crate::{self as tg},
	byteorder::ReadBytesExt as _,
	bytes::Bytes,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
	tangram_util::serde::is_false,
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Graph {
	#[tangram_serialize(id = 0)]
	pub nodes: Vec<tg::graph::data::Node>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Node {
	#[tangram_serialize(id = 0)]
	Directory(Directory),

	#[tangram_serialize(id = 1)]
	File(File),

	#[tangram_serialize(id = 2)]
	Symlink(Symlink),
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Directory {
	#[serde_as(as = "BTreeMap<_, PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "BTreeMap::is_empty")]
	pub entries: BTreeMap<String, tg::graph::data::Edge<tg::artifact::Id>>,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct File {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub contents: Option<tg::blob::Id>,

	#[serde_as(as = "BTreeMap<_, Option<PickFirst<(_, DisplayFromStr)>>>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies:
		BTreeMap<tg::Reference, Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_false")]
	pub executable: bool,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Symlink {
	#[serde_as(as = "Option<PickFirst<(_, DisplayFromStr)>>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::graph::data::Edge<tg::artifact::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(untagged)]
#[tangram_serialize(untagged)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
pub enum Edge<T> {
	Reference(Reference),
	Object(T),
}

#[derive(
	Clone,
	Debug,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Reference {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub graph: Option<tg::graph::Id>,

	#[tangram_serialize(id = 1)]
	pub node: usize,
}

impl Graph {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		serde_json::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the format"))?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			_ => Err(tg::error!("invalid format")),
		}
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		for node in &self.nodes {
			match node {
				tg::graph::data::Node::Directory(node) => node.children(children),
				tg::graph::data::Node::File(file) => file.children(children),
				tg::graph::data::Node::Symlink(symlink) => symlink.children(children),
			}
		}
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
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		for edge in self.entries.values() {
			edge.children(children);
		}
	}
}

impl File {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(contents) = &self.contents {
			children.insert(contents.clone().into());
		}
		for referent in self.dependencies.values().flatten() {
			referent.item.children(children);
		}
	}
}

impl Symlink {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(edge) = &self.artifact {
			edge.children(children);
		}
	}
}

impl<T> Edge<T>
where
	T: Into<tg::object::Id> + Clone,
{
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Reference(reference) => {
				reference.children(children);
			},
			Self::Object(object) => {
				children.insert(object.clone().into());
			},
		}
	}
}

impl Reference {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(graph) = &self.graph {
			children.insert(graph.clone().into());
		}
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
