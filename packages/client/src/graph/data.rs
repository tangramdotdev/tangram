use {
	crate::prelude::*,
	byteorder::ReadBytesExt as _,
	bytes::Bytes,
	serde_with::{DisplayFromStr, PickFirst, serde_as},
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
	tangram_uri::Uri,
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
	pub dependencies: BTreeMap<tg::Reference, Option<Dependency>>,

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
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
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
	Pointer(Pointer),
	Object(T),
}

impl From<Edge<tg::artifact::Id>> for Edge<tg::object::Id> {
	fn from(value: Edge<tg::artifact::Id>) -> Self {
		match value {
			Edge::Pointer(pointer) => Self::Pointer(pointer),
			Edge::Object(id) => Self::Object(id.into()),
		}
	}
}

impl TryFrom<Edge<tg::object::Id>> for Edge<tg::artifact::Id> {
	type Error = tg::Error;

	fn try_from(value: Edge<tg::object::Id>) -> tg::Result<Self> {
		match value {
			Edge::Pointer(pointer) => Ok(Self::Pointer(pointer)),
			Edge::Object(id) => Ok(Self::Object(id.try_into()?)),
		}
	}
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Pointer {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub graph: Option<tg::graph::Id>,

	#[tangram_serialize(id = 1)]
	pub index: usize,

	#[tangram_serialize(id = 2)]
	pub kind: tg::artifact::Kind,
}

/// A file dependency, wrapping a Referent where the item may be None.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(transparent)]
#[tangram_serialize(transparent)]
pub struct Dependency(pub tg::Referent<Option<Edge<tg::object::Id>>>);

impl Dependency {
	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self
			.0
			.item
			.as_ref()
			.map_or_else(String::new, ToString::to_string);
		let mut builder = Uri::builder().path(&path);
		let mut query = Vec::new();
		if let Some(artifact) = &self.0.options.artifact {
			let artifact = artifact.to_string();
			let artifact = tangram_uri::encode_query_value(&artifact);
			let artifact = format!("artifact={artifact}");
			query.push(artifact);
		}
		if let Some(id) = &self.0.options.id {
			let id = id.to_string();
			let id = tangram_uri::encode_query_value(&id);
			let id = format!("id={id}");
			query.push(id);
		}
		if let Some(name) = &self.0.options.name {
			let name = tangram_uri::encode_query_value(name);
			let name = format!("name={name}");
			query.push(name);
		}
		if let Some(path) = &self.0.options.path {
			let path = path.to_string_lossy();
			let path = tangram_uri::encode_query_value(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if let Some(tag) = &self.0.options.tag {
			let tag = tag.to_string();
			let tag = tangram_uri::encode_query_value(&tag);
			let tag = format!("tag={tag}");
			query.push(tag);
		}
		if !query.is_empty() {
			let query = query.join("&");
			builder = builder.query_raw(&query);
		}
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let item = if uri.path().is_empty() {
			None
		} else {
			Some(
				uri.path()
					.parse()
					.map_err(|_| tg::error!("failed to parse the item"))?,
			)
		};
		let mut options = tg::referent::Options::default();
		if let Some(query) = uri.query_raw() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=') {
					let value = tangram_uri::decode_query_value(value)
						.map_err(|_| tg::error!("failed to decode the value"))?;
					match key {
						"artifact" => {
							options.artifact.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the artifact"))?,
							);
						},
						"id" => {
							options.id.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the id"))?,
							);
						},
						"name" => {
							options.name.replace(value.into_owned());
						},
						"path" => {
							options.path.replace(value.into_owned().into());
						},
						"tag" => {
							options.tag.replace(
								value
									.parse()
									.map_err(|_| tg::error!("failed to parse the tag"))?,
							);
						},
						_ => {},
					}
				}
			}
		}
		Ok(Self(tg::Referent { item, options }))
	}
}

impl std::fmt::Display for Dependency {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Dependency {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		Self::with_uri(&uri)
	}
}

impl std::ops::Deref for Dependency {
	type Target = tg::Referent<Option<Edge<tg::object::Id>>>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl std::ops::DerefMut for Dependency {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
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
		for dependency in self.dependencies.values().flatten() {
			if let Some(item) = &dependency.item {
				item.children(children);
			}
		}
	}

	#[must_use]
	pub fn solvable(&self) -> bool {
		self.dependencies.keys().any(tg::Reference::is_solvable)
	}

	#[must_use]
	pub fn solved(&self) -> bool {
		self.dependencies
			.iter()
			.filter(|(reference, _)| reference.is_solvable())
			.all(|(_, option)| option.is_some())
	}
}

impl Symlink {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(edge) = &self.artifact {
			edge.children(children);
		}
	}
}

impl Edge<tg::object::Id> {
	#[must_use]
	pub fn kind(&self) -> tg::object::Kind {
		match self {
			Edge::Pointer(pointer) => pointer.kind.into(),
			Edge::Object(object) => object.kind(),
		}
	}
}

impl Edge<tg::artifact::Id> {
	#[must_use]
	pub fn artifact_kind(&self) -> tg::artifact::Kind {
		match self {
			Edge::Pointer(pointer) => pointer.kind,
			Edge::Object(object) => object.kind(),
		}
	}
}

impl<T> Edge<T>
where
	T: Into<tg::object::Id> + Clone,
{
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Pointer(pointer) => {
				pointer.children(children);
			},
			Self::Object(object) => {
				children.insert(object.clone().into());
			},
		}
	}
}

impl Pointer {
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
			Self::Pointer(pointer) => write!(f, "{pointer}"),
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
		if let Ok(pointer) = s.parse() {
			Ok(Self::Pointer(pointer))
		} else if let Ok(object) = s.parse() {
			Ok(Self::Object(object))
		} else {
			Err(tg::error!("expected an edge"))
		}
	}
}

impl std::fmt::Display for Pointer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(graph) = &self.graph {
			write!(f, "graph={graph}&")?;
		}
		write!(f, "index={}&kind={}", self.index, self.kind)?;
		Ok(())
	}
}

impl std::str::FromStr for Pointer {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let value = serde_urlencoded::from_str::<BTreeMap<String, String>>(s)
			.map_err(|_| tg::error!("failed to deserialize edge"))?;
		let graph = value.get("graph").map(|s| s.parse()).transpose()?;
		let index = value
			.get("index")
			.ok_or_else(|| tg::error!("missing index"))?
			.parse()
			.map_err(|_| tg::error!("expected a number"))?;
		let kind = value
			.get("kind")
			.ok_or_else(|| tg::error!("missing kind"))?
			.parse()
			.map_err(|source| tg::error!(!source, "invalid kind"))?;
		Ok(Self { graph, index, kind })
	}
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum PointerSerde {
	String(String),
	Object {
		graph: Option<tg::graph::Id>,
		index: usize,
		kind: tg::artifact::Kind,
	},
}

impl TryFrom<PointerSerde> for Pointer {
	type Error = tg::Error;

	fn try_from(value: PointerSerde) -> Result<Self, Self::Error> {
		match value {
			PointerSerde::String(string) => string.parse(),
			PointerSerde::Object { graph, index, kind } => Ok(Self { graph, index, kind }),
		}
	}
}

impl From<Pointer> for PointerSerde {
	fn from(value: Pointer) -> Self {
		Self::String(value.to_string())
	}
}
