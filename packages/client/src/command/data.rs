use {
	crate::prelude::*,
	byteorder::ReadBytesExt as _,
	bytes::Bytes,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Vec::is_empty")]
	pub args: tg::value::data::Array,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: tg::value::data::Map,

	#[tangram_serialize(id = 3)]
	pub executable: tg::command::data::Executable,

	#[tangram_serialize(id = 4)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::command::data::Mount>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 6, default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 7, default, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(untagged)]
pub enum Executable {
	#[tangram_serialize(id = 0)]
	Artifact(ArtifactExecutable),

	#[tangram_serialize(id = 1)]
	Module(ModuleExecutable),

	#[tangram_serialize(id = 2)]
	Path(PathExecutable),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ArtifactExecutable {
	#[tangram_serialize(id = 0)]
	pub artifact: tg::artifact::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ModuleExecutable {
	#[tangram_serialize(id = 0)]
	pub module: tg::module::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub export: Option<String>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PathExecutable {
	#[tangram_serialize(id = 0)]
	pub path: PathBuf,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Mount {
	#[tangram_serialize(id = 0)]
	pub source: tg::artifact::Id,

	#[tangram_serialize(id = 1)]
	pub target: PathBuf,
}

impl Command {
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
		self.executable.children(children);
		for value in &self.args {
			value.children(children);
		}
		for value in self.env.values() {
			value.children(children);
		}
		for mount in &self.mounts {
			mount.children(children);
		}
	}
}

impl Executable {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Artifact(artifact) => artifact.children(children),
			Self::Module(module) => module.children(children),
			Self::Path(_) => (),
		}
	}

	#[must_use]
	pub fn to_uri(&self) -> Uri {
		match self {
			Self::Artifact(artifact) => artifact.to_uri(),
			Self::Module(module) => module.to_uri(),
			Self::Path(path) => path.to_uri(),
		}
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		if let Ok(executable) = ModuleExecutable::with_uri(uri) {
			Ok(Self::Module(executable))
		} else if let Ok(executable) = ArtifactExecutable::with_uri(uri) {
			Ok(Self::Artifact(executable))
		} else if let Ok(executable) = PathExecutable::with_uri(uri) {
			Ok(Self::Path(executable))
		} else {
			Err(tg::error!("invalid uri"))
		}
	}
}

impl ArtifactExecutable {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		children.insert(self.artifact.clone().into());
	}
}

impl ModuleExecutable {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let tg::module::data::Item::Object(object) = &self.module.referent.item {
			children.insert(object.clone());
		}
	}
}

impl Mount {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		children.insert(self.source.clone().into());
	}
}

impl ArtifactExecutable {
	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let mut builder = Uri::builder().path(self.artifact.to_string());
		let mut query = Vec::new();
		if let Some(path) = &self.path {
			let path = path.to_string_lossy();
			let path = urlencoding::encode(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if !query.is_empty() {
			builder = builder.query(query.join("&"));
		}
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let artifact = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the artifact"))?;
		let mut path = None;
		if let Some(query) = uri.query() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=')
					&& key == "path"
				{
					path.replace(
						urlencoding::decode(value)
							.map_err(|_| tg::error!("failed to decode the path"))?
							.into_owned()
							.into(),
					);
				}
			}
		}
		Ok(Self { artifact, path })
	}
}

impl ModuleExecutable {
	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let uri = self.module.to_uri();
		let mut builder = Uri::builder();
		builder = builder.path(uri.path().to_owned());
		if let Some(query) = uri.query() {
			builder = builder.query(query.to_owned());
		}
		if let Some(export) = &self.export {
			builder = builder.fragment(export.to_owned());
		}
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let module = tg::module::Data::with_uri(uri)?;
		let export = uri.fragment().map(ToOwned::to_owned);
		Ok(Self { module, export })
	}
}

impl PathExecutable {
	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self.path.to_string_lossy();
		Uri::builder().path(path.to_string()).build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let path = uri.path().into();
		Ok(Self { path })
	}
}

impl std::fmt::Display for Executable {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Executable {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let executable = Self::with_uri(&uri)?;
		Ok(executable)
	}
}
