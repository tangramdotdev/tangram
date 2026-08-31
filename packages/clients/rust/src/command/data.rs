use {
	crate::prelude::*,
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
#[serde(deny_unknown_fields)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub args: Vec<Value>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: BTreeMap<String, Value>,

	#[tangram_serialize(id = 3)]
	pub executable: Executable,

	#[tangram_serialize(id = 4)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
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
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Value {
	#[tangram_serialize(id = 0)]
	String(tg::value::Data),

	#[tangram_serialize(id = 1)]
	Value(tg::value::Data),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Executable {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

impl Command {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		serde_json::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|error| tg::error!(!error, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the data")),
			_ => Err(tg::error!("invalid format")),
		}
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		self.executable.children(children);
		for arg in &self.args {
			arg.children(children);
		}
		for value in self.env.values() {
			value.children(children);
		}
		if let Some(stdin) = &self.stdin {
			children.insert(stdin.clone().into());
		}
	}

	#[must_use]
	pub fn without_location_and_tokens(mut self) -> Self {
		self.args = self
			.args
			.into_iter()
			.map(Value::without_location_and_tokens)
			.collect();
		self.env = self
			.env
			.into_iter()
			.map(|(key, value)| (key, value.without_location_and_tokens()))
			.collect();

		self
	}
}

impl Value {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::String(value) | Self::Value(value) => value.children(children),
		}
	}

	#[must_use]
	pub fn without_location_and_tokens(self) -> Self {
		match self {
			Self::String(value) => Self::String(value.without_location_and_tokens()),
			Self::Value(value) => Self::Value(value.without_location_and_tokens()),
		}
	}
}

impl Executable {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Some(artifact) = &self.artifact {
			children.insert(artifact.clone().into());
		}
	}

	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self
			.artifact
			.as_ref()
			.map(ToString::to_string)
			.or_else(|| {
				self.path
					.as_ref()
					.map(|path| path.to_string_lossy().into_owned())
			})
			.unwrap_or_default();
		let mut builder = Uri::builder().path(&path);
		if self.artifact.is_some()
			&& let Some(path) = &self.path
		{
			let path = tangram_uri::encode_query_value(&path.to_string_lossy());
			builder = builder.query_raw(&format!("path={path}"));
		}
		builder.build().unwrap()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let artifact = uri.path().parse().ok();
		if artifact.is_none() {
			let path = Some(uri.path().into());
			return Ok(Self { artifact, path });
		}
		let mut path = None;
		if let Some(query) = uri.query_raw() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=')
					&& key == "path"
				{
					path.replace(
						tangram_uri::decode_query_value(value)
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

impl std::fmt::Display for Executable {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Executable {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|error| tg::error!(!error, "invalid uri"))?;
		let executable = Self::with_uri(&uri)?;
		Ok(executable)
	}
}
