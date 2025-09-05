use crate as tg;
use byteorder::ReadBytesExt as _;
use bytes::Bytes;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_itertools::IteratorExt as _;

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

	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		let executable = self.executable.children();
		let args = self.args.iter().flat_map(tg::value::Data::children);
		let env = self.env.values().flat_map(tg::value::Data::children);
		let mounts = self
			.mounts
			.iter()
			.flat_map(tg::command::data::Mount::children);
		std::iter::empty()
			.chain(executable)
			.chain(args)
			.chain(env)
			.chain(mounts)
			.boxed()
	}
}

impl Executable {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		match self {
			Self::Artifact(artifact) => artifact.children().boxed(),
			Self::Module(module) => module.children().boxed(),
			Self::Path(_) => std::iter::empty().boxed(),
		}
	}
}

impl ArtifactExecutable {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		std::iter::once(self.artifact.clone().into())
	}
}

impl ModuleExecutable {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		if let tg::module::data::Item::Object(object) = &self.module.referent.item {
			std::iter::once(object.clone()).left_iterator()
		} else {
			std::iter::empty().right_iterator()
		}
	}
}

impl Mount {
	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		std::iter::once(self.source.clone().into())
	}
}
