use crate as tg;
use bytes::Bytes;
use std::{
	collections::{BTreeMap, BTreeSet},
	path::PathBuf,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: tg::value::data::Array,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: tg::value::data::Map,

	pub executable: tg::command::data::Executable,

	pub host: String,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::command::data::Mount>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::blob::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Executable {
	Artifact(ArtifactExecutable),
	Module(ModuleExecutable),
	Path(PathExecutable),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ArtifactExecutable {
	pub artifact: tg::artifact::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub subpath: Option<PathBuf>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ModuleExecutable {
	pub module: tg::module::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub export: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PathExecutable {
	pub path: PathBuf,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mount {
	pub source: tg::artifact::Id,
	pub target: PathBuf,
}

impl Command {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		serde_json::from_reader(bytes.into().as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
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
			.collect()
	}
}

impl Executable {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Artifact(artifact) => artifact.children(),
			Self::Module(module) => module.children(),
			Self::Path(_) => [].into(),
		}
	}
}

impl ArtifactExecutable {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[self.artifact.clone().into()].into()
	}
}

impl ModuleExecutable {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		if let tg::module::data::Item::Object(object) = &self.module.referent.item {
			[object.clone()].into()
		} else {
			[].into()
		}
	}
}

impl Mount {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[self.source.clone().into()].into()
	}
}
