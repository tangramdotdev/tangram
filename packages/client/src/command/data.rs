use crate::{self as tg};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: tg::value::data::Array,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: tg::value::data::Map,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub executable: Option<tg::command::data::Executable>,

	pub host: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Executable {
	Artifact(tg::artifact::Id),
	Module(Module),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Module {
	pub kind: tg::module::Kind,
	pub referent: tg::Referent<tg::object::Id>,
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
		let executable = self
			.executable
			.iter()
			.flat_map(tg::command::data::Executable::children);
		let args = self.args.iter().flat_map(tg::value::Data::children);
		let env = self.env.values().flat_map(tg::value::Data::children);
		std::iter::empty()
			.chain(executable)
			.chain(args)
			.chain(env)
			.collect()
	}
}

impl Executable {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Artifact(id) => [id.clone().into()].into(),
			Self::Module(module) => module.children(),
		}
	}
}

impl Module {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[self.referent.item.clone()].into()
	}
}
