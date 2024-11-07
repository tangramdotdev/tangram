use crate as tg;
use std::collections::BTreeSet;

pub use self::module::Module;

pub mod module;

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Executable {
	Artifact(tg::Artifact),
	Module(tg::target::executable::Module),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Data {
	Artifact(tg::artifact::Id),
	Module(tg::target::executable::module::Data),
}

impl Executable {
	#[must_use]
	pub fn object(&self) -> Vec<tg::Object> {
		match self {
			Self::Artifact(artifact) => [artifact.clone().into()].into(),
			Self::Module(module) => module.objects(),
		}
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Data::Artifact(id) => [id.clone().into()].into(),
			Data::Module(module) => module.children(),
		}
	}
}

impl TryFrom<Data> for Executable {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		match data {
			Data::Artifact(id) => Ok(Self::Artifact(tg::Artifact::with_id(id))),
			Data::Module(module) => Ok(Self::Module(module.try_into()?)),
		}
	}
}

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self::Artifact(value.into())
	}
}
