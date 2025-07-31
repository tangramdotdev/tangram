use crate as tg;

#[derive(
	Clone,
	Debug,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Template {
	#[tangram_serialize(id = 0)]
	pub components: Vec<Component>,
}

#[derive(
	Clone,
	Debug,
	PartialEq,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Component {
	#[tangram_serialize(id = 0)]
	String(String),
	#[tangram_serialize(id = 1)]
	Artifact(tg::artifact::Id),
}

impl From<tg::directory::Id> for Component {
	fn from(value: tg::directory::Id) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::file::Id> for Component {
	fn from(value: tg::file::Id) -> Self {
		Self::Artifact(value.into())
	}
}

impl From<tg::symlink::Id> for Component {
	fn from(value: tg::symlink::Id) -> Self {
		Self::Artifact(value.into())
	}
}
