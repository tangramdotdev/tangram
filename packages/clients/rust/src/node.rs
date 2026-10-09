use tangram_util::serde::{is_default, is_false};

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Ancestors {
	#[serde(default, skip_serializing_if = "is_false")]
	pub create: bool,

	#[serde(default, skip_serializing_if = "is_default")]
	pub pull: AncestorsPull,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum AncestorsPull {
	#[tangram_serialize(id = 0)]
	Always,
	#[default]
	#[tangram_serialize(id = 1)]
	Missing,
	#[tangram_serialize(id = 2)]
	Never,
}
