use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Default,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Checksum {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub actual: Option<tg::Checksum>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub expected: Option<tg::Checksum>,
}

impl Checksum {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.actual.is_none() && self.expected.is_none()
	}
}
