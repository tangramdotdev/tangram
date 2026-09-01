use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
};

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Data {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub cpu: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub creator: Option<tg::Principal>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub hostname: Option<String>,

	#[tangram_serialize(id = 3)]
	pub id: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Option::is_none")]
	pub isolation: Option<tg::sandbox::Isolation>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Option::is_none")]
	pub network: Option<tg::sandbox::Network>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Option::is_none")]
	pub owner: Option<tg::Principal>,

	#[tangram_serialize(id = 9)]
	pub status: tg::sandbox::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[tangram_serialize(
		deserialize_with = "super::get::deserialize_duration",
		id = 10,
		serialize_with = "super::get::serialize_duration"
	)]
	pub ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Option::is_none")]
	pub usage: Option<Usage>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Usage {
	#[tangram_serialize(id = 0)]
	pub cpu: u64,

	#[tangram_serialize(id = 1)]
	pub memory: u64,
}
