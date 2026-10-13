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
	pub cpu: Option<tg::sandbox::Cpu>,

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
	pub memory: Option<tg::sandbox::Resource<u64>>,

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
	#[tangram_serialize(default, id = 12, skip_serializing_if = "Option::is_none")]
	pub storage: Option<tg::sandbox::Resource<u64>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Option::is_none")]
	pub swap: Option<tg::sandbox::Resource<u64>>,

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
	/// Hardware thread-milliseconds.
	#[tangram_serialize(id = 0)]
	pub cpu: ResourceUsage,

	/// The integral of actual memory use in mebibyte-milliseconds.
	#[tangram_serialize(id = 1)]
	pub memory: ResourceUsage,
	#[tangram_serialize(id = 2)]
	pub storage: ResourceUsage,
	#[tangram_serialize(id = 3)]
	pub swap: ResourceUsage,
}

/// Resource time charged for a reservation and for consumption above it.
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ResourceUsage {
	#[tangram_serialize(id = 0)]
	pub consumption: u64,
	#[tangram_serialize(id = 1)]
	pub reservation: u64,
}
