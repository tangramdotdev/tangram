use {tangram_client::prelude::*, tangram_util::serde::is_default};

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Entry {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub touched_at: i64,
}

impl Entry {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the cache entry"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the cache entry"))
	}
}
