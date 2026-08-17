use {tangram_client::prelude::*, tangram_util::serde::is_default};

pub use tangram_client::object::Stored;

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Object {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub checkout: Option<tg::artifact::Id>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub stored: Stored,

	#[tangram_serialize(id = 4)]
	pub touched_at: i64,
}

impl Object {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))
	}
}
