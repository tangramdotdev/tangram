use tangram_client::prelude::*;

/// A batch of objects to archive.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub items: Vec<Item>,
}

/// An object to archive.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Item {
	#[tangram_serialize(id = 0)]
	pub object: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub put: [u8; 16],
}

impl Arg {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the archive batch arg"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the archive batch arg"))
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}
