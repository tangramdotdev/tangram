use {
	tangram_client::prelude::*,
	tangram_util::serde::{is_default, is_false},
};

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Object {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub cache_entry: Option<tg::artifact::Id>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub stored: Stored,

	#[tangram_serialize(id = 4)]
	pub touched_at: i64,
}

/// The stored status of an object in the index.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Stored {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub subtree: bool,
}

bitflags::bitflags! {
	#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
	pub struct Permissions: u8 {
		const NODE = 1 << 0;
		const SUBTREE = 1 << 1;
	}
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

impl Stored {
	pub fn merge(&mut self, other: &Self) {
		self.subtree = self.subtree || other.subtree;
	}
}

impl Permissions {
	#[must_use]
	pub fn from_grant_permission(permission: tg::grant::permission::object::Permission) -> Self {
		match permission {
			tg::grant::permission::object::Permission::Node => Self::NODE,
			tg::grant::permission::object::Permission::Subtree => Self::SUBTREE,
		}
	}

	#[must_use]
	pub fn contains_grant_permission(
		self,
		permission: tg::grant::permission::object::Permission,
	) -> bool {
		self.contains(Self::from_grant_permission(permission))
	}
}
