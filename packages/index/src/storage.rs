use tangram_client::prelude::*;

pub mod put;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Owner {
	#[tangram_serialize(id = 0)]
	Organization(tg::organization::Id),

	#[tangram_serialize(id = 1)]
	User(tg::user::Id),
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[repr(u8)]
pub enum Kind {
	#[tangram_serialize(id = 0)]
	ObjectCount,

	#[tangram_serialize(id = 1)]
	ObjectSize,

	#[tangram_serialize(id = 2)]
	ProcessCount,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Association {
	#[tangram_serialize(id = 0)]
	pub reference_count: u64,

	#[tangram_serialize(id = 1)]
	pub touched_at: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Usage {
	pub object_count: u64,
	pub object_size: u64,
	pub process_count: u64,
}

impl Association {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the storage association"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the storage association"))
	}
}

impl Owner {
	#[must_use]
	pub fn id(&self) -> tg::Id {
		match self {
			Self::Organization(id) => id.clone().into(),
			Self::User(id) => id.clone().into(),
		}
	}

	#[must_use]
	pub fn principal(&self) -> tg::Principal {
		match self {
			Self::Organization(id) => tg::Principal::Organization(id.clone()),
			Self::User(id) => tg::Principal::User(id.clone()),
		}
	}
}

impl TryFrom<tg::Id> for Owner {
	type Error = tg::Error;

	fn try_from(id: tg::Id) -> tg::Result<Self> {
		match id.kind() {
			tg::id::Kind::Organization => Ok(Self::Organization(id.try_into()?)),
			tg::id::Kind::User => Ok(Self::User(id.try_into()?)),
			_ => Err(tg::error!(%id, "invalid storage owner")),
		}
	}
}

impl TryFrom<tg::Principal> for Owner {
	type Error = tg::Error;

	fn try_from(principal: tg::Principal) -> tg::Result<Self> {
		match principal {
			tg::Principal::Organization(id) => Ok(Self::Organization(id)),
			tg::Principal::User(id) => Ok(Self::User(id)),
			_ => Err(tg::error!(%principal, "invalid storage owner")),
		}
	}
}
