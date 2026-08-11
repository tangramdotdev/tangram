use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Account {
	#[tangram_serialize(id = 0)]
	Organization(tg::organization::Id),

	#[tangram_serialize(id = 1)]
	User(tg::user::Id),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub object_count: u64,
	pub object_size: u64,
	pub process_count: u64,
}

impl Account {
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

impl TryFrom<tg::Id> for Account {
	type Error = tg::Error;

	fn try_from(id: tg::Id) -> tg::Result<Self> {
		match id.kind() {
			tg::id::Kind::Organization => Ok(Self::Organization(id.try_into()?)),
			tg::id::Kind::User => Ok(Self::User(id.try_into()?)),
			_ => Err(tg::error!(%id, "invalid usage account")),
		}
	}
}

impl TryFrom<tg::Principal> for Account {
	type Error = tg::Error;

	fn try_from(principal: tg::Principal) -> tg::Result<Self> {
		match principal {
			tg::Principal::Organization(id) => Ok(Self::Organization(id)),
			tg::Principal::User(id) => Ok(Self::User(id)),
			_ => Err(tg::error!(%principal, "invalid usage account")),
		}
	}
}
