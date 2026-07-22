use crate::prelude::*;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::IsVariant,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub enum Member {
	#[display("{_0}")]
	Group(tg::group::Id),

	#[display("{_0}")]
	User(tg::user::Id),
}

impl std::str::FromStr for Member {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			return Ok(Self::Group(id));
		}
		if let Ok(id) = s.parse() {
			return Ok(Self::User(id));
		}
		Err(tg::error!("invalid organization member"))
	}
}

impl From<tg::group::Id> for Member {
	fn from(value: tg::group::Id) -> Self {
		Self::Group(value)
	}
}

impl From<tg::user::Id> for Member {
	fn from(value: tg::user::Id) -> Self {
		Self::User(value)
	}
}

impl From<Member> for tg::Id {
	fn from(value: Member) -> Self {
		match value {
			Member::Group(id) => id.into(),
			Member::User(id) => id.into(),
		}
	}
}

impl TryFrom<tg::Id> for Member {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Group => Ok(Self::Group(value.try_into()?)),
			tg::id::Kind::User => Ok(Self::User(value.try_into()?)),
			_ => Err(tg::error!("invalid organization member")),
		}
	}
}
