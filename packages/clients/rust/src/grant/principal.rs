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
)]
pub enum Principal {
	#[display("{_0}")]
	Group(tg::group::Id),

	#[display("{_0}")]
	Organization(tg::organization::Id),

	#[display("root")]
	Root,

	#[display("{_0}")]
	User(tg::user::Id),
}

impl std::str::FromStr for Principal {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse::<tg::group::Id>() {
			return Ok(Self::Group(id));
		}
		if let Ok(id) = s.parse::<tg::organization::Id>() {
			return Ok(Self::Organization(id));
		}
		if s == "root" {
			return Ok(Self::Root);
		}
		if let Ok(id) = s.parse::<tg::user::Id>() {
			return Ok(Self::User(id));
		}
		Err(tg::error!("invalid grant principal"))
	}
}

impl From<tg::Principal> for Principal {
	fn from(value: tg::Principal) -> Self {
		match value {
			tg::Principal::Group(id) => Self::Group(id),
			tg::Principal::Organization(id) => Self::Organization(id),
			tg::Principal::Root => Self::Root,
			tg::Principal::User(id) => Self::User(id),
		}
	}
}

impl From<Principal> for tg::Principal {
	fn from(value: Principal) -> Self {
		match value {
			Principal::Group(id) => Self::Group(id),
			Principal::Organization(id) => Self::Organization(id),
			Principal::Root => Self::Root,
			Principal::User(id) => Self::User(id),
		}
	}
}
