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
	#[display("all")]
	All,

	#[display("{_0}")]
	Group(tg::group::Id),

	#[display("root")]
	Root,

	#[display("{_0}")]
	User(tg::user::Id),
}

impl std::str::FromStr for Principal {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s == "all" {
			return Ok(Self::All);
		}
		if s == "root" {
			return Ok(Self::Root);
		}
		if let Ok(id) = s.parse::<tg::group::Id>() {
			return Ok(Self::Group(id));
		}
		if let Ok(id) = s.parse::<tg::user::Id>() {
			return Ok(Self::User(id));
		}
		Err(tg::error!("invalid principal"))
	}
}
