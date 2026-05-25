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
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Principal {
	#[display("all")]
	All,
	#[display("{_0}")]
	User(tg::user::Id),
	#[display("{_0}")]
	Group(tg::group::Id),
}

impl std::str::FromStr for Principal {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s == "all" {
			return Ok(Self::All);
		}
		if let Ok(id) = s.parse::<tg::user::Id>() {
			return Ok(Self::User(id));
		}
		if let Ok(id) = s.parse::<tg::group::Id>() {
			return Ok(Self::Group(id));
		}
		Err(tg::error!("invalid principal"))
	}
}
