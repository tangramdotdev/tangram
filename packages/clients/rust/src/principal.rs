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

	#[display("{_0}")]
	Process(tg::process::Id),

	#[display("root")]
	Root,

	#[display("runner")]
	Runner,

	#[display("{_0}")]
	Sandbox(tg::sandbox::Id),

	#[display("{_0}")]
	User(tg::user::Id),
}

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
pub enum Selector {
	#[display("{_0}")]
	Principal(tg::Principal),

	#[display("{_0}")]
	Specifier(tg::Specifier),
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
		if let Ok(id) = s.parse::<tg::process::Id>() {
			return Ok(Self::Process(id));
		}
		if s == "root" {
			return Ok(Self::Root);
		}
		if s == "runner" {
			return Ok(Self::Runner);
		}
		if let Ok(id) = s.parse::<tg::sandbox::Id>() {
			return Ok(Self::Sandbox(id));
		}
		if let Ok(id) = s.parse::<tg::user::Id>() {
			return Ok(Self::User(id));
		}
		Err(tg::error!("invalid principal"))
	}
}

impl std::str::FromStr for Selector {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(principal) = s.parse() {
			Ok(Self::Principal(principal))
		} else {
			Ok(Self::Specifier(s.parse()?))
		}
	}
}

impl From<tg::Principal> for Selector {
	fn from(value: tg::Principal) -> Self {
		Self::Principal(value)
	}
}

impl From<tg::grant::Principal> for Selector {
	fn from(value: tg::grant::Principal) -> Self {
		Self::Principal(value.into())
	}
}
