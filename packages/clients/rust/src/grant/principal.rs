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

	#[display("public")]
	Public,

	#[display("root")]
	Root,

	#[display("runner")]
	Runner,

	#[display("{_0}")]
	Sandbox(tg::sandbox::Id),

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
		if let Ok(id) = s.parse::<tg::process::Id>() {
			return Ok(Self::Process(id));
		}
		if s == "public" {
			return Ok(Self::Public);
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
		Err(tg::error!("invalid grant principal"))
	}
}

impl From<tg::Principal> for Principal {
	fn from(value: tg::Principal) -> Self {
		match value {
			tg::Principal::Group(id) => Self::Group(id),
			tg::Principal::Organization(id) => Self::Organization(id),
			tg::Principal::Process(id) => Self::Process(id),
			tg::Principal::Root => Self::Root,
			tg::Principal::Runner => Self::Runner,
			tg::Principal::Sandbox(id) => Self::Sandbox(id),
			tg::Principal::User(id) => Self::User(id),
		}
	}
}
