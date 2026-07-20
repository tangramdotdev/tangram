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

	#[display("{_0}")]
	Runner(tg::runner::Id),

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
		if let Ok(id) = s.parse::<tg::runner::Id>() {
			return Ok(Self::Runner(id));
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

impl Principal {
	pub fn try_to_principal(&self) -> tg::Result<tg::Principal> {
		match self {
			Self::Group(id) => Ok(tg::Principal::Group(id.clone())),
			Self::Organization(id) => Ok(tg::Principal::Organization(id.clone())),
			Self::Process(id) => Ok(tg::Principal::Process(id.clone())),
			Self::Public => Err(tg::error!("invalid principal")),
			Self::Root => Ok(tg::Principal::Root),
			Self::Runner(id) => Ok(tg::Principal::Runner(id.clone())),
			Self::Sandbox(id) => Ok(tg::Principal::Sandbox(id.clone())),
			Self::User(id) => Ok(tg::Principal::User(id.clone())),
		}
	}
}
