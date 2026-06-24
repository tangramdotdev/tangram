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
	#[display("anonymous")]
	Anonymous,

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
	Principal(tg::grant::Principal),

	#[display("{_0}")]
	Specifier(tg::Specifier),
}

impl std::str::FromStr for Principal {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s == "anonymous" {
			return Ok(Self::Anonymous);
		}
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

impl Principal {
	#[must_use]
	pub fn to_id(&self) -> Option<tg::Id> {
		match self {
			Self::Anonymous | Self::Root | Self::Runner => None,
			Self::Group(id) => Some(id.clone().into()),
			Self::Organization(id) => Some(id.clone().into()),
			Self::Process(id) => Some(id.clone().into()),
			Self::Sandbox(id) => Some(id.clone().into()),
			Self::User(id) => Some(id.clone().into()),
		}
	}

	#[must_use]
	pub fn to_grant_requester(&self) -> tg::grant::Principal {
		match self {
			Self::Anonymous => tg::grant::Principal::Public,
			Self::Group(id) => tg::grant::Principal::Group(id.clone()),
			Self::Organization(id) => tg::grant::Principal::Organization(id.clone()),
			Self::Process(id) => tg::grant::Principal::Process(id.clone()),
			Self::Root => tg::grant::Principal::Root,
			Self::Runner => tg::grant::Principal::Runner,
			Self::Sandbox(id) => tg::grant::Principal::Sandbox(id.clone()),
			Self::User(id) => tg::grant::Principal::User(id.clone()),
		}
	}

	pub fn try_to_grant_principal(&self) -> tg::Result<tg::grant::Principal> {
		match self {
			Self::Anonymous => Err(tg::error!("invalid grant principal")),
			Self::Group(id) => Ok(tg::grant::Principal::Group(id.clone())),
			Self::Organization(id) => Ok(tg::grant::Principal::Organization(id.clone())),
			Self::Process(id) => Ok(tg::grant::Principal::Process(id.clone())),
			Self::Root => Ok(tg::grant::Principal::Root),
			Self::Runner => Ok(tg::grant::Principal::Runner),
			Self::Sandbox(id) => Ok(tg::grant::Principal::Sandbox(id.clone())),
			Self::User(id) => Ok(tg::grant::Principal::User(id.clone())),
		}
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
		Self::Principal(
			value
				.try_to_grant_principal()
				.expect("expected the principal to be valid as a grant principal"),
		)
	}
}

impl From<tg::grant::Principal> for Selector {
	fn from(value: tg::grant::Principal) -> Self {
		Self::Principal(value)
	}
}
