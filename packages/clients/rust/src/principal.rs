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

	#[display("{_0}")]
	Runner(tg::runner::Id),

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
	Principal(Principal),

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
		if let Ok(id) = s.parse::<tg::runner::Id>() {
			return Ok(Self::Runner(id));
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
			Self::Anonymous | Self::Root => None,
			Self::Group(id) => Some(id.clone().into()),
			Self::Organization(id) => Some(id.clone().into()),
			Self::Process(id) => Some(id.clone().into()),
			Self::Runner(id) => Some(id.clone().into()),
			Self::Sandbox(id) => Some(id.clone().into()),
			Self::User(id) => Some(id.clone().into()),
		}
	}

	#[must_use]
	pub fn to_subject(&self) -> tg::authorization::Subject {
		match self {
			Self::Anonymous => tg::authorization::Subject::Public,
			Self::Group(id) => tg::authorization::Subject::Group(id.clone()),
			Self::Organization(id) => tg::authorization::Subject::Organization(id.clone()),
			Self::Process(id) => tg::authorization::Subject::Process(id.clone()),
			Self::Root => tg::authorization::Subject::Root,
			Self::Runner(id) => tg::authorization::Subject::Runner(id.clone()),
			Self::Sandbox(id) => tg::authorization::Subject::Sandbox(id.clone()),
			Self::User(id) => tg::authorization::Subject::User(id.clone()),
		}
	}

	pub fn try_to_subject(&self) -> tg::Result<tg::authorization::Subject> {
		match self {
			Self::Anonymous => Err(tg::error!("invalid authorization subject")),
			Self::Group(id) => Ok(tg::authorization::Subject::Group(id.clone())),
			Self::Organization(id) => Ok(tg::authorization::Subject::Organization(id.clone())),
			Self::Process(id) => Ok(tg::authorization::Subject::Process(id.clone())),
			Self::Root => Ok(tg::authorization::Subject::Root),
			Self::Runner(id) => Ok(tg::authorization::Subject::Runner(id.clone())),
			Self::Sandbox(id) => Ok(tg::authorization::Subject::Sandbox(id.clone())),
			Self::User(id) => Ok(tg::authorization::Subject::User(id.clone())),
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
		Self::Principal(value)
	}
}
