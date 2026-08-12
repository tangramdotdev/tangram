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
pub enum Subject {
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
	Specifier(tg::Specifier),

	#[display("{_0}")]
	Subject(Subject),
}

impl std::str::FromStr for Subject {
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
		Err(tg::error!("invalid authorization subject"))
	}
}

impl Subject {
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

impl std::str::FromStr for Selector {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(subject) = s.parse() {
			Ok(Self::Subject(subject))
		} else {
			Ok(Self::Specifier(s.parse()?))
		}
	}
}

impl From<tg::Principal> for Selector {
	fn from(value: tg::Principal) -> Self {
		Self::Subject(
			value
				.try_to_subject()
				.expect("expected the principal to be a valid authorization subject"),
		)
	}
}

impl From<Subject> for Selector {
	fn from(value: Subject) -> Self {
		Self::Subject(value)
	}
}
