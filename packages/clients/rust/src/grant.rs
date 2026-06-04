use crate::prelude::*;

pub mod create;
pub mod delete;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub created_at: i64,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_by: Option<tg::user::Id>,
	pub permission: tg::grant::Permission,
	pub principal: tg::grant::Principal,
	pub resource: tg::Id,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::FromStr,
	serde::Deserialize,
	serde::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum Permission {
	Admin,
	Read,
	Write,
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
pub enum Resource {
	#[display("{_0}")]
	Id(tg::Id),

	#[display("{_0}")]
	Specifier(tg::Specifier),
}

impl Permission {
	#[must_use]
	pub fn implies(self, permission: Self) -> bool {
		match (self, permission) {
			(Self::Admin, Self::Admin | Self::Read | Self::Write)
			| (Self::Write, Self::Read | Self::Write)
			| (Self::Read, Self::Read) => true,
			(Self::Read | Self::Write, Self::Admin) | (Self::Read, Self::Write) => false,
		}
	}
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

impl std::str::FromStr for Resource {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			Ok(Self::Id(id))
		} else {
			Ok(Self::Specifier(s.parse()?))
		}
	}
}
