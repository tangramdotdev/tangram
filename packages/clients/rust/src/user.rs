use crate::prelude::*;

pub mod get;
pub mod login;

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[debug("tg::user::Id(\"{_0}\")]")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct User {
	pub id: Id,
	pub emails: Vec<String>,
}

impl tg::user::Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::User))
	}
}

impl From<tg::user::Id> for tg::Id {
	fn from(value: tg::user::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::user::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::User {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for tg::user::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}
