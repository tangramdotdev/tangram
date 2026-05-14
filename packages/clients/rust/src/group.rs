use crate::prelude::*;

pub mod create;
pub mod delete;
pub mod get;
pub mod grants;
pub mod list;
pub mod member;

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
#[debug(r#"tg::group::Id("{_0}")"#)]
#[serde(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Group {
	pub id: Id,
	pub handle: String,
}

impl tg::group::Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::Group))
	}
}

impl From<tg::group::Id> for tg::Id {
	fn from(value: tg::group::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::group::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Group {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for tg::group::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}
