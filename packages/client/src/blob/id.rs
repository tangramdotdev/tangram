use super::Kind;
use crate as tg;
use bytes::Bytes;

/// A blob ID.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub enum Id {
	Leaf(tg::leaf::Id),
	Branch(tg::branch::Id),
}

impl Id {
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		match kind {
			Kind::Leaf => tg::leaf::Id::new(bytes).into(),
			Kind::Branch => tg::branch::Id::new(bytes).into(),
		}
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf(id) => write!(f, "{id}"),
			Self::Branch(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			tg::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Leaf(id) => id.into(),
			Id::Branch(id) => id.into(),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Id::Leaf(value) => Ok(value.into()),
			tg::object::Id::Branch(value) => Ok(value.into()),
			value => Err(tg::error!(%value, "expected a blob ID")),
		}
	}
}
