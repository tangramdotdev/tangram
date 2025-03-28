use crate as tg;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(pub(crate) crate::Id);

impl Id {
	#[allow(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(crate::Id::new_uuidv7(tg::id::Kind::Pty))
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Pty {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}
