use crate::tg;
use std::str::FromStr;

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub struct Id(tg::Id);

impl TryFrom<tg::Id> for Id {
	type Error = tg::Error;
	fn try_from(value: tg::Id) -> Result<Self, Self::Error> {
		match value.kind {
			tg::id::Kind::Token => Ok(Self(value)),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}

impl FromStr for Id {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		s.parse::<tg::Id>()?.try_into()
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Id {
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::Token))
	}
}

impl std::fmt::Debug for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{self}")
	}
}

impl Default for Id {
	fn default() -> Self {
		Self::new()
	}
}
