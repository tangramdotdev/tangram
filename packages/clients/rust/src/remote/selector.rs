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
pub enum Selector {
	#[display("{_0}")]
	Id(tg::remote::Id),

	#[display("{_0}")]
	Name(String),
}

impl From<tg::remote::Id> for Selector {
	fn from(value: tg::remote::Id) -> Self {
		Self::Id(value)
	}
}

impl From<String> for Selector {
	fn from(value: String) -> Self {
		Self::Name(value)
	}
}

impl From<&str> for Selector {
	fn from(value: &str) -> Self {
		Self::Name(value.to_owned())
	}
}

impl std::str::FromStr for Selector {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			return Ok(Self::Id(id));
		}
		if s.is_empty() {
			return Err(tg::error!("the remote name must not be empty"));
		}

		Ok(Self::Name(s.to_owned()))
	}
}
