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
pub enum Resource {
	#[display("{_0}")]
	Id(tg::Id),

	#[display("{_0}")]
	Specifier(tg::Specifier),
}

impl std::str::FromStr for Resource {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			return Ok(Self::Id(id));
		}
		Ok(Self::Specifier(s.parse()?))
	}
}
