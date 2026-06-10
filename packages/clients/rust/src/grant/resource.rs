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

impl<I> From<tg::Selector<I>> for Resource
where
	I: Into<tg::Id>,
{
	fn from(value: tg::Selector<I>) -> Self {
		match value {
			tg::Selector::Id(id) => Self::Id(id.into()),
			tg::Selector::Specifier(specifier) => Self::Specifier(specifier),
		}
	}
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
