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
pub enum Selector<I> {
	#[display("{_0}")]
	Id(I),

	#[display("{_0}")]
	Specifier(tg::Specifier),
}

impl<I> From<tg::Specifier> for Selector<I> {
	fn from(value: tg::Specifier) -> Self {
		Self::Specifier(value)
	}
}

impl<I> std::str::FromStr for Selector<I>
where
	I: std::str::FromStr<Err = tg::Error>,
{
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(id) = s.parse() {
			Ok(Self::Id(id))
		} else {
			Ok(Self::Specifier(s.parse()?))
		}
	}
}
