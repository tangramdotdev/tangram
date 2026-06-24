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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, derive_more::Display)]
#[display(rename_all = "snake_case")]
pub enum Kind {
	Group,
	Object,
	Organization,
	Process,
	Sandbox,
	Tag,
	User,
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

impl Kind {
	#[must_use]
	pub fn from_id_kind(kind: tg::id::Kind) -> Option<Self> {
		if kind.is_object() {
			return Some(Self::Object);
		}
		match kind {
			tg::id::Kind::Group => Some(Self::Group),
			tg::id::Kind::Organization => Some(Self::Organization),
			tg::id::Kind::Process => Some(Self::Process),
			tg::id::Kind::Sandbox => Some(Self::Sandbox),
			tg::id::Kind::Tag => Some(Self::Tag),
			tg::id::Kind::User => Some(Self::User),
			_ => None,
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
