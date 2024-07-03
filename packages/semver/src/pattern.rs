use crate::Version;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pattern {
	pub comparators: Vec<Component>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Component {
	pub operator: Operator,
	pub major: u64,
	pub minor: Option<u64>,
	pub patch: Option<u64>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Operator {
	Equal,
	Greater,
	GreaterEq,
	Less,
	LessEq,
	Tilde,
	Caret,
}

impl Pattern {
	#[must_use]
	pub fn matches(&self, version: &Version) -> bool {
		todo!()
	}
}

impl Component {
	#[must_use]
	pub fn matches(&self, version: &Version) -> bool {
		todo!()
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl std::str::FromStr for Pattern {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		todo!()
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl std::str::FromStr for Component {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		todo!()
	}
}