#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version {
	pub major: u64,
	pub minor: u64,
	pub patch: u64,
	pub prerelease: String,
	pub build: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Pattern {
	pub comparators: Vec<Comparator>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Comparator {
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
	pub fn matches(&self, version: &Version) -> bool {
		todo!()
	}
}

impl Comparator {
	pub fn matches(&self, version: &Version) -> bool {
		todo!()
	}
}

impl std::fmt::Display for Version {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl std::str::FromStr for Version {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
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

impl std::fmt::Display for Comparator {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl std::str::FromStr for Comparator {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		todo!()
	}
}
