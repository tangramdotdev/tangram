use crate as tg;
use tangram_semver as semver;

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub struct Pattern {
	pub(super) string: String,
	pub(super) components: Vec<Component>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Component {
	Normal(String),
	Semver(semver::Pattern),
}

impl Pattern {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.string.is_empty()
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.string.as_str()
	}

	#[must_use]
	pub fn components(&self) -> &Vec<Component> {
		&self.components
	}

	#[must_use]
	pub fn into_components(self) -> Vec<Component> {
		self.components
	}
}

impl AsRef<str> for Pattern {
	fn as_ref(&self) -> &str {
		self.string.as_str()
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let string = s.to_owned();
		let components = s.split('/').map(str::parse).collect::<tg::Result<_>>()?;
		Ok(Self { string, components })
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Component::Normal(string) => write!(f, "{string}"),
			Component::Semver(version) => write!(f, "{version}"),
		}
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(pattern) = s.parse() {
			return Ok(Self::Semver(pattern));
		}
		Ok(Self::Normal(s.to_owned()))
	}
}

impl From<tg::Tag> for Pattern {
	fn from(value: tg::Tag) -> Self {
		let string = value.string;
		let components = value
			.components
			.into_iter()
			.map(Component::Normal)
			.collect();
		Self { string, components }
	}
}