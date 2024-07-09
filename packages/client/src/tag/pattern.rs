use crate as tg;
use itertools::Itertools as _;
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
	string: String,
	components: Vec<Component>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Component {
	Normal(tg::tag::Component),
	Semver(semver::Pattern),
}

impl Pattern {
	#[must_use]
	pub fn with_components(components: Vec<Component>) -> Self {
		let string = components.iter().map(ToString::to_string).join("/");
		Self { string, components }
	}

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

	#[must_use]
	pub fn into_string_and_components(self) -> (String, Vec<Component>) {
		(self.string, self.components)
	}

	#[must_use]
	pub fn matches(&self, tag: &tg::Tag) -> bool {
		if tag.components().len() != self.components().len() {
			return false;
		}
		for (tag, pattern) in std::iter::zip(tag.components(), self.components()) {
			match pattern {
				Component::Normal(pattern) => {
					if tag.as_str() != pattern.as_str() {
						return false;
					}
				},
				Component::Semver(pattern) => {
					let Ok(tag) = tag.as_str().parse::<semver::Version>() else {
						return false;
					};
					if !pattern.matches(&tag) {
						return false;
					}
				},
			}
		}
		true
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
		if let Ok(component) = s.parse() {
			return Ok(Self::Normal(component));
		}
		if let Ok(pattern) = s.parse() {
			return Ok(Self::Semver(pattern));
		}
		Err(tg::error!("invalid component"))
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
