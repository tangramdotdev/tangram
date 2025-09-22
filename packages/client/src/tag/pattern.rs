use crate as tg;

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
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Component {
	Normal(tg::tag::Component),
	Version(tangram_version::Pattern),
	Wildcard,
}

impl Pattern {
	#[must_use]
	pub fn wildcard() -> Self {
		Self::with_components(vec![Component::Wildcard])
	}

	#[must_use]
	pub fn with_components(components: Vec<Component>) -> Self {
		Self { components }
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.components.is_empty()
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
	pub fn matches(&self, tag: &tg::Tag) -> bool {
		if tag.components().len() != self.components().len() {
			return false;
		}
		for (pattern, tag) in std::iter::zip(self.components(), tag.components()) {
			match pattern {
				Component::Normal(pattern) => {
					if tag != pattern {
						return false;
					}
				},
				Component::Version(pattern) => {
					let tg::tag::Component::Version(tag) = tag else {
						return false;
					};
					if !pattern.matches(tag) {
						return false;
					}
				},
				Component::Wildcard => (),
			}
		}
		true
	}

	#[must_use]
	pub fn parent(&self) -> Option<Self> {
		let mut components = self.components.clone();
		components.pop()?;
		let parent = Self::with_components(components);
		Some(parent)
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, component) in self.components.iter().enumerate() {
			if i > 0 {
				write!(f, "/")?;
			}
			write!(f, "{component}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let components = s.split('/').map(str::parse).collect::<tg::Result<_>>()?;
		Ok(Self { components })
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Component::Normal(string) => write!(f, "{string}"),
			Component::Version(version) => write!(f, "{version}"),
			Component::Wildcard => write!(f, "*"),
		}
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() || s == "*" {
			return Ok(Self::Wildcard);
		}
		if let Ok(component) = s.parse() {
			return Ok(Self::Normal(component));
		}
		if let Ok(pattern) = s.parse() {
			return Ok(Self::Version(pattern));
		}
		Err(tg::error!(%component = s, "invalid component"))
	}
}

impl From<tg::Tag> for Pattern {
	fn from(value: tg::Tag) -> Self {
		let components = value
			.components
			.into_iter()
			.map(Component::Normal)
			.collect();
		Self { components }
	}
}
