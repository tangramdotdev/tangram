use crate::prelude::*;

pub mod pattern;

pub use self::pattern::Pattern;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Specifier(Vec<Component>);

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Component(String);

impl Specifier {
	#[must_use]
	pub fn with_components(components: impl IntoIterator<Item = Component>) -> Self {
		Self(components.into_iter().collect())
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.0.iter().map(Component::as_str)
	}
}

impl Component {
	#[must_use]
	pub fn new(s: impl Into<String>) -> Self {
		Self(s.into())
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl std::fmt::Display for Specifier {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (index, component) in self.0.iter().enumerate() {
			if index > 0 {
				write!(f, "/")?;
			}
			write!(f, "{component}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Specifier {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s.is_empty() || s.starts_with('/') || s.starts_with("./") || s.starts_with("../") {
			return Err(tg::error!("invalid specifier"));
		}
		if s.parse::<tg::Id>().is_ok() {
			return Err(tg::error!("invalid specifier"));
		}
		let components = s
			.split(['/', ':'])
			.map(str::parse)
			.collect::<tg::Result<Vec<Component>>>()?;
		if components.is_empty() {
			return Err(tg::error!("invalid specifier"));
		}
		Ok(Self(components))
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s.is_empty()
			|| s.contains(['/', ':'])
			|| s.parse::<tg::Id>().is_ok()
			|| tg::specifier::pattern::contains_operators(s)
			|| !s.chars().all(is_component_character)
		{
			return Err(tg::error!("invalid specifier component"));
		}
		Ok(Self::new(s))
	}
}

fn is_component_character(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
}
