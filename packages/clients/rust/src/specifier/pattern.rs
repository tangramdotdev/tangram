use crate::prelude::*;

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
	pub parent: Option<tg::Specifier>,
	pub component: Component,
}

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
pub struct Component(String);

impl Pattern {
	#[must_use]
	pub fn with_component(component: Component) -> Self {
		Self {
			parent: None,
			component,
		}
	}

	#[must_use]
	pub fn with_parent_and_component(parent: tg::Specifier, component: Component) -> Self {
		Self {
			parent: Some(parent),
			component,
		}
	}

	#[must_use]
	pub fn any_in_parent(parent: Option<tg::Specifier>) -> Self {
		Self {
			parent,
			component: Component::new("*"),
		}
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.parent.is_none() && self.component.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.parent
			.iter()
			.flat_map(tg::Specifier::components)
			.chain(std::iter::once(self.component.as_str()))
	}

	#[must_use]
	pub fn contains_operators(&self) -> bool {
		self.component.contains_operators()
	}

	#[must_use]
	pub fn exact(&self) -> Option<Self> {
		if self.is_empty() || self.contains_operators() {
			return None;
		}
		Some(Self {
			parent: self.parent.clone(),
			component: Component::new(format!("={}", self.component)),
		})
	}

	#[must_use]
	pub fn children(&self) -> Option<Self> {
		if self.is_empty() || self.contains_operators() {
			return None;
		}
		Some(Self::any_in_parent(Some(self.to_specifier())))
	}

	#[must_use]
	pub fn for_list(&self) -> Self {
		if self.is_empty() || self.contains_operators() {
			return self.clone();
		}
		Self::any_in_parent(Some(self.to_specifier()))
	}

	#[must_use]
	pub fn matches(&self, tag: &tg::Tag) -> bool {
		if self.is_empty() {
			return true;
		}
		self.matches_specifier(&tag.specifier)
	}

	#[must_use]
	pub fn matches_for_list(&self, tag: &tg::Tag) -> bool {
		self.matches_specifier_for_list(&tag.specifier)
	}

	#[must_use]
	pub fn matches_in_parent_subtree(&self, tag: &tg::Tag) -> bool {
		self.matches_specifier_in_parent_subtree(&tag.specifier)
	}

	#[must_use]
	pub fn matches_specifier(&self, specifier: &tg::Specifier) -> bool {
		if self.is_empty() {
			return true;
		}
		self.parent == specifier.parent() && self.component.matches(specifier.name())
	}

	#[must_use]
	pub fn matches_specifier_for_list(&self, specifier: &tg::Specifier) -> bool {
		self.matches_specifier(specifier)
			|| self
				.children()
				.is_some_and(|pattern| pattern.matches_specifier(specifier))
	}

	#[must_use]
	pub fn matches_specifier_in_parent_subtree(&self, specifier: &tg::Specifier) -> bool {
		if self.is_empty() {
			return true;
		}
		let parent = specifier
			.components()
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>();
		let parent_matches = match self.parent.as_ref() {
			None => parent.is_empty(),
			Some(prefix) => {
				let mut parent = parent.iter().map(String::as_str);
				prefix
					.components()
					.all(|component| parent.next() == Some(component))
			},
		};
		parent_matches && self.matches_specifier(specifier)
	}

	#[must_use]
	pub fn to_specifier(&self) -> tg::Specifier {
		self.to_string()
			.parse()
			.expect("a pattern without operators should be a valid specifier")
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

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	#[must_use]
	fn contains_operators(&self) -> bool {
		self.0.contains(['*', '=', '>', '<', '^', ','])
	}

	#[must_use]
	fn matches(&self, name: &str) -> bool {
		self.is_empty() || tg::list::matches(name, self.as_str())
	}
}

impl AsRef<str> for Component {
	fn as_ref(&self) -> &str {
		self.0.as_str()
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.parent {
			None => write!(f, "{}", self.component),
			Some(parent) => write!(f, "{parent}/{}", self.component),
		}
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Err(tg::error!("invalid specifier pattern"));
		}
		let Some((parent, component)) = s.rsplit_once('/') else {
			let component = s.parse()?;
			return Ok(Self::with_component(component));
		};
		if component.is_empty() {
			return Err(tg::error!("expected a specifier pattern"));
		}
		if parent.split('/').any(|component| {
			component.is_empty() || component.contains(['*', '=', '>', '<', '^', ','])
		}) {
			return Err(tg::error!("invalid parent"));
		}
		let parent = Some(parent.parse::<tg::Specifier>()?);
		let component = component.parse()?;
		Ok(Self { parent, component })
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Err(tg::error!("invalid specifier pattern"));
		}
		if s.contains('/') {
			return Err(tg::error!("invalid specifier pattern"));
		}
		if !s.is_empty()
			&& !s.split(',').all(|constraint| {
				if constraint == "*" {
					return true;
				}
				let value = constraint
					.strip_prefix(">=")
					.or_else(|| constraint.strip_prefix("<="))
					.or_else(|| constraint.strip_prefix('>'))
					.or_else(|| constraint.strip_prefix('<'))
					.or_else(|| constraint.strip_prefix('='))
					.or_else(|| constraint.strip_prefix('^'))
					.unwrap_or(constraint);
				value.parse::<tg::specifier::Component>().is_ok()
			}) {
			return Err(tg::error!("invalid specifier pattern"));
		}
		if !s.contains(['*', '=', '>', '<', '^', ',']) {
			s.parse::<tg::specifier::Component>()?;
			if s.parse::<tg::graph::data::Edge<tg::object::Id>>().is_ok()
				|| s.parse::<tg::process::Id>().is_ok()
			{
				return Err(tg::error!("invalid specifier pattern"));
			}
		}
		Ok(Self(s.to_owned()))
	}
}

impl TryFrom<Pattern> for tg::Specifier {
	type Error = tg::Error;

	fn try_from(value: Pattern) -> Result<Self, Self::Error> {
		if value.component.contains_operators() {
			return Err(tg::error!("the pattern contains operators"));
		}
		if value.component.is_empty() {
			return Err(tg::error!("expected a specifier"));
		}
		value.to_string().parse()
	}
}

impl From<tg::Tag> for Pattern {
	fn from(value: tg::Tag) -> Self {
		value.specifier.into()
	}
}

impl From<tg::Specifier> for Pattern {
	fn from(value: tg::Specifier) -> Self {
		if value.components().next().is_none() {
			return Self::default();
		}
		Self {
			parent: value.parent(),
			component: Component(value.name().to_owned()),
		}
	}
}
