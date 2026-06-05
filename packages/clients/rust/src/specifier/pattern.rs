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
	pub parent: Vec<String>,
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
			parent: Vec::new(),
			component,
		}
	}

	#[must_use]
	pub fn with_parent_and_component(parent: Vec<String>, component: Component) -> Self {
		Self { parent, component }
	}

	#[must_use]
	pub fn any_in_parent(parent: Vec<String>) -> Self {
		Self {
			parent,
			component: Component::new("*"),
		}
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.parent.is_empty() && self.component.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.parent
			.iter()
			.map(String::as_str)
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
		Some(Self::any_in_parent(
			self.components().map(ToOwned::to_owned).collect(),
		))
	}

	#[must_use]
	pub fn for_list(&self) -> Self {
		if self.is_empty() || self.contains_operators() {
			return self.clone();
		}
		Self::any_in_parent(self.components().map(ToOwned::to_owned).collect())
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
		let mut components = specifier.components().collect::<Vec<_>>();
		let Some(component) = components.pop() else {
			return false;
		};
		let parent = components
			.into_iter()
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>();
		self.parent == parent && self.component.matches(component)
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
		is_parent_prefix(&self.parent, &parent) && self.matches_specifier(specifier)
	}

	#[must_use]
	pub fn to_specifier(&self) -> tg::Specifier {
		tg::Specifier::with_components(
			self.components()
				.map(|component| tg::specifier::Component::new(component.to_owned())),
		)
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
		contains_operators(&self.0)
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
		if self.parent.is_empty() {
			write!(f, "{}", self.component)
		} else {
			write!(f, "{}/{}", self.parent.join("/"), self.component)
		}
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let Some((parent, component)) = s.rsplit_once(['/', ':']) else {
			let component = s.parse()?;
			return Ok(Self::with_component(component));
		};
		if component.is_empty() {
			return Err(tg::error!("expected a specifier pattern"));
		}
		let parent = parent
			.split(['/', ':'])
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>();
		if parent
			.iter()
			.any(|component| component.is_empty() || contains_operators(component))
			|| !parent
				.iter()
				.all(|component| component.chars().all(is_component_character))
		{
			return Err(tg::error!("invalid parent"));
		}
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
		if s.contains(['/', ':']) {
			return Err(tg::error!("invalid specifier pattern"));
		}
		if !is_valid_name(s) {
			return Err(tg::error!("invalid specifier pattern"));
		}
		if !contains_operators(s)
			&& (s.parse::<tg::graph::data::Edge<tg::object::Id>>().is_ok()
				|| s.parse::<tg::process::Id>().is_ok())
		{
			return Err(tg::error!("invalid specifier pattern"));
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
		let mut components = value.components().collect::<Vec<_>>();
		let component = components.pop().unwrap_or_default();
		Self {
			parent: components.into_iter().map(ToOwned::to_owned).collect(),
			component: Component(component.to_owned()),
		}
	}
}

pub(crate) fn contains_operators(s: &str) -> bool {
	s.contains(['*', '=', '>', '<', '^', ','])
}

fn is_valid_name(s: &str) -> bool {
	if s.is_empty() {
		return true;
	}
	s.split(',').all(is_valid_constraint)
}

fn is_valid_constraint(s: &str) -> bool {
	if s == "*" {
		return true;
	}
	let value = s
		.strip_prefix(">=")
		.or_else(|| s.strip_prefix("<="))
		.or_else(|| s.strip_prefix('>'))
		.or_else(|| s.strip_prefix('<'))
		.or_else(|| s.strip_prefix('='))
		.or_else(|| s.strip_prefix('^'))
		.unwrap_or(s);
	!value.is_empty() && value.chars().all(is_component_character)
}

fn is_component_character(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
}

fn is_parent_prefix(prefix: &[String], parent: &[String]) -> bool {
	let mut parent = parent.iter().map(String::as_str);
	for component in prefix {
		if parent.next() != Some(component.as_str()) {
			return false;
		}
	}
	true
}
