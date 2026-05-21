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
	pub namespace: tg::Namespace,
	pub name: Name,
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
pub struct Name(String);

impl Pattern {
	#[must_use]
	pub fn with_name(name: Name) -> Self {
		Self {
			namespace: tg::Namespace::root(),
			name,
		}
	}

	#[must_use]
	pub fn with_namespace_and_name(namespace: tg::Namespace, name: Name) -> Self {
		Self { namespace, name }
	}

	#[must_use]
	pub fn any_in_namespace(namespace: tg::Namespace) -> Self {
		Self {
			namespace,
			name: Name::new("*"),
		}
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.namespace.is_root() && self.name.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.namespace
			.components()
			.chain(std::iter::once(self.name.as_str()))
	}

	#[must_use]
	pub fn contains_operators(&self) -> bool {
		self.name.contains_operators()
	}

	#[must_use]
	pub fn exact(&self) -> Option<Self> {
		if self.is_empty() || self.contains_operators() {
			return None;
		}
		Some(Self {
			namespace: self.namespace.clone(),
			name: Name::new(format!("={}", self.name)),
		})
	}

	#[must_use]
	pub fn children(&self) -> Option<Self> {
		if self.is_empty() || self.contains_operators() {
			return None;
		}
		Some(Self::any_in_namespace(self.to_namespace()))
	}

	#[must_use]
	pub fn for_list(&self) -> Self {
		if self.is_empty() || self.contains_operators() {
			return self.clone();
		}
		Self::any_in_namespace(self.to_namespace())
	}

	#[must_use]
	pub fn matches(&self, tag: &tg::Tag) -> bool {
		if self.is_empty() {
			return true;
		}
		self.namespace == tag.namespace && self.name.matches(&tag.name)
	}

	#[must_use]
	pub fn matches_for_list(&self, tag: &tg::Tag) -> bool {
		self.matches(tag) || self.children().is_some_and(|pattern| pattern.matches(tag))
	}

	#[must_use]
	pub fn matches_in_namespace_subtree(&self, tag: &tg::Tag) -> bool {
		if self.is_empty() {
			return true;
		}
		is_namespace_prefix(&self.namespace, &tag.namespace) && self.name.matches(&tag.name)
	}

	#[must_use]
	pub fn to_namespace(&self) -> tg::Namespace {
		tg::Namespace::with_components(self.components().map(ToOwned::to_owned))
	}
}

impl Name {
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
	fn matches(&self, name: &tg::tag::Name) -> bool {
		self.is_empty() || super::matches(name.as_str(), self.as_str())
	}
}

impl AsRef<str> for Name {
	fn as_ref(&self) -> &str {
		self.0.as_str()
	}
}

impl std::fmt::Display for Pattern {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.namespace.is_root() {
			write!(f, "{}", self.name)
		} else {
			write!(f, "{}/{}", self.namespace, self.name)
		}
	}
}

impl std::str::FromStr for Pattern {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let Some((namespace, name)) = s.rsplit_once('/') else {
			let name = s.parse()?;
			return Ok(Self::with_name(name));
		};
		if name.is_empty() {
			return Err(tg::error!("expected a tag pattern"));
		}
		let namespace = namespace.parse::<tg::Namespace>()?;
		if namespace.components().any(contains_operators) {
			return Err(tg::error!("invalid namespace"));
		}
		let name = name.parse()?;
		Ok(Self { namespace, name })
	}
}

impl std::fmt::Display for Name {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Name {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.contains('/') {
			return Err(tg::error!("invalid tag pattern"));
		}
		if !is_valid_name(s) {
			return Err(tg::error!("invalid tag pattern"));
		}
		if !contains_operators(s)
			&& (s.parse::<tg::graph::data::Edge<tg::object::Id>>().is_ok()
				|| s.parse::<tg::process::Id>().is_ok())
		{
			return Err(tg::error!("invalid tag pattern"));
		}
		Ok(Self(s.to_owned()))
	}
}

impl TryFrom<Pattern> for tg::Tag {
	type Error = tg::Error;

	fn try_from(value: tg::list::Pattern) -> Result<Self, Self::Error> {
		if value.name.contains_operators() {
			return Err(tg::error!("the pattern contains operators"));
		}
		if value.name.is_empty() {
			return Err(tg::error!("expected a tag"));
		}
		Ok(tg::Tag {
			namespace: value.namespace,
			name: value.name.as_str().parse()?,
		})
	}
}

impl From<tg::Tag> for Pattern {
	fn from(value: tg::Tag) -> Self {
		Self {
			namespace: value.namespace,
			name: Name(value.name.as_str().to_owned()),
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
	!value.is_empty() && value.chars().all(tg::tag::is_name_character)
}

fn is_namespace_prefix(prefix: &tg::Namespace, namespace: &tg::Namespace) -> bool {
	let mut namespace = namespace.components();
	for component in prefix.components() {
		if namespace.next() != Some(component) {
			return false;
		}
	}
	true
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_name_from_str_accepts_valid_literals_and_operators() {
		for value in [
			"",
			"foo.bar",
			"*",
			"=foo.bar",
			">=1.2.3",
			"<2.0",
			"^1.2",
			">=1.0,<2.0",
		] {
			assert!(value.parse::<Name>().is_ok(), "{value}");
		}
	}

	#[test]
	fn test_name_from_str_rejects_invalid_operator_positions_and_literals() {
		for value in [
			"foo/bar", "foo bar", "foo*", "*foo", "=", ">=", "^", "foo,", ",foo", "foo,,bar",
			"foo=bar", "é",
		] {
			assert!(value.parse::<Name>().is_err(), "{value}");
		}
	}

	#[test]
	fn test_pattern_from_str_rejects_invalid_namespaces() {
		for value in ["foo/é", "foo*/bar"] {
			assert!(value.parse::<Pattern>().is_err(), "{value}");
		}
	}
}
