use crate::prelude::*;

pub mod batch;
pub mod delete;
pub mod grants;
pub mod put;

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Tag {
	pub namespace: tg::Namespace,
	pub name: Name,
}

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Name(String);

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Grant {
	pub tag: tg::Tag,
	pub principal: tg::Principal,
	pub permission: tg::Permission,
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub created_by: Option<tg::user::Id>,
}

impl Tag {
	#[must_use]
	pub fn new(name: Name) -> Self {
		Self::with_name(name)
	}

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
	pub fn is_empty(&self) -> bool {
		self.name.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.namespace
			.components()
			.chain(std::iter::once(self.name.as_str()))
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
}

impl AsRef<str> for Name {
	fn as_ref(&self) -> &str {
		self.0.as_str()
	}
}

impl std::fmt::Display for Tag {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.namespace.is_root() {
			write!(f, "{}", self.name)
		} else {
			write!(f, "{}/{}", self.namespace, self.name)
		}
	}
}

impl std::str::FromStr for Tag {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let Some((namespace, name)) = s.rsplit_once('/') else {
			let name = s.parse()?;
			return Ok(Self::with_name(name));
		};
		if name.is_empty() {
			return Err(tg::error!("expected a tag"));
		}
		Ok(Self {
			namespace: namespace.parse()?,
			name: name.parse()?,
		})
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
		if s.is_empty() {
			return Err(tg::error!("expected a tag"));
		}
		if !s.chars().all(is_name_character) {
			return Err(tg::error!("invalid tag"));
		}
		if s.parse::<tg::graph::data::Edge<tg::object::Id>>().is_ok()
			|| s.parse::<tg::process::Id>().is_ok()
		{
			return Err(tg::error!("invalid tag"));
		}
		Ok(Self::new(s.to_owned()))
	}
}

impl std::cmp::PartialEq for Tag {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == std::cmp::Ordering::Equal
	}
}

impl std::cmp::Eq for Tag {}

impl std::hash::Hash for Tag {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.namespace.hash(state);
		self.name.hash(state);
	}
}

impl std::cmp::PartialOrd for Tag {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl std::cmp::Ord for Tag {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		let mut a = self.components();
		let mut b = other.components();
		for (a, b) in a.by_ref().zip(b.by_ref()) {
			let order = tg::list::compare(a, b);
			if order != std::cmp::Ordering::Equal {
				return order;
			}
		}
		match (a.next(), b.next()) {
			(Some(_), None) => std::cmp::Ordering::Greater,
			(None, Some(_)) => std::cmp::Ordering::Less,
			(None, None) => std::cmp::Ordering::Equal,
			_ => unreachable!(),
		}
	}
}

impl std::cmp::PartialEq for Name {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == std::cmp::Ordering::Equal
	}
}

impl std::cmp::Eq for Name {}

impl std::hash::Hash for Name {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.hash(state);
	}
}

impl std::cmp::PartialOrd for Name {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl std::cmp::Ord for Name {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		tg::list::compare(self.as_str(), other.as_str())
	}
}

pub(crate) fn is_name_character(c: char) -> bool {
	c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.'
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_name_from_str_accepts_alphanumerics_underscore_dash_and_dot() {
		assert!("abc-DEF_123.foo".parse::<Name>().is_ok());
	}

	#[test]
	fn test_name_from_str_rejects_other_characters() {
		for value in ["", "foo/bar", "foo bar", "foo*", "é"] {
			assert!(value.parse::<Name>().is_err());
		}
	}
}
