use crate::prelude::*;

pub mod batch;
pub mod delete;
pub mod list;
pub mod pattern;
pub mod put;

pub use self::pattern::Pattern;

#[derive(
	Clone,
	Debug,
	Default,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Tag(String);

impl Tag {
	#[must_use]
	pub fn new(s: impl Into<String>) -> Self {
		Self(s.into())
	}

	#[must_use]
	pub fn empty() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		&self.0
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn components(&self) -> impl Iterator<Item = &str> {
		self.0.split('/')
	}

	pub fn push(&mut self, component: &str) {
		if !self.is_empty() {
			self.0.push('/');
		}
		self.0.push_str(component);
	}

	#[must_use]
	pub fn parent(&self) -> Option<Self> {
		let mut components: Vec<_> = self.components().collect();
		components.pop()?;
		Some(Self(components.join("/")))
	}
}

impl AsRef<str> for Tag {
	fn as_ref(&self) -> &str {
		self.0.as_str()
	}
}

impl std::fmt::Display for Tag {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Tag {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
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
		self.0.hash(state);
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
			let order = self::pattern::compare(a, b);
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
