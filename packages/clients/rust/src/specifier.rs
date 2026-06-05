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

	#[must_use]
	pub fn parent(&self) -> Option<Self> {
		if self.0.len() <= 1 {
			return None;
		}
		let mut components = self.0.clone();
		components.pop();
		Some(Self(components))
	}

	#[must_use]
	pub fn name(&self) -> &str {
		self.0
			.last()
			.expect("a specifier should have a component")
			.as_str()
	}

	pub fn ancestors(&self) -> impl Iterator<Item = Self> + '_ {
		(1..self.0.len()).map(|length| Self(self.0[..length].to_vec()))
	}

	pub fn prefixes(&self) -> impl Iterator<Item = Self> + '_ {
		(1..=self.0.len()).map(|length| Self(self.0[..length].to_vec()))
	}
}

impl Component {
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
		let components = s
			.split('/')
			.map(str::parse)
			.collect::<tg::Result<Vec<_>>>()?;
		let specifier = Self(components);
		Ok(specifier)
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if s.is_empty() || s.parse::<tg::Id>().is_ok() {
			return Err(tg::error!("invalid specifier component"));
		}
		for character in s.chars() {
			if !(character.is_ascii_alphanumeric()
				|| character == '_'
				|| character == '-'
				|| character == '.')
			{
				return Err(tg::error!("invalid specifier component"));
			}
		}
		let component = Component(s.to_owned());
		Ok(component)
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::str::FromStr};

	#[test]
	fn component_rejects_id() {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Tag).to_string();
		assert!(Component::from_str(&id).is_err());
		assert!(Specifier::from_str(&id).is_err());
		assert!(Specifier::from_str(&format!("foo/{id}")).is_err());
	}

	#[test]
	fn component_allows_id_like_strings() {
		assert!(Component::from_str("tag_00abc").is_ok());
	}

	#[test]
	fn specifier_is_slash_separated() {
		assert!(Specifier::from_str("foo/bar").is_ok());
		assert!(Specifier::from_str("foo//bar").is_err());
		assert!(Specifier::from_str("/foo").is_err());
	}

	#[test]
	fn specifier_parent_and_name() {
		let specifier = Specifier::from_str("foo/bar/baz").unwrap();
		assert_eq!(specifier.name(), "baz");
		assert_eq!(specifier.parent().unwrap().to_string(), "foo/bar");
		assert_eq!(Specifier::from_str("foo").unwrap().parent(), None);
		assert_eq!(
			specifier
				.prefixes()
				.map(|specifier| specifier.to_string())
				.collect::<Vec<_>>(),
			vec!["foo", "foo/bar", "foo/bar/baz"]
		);
		assert_eq!(
			specifier
				.ancestors()
				.map(|specifier| specifier.to_string())
				.collect::<Vec<_>>(),
			vec!["foo", "foo/bar"]
		);
	}
}
