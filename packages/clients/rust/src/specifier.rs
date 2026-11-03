use crate::prelude::*;

pub mod pattern;

pub use self::pattern::Pattern;

/// The byte that separates a specifier's components from the specifier in its key, and that ends a run of letters in a component's key.
const KEY_TERMINATOR: u8 = 0x00;

/// The byte that ends a component's key.
const KEY_END: u8 = 0x01;

/// The byte that starts a run of letters, which sorts after the end of a component.
const KEY_LETTERS: u8 = 0x02;

/// The byte that starts a run of digits, which sorts after a run of letters.
const KEY_DIGITS: u8 = 0x03;

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

	/// Get the key that sorts specifiers component by component, comparing each component as a version, so that a specifier sorts before its descendants and the descendants of each specifier are contiguous.
	#[must_use]
	pub fn key(&self) -> Vec<u8> {
		let mut key = self.prefix_key();
		key.push(KEY_TERMINATOR);
		key.extend_from_slice(self.to_string().as_bytes());
		key
	}

	/// Get the key of the specifier's components, which every key of the specifier and its descendants starts with.
	#[must_use]
	pub fn prefix_key(&self) -> Vec<u8> {
		let mut key = Vec::new();
		for component in &self.0 {
			component.write_key(&mut key);
		}
		key
	}

	/// Get the range of the keys of the specifier's descendants, from the inclusive start to the exclusive end.
	#[must_use]
	pub fn descendants_key_range(&self) -> (Vec<u8>, Vec<u8>) {
		let prefix = self.prefix_key();
		let mut start = prefix.clone();
		start.push(KEY_END);
		let mut end = prefix;
		end.push(KEY_DIGITS + 1);
		(start, end)
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

	/// Write the component's key, which skips punctuation, sorts the end before letters and letters before digits, and sorts runs of digits by their value.
	fn write_key(&self, key: &mut Vec<u8>) {
		let mut chars = self.0.chars().peekable();
		loop {
			while chars.next_if(|c| !c.is_ascii_alphanumeric()).is_some() {}
			let Some(c) = chars.peek().copied() else {
				key.push(KEY_END);
				return;
			};
			if c.is_ascii_digit() {
				let mut digits = String::new();
				while let Some(c) = chars.next_if(char::is_ascii_digit) {
					digits.push(c);
				}
				let digits = digits.trim_start_matches('0');
				let digits = if digits.is_empty() { "0" } else { digits };
				let length = u16::try_from(digits.len()).unwrap_or(u16::MAX);
				key.push(KEY_DIGITS);
				key.extend_from_slice(&length.to_be_bytes());
				key.extend_from_slice(digits.as_bytes());
			} else {
				key.push(KEY_LETTERS);
				while let Some(c) = chars.next_if(char::is_ascii_alphabetic) {
					key.push(u8::try_from(c).unwrap());
				}
				key.push(KEY_TERMINATOR);
			}
		}
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
		if s.is_empty() || matches!(s, "." | "..") || tg::Id::try_parse_prefix(s).is_some() {
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

	fn assert_key_order(specifiers: &[&str]) {
		for pair in specifiers.windows(2) {
			let a = Specifier::from_str(pair[0]).unwrap();
			let b = Specifier::from_str(pair[1]).unwrap();
			assert!(a.key() < b.key(), "expected {a} to sort before {b}");
		}
	}

	// Components compare as versions, with numbers by value and a tilde before everything.
	#[test]
	fn key_sorts_components_as_versions() {
		assert_key_order(&["1", "2", "10", "100"]);
		assert_key_order(&["1.0", "1.0.1", "1.1", "1.1a", "1.2"]);
		assert_key_order(&["a", "a1", "ab", "b"]);
		assert_key_order(&["1.05", "1.5", "1.6"]);
	}

	// A specifier sorts before its descendants, and the descendants of each specifier are contiguous.
	#[test]
	fn key_sorts_specifiers_by_component() {
		assert_key_order(&["a", "a/c", "a/c/d", "a/d", "ab"]);
		let a = Specifier::from_str("a").unwrap().prefix_key();
		for descendant in ["a/c", "a/c/d", "a/d"] {
			let key = Specifier::from_str(descendant).unwrap().key();
			assert!(
				key.starts_with(&a),
				"expected {descendant} to start with the key of a"
			);
		}
		let key = Specifier::from_str("ab").unwrap().key();
		assert!(
			!key.starts_with(&a),
			"expected ab not to start with the key of a"
		);
		let (start, end) = Specifier::from_str("a").unwrap().descendants_key_range();
		for (specifier, expected) in [
			("a", false),
			("a/c", true),
			("a/c/d", true),
			("a/d", true),
			("ab", false),
		] {
			let key = Specifier::from_str(specifier).unwrap().key();
			let contained = start <= key && key < end;
			assert_eq!(contained, expected, "{specifier}");
		}
	}

	// The key agrees with the list comparison within a component.
	#[test]
	fn key_agrees_with_the_list_comparison() {
		let components = [
			"0", "00", "1", "01", "1.0", "1.0.1", "1-2", "a", "a.1", "a1", "1a", "ab", "a_b", "b",
			"10", "9",
		];
		for a in components {
			for b in components {
				let order = crate::list::compare(a, b);
				let a_key = Specifier::from_str(a).unwrap().prefix_key();
				let b_key = Specifier::from_str(b).unwrap().prefix_key();
				assert_eq!(a_key.cmp(&b_key), order, "{a} and {b}");
			}
		}
	}

	#[test]
	fn component_rejects_id() {
		let id = tg::Id::new_uuidv7(tg::id::Kind::Tag).to_string();
		assert!(Component::from_str(&id).is_err());
		assert!(Component::from_str(&format!("{id}.tg.ts")).is_err());
		assert!(Component::from_str(&format!("{id}-suffix")).is_err());
		assert!(Specifier::from_str(&id).is_err());
		assert!(Specifier::from_str(&format!("foo/{id}")).is_err());
	}

	#[test]
	fn component_allows_id_like_strings() {
		assert!(Component::from_str("tag_00abc").is_ok());
	}

	#[test]
	fn component_rejects_dot_components() {
		assert!(Component::from_str(".").is_err());
		assert!(Component::from_str("..").is_err());
		assert!(Specifier::from_str(".").is_err());
		assert!(Specifier::from_str("..").is_err());
		assert!(Specifier::from_str("foo/./bar").is_err());
		assert!(Specifier::from_str("foo/../bar").is_err());
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
