use crate::{Error, Result};
use derive_more::{TryUnwrap, Unwrap};
use std::path::PathBuf;
use tangram_error::WrapErr;

/// Any path.
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Path {
	string: String,
	components: Vec<Component>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, TryUnwrap, Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Component {
	Root,
	Current,
	Parent,
	Normal(String),
}

impl Path {
	pub fn with_components(components: impl IntoIterator<Item = Component>) -> Self {
		let mut path = Self::default();
		for component in components {
			path.push(component);
		}
		path
	}

	#[must_use]
	pub fn components(&self) -> &[Component] {
		&self.components
	}

	#[must_use]
	pub fn into_components(self) -> Vec<Component> {
		self.components
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.components.is_empty()
	}

	pub fn push(&mut self, component: Component) {
		// Ignore the component if it is a current directory component and the path is not empty.
		if component == Component::Current && !self.is_empty() {
			return;
		}

		// If the component is a root component, then clear the path.
		if component == Component::Root {
			self.string = String::new();
			self.components.clear();
		}

		// Add the separator.
		match self.components.last() {
			Some(Component::Root) => (),
			Some(_) => self.string.push('/'),
			None => (),
		}

		// Add the component.
		match &component {
			Component::Root => self.string.push('/'),
			Component::Current => self.string.push('.'),
			Component::Parent => self.string.push_str(".."),
			Component::Normal(name) => self.string.push_str(name),
		}
		self.components.push(component);
	}

	pub fn pop(&mut self) {
		let component = self.components.pop();
		let n = match component {
			Some(Component::Root | Component::Current) => 1,
			Some(Component::Parent) => 2,
			Some(Component::Normal(name)) => name.len(),
			None => 0,
		};
		self.string.truncate(self.string.len() - n);
		match self.components().last() {
			Some(Component::Root) => (),
			Some(_) => self.string.truncate(self.string.len() - 1),
			None => (),
		}
	}

	#[must_use]
	pub fn parent(self) -> Self {
		self.join(Component::Parent.into())
	}

	#[must_use]
	pub fn join(mut self, other: Self) -> Self {
		for component in other.into_components() {
			self.push(component);
		}
		self
	}

	#[must_use]
	pub fn normalize(self) -> Self {
		let mut path = Self::default();
		for component in self.into_components() {
			match (component, path.components().last()) {
				// If any component is root, then the normalized path is root.
				(Component::Root, _) => {
					path = Self::with_components(vec![Component::Root]);
				},

				// Drop any current paths.
				(Component::Current, _) => (),

				// Flatten any parent paths.
				(Component::Parent, Some(Component::Normal(_))) => {
					path.pop();
				},

				// If the parent is root then skip.
				(Component::Parent, Some(Component::Root)) => (),

				// Otherwise add the component.
				(component, _) => {
					path.push(component);
				},
			}
		}
		path
	}

	#[must_use]
	pub fn is_absolute(&self) -> bool {
		matches!(self.components().first(), Some(Component::Root))
	}

	#[must_use]
	pub fn extension(&self) -> Option<&str> {
		self.components()
			.last()
			.and_then(|component| component.try_unwrap_normal_ref().ok())
			.and_then(|name| name.split('.').last())
	}
}

impl std::fmt::Display for Path {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)?;
		Ok(())
	}
}

impl std::str::FromStr for Path {
	type Err = Error;

	fn from_str(mut s: &str) -> std::result::Result<Self, Self::Err> {
		let mut path = Self::default();
		if s.starts_with('/') {
			path.push(Component::Root);
			s = &s[1..];
		}
		for component in s.split('/') {
			match component {
				"" => (),
				"." => {
					path.push(Component::Current);
				},
				".." => {
					path.push(Component::Parent);
				},
				_ => {
					path.push(Component::Normal(component.to_owned()));
				},
			}
		}
		Ok(path)
	}
}

impl From<Path> for String {
	fn from(value: Path) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Path {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

impl From<Component> for Path {
	fn from(value: Component) -> Self {
		Self::with_components(vec![value])
	}
}

impl FromIterator<Component> for Path {
	fn from_iter<T: IntoIterator<Item = Component>>(iter: T) -> Self {
		Self::with_components(iter)
	}
}

impl From<Path> for PathBuf {
	fn from(value: Path) -> Self {
		value.to_string().into()
	}
}

impl TryFrom<PathBuf> for Path {
	type Error = Error;

	fn try_from(value: PathBuf) -> Result<Self, Self::Error> {
		value
			.as_os_str()
			.to_str()
			.wrap_err("The path must be valid UTF-8.")?
			.parse()
	}
}

impl<'a> TryFrom<&'a std::path::Path> for Path {
	type Error = Error;

	fn try_from(value: &'a std::path::Path) -> std::prelude::v1::Result<Self, Self::Error> {
		value
			.as_os_str()
			.to_str()
			.wrap_err("The path must be valid UTF-8.")?
			.parse()
	}
}

impl AsRef<std::path::Path> for Path {
	fn as_ref(&self) -> &std::path::Path {
		std::path::Path::new(self.string.as_str())
	}
}

#[cfg(test)]
mod tests {
	use super::Path;
	#[test]
	fn path_normalization() {
		let path: Path = "/foo/../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "/bar/baz");

		let path: Path = "/../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "/bar/baz");

		let path: Path = "../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "../bar/baz");

		let path: Path = "./bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "bar/baz");
	}
}
