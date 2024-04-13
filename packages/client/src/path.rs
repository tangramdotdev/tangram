use crate as tg;
use derive_more::{TryUnwrap, Unwrap};
use std::{ffi::OsStr, path::PathBuf};

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
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
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
			Some(Component::Root) | None => (),
			Some(_) => self.string.push('/'),
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
			None | Some(Component::Root) => (),
			Some(_) => self.string.truncate(self.string.len() - 1),
		}
	}

	#[must_use]
	pub fn parent(self) -> Self {
		self.join(Component::Parent)
	}

	#[must_use]
	pub fn join(mut self, other: impl Into<Self>) -> Self {
		for component in other.into().into_components() {
			self.push(component);
		}
		self
	}

	#[must_use]
	pub fn normalize(self) -> Self {
		let mut path = Self::default();
		for component in self.into_components() {
			match (component, path.components().last()) {
				(Component::Parent, Some(Component::Normal(_))) => path.pop(),
				(Component::Parent, Some(Component::Root)) | (Component::Current, _) => (),
				(component, _) => path.push(component),
			}
		}
		path
	}

	#[must_use]
	pub fn strip_prefix(&self, prefix: &Self) -> Option<Self> {
		if self
			.components()
			.iter()
			.zip(prefix.components())
			.take_while(|(s, p)| s == p)
			.count() < prefix.components().len()
		{
			None
		} else {
			Some(
				self.components()
					.iter()
					.skip(prefix.components().len())
					.cloned()
					.collect(),
			)
		}
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

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.string.as_str()
	}

	#[must_use]
	pub fn as_os_str(&self) -> &OsStr {
		std::path::Path::new(self.string.as_str()).as_os_str()
	}

	#[must_use]
	pub fn as_path(&self) -> &std::path::Path {
		std::path::Path::new(self.string.as_str())
	}
}

impl std::fmt::Display for Path {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)?;
		Ok(())
	}
}

impl std::str::FromStr for Path {
	type Err = tg::Error;

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

impl From<String> for Path {
	fn from(value: String) -> Self {
		value.parse().unwrap()
	}
}

impl From<&String> for Path {
	fn from(value: &String) -> Self {
		value.parse().unwrap()
	}
}

impl From<&str> for Path {
	fn from(value: &str) -> Self {
		value.parse().unwrap()
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
	type Error = tg::Error;

	fn try_from(value: PathBuf) -> tg::Result<Self, Self::Error> {
		value
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("the path must be valid UTF-8"))?
			.parse()
	}
}

impl<'a> TryFrom<&'a std::path::Path> for Path {
	type Error = tg::Error;

	fn try_from(value: &'a std::path::Path) -> tg::Result<Self, Self::Error> {
		value
			.as_os_str()
			.to_str()
			.ok_or_else(|| {
				let path = value.display();
				tg::error!(%path, "the path must be valid UTF-8")
			})?
			.parse()
	}
}

impl AsRef<str> for Path {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl AsRef<OsStr> for Path {
	fn as_ref(&self) -> &OsStr {
		self.as_os_str()
	}
}

impl AsRef<std::path::Path> for Path {
	fn as_ref(&self) -> &std::path::Path {
		self.as_path()
	}
}

#[cfg(test)]
mod tests {
	use super::Path;
	#[test]
	fn normalization() {
		let path: Path = ".".parse().unwrap();
		assert_eq!(path.normalize().components(), &[]);

		let path: Path = "./".parse().unwrap();
		assert_eq!(path.normalize().components(), &[]);

		let path: Path = "/foo/../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "/bar/baz");

		let path: Path = "/../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "/bar/baz");

		let path: Path = "../bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "../bar/baz");

		let path: Path = "./bar/baz".parse().unwrap();
		assert_eq!(path.normalize().to_string(), "bar/baz");
	}

	#[test]
	fn strip_prefix() {
		let path: Path = "/hello/world".parse().unwrap();
		let prefix: Path = "/hello".parse().unwrap();
		let left = path.strip_prefix(&prefix);
		let right = Some("world".parse().unwrap());
		assert_eq!(left, right);

		let path: Path = "/hello/world".parse().unwrap();
		let prefix: Path = "/world".parse().unwrap();
		let left = path.strip_prefix(&prefix);
		let right = None;
		assert_eq!(left, right);

		let path: Path = "/foo/bar".parse().unwrap();
		let prefix: Path = "/foo/bar/baz".parse().unwrap();
		let left = path.strip_prefix(&prefix);
		let right = None;
		assert_eq!(left, right);
	}
}
