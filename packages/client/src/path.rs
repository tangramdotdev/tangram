use crate as tg;
use std::path::PathBuf;

/// A path.
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
)]
pub struct Path {
	string: String,
	components: Vec<Component>,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, derive_more::TryUnwrap, derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Component {
	Normal(String),
	Current,
	Parent,
	Root,
}

impl Path {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
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
	pub fn as_str(&self) -> &str {
		self.string.as_str()
	}

	#[must_use]
	pub fn as_path(&self) -> &std::path::Path {
		std::path::Path::new(self.string.as_str())
	}

	pub fn push(&mut self, component: Component) {
		match (component, self.components.last().unwrap()) {
			// If the component is a current component, or is a parent component and follows a root component, then ignore it.
			(Component::Current, _) | (Component::Parent, Component::Root) => (),

			// If the component is a parent component and follows a normal or parent component, then add it.
			(Component::Parent, Component::Normal(_) | Component::Parent) => {
				self.string.push_str("/..");
				self.components.push(Component::Parent);
			},

			// If the component is a parent component and follows a current component, then replace the path with a parent component.
			(Component::Parent, Component::Current) => {
				self.string = "..".to_owned();
				self.components = vec![Component::Parent];
			},

			// If the component is a root component, then replace the path with a root component.
			(Component::Root, _) => {
				self.string = "/".to_owned();
				self.components = vec![Component::Root];
			},

			// If the component is a normal component, then add it.
			(Component::Normal(name), _) => {
				self.string.push('/');
				self.string.push_str(&name);
				self.components.push(Component::Normal(name));
			},
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
		let mut components: Vec<Component> = Vec::new();
		for component in self.into_components() {
			match (component, components.last()) {
				// If the component is a parent component following a normal component, then remove the normal component.
				(Component::Parent, Some(Component::Normal(_))) => {
					components.pop();
				},

				// Otherwise, add the component.
				(component, _) => {
					components.push(component);
				},
			}
		}
		Self::with_components(components)
	}

	#[must_use]
	pub fn diff(&self, src: &Self) -> Option<Self> {
		let dst = self;
		if dst.is_absolute() != src.is_absolute() {
			return if dst.is_absolute() {
				Some(dst.clone())
			} else {
				None
			};
		}
		let mut components = Vec::new();
		let mut dst = dst.components().iter();
		let mut src = src.components().iter();
		loop {
			match (dst.next(), src.next()) {
				(None, None) => break,
				(None, Some(_)) => components.push(Component::Parent),
				(Some(d), None) => {
					components.push(d.clone());
					components.extend(dst.cloned());
					break;
				},
				(Some(d), Some(s)) if components.is_empty() && d == s => (),
				(Some(d), Some(s)) if s == &Component::Current => components.push(d.clone()),
				(Some(_), Some(s)) if s == &Component::Parent => return None,
				(Some(d), Some(_)) => {
					components.push(Component::Parent);
					for _ in src {
						components.push(Component::Parent);
					}
					components.push(d.clone());
					components.extend(dst.cloned());
					break;
				},
			}
		}
		Some(Self::with_components(components))
	}

	#[must_use]
	pub fn is_internal(&self) -> bool {
		self.components()
			.first()
			.unwrap()
			.try_unwrap_current_ref()
			.is_ok()
	}

	#[must_use]
	pub fn is_external(&self) -> bool {
		self.components()
			.first()
			.unwrap()
			.try_unwrap_parent_ref()
			.is_ok()
	}

	#[must_use]
	pub fn is_absolute(&self) -> bool {
		self.components()
			.first()
			.unwrap()
			.try_unwrap_root_ref()
			.is_ok()
	}
}

impl Default for Path {
	fn default() -> Self {
		let string = ".".to_owned();
		let components = vec![Component::Current];
		Self { string, components }
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
				component => {
					path.push(Component::Normal(component.to_owned()));
				},
			}
		}
		Ok(path)
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let s = match &self {
			Component::Normal(s) => s,
			Component::Current => ".",
			Component::Parent => "..",
			Component::Root => "/",
		};
		write!(f, "{s}")?;
		Ok(())
	}
}

impl std::str::FromStr for Component {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(match s {
			"." => Self::Current,
			".." => Self::Parent,
			"/" => Self::Root,
			_ => Self::Normal(s.to_owned()),
		})
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

impl AsRef<std::path::Path> for Path {
	fn as_ref(&self) -> &std::path::Path {
		self.as_path()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse() {
		let left = "".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current];
		assert_eq!(left, right);

		let left = ".".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current];
		assert_eq!(left, right);

		let left = "./".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current];
		assert_eq!(left, right);

		let left = "./.".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current];
		assert_eq!(left, right);

		let left = "./hello".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current, Component::Normal("hello".to_owned())];
		assert_eq!(left, right);

		let left = "hello".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Current, Component::Normal("hello".to_owned())];
		assert_eq!(left, right);

		let left = "hello/world".parse::<Path>().unwrap().into_components();
		let right = vec![
			Component::Current,
			Component::Normal("hello".to_owned()),
			Component::Normal("world".to_owned()),
		];
		assert_eq!(left, right);

		let left = "..".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Parent];
		assert_eq!(left, right);

		let left = "../".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Parent];
		assert_eq!(left, right);

		let left = "../hello".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Parent, Component::Normal("hello".to_owned())];
		assert_eq!(left, right);

		let left = "../..".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Parent, Component::Parent];
		assert_eq!(left, right);

		let left = "/".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Root];
		assert_eq!(left, right);

		let left = "/hello".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Root, Component::Normal("hello".to_owned())];
		assert_eq!(left, right);

		let left = "/..".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Root];
		assert_eq!(left, right);

		let left = "/.".parse::<Path>().unwrap().into_components();
		let right = vec![Component::Root];
		assert_eq!(left, right);
	}

	#[test]
	fn normalize() {
		let left = "./hello/world".parse::<Path>().unwrap().normalize();
		let right = "./hello/world".parse::<Path>().unwrap();
		assert_eq!(left, right);

		let left = "./hello/../world".parse::<Path>().unwrap().normalize();
		let right = "./world".parse::<Path>().unwrap();
		assert_eq!(left, right);

		let left = "./hello/../../world".parse::<Path>().unwrap().normalize();
		let right = "../world".parse::<Path>().unwrap();
		assert_eq!(left, right);

		let left = "/hello/../../world".parse::<Path>().unwrap().normalize();
		let right = "/world".parse::<Path>().unwrap();
		assert_eq!(left, right);
	}

	#[test]
	fn diff() {
		let dst = "/hello/world".parse::<Path>().unwrap();
		let src = "/hello".parse::<Path>().unwrap();
		let left = dst.diff(&src);
		let right = Some("./world".parse().unwrap());
		assert_eq!(left, right);

		let dst = "/hello/world".parse::<Path>().unwrap();
		let src = "/world".parse::<Path>().unwrap();
		let left = dst.diff(&src);
		let right = Some("../hello/world".parse().unwrap());
		assert_eq!(left, right);

		let dst = "/foo/bar".parse::<Path>().unwrap();
		let src = "/foo/bar/baz".parse::<Path>().unwrap();
		let left = dst.diff(&src);
		let right = Some("..".parse().unwrap());
		assert_eq!(left, right);
	}
}
