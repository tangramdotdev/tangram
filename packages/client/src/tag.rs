use crate as tg;

pub mod delete;
pub mod get;
pub mod list;
pub mod pattern;
pub mod put;

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
)]
pub struct Tag {
	string: String,
	components: Vec<String>,
}

impl Tag {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.string.is_empty()
	}

	pub fn components(&self) -> &Vec<String> {
		&self.components
	}
}

impl AsRef<str> for Tag {
	fn as_ref(&self) -> &str {
		self.string.as_str()
	}
}

impl std::fmt::Display for Tag {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)
	}
}

impl std::str::FromStr for Tag {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let string = s.to_owned();
		let components = s.split('/').map(ToOwned::to_owned).collect();
		Ok(Self { string, components })
	}
}

impl TryFrom<Pattern> for Tag {
	type Error = tg::Error;

	fn try_from(value: Pattern) -> Result<Self, Self::Error> {
		let (string, components) = value.into_string_and_components();
		let components = components
			.into_iter()
			.map(|component| match component {
				self::pattern::Component::Normal(name) => Ok(name),
				self::pattern::Component::Semver(_) => Err(tg::error!(
					"pattern is not a tag if it has a semver component"
				)),
			})
			.collect::<tg::Result<_>>()?;
		Ok(Self { string, components })
	}
}
