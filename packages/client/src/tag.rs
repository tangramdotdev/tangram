use {
	crate as tg,
	tangram_version::Version,
	winnow::{
		ascii::{alphanumeric1, dec_uint},
		combinator::{alt, opt, preceded, separated},
		prelude::*,
		token::take_while,
	},
};

pub mod delete;
pub mod get;
pub mod list;
pub mod pattern;
pub mod put;

pub use self::pattern::Pattern;

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Tag {
	components: Vec<Component>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	PartialEq,
	derive_more::IsVariant,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Component {
	String(String),
	Version(Version),
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
#[display("parse error")]
pub struct ParseError;

impl Tag {
	#[must_use]
	pub fn empty() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn with_components(components: Vec<Component>) -> Self {
		Self { components }
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.components.is_empty()
	}

	#[must_use]
	pub fn components(&self) -> &Vec<Component> {
		&self.components
	}

	#[must_use]
	pub fn into_components(self) -> Vec<Component> {
		self.components
	}

	#[must_use]
	pub fn parent(&self) -> Option<Self> {
		let mut components = self.components.clone();
		components.pop()?;
		let parent = Self::with_components(components);
		Some(parent)
	}
}

impl std::fmt::Display for Tag {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, component) in self.components.iter().enumerate() {
			if i > 0 {
				write!(f, "/")?;
			}
			write!(f, "{component}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Tag {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		tag.parse(s).ok().ok_or(ParseError)
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::String(string) => write!(f, "{string}"),
			Self::Version(version) => write!(f, "{version}"),
		}
	}
}

impl std::str::FromStr for Component {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		component.parse(s).ok().ok_or(ParseError)
	}
}

fn tag(input: &mut &str) -> ModalResult<Tag> {
	let components: Vec<_> = separated(1.., component, "/").parse_next(input)?;
	Ok(Tag { components })
}

fn component(input: &mut &str) -> ModalResult<Component> {
	alt((
		version.map(Component::Version),
		string.map(Component::String),
	))
	.parse_next(input)
}

fn version(input: &mut &str) -> ModalResult<Version> {
	let prerelease = opt(preceded("-", dot_separated_identifier));
	let build = opt(preceded("+", dot_separated_identifier));
	let (major, _, minor, _, patch, prerelease, build) =
		(dec_uint, ".", dec_uint, ".", dec_uint, prerelease, build).parse_next(input)?;
	let prerelease = prerelease.map(ToOwned::to_owned);
	let build = build.map(ToOwned::to_owned);
	let version = Version {
		major,
		minor,
		patch,
		prerelease,
		build,
	};
	Ok(version)
}

fn dot_separated_identifier<'a>(input: &mut &'a str) -> ModalResult<&'a str> {
	separated::<_, _, Vec<_>, _, _, _, _>(1.., alphanumeric1, ".")
		.take()
		.parse_next(input)
}

fn string(input: &mut &str) -> ModalResult<String> {
	let string = take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '-')
		.verify(|value: &str| {
			if value.parse::<tg::process::Id>().is_ok() {
				return false;
			}
			if value.parse::<tg::object::Id>().is_ok() {
				return false;
			}
			true
		})
		.parse_next(input)?;
	Ok(string.to_owned())
}

impl std::cmp::PartialOrd for Tag {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl std::cmp::Ord for Tag {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.components().cmp(other.components())
	}
}

impl std::cmp::PartialOrd for Component {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl std::cmp::Ord for Component {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		match (self, other) {
			(Component::String(a), Component::String(b)) => a.cmp(b),
			(Component::String(_), Component::Version(_)) => std::cmp::Ordering::Less,
			(Component::Version(_), Component::String(_)) => std::cmp::Ordering::Greater,
			(Component::Version(a), Component::Version(b)) => a.cmp(b),
		}
	}
}

impl TryFrom<tg::tag::Pattern> for Tag {
	type Error = tg::Error;

	fn try_from(value: tg::tag::Pattern) -> Result<Self, Self::Error> {
		let components = value.into_components();
		let components = components
			.into_iter()
			.map(|component| match component {
				self::pattern::Component::Normal(Component::String(string)) => {
					Ok(Component::String(string))
				},
				self::pattern::Component::Normal(Component::Version(version)) => {
					Ok(Component::Version(version))
				},
				self::pattern::Component::Version(_) => Err(tg::error!(
					"pattern is not a tag if it has a version component"
				)),
				self::pattern::Component::Wildcard => Err(tg::error!(
					"pattern is not a tag if it has a wildcard component"
				)),
			})
			.collect::<tg::Result<_>>()?;
		Ok(Self { components })
	}
}

#[cfg(test)]
mod tests {
	use crate::tg;

	#[test]
	fn tag() {
		let tag = "tag".parse::<tg::Tag>().unwrap();
		let left = tag.components();
		let right = &["tag".parse().unwrap()];
		assert_eq!(left, right);

		let tag = "tag/1.0.0".parse::<tg::Tag>().unwrap();
		let left = tag.components();
		let right = &["tag".parse().unwrap(), "1.0.0".parse().unwrap()];
		assert_eq!(left, right);

		assert!("".parse::<tg::Tag>().is_err());
		assert!("hello/".parse::<tg::Tag>().is_err());
		assert!("hello//world".parse::<tg::Tag>().is_err());

		assert!(
			"fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
				.parse::<tg::Tag>()
				.is_err()
		);
		assert!(
			"hello/fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260/world"
				.parse::<tg::Tag>()
				.is_err()
		);
	}
}
