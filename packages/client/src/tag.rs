use crate as tg;
use itertools::Itertools as _;
use tangram_version::Version;
use winnow::{
	ascii::{alphanumeric1, dec_uint},
	combinator::{alt, opt, preceded, separated},
	error::{ErrMode, FromExternalError as _},
	prelude::*,
	token::take_while,
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
	components: Vec<Component>,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Component(String);

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
pub struct ParseError;

impl Tag {
	#[must_use]
	pub fn with_components(components: Vec<Component>) -> Self {
		let string = components.iter().map(Component::as_str).join("/");
		Self { string, components }
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.string.is_empty()
	}

	#[must_use]
	pub fn components(&self) -> &Vec<Component> {
		&self.components
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.string.as_str()
	}
}

impl Component {
	#[must_use]
	pub fn new(name: String) -> Self {
		Self(name)
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}

	#[must_use]
	pub fn into_string(self) -> String {
		self.0
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
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		tag.parse(s).ok().ok_or(ParseError)
	}
}

impl TryFrom<Pattern> for Tag {
	type Error = tg::Error;

	fn try_from(value: Pattern) -> Result<Self, Self::Error> {
		let (string, components) = value.into_string_and_components();
		let components = components
			.into_iter()
			.map(|component| match component {
				self::pattern::Component::Normal(component) => Ok(Component(component.0)),
				self::pattern::Component::Version(_) => Err(tg::error!(
					"pattern is not a tag if it has a semver component"
				)),
				self::pattern::Component::Glob => Err(tg::error!(
					"pattern is not a tag if it has a glob component"
				)),
			})
			.collect::<tg::Result<_>>()?;
		Ok(Self { string, components })
	}
}

impl std::fmt::Display for Component {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::str::FromStr for Component {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		component.parse(s).ok().ok_or(ParseError)
	}
}

fn tag(input: &mut &str) -> PResult<Tag> {
	let string = input.to_owned();
	let components: Vec<_> = separated(1.., component, "/").parse_next(input)?;
	Ok(Tag { string, components })
}

fn component(input: &mut &str) -> PResult<Component> {
	alt((version, string)).parse_next(input)
}

fn string(input: &mut &str) -> PResult<Component> {
	let component = take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '-')
		.verify(|value: &str| {
			if value.parse::<tg::build::Id>().is_ok() {
				return false;
			}
			if value.parse::<tg::object::Id>().is_ok() {
				return false;
			}
			true
		})
		.parse_next(input)?;
	Ok(Component(component.to_owned()))
}

fn version(input: &mut &str) -> PResult<Component> {
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
	Ok(Component(version.to_string()))
}

fn dot_separated_identifier<'a>(input: &mut &'a str) -> PResult<&'a str> {
	separated::<_, _, Vec<_>, _, _, _, _>(1.., alphanumeric1, ".")
		.take()
		.parse_next(input)
}

#[cfg(test)]
mod tests {
	use crate::tg;

	#[test]
	fn tag() {
		let tag = "tag".parse::<tg::Tag>().unwrap();
		let left = tag.components();
		let right = &[tg::tag::Component::new("tag".to_owned())];
		assert_eq!(left, right);

		let tag = "tag/1.0.0".parse::<tg::Tag>().unwrap();
		let left = tag.components();
		let right = &[
			tg::tag::Component::new("tag".to_owned()),
			tg::tag::Component::new("1.0.0".to_owned()),
		];
		assert_eq!(left, right);

		assert!("".parse::<tg::Tag>().is_err());
		assert!("hello/".parse::<tg::Tag>().is_err());
		assert!("hello//world".parse::<tg::Tag>().is_err());

		assert!("fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260"
			.parse::<tg::Tag>()
			.is_err());
		assert!(
			"hello/fil_010kectq93xrz0cdy3bvkb43sdx2b0exppwwdfcy34ve5aktn8z260/world"
				.parse::<tg::Tag>()
				.is_err()
		);
	}
}
