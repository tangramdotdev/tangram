use self::builder::Builder;
use regex::Regex;
use std::{ops::Range, sync::LazyLock};

pub mod builder;

static REGEX: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?").unwrap()
});

#[derive(Clone, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub struct Uri {
	string: String,
	scheme: Option<Range<usize>>,
	authority: Option<Range<usize>>,
	path: Range<usize>,
	query: Option<Range<usize>>,
	fragment: Option<Range<usize>>,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum ParseError {
	#[display("invalid uri")]
	Invalid,
	Regex(regex::Error),
	Utf8(std::string::FromUtf8Error),
}

impl Uri {
	pub fn parse(string: &str) -> Result<Self, ParseError> {
		string.parse()
	}

	#[must_use]
	pub fn builder() -> Builder {
		Builder::default()
	}

	pub fn to_builder(&self) -> Builder {
		Builder::default()
			.scheme(self.scheme().map(ToOwned::to_owned))
			.authority(self.authority().map(ToOwned::to_owned))
			.path(self.path())
			.query(self.query().map(ToOwned::to_owned))
			.fragment(self.fragment().map(ToOwned::to_owned))
	}

	#[must_use]
	pub fn scheme(&self) -> Option<&str> {
		self.scheme.clone().map(|range| &self.string[range])
	}

	#[must_use]
	pub fn authority(&self) -> Option<&str> {
		self.authority.clone().map(|range| &self.string[range])
	}

	#[must_use]
	pub fn path(&self) -> &str {
		&self.string[self.path.clone()]
	}

	#[must_use]
	pub fn query(&self) -> Option<&str> {
		self.query.clone().map(|range| &self.string[range])
	}

	#[must_use]
	pub fn fragment(&self) -> Option<&str> {
		self.fragment.clone().map(|range| &self.string[range])
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		self.string.as_str()
	}
}

impl AsRef<str> for Uri {
	fn as_ref(&self) -> &str {
		self.string.as_str()
	}
}

impl std::fmt::Display for Uri {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)
	}
}

impl std::str::FromStr for Uri {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let captures = REGEX.captures(s).ok_or(ParseError::Invalid)?;
		let scheme = captures.get(2).map(|m| m.range());
		let authority = captures.get(4).map(|m| m.range());
		let path = captures
			.get(5)
			.map(|m| m.range())
			.ok_or(ParseError::Invalid)?;
		let query = captures.get(7).map(|m| m.range());
		let fragment = captures.get(9).map(|m| m.range());
		Ok(Self {
			string: s.to_owned(),
			scheme,
			authority,
			path,
			query,
			fragment,
		})
	}
}

impl std::cmp::Eq for Uri {}

impl std::hash::Hash for Uri {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.string.hash(state);
	}
}

impl std::cmp::Ord for Uri {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.string.cmp(&other.string)
	}
}

impl std::cmp::PartialEq for Uri {
	fn eq(&self, other: &Self) -> bool {
		self.string.eq(&other.string)
	}
}

impl std::cmp::PartialOrd for Uri {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
