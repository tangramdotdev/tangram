use {
	self::builder::Builder,
	percent_encoding::percent_decode_str,
	regex::Regex,
	std::{borrow::Cow, ops::Range, sync::LazyLock},
};

pub mod builder;

static REGEX: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(r"^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?").unwrap()
});

/// Characters that must be percent-encoded in query parameter values.
/// This encodes `=`, `&`, `#`, and other special chars, but NOT `/`.
const QUERY_VALUE_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
	.add(b' ')
	.add(b'"')
	.add(b'#')
	.add(b'<')
	.add(b'>')
	.add(b'=')
	.add(b'&')
	.add(b'+');

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Uri {
	// Original raw string and ranges.
	string: String,
	scheme: Option<Range<usize>>,
	authority_raw: Option<Range<usize>>,
	path_raw: Range<usize>,
	query_raw: Option<Range<usize>>,
	fragment_raw: Option<Range<usize>>,

	// Decoded strings - only Some if decoding changed the value.
	authority: Option<String>,
	path: Option<String>,
	query: Option<String>,
	fragment: Option<String>,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum ParseError {
	#[display("invalid uri")]
	Invalid,
	Regex(regex::Error),
	Utf8(std::str::Utf8Error),
}

impl Uri {
	pub fn parse(string: &str) -> Result<Self, ParseError> {
		string.parse()
	}

	#[must_use]
	pub fn builder() -> Builder {
		Builder::default()
	}

	#[must_use]
	pub fn to_builder(&self) -> Builder {
		let mut builder = Builder::default();
		if let Some(scheme) = self.scheme() {
			builder = builder.scheme(scheme);
		}
		if let Some(authority) = self.authority() {
			builder = builder.authority(authority);
		}
		builder = builder.path(self.path());
		if let Some(query) = self.query() {
			builder = builder.query(query);
		}
		if let Some(fragment) = self.fragment() {
			builder = builder.fragment(fragment);
		}
		builder
	}

	#[must_use]
	pub fn scheme(&self) -> Option<&str> {
		self.scheme.clone().map(|range| &self.string[range])
	}

	#[must_use]
	pub fn authority_raw(&self) -> Option<&str> {
		self.authority_raw
			.as_ref()
			.map(|range| &self.string[range.clone()])
	}

	#[must_use]
	pub fn authority(&self) -> Option<&str> {
		self.authority
			.as_deref()
			.or(self.authority_raw.as_ref().map(|r| &self.string[r.clone()]))
	}

	#[must_use]
	pub fn path_raw(&self) -> &str {
		&self.string[self.path_raw.clone()]
	}

	#[must_use]
	pub fn path(&self) -> &str {
		self.path
			.as_deref()
			.unwrap_or(&self.string[self.path_raw.clone()])
	}

	#[must_use]
	pub fn query_raw(&self) -> Option<&str> {
		self.query_raw
			.as_ref()
			.map(|range| &self.string[range.clone()])
	}

	#[must_use]
	pub fn query(&self) -> Option<&str> {
		self.query
			.as_deref()
			.or(self.query_raw.as_ref().map(|r| &self.string[r.clone()]))
	}

	#[must_use]
	pub fn fragment_raw(&self) -> Option<&str> {
		self.fragment_raw
			.as_ref()
			.map(|range| &self.string[range.clone()])
	}

	#[must_use]
	pub fn fragment(&self) -> Option<&str> {
		self.fragment
			.as_deref()
			.or(self.fragment_raw.as_ref().map(|r| &self.string[r.clone()]))
	}

	#[must_use]
	pub fn host_raw(&self) -> Option<&str> {
		self.authority_raw().and_then(|authority| {
			let authority = authority.split('@').next_back()?;
			if authority.starts_with('[') {
				authority.split(']').next()?.strip_prefix('[')
			} else {
				authority.split(':').next()
			}
		})
	}

	#[must_use]
	pub fn host(&self) -> Option<&str> {
		self.authority().and_then(|authority| {
			let authority = authority.split('@').next_back()?;
			if authority.starts_with('[') {
				authority.split(']').next()?.strip_prefix('[')
			} else {
				authority.split(':').next()
			}
		})
	}

	#[must_use]
	pub fn port(&self) -> Option<u16> {
		self.authority_raw().and_then(|authority| {
			let authority = authority.split('@').next_back()?;
			if authority.starts_with('[') {
				authority.split("]:").nth(1)?.parse().ok()
			} else {
				authority.split(':').nth(1)?.parse().ok()
			}
		})
	}

	#[must_use]
	pub fn port_or_known_default(&self) -> Option<u16> {
		self.port().or_else(|| match self.scheme()? {
			"http" | "http+unix" => Some(80),
			"https" => Some(443),
			_ => None,
		})
	}

	#[must_use]
	pub fn domain_raw(&self) -> Option<&str> {
		self.host_raw()
	}

	#[must_use]
	pub fn domain(&self) -> Option<&str> {
		self.host()
	}

	#[must_use]
	pub fn as_str(&self) -> &str {
		&self.string
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
		let authority_raw = captures.get(4).map(|m| m.range());
		let path_raw = captures
			.get(5)
			.map(|m| m.range())
			.ok_or(ParseError::Invalid)?;
		let query_raw = captures.get(7).map(|m| m.range());
		let fragment_raw = captures.get(9).map(|m| m.range());

		// Decode and return Some only if decoding changed the value.
		let decode_if_changed = |raw: &str| -> Result<Option<String>, std::str::Utf8Error> {
			let decoded = percent_decode_str(raw).decode_utf8()?;
			Ok(match decoded {
				Cow::Borrowed(_) => None,
				Cow::Owned(s) => Some(s),
			})
		};

		let authority = authority_raw
			.clone()
			.map(|r| decode_if_changed(&s[r]))
			.transpose()?
			.flatten();
		let path = decode_if_changed(&s[path_raw.clone()])?;
		let query = query_raw
			.clone()
			.map(|r| decode_if_changed(&s[r]))
			.transpose()?
			.flatten();
		let fragment = fragment_raw
			.clone()
			.map(|r| decode_if_changed(&s[r]))
			.transpose()?
			.flatten();

		Ok(Self {
			string: s.to_owned(),
			scheme,
			authority_raw,
			path_raw,
			query_raw,
			fragment_raw,
			authority,
			path,
			query,
			fragment,
		})
	}
}

/// Encode a query parameter value, preserving `/` characters.
#[must_use]
pub fn encode_query_value(value: &str) -> String {
	percent_encoding::utf8_percent_encode(value, QUERY_VALUE_ENCODE_SET).to_string()
}

/// Decode a percent-encoded query parameter value.
pub fn decode_query_value(value: &str) -> Result<Cow<'_, str>, std::str::Utf8Error> {
	percent_encoding::percent_decode_str(value).decode_utf8()
}
