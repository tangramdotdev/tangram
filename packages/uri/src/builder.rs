use crate::Uri;

/// Characters that must be percent-encoded in the authority component.
/// RFC 3986 Section 3.2: reg-name allows unreserved / pct-encoded / sub-delims.
/// We encode everything not in unreserved or sub-delims, plus "/" to support Unix socket paths.
const AUTHORITY_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
	.add(b' ')
	.add(b'"')
	.add(b'#')
	.add(b'/')
	.add(b'<')
	.add(b'>')
	.add(b'?')
	.add(b'[')
	.add(b'\\')
	.add(b']')
	.add(b'^')
	.add(b'`')
	.add(b'{')
	.add(b'|')
	.add(b'}');

/// Characters that must be percent-encoded in path segments.
/// RFC 3986 Section 3.3: pchar allows unreserved / pct-encoded / sub-delims / ":" / "@".
/// We preserve "/" as the path separator.
const PATH_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
	.add(b' ')
	.add(b'"')
	.add(b'#')
	.add(b'<')
	.add(b'>')
	.add(b'?')
	.add(b'[')
	.add(b'\\')
	.add(b']')
	.add(b'^')
	.add(b'`')
	.add(b'{')
	.add(b'|')
	.add(b'}');

/// Characters that must be percent-encoded in the query component.
/// RFC 3986 Section 3.4: query allows pchar / "/" / "?".
const QUERY_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
	.add(b' ')
	.add(b'"')
	.add(b'#')
	.add(b'<')
	.add(b'>')
	.add(b'[')
	.add(b'\\')
	.add(b']')
	.add(b'^')
	.add(b'`')
	.add(b'{')
	.add(b'|')
	.add(b'}');

/// Characters that must be percent-encoded in the fragment component.
/// RFC 3986 Section 3.5: fragment allows pchar / "/" / "?".
const FRAGMENT_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
	.add(b' ')
	.add(b'"')
	.add(b'#')
	.add(b'<')
	.add(b'>')
	.add(b'[')
	.add(b'\\')
	.add(b']')
	.add(b'^')
	.add(b'`')
	.add(b'{')
	.add(b'|')
	.add(b'}');

#[derive(Clone, Debug, Default)]
pub struct Builder {
	scheme: Option<String>,
	authority: Option<String>,
	path: Option<String>,
	query: Option<String>,
	fragment: Option<String>,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
#[display("invalid uri")]
pub struct Error;

impl Builder {
	#[must_use]
	pub fn scheme(mut self, scheme: &str) -> Self {
		self.scheme = Some(scheme.to_owned());
		self
	}

	#[must_use]
	pub fn authority_raw(mut self, authority: &str) -> Self {
		self.authority = Some(authority.to_owned());
		self
	}

	#[must_use]
	pub fn authority(mut self, authority: &str) -> Self {
		self.authority = Some(
			percent_encoding::utf8_percent_encode(authority, AUTHORITY_ENCODE_SET).to_string(),
		);
		self
	}

	#[must_use]
	pub fn path_raw(mut self, path: &str) -> Self {
		self.path = Some(path.to_owned());
		self
	}

	#[must_use]
	pub fn path(mut self, path: &str) -> Self {
		self.path = Some(percent_encoding::utf8_percent_encode(path, PATH_ENCODE_SET).to_string());
		self
	}

	#[must_use]
	pub fn query_raw(mut self, query: &str) -> Self {
		self.query = Some(query.to_owned());
		self
	}

	#[must_use]
	pub fn query(mut self, query: &str) -> Self {
		self.query =
			Some(percent_encoding::utf8_percent_encode(query, QUERY_ENCODE_SET).to_string());
		self
	}

	#[must_use]
	pub fn fragment_raw(mut self, fragment: &str) -> Self {
		self.fragment = Some(fragment.to_owned());
		self
	}

	#[must_use]
	pub fn fragment(mut self, fragment: &str) -> Self {
		self.fragment =
			Some(percent_encoding::utf8_percent_encode(fragment, FRAGMENT_ENCODE_SET).to_string());
		self
	}

	pub fn build(self) -> Result<Uri, Error> {
		let mut string = String::new();
		if let Some(scheme) = &self.scheme {
			string.push_str(scheme);
			string.push(':');
		}
		if let Some(authority) = &self.authority {
			string.push_str("//");
			string.push_str(authority);
		}
		let path = self.path.as_ref().ok_or(Error)?;
		string.push_str(path);
		if let Some(query) = &self.query {
			string.push('?');
			string.push_str(query);
		}
		if let Some(fragment) = &self.fragment {
			string.push('#');
			string.push_str(fragment);
		}
		let uri = string.parse().ok().ok_or(Error)?;
		Ok(uri)
	}
}
