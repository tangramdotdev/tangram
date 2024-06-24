use crate::Reference;

#[derive(Clone, Debug, Default)]
pub struct Builder {
	scheme: Option<String>,
	authority: Option<String>,
	path: Option<String>,
	query: Option<String>,
	fragment: Option<String>,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
pub struct Error;

impl Builder {
	#[must_use]
	pub fn scheme(mut self, scheme: impl Into<Option<String>>) -> Self {
		self.scheme = scheme.into();
		self
	}

	#[must_use]
	pub fn authority(mut self, authority: impl Into<Option<String>>) -> Self {
		self.authority = authority.into();
		self
	}

	#[must_use]
	pub fn path(mut self, path: impl Into<String>) -> Self {
		self.path = Some(path.into());
		self
	}

	#[must_use]
	pub fn query(mut self, query: impl Into<Option<String>>) -> Self {
		self.query = query.into();
		self
	}

	#[must_use]
	pub fn fragment(mut self, fragment: impl Into<Option<String>>) -> Self {
		self.fragment = fragment.into();
		self
	}

	pub fn build(self) -> Result<Reference, Error> {
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
		let reference = string.parse().ok().ok_or(Error)?;
		Ok(reference)
	}
}
