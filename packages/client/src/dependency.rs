use crate::{directory, Error, Result};
use tangram_error::WrapErr;

/// A dependency.
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
pub struct Dependency {
	/// The package's ID.
	pub id: Option<directory::Id>,

	/// The name of the package.
	pub name: Option<String>,

	/// The package's path.
	pub path: Option<crate::Path>,

	/// The package's version.
	pub version: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Params {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<directory::Id>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub path: Option<crate::Path>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
}

impl Dependency {
	#[must_use]
	pub fn with_id(id: directory::Id) -> Self {
		Self {
			id: Some(id),
			..Default::default()
		}
	}

	#[must_use]
	pub fn with_name(name: String) -> Self {
		Self {
			name: Some(name),
			..Default::default()
		}
	}

	#[must_use]
	pub fn with_name_and_version(name: String, version: String) -> Self {
		Self {
			name: Some(name),
			version: Some(version),
			..Default::default()
		}
	}

	#[must_use]
	pub fn with_path(path: crate::Path) -> Self {
		Self {
			path: Some(path),
			..Default::default()
		}
	}

	pub fn apply_params(&mut self, params: Params) {
		if let Some(id) = params.id {
			self.id = Some(id);
		}
		if let Some(name) = params.name {
			self.name = Some(name);
		}
		if let Some(path) = params.path {
			self.path = Some(path);
		}
		if let Some(version) = params.version {
			self.version = Some(version);
		}
	}
}

impl std::fmt::Display for Dependency {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut dependency = self.clone();
		if let Some(id) = dependency.id.take() {
			write!(f, "{id}")?;
		} else if let Some(name) = dependency.name.take() {
			write!(f, "{name}")?;
			if let Some(version) = dependency.version.take() {
				write!(f, "@{version}")?;
			}
		} else if let Some(path) = dependency.path.take() {
			if path
				.components()
				.first()
				.map_or(false, |component| component.try_unwrap_normal_ref().is_ok())
			{
				write!(f, "./")?;
			}
			write!(f, "{path}")?;
		}
		let params = serde_urlencoded::to_string(Params::from(dependency)).unwrap();
		if !params.is_empty() {
			write!(f, "?{params}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Dependency {
	type Err = Error;

	fn from_str(value: &str) -> Result<Dependency> {
		let mut dependency = Dependency::default();

		// Split the string.
		let split = value.split_once('?');
		let path = match split {
			Some((path, _)) if !path.is_empty() => Some(path),
			Some(_) => None,
			None => (!value.is_empty()).then_some(value),
		};
		let query = match split {
			Some((_, query)) if !query.is_empty() => Some(query),
			_ => None,
		};

		// Parse the path.
		if let Some(path) = path {
			if let Ok(id) = path.parse() {
				dependency.id = Some(id);
			} else if path.starts_with('/') || path.starts_with('.') {
				dependency.path = Some(path.parse()?);
			} else {
				let split = path.split_once('@');
				let name = match split {
					Some((name, _)) if !name.is_empty() => Some(name),
					Some(_) => None,
					None => (!path.is_empty()).then_some(path),
				};
				if let Some(name) = name {
					dependency.name = Some(name.to_owned());
				}
				let version = match split {
					Some((_, version)) if !version.is_empty() => Some(version),
					_ => None,
				};
				if let Some(version) = version {
					dependency.version = Some(version.to_owned());
				}
			}
		}

		// Parse the params from the query.
		let params = if let Some(query) = query {
			Some(
				serde_urlencoded::from_str::<Params>(query)
					.wrap_err("Failed to deserialize the query.")?,
			)
		} else {
			None
		};

		// Apply the params.
		if let Some(params) = params {
			dependency.apply_params(params);
		}

		Ok(dependency)
	}
}

impl TryFrom<String> for Dependency {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

impl From<Dependency> for String {
	fn from(value: Dependency) -> Self {
		value.to_string()
	}
}

impl From<Dependency> for Params {
	fn from(value: Dependency) -> Self {
		Self {
			id: value.id,
			name: value.name,
			path: value.path,
			version: value.version,
		}
	}
}

impl From<Params> for Dependency {
	fn from(value: Params) -> Self {
		Self {
			id: value.id,
			name: value.name,
			path: value.path,
			version: value.version,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::Dependency;

	#[test]
	fn display() {
		let left = Dependency {
			id: None,
			name: Some("foo".into()),
			path: None,
			version: None,
		};
		let right = "foo";
		assert_eq!(left.to_string(), right);

		let left = Dependency {
			id: None,
			name: Some("foo".into()),
			path: None,
			version: Some("1.2.3".into()),
		};
		let right = "foo@1.2.3";
		assert_eq!(left.to_string(), right);

		let left = Dependency {
			id: None,
			name: Some("foo".into()),
			path: Some("path/to/foo".parse().unwrap()),
			version: Some("1.2.3".into()),
		};
		let right = "foo@1.2.3?path=path%2Fto%2Ffoo";
		assert_eq!(left.to_string(), right);

		let left = Dependency {
			id: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			version: None,
		};
		let right = "./path/to/foo";
		assert_eq!(left.to_string(), right);
	}

	#[test]
	fn parse() {
		let left: Dependency = "foo".parse().unwrap();
		let right = Dependency {
			id: None,
			name: Some("foo".into()),
			path: None,
			version: None,
		};
		assert_eq!(left, right);

		let left: Dependency = "foo@1.2.3".parse().unwrap();
		let right = Dependency {
			id: None,
			name: Some("foo".into()),
			path: None,
			version: Some("1.2.3".into()),
		};
		assert_eq!(left, right);

		let left: Dependency = "foo@1.2.3?path=path%2Fto%2Ffoo".parse().unwrap();
		let right = Dependency {
			id: None,
			name: Some("foo".into()),
			path: Some("path/to/foo".parse().unwrap()),
			version: Some("1.2.3".into()),
		};
		assert_eq!(left, right);

		let left: Dependency = "./path/to/foo".parse().unwrap();
		let right = Dependency {
			id: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			version: None,
		};
		assert_eq!(left, right);

		let left: Dependency = "?path=path%2Fto%2Ffoo".parse().unwrap();
		let right = Dependency {
			id: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			version: None,
		};
		assert_eq!(left, right);
	}
}
