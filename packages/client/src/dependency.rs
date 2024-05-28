use crate as tg;

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
pub struct Dependency {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub artifact: Option<tg::artifact::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<tg::Path>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub version: Option<String>,
}

impl Dependency {
	#[must_use]
	pub fn with_artifact(artifact: tg::artifact::Id) -> Self {
		Self {
			artifact: Some(artifact),
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
	pub fn with_path(path: tg::Path) -> Self {
		let path = path.normalize();
		Self {
			path: Some(path),
			..Default::default()
		}
	}

	pub fn merge(&mut self, other: Self) {
		if let Some(artifact) = other.artifact {
			self.artifact = Some(artifact);
		}
		if let Some(name) = other.name {
			self.name = Some(name);
		}
		if let Some(path) = other.path {
			self.path = Some(path);
		}
		if let Some(version) = other.version {
			self.version = Some(version);
		}
	}

	/// Check if a `version` string satisfies this dependency's `version` constraint.
	pub fn try_match_version(&self, version: &str) -> tg::Result<bool> {
		let Some(constraint) = &self.version else {
			return Ok(true);
		};

		if constraint.starts_with('/') {
			let (_, constraint) = constraint.split_at(1);
			let regex = format!("^{constraint}$");
			let matched = regex::Regex::new(&regex)
				.map_err(|source| tg::error!(!source, "failed to parse regex"))?
				.is_match(version);
			return Ok(matched);
		}

		if "=<>^~*".chars().any(|ch| constraint.starts_with(ch)) {
			let req = semver::VersionReq::parse(constraint).map_err(|error| {
				tg::error!(
					source = error,
					"failed to parse version constraint as semver"
				)
			})?;
			let semver = semver::Version::parse(version)
				.map_err(|source| tg::error!(!source, "failed to parse version as semver"))?;
			return Ok(req.matches(&semver));
		}

		// Fall back on string equality.
		Ok(constraint == version)
	}
}

impl std::fmt::Display for Dependency {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match (&self.artifact, &self.name, &self.version, &self.path) {
			(Some(artifact), None, None, None) => {
				write!(f, "{artifact}")?;
			},
			(None, Some(name), None, None) => write!(f, "{name}")?,
			(None, Some(name), Some(version), None) => {
				write!(f, "{name}@{version}")?;
			},
			(None, None, None, Some(path)) => {
				write!(f, "{path}")?;
			},
			_ => {
				let json = serde_json::to_string(self).unwrap();
				write!(f, "{json}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Dependency {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		if value.starts_with('{') {
			serde_json::from_str(value)
				.map_err(|source| tg::error!(!source, "failed to deserialize the dependency"))
		} else if let Ok(artifact) = value.parse() {
			Ok(Self {
				artifact: Some(artifact),
				..Default::default()
			})
		} else if value.starts_with('/') || value.starts_with('.') {
			Ok(Self::with_path(value.parse()?))
		} else {
			let (name, version) = match value.split_once('@') {
				None => (Some(value.to_owned()), None),
				Some((name, version)) => (Some(name.to_owned()), Some(version.to_owned())),
			};
			Ok(Self {
				name,
				version,
				..Default::default()
			})
		}
	}
}

impl TryFrom<String> for Dependency {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self, Self::Error> {
		value.parse()
	}
}

impl From<Dependency> for String {
	fn from(value: Dependency) -> Self {
		value.to_string()
	}
}

impl From<tg::artifact::Id> for Dependency {
	fn from(value: tg::artifact::Id) -> Self {
		Self {
			artifact: Some(value),
			..Default::default()
		}
	}
}

#[cfg(test)]
mod tests {
	use crate as tg;

	#[test]
	fn display() {
		let left = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: None,
			remote: None,
			version: None,
		};
		let right = "foo";
		assert_eq!(left.to_string(), right);

		let left = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: None,
			remote: None,
			version: Some("1.2.3".into()),
		};
		let right = "foo@1.2.3";
		assert_eq!(left.to_string(), right);

		let left = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: None,
			remote: None,
			version: Some(r"/1\.2\.*".into()),
		};
		let right = r"foo@/1\.2\.*";
		assert_eq!(left.to_string(), right);

		let left = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: Some("path/to/foo".parse().unwrap()),
			remote: None,
			version: Some("1.2.3".into()),
		};
		let right = r#"{"name":"foo","path":"./path/to/foo","version":"1.2.3"}"#;
		assert_eq!(left.to_string(), right);

		let left = tg::Dependency {
			artifact: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			remote: None,
			version: None,
		};
		let right = "./path/to/foo";
		assert_eq!(left.to_string(), right);
	}

	#[test]
	fn parse() {
		let left: tg::Dependency = "foo".parse().unwrap();
		let right = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: None,
			remote: None,
			version: None,
		};
		assert_eq!(left, right);

		let left: tg::Dependency = "foo@1.2.3".parse().unwrap();
		let right = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: None,
			remote: None,
			version: Some("1.2.3".into()),
		};
		assert_eq!(left, right);

		let left: tg::Dependency = r#"{"name":"foo","path":"path/to/foo","version":"1.2.3"}"#
			.parse()
			.unwrap();
		let right = tg::Dependency {
			artifact: None,
			name: Some("foo".into()),
			path: Some("path/to/foo".parse().unwrap()),
			remote: None,
			version: Some("1.2.3".into()),
		};
		assert_eq!(left, right);

		let left: tg::Dependency = "./path/to/foo".parse().unwrap();
		let right = tg::Dependency {
			artifact: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			remote: None,
			version: None,
		};
		assert_eq!(left, right);

		let left: tg::Dependency = r#"{"path":"path/to/foo"}"#.parse().unwrap();
		let right = tg::Dependency {
			artifact: None,
			name: None,
			path: Some("path/to/foo".parse().unwrap()),
			remote: None,
			version: None,
		};
		assert_eq!(left, right);
	}

	#[test]
	fn matches() {
		// Semver.
		let dep = tg::Dependency::with_name_and_version("A".into(), "=1.*".into());
		assert!(dep.try_match_version("1.2.3").unwrap());

		// String comparison.
		let dep = tg::Dependency::with_name_and_version("A".into(), "1.2".into());
		assert!(dep.try_match_version("1.2").unwrap());

		// <date>-<major>.<minor> matched with a regex.
		let dep = tg::Dependency::with_name_and_version("A".into(), "/2024-01-01-3.[0-9]+".into());
		assert!(dep.try_match_version("2024-01-01-3.1").unwrap());
	}
}
