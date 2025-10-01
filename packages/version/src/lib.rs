use winnow::{
	ascii::{alphanumeric1, dec_uint},
	combinator::{opt, preceded, separated},
	prelude::*,
};

pub use self::pattern::Pattern;

pub mod pattern;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Version {
	pub major: u64,
	pub minor: Option<u64>,
	pub patch: Option<u64>,
	pub prerelease: Option<Prerelease>,
	pub build: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Prerelease {
	pub components: Vec<PrereleaseComponent>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PrereleaseComponent {
	Number(u64),
	String(String),
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
pub struct ParseError;

impl std::fmt::Display for Version {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.major)?;
		if let Some(minor) = self.minor {
			write!(f, ".{minor}")?;
		}
		if let Some(patch) = self.patch {
			write!(f, ".{patch}")?;
		}
		if let Some(prerelease) = &self.prerelease {
			write!(f, "-{prerelease}")?;
		}
		if let Some(build) = &self.build {
			write!(f, "+{build}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Prerelease {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for (i, component) in self.components.iter().enumerate() {
			if i > 0 {
				write!(f, ".")?;
			}
			write!(f, "{component}")?;
		}
		Ok(())
	}
}

impl std::fmt::Display for PrereleaseComponent {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			PrereleaseComponent::Number(n) => write!(f, "{n}"),
			PrereleaseComponent::String(s) => write!(f, "{s}"),
		}
	}
}

impl std::str::FromStr for Version {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		version.parse(s).ok().ok_or(ParseError)
	}
}

fn version(input: &mut &str) -> ModalResult<Version> {
	let prerelease = opt(preceded("-", prerelease));
	let build = opt(preceded("+", dot_separated_identifier));
	let (major, minor, patch, prerelease, build) = (
		dec_uint,
		opt(preceded(".", dec_uint)),
		opt(preceded(".", dec_uint)),
		prerelease,
		build,
	)
		.parse_next(input)?;
	let build = build.map(ToOwned::to_owned);
	Ok(Version {
		major,
		minor,
		patch,
		prerelease,
		build,
	})
}

fn prerelease(input: &mut &str) -> ModalResult<Prerelease> {
	let identifiers: Vec<&str> = separated(1.., alphanumeric1, ".").parse_next(input)?;
	let components = identifiers
		.into_iter()
		.map(|id| {
			if let Ok(n) = id.parse::<u64>() {
				PrereleaseComponent::Number(n)
			} else {
				PrereleaseComponent::String(id.to_owned())
			}
		})
		.collect();
	Ok(Prerelease { components })
}

fn dot_separated_identifier<'a>(input: &mut &'a str) -> ModalResult<&'a str> {
	separated::<_, _, Vec<_>, _, _, _, _>(1.., alphanumeric1, ".")
		.take()
		.parse_next(input)
}

impl PartialOrd for Version {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Version {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.major
			.cmp(&other.major)
			.then_with(|| self.minor.unwrap_or(0).cmp(&other.minor.unwrap_or(0)))
			.then_with(|| self.patch.unwrap_or(0).cmp(&other.patch.unwrap_or(0)))
			.then_with(|| match (&self.prerelease, &other.prerelease) {
				(None, None) => std::cmp::Ordering::Equal,
				(Some(_), None) => std::cmp::Ordering::Less,
				(None, Some(_)) => std::cmp::Ordering::Greater,
				(Some(a), Some(b)) => a.cmp(b),
			})
	}
}

impl PartialOrd for PrereleaseComponent {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PrereleaseComponent {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		match (self, other) {
			(PrereleaseComponent::Number(a), PrereleaseComponent::Number(b)) => a.cmp(b),
			(PrereleaseComponent::String(a), PrereleaseComponent::String(b)) => a.cmp(b),
			(PrereleaseComponent::Number(_), PrereleaseComponent::String(_)) => {
				std::cmp::Ordering::Less
			},
			(PrereleaseComponent::String(_), PrereleaseComponent::Number(_)) => {
				std::cmp::Ordering::Greater
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse() {
		let left = "1";
		let right = Version {
			major: 1,
			minor: None,
			patch: None,
			prerelease: None,
			build: None,
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);

		let left = "1.2";
		let right = Version {
			major: 1,
			minor: Some(2),
			patch: None,
			prerelease: None,
			build: None,
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);

		let left = "1.2.3";
		let right = Version {
			major: 1,
			minor: Some(2),
			patch: Some(3),
			prerelease: None,
			build: None,
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);

		let left = "1.2.3-prerelease.1+build.1.1";
		let right = Version {
			major: 1,
			minor: Some(2),
			patch: Some(3),
			prerelease: Some(Prerelease {
				components: vec![
					PrereleaseComponent::String("prerelease".to_owned()),
					PrereleaseComponent::Number(1),
				],
			}),
			build: Some("build.1.1".to_owned()),
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);
	}

	#[test]
	fn prerelease_ordering() {
		assert!("1.0.0".parse::<Version>().unwrap() > "1.0.0-alpha".parse::<Version>().unwrap());
		assert!("1.0.0-1".parse::<Version>().unwrap() < "1.0.0-2".parse::<Version>().unwrap());
		assert!("1.0.0-2".parse::<Version>().unwrap() < "1.0.0-10".parse::<Version>().unwrap());
		assert!(
			"1.0.0-alpha".parse::<Version>().unwrap() < "1.0.0-beta".parse::<Version>().unwrap()
		);
		assert!("1.0.0-1".parse::<Version>().unwrap() < "1.0.0-alpha".parse::<Version>().unwrap());
		assert!(
			"1.0.0-alpha".parse::<Version>().unwrap() < "1.0.0-alpha.1".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-alpha.1".parse::<Version>().unwrap()
				< "1.0.0-alpha.beta".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-alpha".parse::<Version>().unwrap() < "1.0.0-alpha.1".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-alpha.1".parse::<Version>().unwrap()
				< "1.0.0-alpha.beta".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-alpha.beta".parse::<Version>().unwrap()
				< "1.0.0-beta".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-beta".parse::<Version>().unwrap() < "1.0.0-beta.2".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-beta.2".parse::<Version>().unwrap()
				< "1.0.0-beta.11".parse::<Version>().unwrap()
		);
		assert!(
			"1.0.0-beta.11".parse::<Version>().unwrap() < "1.0.0-rc.1".parse::<Version>().unwrap()
		);
		assert!("1.0.0-rc.1".parse::<Version>().unwrap() < "1.0.0".parse::<Version>().unwrap());
	}
}
