use winnow::{
	ascii::{alphanumeric1, dec_uint},
	combinator::{opt, preceded, separated},
	prelude::*,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version {
	pub major: u64,
	pub minor: u64,
	pub patch: u64,
	pub prerelease: Option<String>,
	pub build: Option<String>,
}

#[derive(Clone, Debug, derive_more::Display, derive_more::Error)]
pub struct ParseError;

impl std::fmt::Display for Version {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
		if let Some(prerelease) = &self.prerelease {
			write!(f, "-{prerelease}")?;
		}
		if let Some(build) = &self.build {
			write!(f, "+{build}")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Version {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		version.parse(s).ok().ok_or(ParseError)
	}
}

fn version(input: &mut &str) -> PResult<Version> {
	let prerelease = opt(preceded("-", dot_separated_identifier));
	let build = opt(preceded("+", dot_separated_identifier));
	let (major, _, minor, _, patch, prerelease, build) =
		(dec_uint, ".", dec_uint, ".", dec_uint, prerelease, build).parse_next(input)?;
	let prerelease = prerelease.map(ToOwned::to_owned);
	let build = build.map(ToOwned::to_owned);
	Ok(Version {
		major,
		minor,
		patch,
		prerelease,
		build,
	})
}

fn dot_separated_identifier<'a>(input: &mut &'a str) -> PResult<&'a str> {
	separated::<_, _, Vec<_>, _, _, _, _>(1.., alphanumeric1, ".")
		.take()
		.parse_next(input)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse() {
		let left = "1.2.3";
		let right = Version {
			major: 1,
			minor: 2,
			patch: 3,
			prerelease: None,
			build: None,
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);

		let left = "1.2.3-prerelease.1+build.1.1";
		let right = Version {
			major: 1,
			minor: 2,
			patch: 3,
			prerelease: Some("prerelease.1".to_owned()),
			build: Some("build.1.1".to_owned()),
		};
		assert_eq!(left.parse::<Version>().unwrap(), right);
	}
}
