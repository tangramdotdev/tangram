#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version {
	pub major: u64,
	pub minor: u64,
	pub patch: u64,
	pub prerelease: String,
	pub build: String,
}

impl std::fmt::Display for Version {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		todo!()
	}
}

impl std::str::FromStr for Version {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		todo!()
	}
}
