#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContentEncoding {
	Zstd,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub struct FromStrError;

impl std::fmt::Display for ContentEncoding {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ContentEncoding::Zstd => write!(f, "zstd"),
		}
	}
}

impl std::str::FromStr for ContentEncoding {
	type Err = FromStrError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"zstd" => Ok(ContentEncoding::Zstd),
			_ => Err(FromStrError),
		}
	}
}
