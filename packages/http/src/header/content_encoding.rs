#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ContentEncoding {
	Zstd,
}

impl std::fmt::Display for ContentEncoding {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ContentEncoding::Zstd => write!(f, "zstd"),
		}
	}
}

impl std::str::FromStr for ContentEncoding {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"zstd" => Ok(ContentEncoding::Zstd),
			_ => Err("unknown content encoding"),
		}
	}
}
