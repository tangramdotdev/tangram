use {
	crate::prelude::*,
	std::{fmt, str::FromStr},
};

pub mod get;
pub mod post;

#[derive(
	Clone,
	Copy,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	Eq,
	PartialEq,
	tangram_serialize::Serialize,
	tangram_serialize::Deserialize,
)]
pub enum Stream {
	#[tangram_serialize(id = 2)]
	Stderr,

	#[tangram_serialize(id = 1)]
	Stdout,
}

impl fmt::Display for Stream {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Stderr => write!(f, "stderr"),
			Self::Stdout => write!(f, "stdout"),
		}
	}
}

impl FromStr for Stream {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"stderr" => Ok(Self::Stderr),
			"stdout" => Ok(Self::Stdout),
			stream => Err(tg::error!(%stream, "unknown stream")),
		}
	}
}
