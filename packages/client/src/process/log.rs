use std::{fmt, str::FromStr};
use crate as tg;

pub mod get;
pub mod post;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
pub enum Stream {
	Stderr,
	Stdout,
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stderr => write!(f, "stderr"),
            Self::Stdout => write!(f, "stderr"),
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
