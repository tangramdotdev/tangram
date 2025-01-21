use crate as tg;

#[derive(
	Copy, Clone, Debug, Default, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub enum Kind {
	#[default]
	Stdout,
	Stderr,
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Stdout => write!(f, "stdout"),
			Self::Stderr => write!(f, "stderr"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"stdout" => Ok(Self::Stdout),
			"stderr" => Ok(Self::Stderr),
			status => Err(tg::error!(%status, "invalid value")),
		}
	}
}
