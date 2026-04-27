use crate::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Debug {
	pub addr: Option<std::net::SocketAddr>,
	pub mode: Mode,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Mode {
	#[default]
	Normal,
	Break,
	Wait,
}

impl std::fmt::Display for Mode {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Normal => write!(f, "normal"),
			Self::Break => write!(f, "break"),
			Self::Wait => write!(f, "wait"),
		}
	}
}

impl std::str::FromStr for Mode {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"normal" => Ok(Self::Normal),
			"break" => Ok(Self::Break),
			"wait" => Ok(Self::Wait),
			_ => Err(tg::error!(mode = %s, "invalid mode")),
		}
	}
}
