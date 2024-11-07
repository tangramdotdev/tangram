use crate as tg;

/// A blob kind.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Leaf => write!(f, "leaf "),
			Self::Branch => write!(f, "branch "),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"leaf " => Ok(Self::Leaf),
			"branch " => Ok(Self::Branch),
			_ => Err(tg::error!(%kind = s, "invalid kind")),
		}
	}
}
