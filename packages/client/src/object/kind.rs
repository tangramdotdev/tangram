use crate as tg;

#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Blob,
	Directory,
	File,
	Symlink,
	Graph,
	Command,
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::Kind::from(*self))
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::id::Kind::from_str(s)?.try_into()
	}
}

impl From<Kind> for tg::id::Kind {
	fn from(value: Kind) -> Self {
		match value {
			Kind::Blob => Self::Blob,
			Kind::Directory => Self::Directory,
			Kind::File => Self::File,
			Kind::Symlink => Self::Symlink,
			Kind::Graph => Self::Graph,
			Kind::Command => Self::Command,
		}
	}
}

impl TryFrom<tg::id::Kind> for Kind {
	type Error = tg::Error;

	fn try_from(value: tg::id::Kind) -> tg::Result<Self, Self::Error> {
		match value {
			tg::id::Kind::Blob => Ok(Self::Blob),
			tg::id::Kind::Directory => Ok(Self::Directory),
			tg::id::Kind::File => Ok(Self::File),
			tg::id::Kind::Symlink => Ok(Self::Symlink),
			tg::id::Kind::Graph => Ok(Self::Graph),
			tg::id::Kind::Command => Ok(Self::Command),
			kind => Err(tg::error!(%kind, "invalid kind")),
		}
	}
}
