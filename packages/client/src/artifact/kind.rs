use crate::prelude::*;

/// An artifact kind.
#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum Kind {
	#[tangram_serialize(id = 0)]
	Directory,

	#[tangram_serialize(id = 1)]
	File,

	#[tangram_serialize(id = 2)]
	Symlink,
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory => write!(f, "directory"),
			Self::File => write!(f, "file"),
			Self::Symlink => write!(f, "symlink"),
		}
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"directory" => Ok(Self::Directory),
			"file" => Ok(Self::File),
			"symlink" => Ok(Self::Symlink),
			_ => Err(tg::error!(kind = %s, "invalid kind")),
		}
	}
}

impl From<Kind> for tg::object::Kind {
	fn from(value: Kind) -> Self {
		match value {
			Kind::Directory => tg::object::Kind::Directory,
			Kind::File => tg::object::Kind::File,
			Kind::Symlink => tg::object::Kind::Symlink,
		}
	}
}
