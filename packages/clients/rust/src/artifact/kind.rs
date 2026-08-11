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
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[from_str(rename_all = "snake_case")]
pub enum Kind {
	#[tangram_serialize(id = 0)]
	Directory,

	#[tangram_serialize(id = 1)]
	File,

	#[tangram_serialize(id = 2)]
	Symlink,
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
