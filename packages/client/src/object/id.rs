use super::Kind;
use crate as tg;
use bytes::Bytes;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[try_unwrap(ref)]
pub enum Id {
	Leaf(tg::leaf::Id),
	Branch(tg::branch::Id),
	Directory(tg::directory::Id),
	File(tg::file::Id),
	Symlink(tg::symlink::Id),
	Graph(tg::graph::Id),
	Command(tg::command::Id),
}

impl Id {
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		match kind {
			Kind::Leaf => tg::leaf::Id::new(bytes).into(),
			Kind::Branch => tg::branch::Id::new(bytes).into(),
			Kind::Directory => tg::directory::Id::new(bytes).into(),
			Kind::File => tg::file::Id::new(bytes).into(),
			Kind::Symlink => tg::symlink::Id::new(bytes).into(),
			Kind::Graph => tg::graph::Id::new(bytes).into(),
			Kind::Command => tg::command::Id::new(bytes).into(),
		}
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Leaf(_) => Kind::Leaf,
			Self::Branch(_) => Kind::Branch,
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
			Self::Graph(_) => Kind::Graph,
			Self::Command(_) => Kind::Command,
		}
	}
}

impl From<self::Id> for crate::Id {
	fn from(value: self::Id) -> Self {
		match value {
			self::Id::Leaf(id) => id.into(),
			self::Id::Branch(id) => id.into(),
			self::Id::Directory(id) => id.into(),
			self::Id::File(id) => id.into(),
			self::Id::Symlink(id) => id.into(),
			self::Id::Graph(id) => id.into(),
			self::Id::Command(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for self::Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			crate::id::Kind::Leaf => Ok(Self::Leaf(value.try_into()?)),
			crate::id::Kind::Branch => Ok(Self::Branch(value.try_into()?)),
			crate::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			crate::id::Kind::File => Ok(Self::File(value.try_into()?)),
			crate::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			crate::id::Kind::Graph => Ok(Self::Graph(value.try_into()?)),
			crate::id::Kind::Command => Ok(Self::Command(value.try_into()?)),
			kind => Err(tg::error!(%kind, "expected an object ID")),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}
