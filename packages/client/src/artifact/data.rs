use super::Kind;
use crate as tg;
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Artifact {
	/// A directory.
	Directory(tg::directory::Data),

	/// A file.
	File(tg::file::Data),

	/// A symlink.
	Symlink(tg::symlink::Data),
}

impl Artifact {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
		}
	}

	pub fn serialize(&self) -> tg::Result<Bytes> {
		match self {
			Self::Directory(directory) => directory.serialize(),
			Self::File(file) => file.serialize(),
			Self::Symlink(symlink) => symlink.serialize(),
		}
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Directory(directory) => directory.children(),
			Self::File(file) => file.children(),
			Self::Symlink(symlink) => symlink.children(),
		}
	}
}
