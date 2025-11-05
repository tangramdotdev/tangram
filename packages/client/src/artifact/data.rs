use {super::Kind, crate::prelude::*, bytes::Bytes, std::collections::BTreeSet};

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

	pub fn deserialize<'a>(kind: Kind, bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		match kind {
			Kind::Directory => Ok(Self::Directory(tg::directory::Data::deserialize(bytes)?)),
			Kind::File => Ok(Self::File(tg::file::Data::deserialize(bytes)?)),
			Kind::Symlink => Ok(Self::Symlink(tg::symlink::Data::deserialize(bytes)?)),
		}
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Directory(directory) => directory.children(children),
			Self::File(file) => file.children(children),
			Self::Symlink(symlink) => symlink.children(children),
		}
	}
}

impl From<Artifact> for tg::object::Data {
	fn from(value: Artifact) -> Self {
		match value {
			Artifact::Directory(directory) => tg::object::Data::Directory(directory),
			Artifact::File(file) => tg::object::Data::File(file),
			Artifact::Symlink(symlink) => tg::object::Data::Symlink(symlink),
		}
	}
}

impl TryFrom<tg::object::Data> for Artifact {
	type Error = tg::Error;

	fn try_from(value: tg::object::Data) -> Result<Self, Self::Error> {
		match value {
			crate::object::Data::Directory(directory) => Ok(Self::Directory(directory)),
			crate::object::Data::File(file) => Ok(Self::File(file)),
			crate::object::Data::Symlink(symlink) => Ok(Self::Symlink(symlink)),
			_ => Err(tg::error!("invalid object")),
		}
	}
}
