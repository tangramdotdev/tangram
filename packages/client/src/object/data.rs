use super::Kind;
use crate as tg;
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
pub enum Object {
	Leaf(tg::leaf::Data),
	Branch(tg::branch::Data),
	Directory(tg::directory::Data),
	File(tg::file::Data),
	Symlink(tg::symlink::Data),
	Graph(tg::graph::Data),
	Command(tg::command::Data),
}

impl Object {
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

	pub fn serialize(&self) -> tg::Result<Bytes> {
		match self {
			Self::Leaf(data) => Ok(data.serialize()?),
			Self::Branch(data) => Ok(data.serialize()?),
			Self::Directory(data) => Ok(data.serialize()?),
			Self::File(data) => Ok(data.serialize()?),
			Self::Symlink(data) => Ok(data.serialize()?),
			Self::Graph(data) => Ok(data.serialize()?),
			Self::Command(data) => Ok(data.serialize()?),
		}
	}

	pub fn deserialize<'a>(kind: Kind, bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		match kind {
			Kind::Leaf => Ok(Self::Leaf(tg::leaf::Data::deserialize(bytes)?)),
			Kind::Branch => Ok(Self::Branch(tg::branch::Data::deserialize(bytes)?)),
			Kind::Directory => Ok(Self::Directory(tg::directory::Data::deserialize(bytes)?)),
			Kind::File => Ok(Self::File(tg::file::Data::deserialize(bytes)?)),
			Kind::Symlink => Ok(Self::Symlink(tg::symlink::Data::deserialize(bytes)?)),
			Kind::Graph => Ok(Self::Graph(tg::graph::Data::deserialize(bytes)?)),
			Kind::Command => Ok(Self::Command(tg::command::Data::deserialize(bytes)?)),
		}
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Leaf(leaf) => leaf.children(),
			Self::Branch(branch) => branch.children(),
			Self::Directory(directory) => directory.children(),
			Self::File(file) => file.children(),
			Self::Symlink(symlink) => symlink.children(),
			Self::Graph(graph) => graph.children(),
			Self::Command(command) => command.children(),
		}
	}
}
