use super::Data;
use crate as tg;
use std::sync::Arc;

#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Object {
	Leaf(Arc<tg::leaf::Object>),
	Branch(Arc<tg::branch::Object>),
	Directory(Arc<tg::directory::Object>),
	File(Arc<tg::file::Object>),
	Symlink(Arc<tg::symlink::Object>),
	Graph(Arc<tg::graph::Object>),
	Command(Arc<tg::command::Object>),
}

impl Object {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
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

impl TryFrom<Data> for Object {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(match data {
			Data::Leaf(data) => Self::Leaf(Arc::new(tg::leaf::Object::try_from(data)?)),
			Data::Branch(data) => Self::Branch(Arc::new(tg::branch::Object::try_from(data)?)),
			Data::Directory(data) => {
				Self::Directory(Arc::new(tg::directory::Object::try_from(data)?))
			},
			Data::File(data) => Self::File(Arc::new(tg::file::Object::try_from(data)?)),
			Data::Symlink(data) => Self::Symlink(Arc::new(tg::symlink::Object::try_from(data)?)),
			Data::Graph(data) => Self::Graph(Arc::new(tg::graph::Object::try_from(data)?)),
			Data::Command(data) => Self::Command(Arc::new(tg::command::Object::try_from(data)?)),
		})
	}
}
