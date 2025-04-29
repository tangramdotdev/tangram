use super::Data;
use crate as tg;
use std::sync::Arc;

#[derive(Clone, Debug, derive_more::From, derive_more::TryInto, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Object {
	Blob(Arc<tg::blob::Object>),
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
			Self::Blob(blob) => blob.children(),
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

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		Ok(match data {
			Data::Blob(data) => Self::Blob(Arc::new(tg::blob::Object::try_from(data)?)),
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
