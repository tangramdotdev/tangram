use {super::Data, crate::prelude::*, std::sync::Arc};

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
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
	pub fn to_data(&self) -> Data {
		match self {
			Self::Blob(object) => Data::Blob(object.to_data()),
			Self::Directory(object) => Data::Directory(object.to_data()),
			Self::File(object) => Data::File(object.to_data()),
			Self::Symlink(object) => Data::Symlink(object.to_data()),
			Self::Graph(object) => Data::Graph(object.to_data()),
			Self::Command(object) => Data::Command(object.to_data()),
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		Ok(match data {
			Data::Blob(data) => {
				let object = tg::blob::Object::try_from_data(data)?;
				Self::Blob(Arc::new(object))
			},
			Data::Directory(data) => {
				let object = tg::directory::Object::try_from_data(data)?;
				Self::Directory(Arc::new(object))
			},
			Data::File(data) => {
				let object = tg::file::Object::try_from_data(data)?;
				Self::File(Arc::new(object))
			},
			Data::Symlink(data) => {
				let object = tg::symlink::Object::try_from_data(data)?;
				Self::Symlink(Arc::new(object))
			},
			Data::Graph(data) => {
				let object = tg::graph::Object::try_from_data(data)?;
				Self::Graph(Arc::new(object))
			},
			Data::Command(data) => {
				let object = tg::command::Object::try_from_data(data)?;
				Self::Command(Arc::new(object))
			},
		})
	}

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

	#[must_use]
	pub fn kind(&self) -> tg::object::Kind {
		match self {
			Self::Blob(_) => tg::object::Kind::Blob,
			Self::Directory(_) => tg::object::Kind::Directory,
			Self::File(_) => tg::object::Kind::File,
			Self::Symlink(_) => tg::object::Kind::Symlink,
			Self::Graph(_) => tg::object::Kind::Graph,
			Self::Command(_) => tg::object::Kind::Command,
		}
	}

	#[must_use]
	pub fn is_artifact(&self) -> bool {
		matches!(self, Self::Directory(_) | Self::File(_) | Self::Symlink(_))
	}
}
