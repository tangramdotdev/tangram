use {super::Data, crate::prelude::*, std::sync::Arc};

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Artifact {
	/// A directory.
	Directory(Arc<tg::directory::Object>),

	/// A file.
	File(Arc<tg::file::Object>),

	/// A symlink.
	Symlink(Arc<tg::symlink::Object>),
}

impl Artifact {
	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Directory(directory) => directory.to_data().into(),
			Self::File(file) => file.to_data().into(),
			Self::Symlink(symlink) => symlink.to_data().into(),
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Directory(directory) => directory.children(),
			Self::File(file) => file.children(),
			Self::Symlink(symlink) => symlink.children(),
		}
	}
}
