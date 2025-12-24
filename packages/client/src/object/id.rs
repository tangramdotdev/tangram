use {super::Kind, crate::prelude::*, bytes::Bytes};

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Id {
	#[debug("tg::object::Id(\"{_0}\")")]
	Blob(tg::blob::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	Directory(tg::directory::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	File(tg::file::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	Symlink(tg::symlink::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	Graph(tg::graph::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	Command(tg::command::Id),
	#[debug("tg::object::Id(\"{_0}\")")]
	Error(tg::error::Id),
}

impl tg::object::Id {
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		match kind {
			Kind::Blob => tg::blob::Id::new(bytes).into(),
			Kind::Directory => tg::directory::Id::new(bytes).into(),
			Kind::File => tg::file::Id::new(bytes).into(),
			Kind::Symlink => tg::symlink::Id::new(bytes).into(),
			Kind::Graph => tg::graph::Id::new(bytes).into(),
			Kind::Command => tg::command::Id::new(bytes).into(),
			Kind::Error => tg::error::Id::new(bytes).into(),
		}
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Blob(_) => Kind::Blob,
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
			Self::Graph(_) => Kind::Graph,
			Self::Command(_) => Kind::Command,
			Self::Error(_) => Kind::Error,
		}
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	#[must_use]
	pub fn is_artifact(&self) -> bool {
		matches!(self, Self::Directory(_) | Self::File(_) | Self::Symlink(_))
	}
}

impl std::ops::Deref for tg::object::Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		match self {
			Self::Blob(id) => id,
			Self::Directory(id) => id,
			Self::File(id) => id,
			Self::Symlink(id) => id,
			Self::Graph(id) => id,
			Self::Command(id) => id,
			Self::Error(id) => id,
		}
	}
}

impl From<tg::object::Id> for tg::Id {
	fn from(value: tg::object::Id) -> Self {
		match value {
			tg::object::Id::Blob(id) => id.into(),
			tg::object::Id::Directory(id) => id.into(),
			tg::object::Id::File(id) => id.into(),
			tg::object::Id::Symlink(id) => id.into(),
			tg::object::Id::Graph(id) => id.into(),
			tg::object::Id::Command(id) => id.into(),
			tg::object::Id::Error(id) => id.into(),
		}
	}
}

impl TryFrom<tg::Id> for tg::object::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self> {
		match value.kind() {
			tg::id::Kind::Blob => Ok(Self::Blob(value.try_into()?)),
			tg::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			tg::id::Kind::File => Ok(Self::File(value.try_into()?)),
			tg::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			tg::id::Kind::Graph => Ok(Self::Graph(value.try_into()?)),
			tg::id::Kind::Command => Ok(Self::Command(value.try_into()?)),
			tg::id::Kind::Error => Ok(tg::object::Id::Error(value.try_into()?)),
			kind => Err(tg::error!(%kind, "expected an object ID")),
		}
	}
}

impl TryFrom<Vec<u8>> for tg::object::Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl std::str::FromStr for tg::object::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for tg::object::Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}
