use super::Kind;
use crate as tg;
use bytes::Bytes;

/// An artifact ID.
#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Id {
	/// A directory ID.
	Directory(tg::directory::Id),

	/// A file ID.
	File(tg::file::Id),

	/// A symlink ID.
	Symlink(tg::symlink::Id),
}

impl Id {
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		match kind {
			Kind::Directory => tg::directory::Id::new(bytes).into(),
			Kind::File => tg::file::Id::new(bytes).into(),
			Kind::Symlink => tg::symlink::Id::new(bytes).into(),
		}
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Directory(_) => Kind::Directory,
			Self::File(_) => Kind::File,
			Self::Symlink(_) => Kind::Symlink,
		}
	}

	#[must_use]
	pub fn to_bytes(&self) -> Vec<u8> {
		self.as_id().to_bytes()
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	#[must_use]
	fn as_id(&self) -> &tg::Id {
		match self {
			Self::Directory(id) => &id.0,
			Self::File(id) => &id.0,
			Self::Symlink(id) => &id.0,
		}
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Directory(id) => write!(f, "{id}"),
			Self::File(id) => write!(f, "{id}"),
			Self::Symlink(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Directory(id) => id.into(),
			Id::File(id) => id.into(),
			Id::Symlink(id) => id.into(),
		}
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Directory => Ok(Self::Directory(value.try_into()?)),
			tg::id::Kind::File => Ok(Self::File(value.try_into()?)),
			tg::id::Kind::Symlink => Ok(Self::Symlink(value.try_into()?)),
			kind => Err(tg::error!(%kind, %value, "expected an artifact ID")),
		}
	}
}

impl From<Id> for tg::object::Id {
	fn from(value: Id) -> Self {
		match value {
			Id::Directory(id) => id.into(),
			Id::File(id) => id.into(),
			Id::Symlink(id) => id.into(),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		match value {
			tg::object::Id::Directory(value) => Ok(value.into()),
			tg::object::Id::File(value) => Ok(value.into()),
			tg::object::Id::Symlink(value) => Ok(value.into()),
			value => Err(tg::error!(%value, "expected an artifact ID")),
		}
	}
}
