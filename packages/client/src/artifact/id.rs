use {super::Kind, crate as tg, bytes::Bytes, std::ops::Deref};

/// An artifact ID.
#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	derive_more::Into,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[debug("tg::artifact::Id(\"{_0}\")")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::object::Id);

#[derive(
	Clone,
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
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Variant {
	Directory(tg::directory::Id),
	File(tg::file::Id),
	Symlink(tg::symlink::Id),
}

pub enum VariantRef<'a> {
	Directory(&'a tg::directory::Id),
	File(&'a tg::file::Id),
	Symlink(&'a tg::symlink::Id),
}

impl Id {
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		let id = match kind {
			Kind::Directory => tg::Id::new_blake3(tg::id::Kind::Directory, bytes),
			Kind::File => tg::Id::new_blake3(tg::id::Kind::File, bytes),
			Kind::Symlink => tg::Id::new_blake3(tg::id::Kind::Symlink, bytes),
		};
		Self(tg::object::Id::try_from(id).unwrap())
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self.0.kind() {
			tg::object::Kind::Directory => Kind::Directory,
			tg::object::Kind::File => Kind::File,
			tg::object::Kind::Symlink => Kind::Symlink,
			_ => unreachable!(),
		}
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	#[must_use]
	pub fn is_directory(&self) -> bool {
		self.0.is_directory()
	}

	#[must_use]
	pub fn is_file(&self) -> bool {
		self.0.is_file()
	}

	#[must_use]
	pub fn is_symlink(&self) -> bool {
		self.0.is_symlink()
	}

	pub fn try_unwrap_directory(self) -> tg::Result<tg::directory::Id> {
		self.0.try_unwrap_directory()
	}

	pub fn try_unwrap_file(self) -> tg::Result<tg::file::Id> {
		self.0.try_unwrap_file()
	}

	pub fn try_unwrap_symlink(self) -> tg::Result<tg::symlink::Id> {
		self.0.try_unwrap_symlink()
	}

	pub fn try_unwrap_directory_ref(&self) -> tg::Result<&tg::directory::Id> {
		self.0.try_unwrap_directory_ref()
	}

	pub fn try_unwrap_file_ref(&self) -> tg::Result<&tg::file::Id> {
		self.0.try_unwrap_file_ref()
	}

	pub fn try_unwrap_symlink_ref(&self) -> tg::Result<&tg::symlink::Id> {
		self.0.try_unwrap_symlink_ref()
	}

	#[must_use]
	pub fn variant(self) -> Variant {
		match self.kind() {
			Kind::Directory => Variant::Directory(self.0.try_unwrap_directory().unwrap()),
			Kind::File => Variant::File(self.0.try_unwrap_file().unwrap()),
			Kind::Symlink => Variant::Symlink(self.0.try_unwrap_symlink().unwrap()),
		}
	}

	#[must_use]
	pub fn variant_ref(&self) -> VariantRef<'_> {
		match self.kind() {
			Kind::Directory => {
				VariantRef::Directory(unsafe { &*(&raw const self.0).cast::<tg::directory::Id>() })
			},
			Kind::File => VariantRef::File(unsafe { &*(&raw const self.0).cast::<tg::file::Id>() }),
			Kind::Symlink => {
				VariantRef::Symlink(unsafe { &*(&raw const self.0).cast::<tg::symlink::Id>() })
			},
		}
	}
}

impl Deref for Id {
	type Target = tg::object::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}

impl TryFrom<tg::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Directory | tg::id::Kind::File | tg::id::Kind::Symlink => {
				Ok(Self(tg::object::Id::try_from(value)?))
			},
			kind => Err(tg::error!(%kind, %value, "expected an artifact ID")),
		}
	}
}

impl TryFrom<tg::object::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::object::Id) -> tg::Result<Self, Self::Error> {
		if value.is_directory() || value.is_file() || value.is_symlink() {
			Ok(Self(value))
		} else {
			Err(tg::error!("expected an artifact ID"))
		}
	}
}

impl TryFrom<Vec<u8>> for Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl TryFrom<Id> for tg::directory::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_directory()
	}
}

impl TryFrom<Id> for tg::file::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_file()
	}
}

impl TryFrom<Id> for tg::symlink::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_symlink()
	}
}

impl From<Id> for tg::Id {
	fn from(value: Id) -> Self {
		value.0.into()
	}
}
