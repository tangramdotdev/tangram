use {super::Kind, crate as tg, bytes::Bytes, std::ops::Deref};

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
#[debug("tg::object::Id(\"{_0}\")")]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

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
	Blob(tg::blob::Id),
	Directory(tg::directory::Id),
	File(tg::file::Id),
	Symlink(tg::symlink::Id),
	Graph(tg::graph::Id),
	Command(tg::command::Id),
}

pub enum VariantRef<'a> {
	Blob(&'a tg::blob::Id),
	Directory(&'a tg::directory::Id),
	File(&'a tg::file::Id),
	Symlink(&'a tg::symlink::Id),
	Graph(&'a tg::graph::Id),
	Command(&'a tg::command::Id),
}

impl Id {
	#[must_use]
	pub fn new(kind: Kind, bytes: &Bytes) -> Self {
		let id = match kind {
			Kind::Blob => tg::Id::new_blake3(tg::id::Kind::Blob, bytes),
			Kind::Directory => tg::Id::new_blake3(tg::id::Kind::Directory, bytes),
			Kind::File => tg::Id::new_blake3(tg::id::Kind::File, bytes),
			Kind::Symlink => tg::Id::new_blake3(tg::id::Kind::Symlink, bytes),
			Kind::Graph => tg::Id::new_blake3(tg::id::Kind::Graph, bytes),
			Kind::Command => tg::Id::new_blake3(tg::id::Kind::Command, bytes),
		};
		Self(id)
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self.0.kind() {
			tg::id::Kind::Blob => Kind::Blob,
			tg::id::Kind::Directory => Kind::Directory,
			tg::id::Kind::File => Kind::File,
			tg::id::Kind::Symlink => Kind::Symlink,
			tg::id::Kind::Graph => Kind::Graph,
			tg::id::Kind::Command => Kind::Command,
			_ => unreachable!(),
		}
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	#[must_use]
	pub fn is_artifact(&self) -> bool {
		matches!(
			self.0.kind(),
			tg::id::Kind::Directory | tg::id::Kind::File | tg::id::Kind::Symlink
		)
	}

	#[must_use]
	pub fn is_blob(&self) -> bool {
		self.0.kind() == tg::id::Kind::Blob
	}

	#[must_use]
	pub fn is_directory(&self) -> bool {
		self.0.kind() == tg::id::Kind::Directory
	}

	#[must_use]
	pub fn is_file(&self) -> bool {
		self.0.kind() == tg::id::Kind::File
	}

	#[must_use]
	pub fn is_symlink(&self) -> bool {
		self.0.kind() == tg::id::Kind::Symlink
	}

	#[must_use]
	pub fn is_graph(&self) -> bool {
		self.0.kind() == tg::id::Kind::Graph
	}

	#[must_use]
	pub fn is_command(&self) -> bool {
		self.0.kind() == tg::id::Kind::Command
	}

	pub fn try_unwrap_blob(self) -> tg::Result<tg::blob::Id> {
		tg::blob::Id::try_from(self.0)
	}

	pub fn try_unwrap_directory(self) -> tg::Result<tg::directory::Id> {
		tg::directory::Id::try_from(self.0)
	}

	pub fn try_unwrap_file(self) -> tg::Result<tg::file::Id> {
		tg::file::Id::try_from(self.0)
	}

	pub fn try_unwrap_symlink(self) -> tg::Result<tg::symlink::Id> {
		tg::symlink::Id::try_from(self.0)
	}

	pub fn try_unwrap_graph(self) -> tg::Result<tg::graph::Id> {
		tg::graph::Id::try_from(self.0)
	}

	pub fn try_unwrap_command(self) -> tg::Result<tg::command::Id> {
		tg::command::Id::try_from(self.0)
	}

	pub fn try_unwrap_blob_ref(&self) -> tg::Result<&tg::blob::Id> {
		if self.is_blob() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::blob::Id>() })
		} else {
			Err(tg::error!("expected a blob ID"))
		}
	}

	pub fn try_unwrap_directory_ref(&self) -> tg::Result<&tg::directory::Id> {
		if self.is_directory() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::directory::Id>() })
		} else {
			Err(tg::error!("expected a directory ID"))
		}
	}

	pub fn try_unwrap_file_ref(&self) -> tg::Result<&tg::file::Id> {
		if self.is_file() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::file::Id>() })
		} else {
			Err(tg::error!("expected a file ID"))
		}
	}

	pub fn try_unwrap_symlink_ref(&self) -> tg::Result<&tg::symlink::Id> {
		if self.is_symlink() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::symlink::Id>() })
		} else {
			Err(tg::error!("expected a symlink ID"))
		}
	}

	pub fn try_unwrap_graph_ref(&self) -> tg::Result<&tg::graph::Id> {
		if self.is_graph() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::graph::Id>() })
		} else {
			Err(tg::error!("expected a graph ID"))
		}
	}

	pub fn try_unwrap_command_ref(&self) -> tg::Result<&tg::command::Id> {
		if self.is_command() {
			Ok(unsafe { &*(&raw const self.0).cast::<tg::command::Id>() })
		} else {
			Err(tg::error!("expected a command ID"))
		}
	}

	#[must_use]
	pub fn variant(self) -> Variant {
		match self.kind() {
			Kind::Blob => Variant::Blob(tg::blob::Id::try_from(self.0).unwrap()),
			Kind::Directory => Variant::Directory(tg::directory::Id::try_from(self.0).unwrap()),
			Kind::File => Variant::File(tg::file::Id::try_from(self.0).unwrap()),
			Kind::Symlink => Variant::Symlink(tg::symlink::Id::try_from(self.0).unwrap()),
			Kind::Graph => Variant::Graph(tg::graph::Id::try_from(self.0).unwrap()),
			Kind::Command => Variant::Command(tg::command::Id::try_from(self.0).unwrap()),
		}
	}

	#[must_use]
	pub fn variant_ref(&self) -> VariantRef<'_> {
		match self.kind() {
			Kind::Blob => VariantRef::Blob(unsafe { &*(&raw const self.0).cast::<tg::blob::Id>() }),
			Kind::Directory => {
				VariantRef::Directory(unsafe { &*(&raw const self.0).cast::<tg::directory::Id>() })
			},
			Kind::File => VariantRef::File(unsafe { &*(&raw const self.0).cast::<tg::file::Id>() }),
			Kind::Symlink => {
				VariantRef::Symlink(unsafe { &*(&raw const self.0).cast::<tg::symlink::Id>() })
			},
			Kind::Graph => {
				VariantRef::Graph(unsafe { &*(&raw const self.0).cast::<tg::graph::Id>() })
			},
			Kind::Command => {
				VariantRef::Command(unsafe { &*(&raw const self.0).cast::<tg::command::Id>() })
			},
		}
	}
}

impl Deref for Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl TryFrom<tg::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		match value.kind() {
			tg::id::Kind::Blob
			| tg::id::Kind::Directory
			| tg::id::Kind::File
			| tg::id::Kind::Symlink
			| tg::id::Kind::Graph
			| tg::id::Kind::Command => Ok(Self(value)),
			kind => Err(tg::error!(%kind, "expected an object ID")),
		}
	}
}

impl TryFrom<Vec<u8>> for Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}

impl TryFrom<Id> for tg::blob::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_blob()
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

impl TryFrom<Id> for tg::graph::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_graph()
	}
}

impl TryFrom<Id> for tg::command::Id {
	type Error = tg::Error;

	fn try_from(value: Id) -> tg::Result<Self> {
		value.try_unwrap_command()
	}
}
