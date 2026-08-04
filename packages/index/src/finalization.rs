use tangram_client::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
	Process,
	Sandbox,
}

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub enum Item {
	#[tangram_serialize(id = 0)]
	Process(tg::process::Id),

	#[tangram_serialize(id = 1)]
	Sandbox(tg::sandbox::Id),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
	pub item: Item,
	pub partition: u64,
	pub(crate) version: Version,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Version([u8; 12]);

impl Item {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Process(_) => Kind::Process,
			Self::Sandbox(_) => Kind::Sandbox,
		}
	}
}

impl Version {
	#[must_use]
	pub(crate) fn new(bytes: [u8; 12]) -> Self {
		Self(bytes)
	}

	#[must_use]
	#[cfg(feature = "foundationdb")]
	pub(crate) fn bytes(&self) -> &[u8; 12] {
		&self.0
	}
}
