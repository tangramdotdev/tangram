use tangram_client::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
	pub process: tg::process::Id,
	pub(crate) position: Position,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Position {
	#[cfg(feature = "foundationdb")]
	Fdb { partition: u64, version: Version },

	#[cfg(feature = "lmdb")]
	Lmdb { version: Version },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Version([u8; 12]);

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
