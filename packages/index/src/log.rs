use tangram_client::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
	pub partition: u64,
	pub process: tg::process::Id,
	pub(crate) version: Version,
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
