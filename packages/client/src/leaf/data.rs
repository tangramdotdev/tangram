use crate as tg;
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct Leaf {
	pub bytes: Bytes,
}

impl Leaf {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		Ok(self.bytes.clone())
	}

	pub fn deserialize(bytes: &Bytes) -> tg::Result<Self> {
		Ok(Self {
			bytes: bytes.clone(),
		})
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[].into()
	}
}
