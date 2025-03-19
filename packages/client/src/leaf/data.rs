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

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		Ok(Self {
			bytes: bytes.into().into_owned(),
		})
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		[].into()
	}
}
