use super::Data;
use crate as tg;
use bytes::Bytes;

#[derive(Clone, Debug, Default)]
pub struct Leaf {
	pub bytes: Bytes,
}

impl Leaf {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		vec![]
	}
}

impl TryFrom<Data> for Leaf {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(Self { bytes: data.bytes })
	}
}
