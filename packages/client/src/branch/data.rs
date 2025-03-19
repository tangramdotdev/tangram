use crate::{self as tg};
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Branch {
	pub children: Vec<Child>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Child {
	pub blob: tg::blob::Id,
	pub length: u64,
}

impl Branch {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		serde_json::from_reader(bytes.into().as_ref())
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		self.children
			.iter()
			.map(|child| child.blob.clone().into())
			.collect()
	}
}
