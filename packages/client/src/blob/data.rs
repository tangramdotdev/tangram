use super::Kind;
use crate as tg;
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
#[try_unwrap(ref)]
pub enum Blob {
	Leaf(tg::leaf::Data),
	Branch(tg::branch::Data),
}

impl Blob {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Leaf(_) => Kind::Leaf,
			Self::Branch(_) => Kind::Branch,
		}
	}

	pub fn serialize(&self) -> tg::Result<Bytes> {
		match self {
			Self::Leaf(leaf) => leaf.serialize(),
			Self::Branch(branch) => branch.serialize(),
		}
	}

	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Leaf(leaf) => leaf.children(),
			Self::Branch(branch) => branch.children(),
		}
	}
}
