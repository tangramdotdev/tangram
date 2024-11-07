use crate as tg;
use std::sync::Arc;

#[derive(Clone, Debug, derive_more::From, derive_more::TryUnwrap)]
pub enum Blob {
	Leaf(Arc<tg::leaf::Object>),
	Branch(Arc<tg::branch::Object>),
}

impl Blob {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Leaf(leaf) => leaf.children(),
			Self::Branch(branch) => branch.children(),
		}
	}
}
