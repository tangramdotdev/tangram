use {super::Data, crate::prelude::*};

#[derive(Clone, Debug)]
pub enum Directory {
	Reference(tg::graph::Reference),
	Node(Node),
}

pub type Node = tg::graph::Directory;

impl Directory {
	#[must_use]
	pub fn with_reference(reference: tg::graph::Reference) -> Self {
		Self::Reference(reference)
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Reference(reference) => Data::Reference(reference.to_data()),
			Self::Node(node) => Data::Node(node.to_data()),
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		match data {
			Data::Reference(data) => {
				let reference = tg::graph::Reference::try_from_data(data)?;
				Ok(Self::Reference(reference))
			},
			Data::Node(data) => {
				let node = tg::graph::Directory::try_from_data(data)?;
				Ok(Self::Node(node))
			},
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Reference(reference) => reference.children(),
			Self::Node(node) => node.children(),
		}
	}
}
