use {super::Data, crate::prelude::*};

#[derive(Clone, Debug)]
pub struct Dependency(pub tg::Referent<Option<tg::Object>>);

#[derive(Clone, Debug)]
pub enum File {
	Pointer(tg::graph::Pointer),
	Node(Node),
}

pub type Node = tg::graph::File;

impl File {
	#[must_use]
	pub fn with_pointer(pointer: tg::graph::Pointer) -> Self {
		Self::Pointer(pointer)
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Pointer(pointer) => Data::Pointer(pointer.to_data()),
			Self::Node(node) => Data::Node(node.to_data()),
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		match data {
			Data::Pointer(data) => {
				let pointer = tg::graph::Pointer::try_from_data(data)?;
				Ok(Self::Pointer(pointer))
			},
			Data::Node(data) => {
				let node = tg::graph::File::try_from_data(data)?;
				Ok(Self::Node(node))
			},
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Pointer(pointer) => pointer.children(),
			Self::Node(node) => node.children(),
		}
	}
}
