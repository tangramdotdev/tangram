use {super::Data, crate as tg};

#[derive(Clone, Debug)]
pub enum File {
	Reference(tg::graph::object::Reference),
	Node(Node),
}

pub type Node = tg::graph::object::File;

impl File {
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
				let reference = tg::graph::object::Reference::try_from_data(data)?;
				Ok(Self::Reference(reference))
			},
			Data::Node(data) => {
				let node = tg::graph::object::File::try_from_data(data)?;
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
