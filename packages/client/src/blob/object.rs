use super::Data;
use crate as tg;
use bytes::Bytes;

#[derive(Clone, Debug, derive_more::IsVariant)]
pub enum Blob {
	Leaf(Leaf),
	Branch(Branch),
}

#[derive(Clone, Debug, Default)]
pub struct Leaf {
	pub bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct Branch {
	pub children: Vec<Child>,
}

#[derive(Clone, Debug)]
pub struct Child {
	pub blob: tg::Blob,
	pub length: u64,
}

impl Blob {
	pub fn to_data(&self) -> Data {
		match self {
			Blob::Leaf(object) => tg::blob::Data::Leaf(tg::blob::data::Leaf {
				bytes: object.bytes.clone(),
			}),
			Blob::Branch(object) => {
				let children = object
					.children
					.iter()
					.map(|child| tg::blob::data::Child {
						blob: child.blob.id(),
						length: child.length,
					})
					.collect();
				tg::blob::Data::Branch(tg::blob::data::Branch { children })
			},
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		match data {
			Data::Leaf(data) => {
				let leaf = Leaf { bytes: data.bytes };
				Ok(Self::Leaf(leaf))
			},
			Data::Branch(data) => {
				let children = data
					.children
					.into_iter()
					.map(|child| Child {
						blob: tg::Blob::with_id(child.blob),
						length: child.length,
					})
					.collect();
				let branch = Branch { children };
				Ok(Self::Branch(branch))
			},
		}
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		match self {
			Self::Leaf(_) => vec![],
			Self::Branch(branch) => branch
				.children
				.iter()
				.map(|child| child.blob.clone().into())
				.collect(),
		}
	}
}
