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
}

impl TryFrom<Data> for Blob {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		match data {
			Data::Leaf(data) => Ok(Self::Leaf(Leaf { bytes: data.bytes })),
			Data::Branch(data) => {
				let children = data
					.children
					.into_iter()
					.map(|child| Child {
						blob: tg::Blob::with_id(child.blob),
						length: child.length,
					})
					.collect();
				Ok(Self::Branch(Branch { children }))
			},
		}
	}
}
