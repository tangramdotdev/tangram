use crate::{self as tg, util::serde::BytesBase64};
use byteorder::WriteBytesExt as _;
use bytes::Bytes;
use serde_with::serde_as;
use std::io::Write as _;
use tangram_itertools::IteratorExt as _;

#[derive(Clone, Debug, derive_more::IsVariant, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Blob {
	Leaf(Leaf),
	Branch(Branch),
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Leaf {
	#[serde_as(as = "BytesBase64")]
	pub bytes: Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Branch {
	pub children: Vec<Child>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Child {
	pub blob: tg::blob::Id,
	pub length: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Leaf,
	Branch,
}

impl Blob {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		match self {
			Self::Leaf(leaf) => {
				bytes.write_u8(0).unwrap();
				bytes.write_all(&leaf.bytes).unwrap();
			},
			Self::Branch(branch) => {
				bytes.write_u8(1).unwrap();
				serde_json::to_writer(&mut bytes, branch)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
			},
		}
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let kind = bytes
			.as_ref()
			.first()
			.ok_or_else(|| tg::error!("missing kind"))?;
		let kind = match kind {
			0 => Kind::Leaf,
			1 => Kind::Branch,
			_ => return Err(tg::error!("invalid kind")),
		};
		let blob = match kind {
			Kind::Leaf => Self::Leaf(Leaf {
				bytes: bytes.into_owned().slice(1..),
			}),
			Kind::Branch => {
				let branch = serde_json::from_reader(&bytes.as_ref()[1..])
					.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
				Self::Branch(branch)
			},
		};
		Ok(blob)
	}

	pub fn children(&self) -> impl Iterator<Item = tg::object::Id> {
		match self {
			Self::Branch(branch) => branch
				.children
				.iter()
				.map(|child| child.blob.clone().into())
				.left_iterator(),
			Self::Leaf(_) => std::iter::empty().right_iterator(),
		}
	}
}
