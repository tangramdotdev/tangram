use crate as tg;
use byteorder::ReadBytesExt as _;
use bytes::Bytes;
use std::collections::BTreeSet;

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(untagged)]
pub enum Directory {
	#[tangram_serialize(id = 0)]
	Reference(tg::graph::data::Reference),

	#[tangram_serialize(id = 1)]
	Node(Node),
}

pub type Node = tg::graph::data::Directory;

impl Directory {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		serde_json::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let mut reader = std::io::Cursor::new(bytes.as_ref());
		let format = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the format"))?;
		match format {
			0 => tangram_serialize::from_reader(&mut reader)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
			_ => Err(tg::error!("invalid format")),
		}
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Reference(reference) => reference.children(children),
			Self::Node(node) => node.children(children),
		}
	}
}
