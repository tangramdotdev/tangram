use {
	crate::prelude::*, bytes::Bytes, serde_with::serde_as, std::collections::BTreeSet,
	tangram_util::serde::BytesBase64,
};

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

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Branch {
	#[tangram_serialize(id = 0)]
	pub children: Vec<Child>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Child {
	#[tangram_serialize(id = 0)]
	pub blob: tg::blob::Id,

	#[tangram_serialize(id = 1)]
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
				bytes.push(0);
				bytes.extend_from_slice(&leaf.bytes);
			},
			Self::Branch(branch) => {
				bytes.push(1);
				bytes.push(0);
				tangram_serialize::to_writer(&mut bytes, branch)
					.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
			},
		}
		Ok(bytes.into())
	}

	pub fn serialize_json(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		match self {
			Self::Leaf(leaf) => {
				bytes.push(0);
				bytes.extend_from_slice(&leaf.bytes);
			},
			Self::Branch(branch) => {
				bytes.push(1);
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
			_ => {
				return Err(tg::error!("invalid kind"));
			},
		};
		let bytes = bytes.slice(1..);
		let blob = match kind {
			Kind::Leaf => Self::Leaf(Leaf {
				bytes: bytes.into_owned(),
			}),
			Kind::Branch => {
				let bytes = bytes.as_ref();
				if bytes.is_empty() {
					return Err(tg::error!("missing format byte"));
				}
				let format = bytes[0];
				let branch = match format {
					0 => tangram_serialize::from_slice(&bytes[1..])
						.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
					b'{' => serde_json::from_slice(bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize the data")),
					_ => Err(tg::error!("invalid format")),
				}?;
				Self::Branch(branch)
			},
		};
		Ok(blob)
	}

	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		if let Self::Branch(branch) = self {
			children.extend(
				branch
					.children
					.iter()
					.map(|child| child.blob.clone().into()),
			);
		}
	}
}
