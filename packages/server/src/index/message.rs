use {
	byteorder::ReadBytesExt as _, bytes::Bytes, std::collections::BTreeSet,
	tangram_client::prelude::*, tangram_either::Either, tangram_util::serde::is_default,
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Message {
	#[tangram_serialize(id = 0)]
	PutCacheEntry(PutCacheEntry),

	#[tangram_serialize(id = 1)]
	PutObject(PutObject),

	#[tangram_serialize(id = 2)]
	TouchObject(TouchObject),

	#[tangram_serialize(id = 3)]
	PutProcess(PutProcess),

	#[tangram_serialize(id = 4)]
	TouchProcess(TouchProcess),

	#[tangram_serialize(id = 5)]
	PutTag(PutTagMessage),

	#[tangram_serialize(id = 6)]
	DeleteTag(DeleteTag),
}

impl Message {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
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
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			b'{' => serde_json::from_slice(&bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			_ => Err(tg::error!("invalid format")),
		}
	}
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutCacheEntry {
	#[tangram_serialize(id = 0)]
	pub id: tg::artifact::Id,
	#[tangram_serialize(id = 1)]
	pub touched_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutObject {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub cache_entry: Option<tg::artifact::Id>,

	#[tangram_serialize(id = 1)]
	pub children: BTreeSet<tg::object::Id>,

	#[tangram_serialize(id = 2)]
	pub id: tg::object::Id,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "is_default")]
	pub metadata: tg::object::Metadata,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 5, default, skip_serializing_if = "is_default")]
	pub stored: crate::object::stored::Output,

	#[tangram_serialize(id = 6)]
	pub touched_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct TouchObject {
	#[tangram_serialize(id = 0)]
	pub id: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub touched_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutProcess {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Vec::is_empty")]
	pub children: Vec<tg::process::Id>,

	#[tangram_serialize(id = 1)]
	pub id: tg::process::Id,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_default")]
	pub metadata: tg::process::Metadata,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Vec::is_empty")]
	pub objects: Vec<(tg::object::Id, ProcessObjectKind)>,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(id = 4, default, skip_serializing_if = "is_default")]
	pub stored: crate::process::stored::Output,

	#[tangram_serialize(id = 5)]
	pub touched_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct TouchProcess {
	#[tangram_serialize(id = 0)]
	pub id: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub touched_at: i64,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct PutTagMessage {
	#[tangram_serialize(id = 0)]
	pub tag: String,

	#[tangram_serialize(id = 1)]
	pub item: Either<tg::object::Id, tg::process::Id>,
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct DeleteTag {
	#[tangram_serialize(id = 0)]
	pub tag: String,
}

#[derive(
	Clone,
	Copy,
	Debug,
	num_derive::FromPrimitive,
	num_derive::ToPrimitive,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub enum ProcessObjectKind {
	#[tangram_serialize(id = 0)]
	Command = 0,
	#[tangram_serialize(id = 1)]
	Error = 1,
	#[tangram_serialize(id = 2)]
	Log = 2,
	#[tangram_serialize(id = 3)]
	Output = 3,
}

impl std::fmt::Display for ProcessObjectKind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Command => write!(formatter, "command"),
			Self::Error => write!(formatter, "error"),
			Self::Log => write!(formatter, "log"),
			Self::Output => write!(formatter, "output"),
		}
	}
}

impl std::str::FromStr for ProcessObjectKind {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"command" => Ok(Self::Command),
			"error" => Ok(Self::Error),
			"log" => Ok(Self::Log),
			"output" => Ok(Self::Output),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
