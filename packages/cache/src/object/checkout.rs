use {bytes::Bytes, std::path::PathBuf, tangram_client::prelude::*};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Pointer {
	#[tangram_serialize(id = 0)]
	pub artifact: tg::artifact::Id,

	#[tangram_serialize(id = 1)]
	pub length: u64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[tangram_serialize(id = 3)]
	pub position: u64,
}

impl Pointer {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the checkout pointer"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("empty checkout pointer data"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|error| tg::error!(!error, "failed to deserialize the checkout pointer")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the checkout pointer")),
			_ => Err(tg::error!("invalid checkout pointer format")),
		}
	}
}
