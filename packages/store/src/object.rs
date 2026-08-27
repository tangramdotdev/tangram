use {bytes::Bytes, std::borrow::Cow, tangram_client::prelude::*};

pub mod archive;
pub mod checkout;
pub mod delete;
pub mod get;
pub mod index;
pub mod put;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Object<'a> {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub bytes: Option<Cow<'a, [u8]>>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub checkout_pointer: Option<checkout::Pointer>,

	/// The length of the blob, if the object is a blob. It lets the length be read without deserializing the bytes.
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[tangram_serialize(id = 2)]
	pub stored_at: i64,
}

impl Object<'_> {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object value"))?;
		Ok(bytes.into())
	}
}

impl Object<'static> {
	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("empty object value data"));
		}
		let format = bytes[0];
		match format {
			0 => {
				let object: Object<'_> =
					tangram_serialize::from_slice(&bytes[1..]).map_err(|error| {
						tg::error!(!error, "failed to deserialize the object value")
					})?;
				Ok(object.into_static())
			},
			_ => Err(tg::error!("invalid object value format")),
		}
	}
}

impl Object<'_> {
	#[must_use]
	pub fn into_static(self) -> Object<'static> {
		Object {
			bytes: self.bytes.map(|bytes| Cow::Owned(bytes.into_owned())),
			checkout_pointer: self.checkout_pointer,
			length: self.length,
			stored_at: self.stored_at,
		}
	}
}
