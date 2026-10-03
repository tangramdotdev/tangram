use {std::borrow::Cow, tangram_client::prelude::*};

mod key;

pub(super) use key::Key;

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(super) struct Value<'a> {
	#[tangram_serialize(id = 0)]
	pub object: tangram_cache::object::Object<'a>,
}

impl Value<'_> {
	#[must_use]
	pub fn new(object: tangram_cache::object::Object<'_>) -> Value<'static> {
		let object = tangram_cache::object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.into_owned())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};

		Value { object }
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let mut bytes = vec![0];
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object value"))?;

		Ok(bytes)
	}
}

impl Value<'static> {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		Self::deserialize_with_bytes(bytes, true)
	}

	pub fn deserialize_with_bytes(bytes: &[u8], include_bytes: bool) -> tg::Result<Self> {
		let Some((&format, bytes)) = bytes.split_first() else {
			return Err(tg::error!("the object value data is empty"));
		};
		if format != 0 {
			return Err(tg::error!("the object value format is invalid"));
		}
		let mut value: Value<'_> = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the object value"))?;
		if !include_bytes {
			value.object.bytes = None;
		}
		let object = value.object.into_static();

		Ok(Self { object })
	}
}
