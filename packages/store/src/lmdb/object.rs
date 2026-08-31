mod key;

pub(super) use key::Key;
use {std::borrow::Cow, tangram_client::prelude::*};

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(super) struct Value<'a> {
	#[tangram_serialize(id = 0)]
	pub object: crate::object::Object<'a>,
}

impl Value<'_> {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let mut bytes = vec![0];
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object value"))?;

		Ok(bytes)
	}
}

impl Value<'static> {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		let Some((&format, bytes)) = bytes.split_first() else {
			return Err(tg::error!("empty object value data"));
		};
		if format != 0 {
			return Err(tg::error!("invalid object value format"));
		}
		let value: Value<'_> = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the object value"))?;
		let object = value.object.into_static();

		Ok(Self { object })
	}
}

impl Value<'_> {
	pub fn new(object: crate::object::Object<'_>) -> Value<'static> {
		let object = crate::object::Object {
			bytes: object.bytes.map(|bytes| Cow::Owned(bytes.into_owned())),
			checkout_pointer: object.checkout_pointer,
			length: object.length,
			put: object.put,
		};

		Value { object }
	}
}
