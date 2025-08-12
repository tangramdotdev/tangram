use super::{FromV8, ToV8, de::Deserializer, ser::Serializer};
use std::ops::Deref;
use tangram_client as tg;

pub struct Serde<T>(pub T);

impl<T> Deref for Serde<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<T> ToV8 for Serde<T>
where
	T: serde::Serialize,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let serializer = Serializer::new(scope);
		let value = self
			.0
			.serialize(serializer)
			.map_err(|source| tg::error!(!source, "failed to convert the value to v8"))?;
		Ok(value)
	}
}

impl<T> FromV8 for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let deserializer = Deserializer::new(scope, value);
		let value = Self(
			T::deserialize(deserializer)
				.map_err(|source| tg::error!(!source, "failed to convert the value from v8"))?,
		);
		Ok(value)
	}
}
