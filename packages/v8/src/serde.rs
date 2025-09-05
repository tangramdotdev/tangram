use self::{deserializer::Deserializer, serializer::Serializer};
use crate::{Deserialize, Serialize};
use std::ops::Deref;
use tangram_client as tg;

mod deserializer;
mod serializer;

pub struct Serde<T>(pub T);

impl<T> Deref for Serde<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<T> Serialize for Serde<T>
where
	T: serde::Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let serializer = Serializer::new(scope);
		let value = self
			.0
			.serialize(serializer)
			.map_err(|source| tg::error!(!source, "failed to serialize the value to v8"))?;
		Ok(value)
	}
}

impl<T> Deserialize for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let deserializer = Deserializer::new(scope, value);
		let value =
			Self(T::deserialize(deserializer).map_err(|source| {
				tg::error!(!source, "failed to deserialize the value from v8")
			})?);
		Ok(value)
	}
}
