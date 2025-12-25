use {
	self::{deserializer::Deserializer, serializer::Serializer},
	crate::{Deserialize, Serialize},
	std::ops::Deref,
	tangram_client::prelude::*,
};

mod deserializer;
mod serializer;

#[derive(Clone, Debug)]
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
	fn serialize<'s>(
		&self,
		scope: &mut v8::PinScope<'s, '_>,
	) -> tg::Result<v8::Local<'s, v8::Value>> {
		let serializer = Serializer::new(scope);
		let value = self
			.0
			.serialize(serializer)
			.map_err(|source| tg::error!(!source, "failed to serialize the value to v8"))?;
		Ok(value)
	}
}

impl<'s, T> Deserialize<'s> for Serde<T>
where
	T: serde::de::DeserializeOwned + 's,
{
	fn deserialize(
		scope: &mut v8::PinScope<'s, '_>,
		value: v8::Local<'s, v8::Value>,
	) -> tg::Result<Self> {
		let deserializer = Deserializer::new(scope, value);
		let value = T::deserialize(deserializer)
			.map_err(|source| tg::error!(!source, "failed to deserialize the value from v8"))?;
		let value = Self(value);
		Ok(value)
	}
}
