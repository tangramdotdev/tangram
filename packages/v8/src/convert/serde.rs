use super::{FromV8, ToV8, de::Deserializer, ser::Serializer};
use tangram_client as tg;

pub struct Serde<T>(T);

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl<T> Serde<T> {
	pub fn new(value: T) -> Self {
		Self(value)
	}

	pub fn into_inner(self) -> T {
		self.0
	}
}

impl<T> ToV8 for Serde<T>
where
	T: serde::Serialize,
{
	fn to_v8<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tangram_client::Result<v8::Local<'a, v8::Value>> {
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
	) -> tangram_client::Result<Self> {
		let deserializer = Deserializer::new(scope, value);
		let value = Self(
			T::deserialize(deserializer)
				.map_err(|source| tg::error!(!source, "failed to convert the value from v8"))?,
		);
		Ok(value)
	}
}
