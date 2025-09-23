use {
	self::serializer::Serializer,
	crate::{
		Value,
		value::{Deserialize, Serialize},
	},
	std::ops::Deref,
};

pub mod deserializer;
pub mod serializer;

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
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = self.0.serialize(Serializer)?;
		Ok(value)
	}
}

impl<T> Deserialize for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = T::deserialize(value)?;
		Ok(Self(value))
	}
}
