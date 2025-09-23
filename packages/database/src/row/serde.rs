use {
	self::serializer::Serializer,
	crate::{
		Row,
		row::{Deserialize, Serialize},
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
	fn serialize(&self) -> Result<Row, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let row = self.0.serialize(Serializer)?;
		Ok(row)
	}
}

impl<T> Deserialize for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(row: Row) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = T::deserialize(row)?;
		Ok(Self(value))
	}
}
