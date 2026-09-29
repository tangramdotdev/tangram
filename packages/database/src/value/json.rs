use crate::{
	Value,
	value::{Deserialize, DeserializeAs, Serialize},
};

#[derive(Debug, Default)]
pub struct Json<T>(pub T);

impl<T> serde::Serialize for Json<T>
where
	T: serde::Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let json = serde_json::to_string(&self.0).map_err(serde::ser::Error::custom)?;
		serializer.serialize_str(&json)
	}
}

impl<'de, T> serde::Deserialize<'de> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let json = <String as serde::Deserialize>::deserialize(deserializer)?;
		let value = serde_json::from_str(&json).map_err(serde::de::Error::custom)?;
		Ok(Self(value))
	}
}

impl<T> Serialize for Json<T>
where
	T: serde::Serialize,
{
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = serde_json::to_string(&self.0)?;
		Ok(Value::Text(json))
	}
}

impl<T> Deserialize for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = value.try_unwrap_text()?;
		let value = serde_json::from_str(&json)?;
		Ok(Self(value))
	}
}

impl<T> DeserializeAs<T> for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize_as(
		value: Value,
	) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let json = value.try_unwrap_text()?;
		let value = serde_json::from_str(&json)?;
		Ok(value)
	}
}
