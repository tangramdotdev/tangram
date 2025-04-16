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
		let json = String::deserialize(deserializer)?;
		let value = serde_json::from_str(&json).map_err(serde::de::Error::custom)?;
		Ok(Json(value))
	}
}
