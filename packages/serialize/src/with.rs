/// Encode existing JSON metadata as a string inside a Tangram frame.
pub mod json {
	use crate::{Deserialize as _, Deserializer, Serialize as _, Serializer};

	pub fn deserialize<T: serde::de::DeserializeOwned>(
		deserializer: &mut Deserializer<'_>,
	) -> std::io::Result<T> {
		let json = String::deserialize(deserializer)?;
		serde_json::from_str(&json).map_err(std::io::Error::other)
	}

	pub fn serialize<T: serde::Serialize>(
		value: &T,
		serializer: &mut Serializer<'_>,
	) -> std::io::Result<()> {
		let json = serde_json::to_string(value).map_err(std::io::Error::other)?;
		json.serialize(serializer)
	}
}

pub mod unwrap_or_skip {
	use crate::{Deserialize, Deserializer, Serialize, Serializer};

	pub fn deserialize<'de, T>(deserializer: &mut Deserializer<'de>) -> std::io::Result<Option<T>>
	where
		T: Deserialize<'de>,
	{
		T::deserialize(deserializer).map(Some)
	}

	pub fn serialize<T>(value: &Option<T>, serializer: &mut Serializer<'_>) -> std::io::Result<()>
	where
		T: Serialize,
	{
		match value {
			Some(value) => value.serialize(serializer),
			None => ().serialize(serializer),
		}
	}
}
