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
