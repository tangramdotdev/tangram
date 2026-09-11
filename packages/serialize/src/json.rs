use crate::{Deserialize, Deserializer, Serialize, Serializer};

/// JSON metadata embedded in a Tangram frame.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Json<T>(pub T);

impl<T> From<T> for Json<T> {
	fn from(value: T) -> Self {
		Self(value)
	}
}

impl<T: serde::Serialize> Serialize for Json<T> {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> std::io::Result<()> {
		crate::with::json::serialize(&self.0, serializer)
	}
}

impl<'de, T: serde::de::DeserializeOwned> Deserialize<'de> for Json<T> {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> std::io::Result<Self> {
		crate::with::json::deserialize(deserializer).map(Self)
	}
}
