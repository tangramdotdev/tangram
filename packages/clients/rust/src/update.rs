/// An explicit change to an optional value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Update<T> {
	Clear,
	Set(T),
}

impl<T> Update<T> {
	#[must_use]
	pub fn as_ref(&self) -> Update<&T> {
		match self {
			Self::Clear => Update::Clear,
			Self::Set(value) => Update::Set(value),
		}
	}

	#[must_use]
	pub fn into_option(self) -> Option<T> {
		match self {
			Self::Clear => None,
			Self::Set(value) => Some(value),
		}
	}
}

impl<T> From<Option<T>> for Update<T> {
	fn from(value: Option<T>) -> Self {
		value.map_or(Self::Clear, Self::Set)
	}
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Update<T> {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		<Option<T> as serde::Deserialize>::deserialize(deserializer).map(Self::from)
	}
}

impl<T: serde::Serialize> serde::Serialize for Update<T> {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serde::Serialize::serialize(&self.as_ref().into_option(), serializer)
	}
}

impl<'de, T: tangram_serialize::Deserialize<'de>> tangram_serialize::Deserialize<'de>
	for Update<T>
{
	fn deserialize(
		deserializer: &mut tangram_serialize::Deserializer<'de>,
	) -> std::io::Result<Self> {
		<Option<T> as tangram_serialize::Deserialize>::deserialize(deserializer).map(Self::from)
	}
}

impl<T: tangram_serialize::Serialize> tangram_serialize::Serialize for Update<T> {
	fn serialize(&self, serializer: &mut tangram_serialize::Serializer<'_>) -> std::io::Result<()> {
		match self {
			Self::Clear => serializer.serialize_null(),
			Self::Set(value) => serializer.serialize(value),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::Update;

	#[test]
	fn field_encoding() {
		#[derive(
			Debug,
			PartialEq,
			serde::Deserialize,
			serde::Serialize,
			tangram_serialize::Deserialize,
			tangram_serialize::Serialize,
		)]
		struct Field {
			#[serde(
				default,
				skip_serializing_if = "Option::is_none",
				with = "serde_with::rust::unwrap_or_skip"
			)]
			#[tangram_serialize(
				default,
				id = 0,
				skip_serializing_if = "Option::is_none",
				with = "tangram_serialize::with::unwrap_or_skip"
			)]
			value: Option<Update<u64>>,
		}

		#[derive(tangram_serialize::Serialize)]
		struct Present {
			#[tangram_serialize(id = 0)]
			value: Option<u64>,
		}

		for (value, json) in [
			(None, "{}"),
			(Some(Update::Clear), r#"{"value":null}"#),
			(Some(Update::Set(42)), r#"{"value":42}"#),
		] {
			let field = Field { value };
			assert_eq!(serde_json::to_string(&field).unwrap(), json);
			assert_eq!(serde_json::from_str::<Field>(json).unwrap(), field);
			let bytes = tangram_serialize::to_vec(&field).unwrap();
			assert_eq!(
				tangram_serialize::from_slice::<Field>(&bytes).unwrap(),
				field
			);
			if let Some(update) = value {
				let present = Present {
					value: update.into_option(),
				};
				assert_eq!(tangram_serialize::to_vec(&present).unwrap(), bytes);
			}
		}
	}
}
