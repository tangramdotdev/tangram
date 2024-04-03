use super::Value;
use tangram_error::{error, Error};

impl Value {
	fn deserialize_json<'de, V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| error!("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)
			.map_err(|source| error!(!source, "failed to deserialize json"))?;
		serde::Deserializer::deserialize_any(value, visitor)
			.map_err(|source| error!(!source, "failed to deserialize the value"))
	}
}

impl<'de> serde::de::IntoDeserializer<'de, Error> for Value {
	type Deserializer = Self;

	fn into_deserializer(self) -> Self::Deserializer {
		self
	}
}

impl<'de> serde::Deserializer<'de> for Value {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		match self {
			Self::Null => visitor.visit_unit(),
			Self::Integer(value) => visitor.visit_i64(value),
			Self::Real(value) => visitor.visit_f64(value),
			Self::Text(value) => visitor.visit_string(value),
			Self::Blob(value) => visitor.visit_byte_buf(value),
		}
	}

	fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let value = self
			.try_unwrap_integer()
			.map_err(|source| error!(!source, "expected an integer value"))?;
		let value = value > 0;
		visitor.visit_bool(value)
	}

	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		match self {
			Self::Null => visitor.visit_none(),
			_ => visitor.visit_some(self),
		}
	}

	fn deserialize_newtype_struct<V>(
		self,
		_name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_tuple_struct<V>(
		self,
		_name: &'static str,
		_len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_struct<V>(
		self,
		_name: &'static str,
		_fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	fn deserialize_enum<V>(
		self,
		_name: &'static str,
		_variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_json(visitor)
	}

	serde::forward_to_deserialize_any!(i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit unit_struct identifier ignored_any);
}
