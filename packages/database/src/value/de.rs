use super::Value;
use serde::de::Error as _;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Json(serde_json::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl serde::de::IntoDeserializer<'_, Error> for Value {
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
			.map_err(|_| Error::custom("expected an integer value"))?;
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
		name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_newtype_struct(name, visitor)?;
		Ok(value)
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_seq(visitor)?;
		Ok(value)
	}

	fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_tuple(len, visitor)?;
		Ok(value)
	}

	fn deserialize_tuple_struct<V>(
		self,
		name: &'static str,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_tuple_struct(name, len, visitor)?;
		Ok(value)
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_map(visitor)?;
		Ok(value)
	}

	fn deserialize_struct<V>(
		self,
		name: &'static str,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_struct(name, fields, visitor)?;
		Ok(value)
	}

	fn deserialize_enum<V>(
		self,
		name: &'static str,
		variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let json = self
			.try_unwrap_text()
			.map_err(|_| Error::custom("expected a text value"))?;
		let value = serde_json::from_str::<serde_json::Value>(&json)?;
		let value = value.deserialize_enum(name, variants, visitor)?;
		Ok(value)
	}

	serde::forward_to_deserialize_any!(i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf unit unit_struct identifier ignored_any);
}

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self::Other(msg.to_string().into())
	}
}
