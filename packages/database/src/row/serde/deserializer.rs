use crate::Row;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl serde::de::IntoDeserializer<'_, Error> for Row {
	type Deserializer = Self;

	fn into_deserializer(self) -> Self::Deserializer {
		self
	}
}

impl<'de> serde::Deserializer<'de> for Row {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		visitor
			.visit_map(serde::de::value::MapDeserializer::new(
				self.entries.into_iter(),
			))
			.map_err(|error| match error {
				crate::value::serde::deserializer::Error::Other(error) => Error::Other(error),
			})
	}

	serde::forward_to_deserialize_any!(bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any);
}

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self::Other(msg.to_string().into())
	}
}
