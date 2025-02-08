use crate::Value;
use indexmap::IndexMap;
use tangram_either::Either;

#[derive(Clone, Debug)]
pub struct Row {
	entries: IndexMap<String, Value>,
}

impl Row {
	pub fn with_entries(entries: impl IntoIterator<Item = (String, Value)>) -> Self {
		let entries = entries.into_iter().collect();
		Self { entries }
	}

	pub fn entries(&self) -> impl Iterator<Item = (&String, &Value)> {
		self.entries.iter()
	}

	pub fn into_entries(self) -> impl Iterator<Item = (String, Value)> {
		self.entries.into_iter()
	}

	pub fn into_values(self) -> impl Iterator<Item = Value> {
		self.entries.into_values()
	}

	pub fn get<'a>(&self, index: impl Into<Either<usize, &'a str>>) -> Option<&Value> {
		match index.into() {
			Either::Left(index) => self.entries.get_index(index).map(|(_, value)| value),
			Either::Right(index) => self.entries.get(index),
		}
	}
}

impl<'de> serde::Deserializer<'de> for Row {
	type Error = crate::value::de::Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		visitor.visit_map(serde::de::value::MapDeserializer::new(
			self.entries.into_iter(),
		))
	}

	serde::forward_to_deserialize_any!(bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any);
}
