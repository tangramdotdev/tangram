use serde::de::{
	self, DeserializeSeed, Deserializer, IntoDeserializer as _, MapAccess, SeqAccess, Visitor,
};

/// Deserialize a value from JSON, treating a `null` struct field as if it were
/// absent so that the field's default applies. A `null` in a value position (an
/// array element or a map entry) is left alone and deserializes to the empty
/// value. The JavaScript client writes an `undefined` field as `null` on the
/// wire so that a present-but-empty entry stays distinct from an absent one.
pub fn from_slice<T>(bytes: &[u8]) -> Result<T, serde_json::Error>
where
	T: serde::de::DeserializeOwned,
{
	let mut deserializer = serde_json::Deserializer::from_slice(bytes);
	let value = T::deserialize(NullFieldsAsMissing(&mut deserializer))?;
	deserializer.end()?;
	Ok(value)
}

pub struct NullFieldsAsMissing<D>(pub D);

struct StructVisitor<V>(V);

struct NullSkippingMap<M> {
	inner: M,
	pending: Option<serde_json::Value>,
}

macro_rules! forward {
	($($method:ident)*) => {
		$(
			fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
			where
				V: Visitor<'de>,
			{
				self.0.$method(visitor)
			}
		)*
	};
}

impl<'de, D> Deserializer<'de> for NullFieldsAsMissing<D>
where
	D: Deserializer<'de>,
{
	type Error = D::Error;

	fn deserialize_struct<V>(
		self,
		name: &'static str,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_struct(name, fields, StructVisitor(visitor))
	}

	forward! {
		deserialize_any
		deserialize_bool
		deserialize_i8 deserialize_i16 deserialize_i32 deserialize_i64 deserialize_i128
		deserialize_u8 deserialize_u16 deserialize_u32 deserialize_u64 deserialize_u128
		deserialize_f32 deserialize_f64 deserialize_char
		deserialize_str deserialize_string deserialize_bytes deserialize_byte_buf
		deserialize_option deserialize_unit deserialize_seq deserialize_map
		deserialize_identifier deserialize_ignored_any
	}

	fn deserialize_unit_struct<V>(
		self,
		name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_unit_struct(name, visitor)
	}

	fn deserialize_newtype_struct<V>(
		self,
		name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_newtype_struct(name, visitor)
	}

	fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_tuple(len, visitor)
	}

	fn deserialize_tuple_struct<V>(
		self,
		name: &'static str,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_tuple_struct(name, len, visitor)
	}

	fn deserialize_enum<V>(
		self,
		name: &'static str,
		variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		self.0.deserialize_enum(name, variants, visitor)
	}

	fn is_human_readable(&self) -> bool {
		self.0.is_human_readable()
	}
}

impl<'de, V> Visitor<'de> for StructVisitor<V>
where
	V: Visitor<'de>,
{
	type Value = V::Value;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		self.0.expecting(formatter)
	}

	fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
	where
		A: MapAccess<'de>,
	{
		self.0.visit_map(NullSkippingMap {
			inner: map,
			pending: None,
		})
	}

	fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
	where
		A: SeqAccess<'de>,
	{
		self.0.visit_seq(seq)
	}
}

impl<'de, M> MapAccess<'de> for NullSkippingMap<M>
where
	M: MapAccess<'de>,
{
	type Error = M::Error;

	fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
	where
		K: DeserializeSeed<'de>,
	{
		loop {
			let Some(key) = self.inner.next_key::<String>()? else {
				return Ok(None);
			};
			let value = self.inner.next_value::<serde_json::Value>()?;
			if value.is_null() {
				continue;
			}
			self.pending = Some(value);
			let field = seed.deserialize(key.into_deserializer())?;
			return Ok(Some(field));
		}
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: DeserializeSeed<'de>,
	{
		let value = self
			.pending
			.take()
			.expect("next_value_seed called before next_key_seed");
		seed.deserialize(NullFieldsAsMissing(value))
			.map_err(de::Error::custom)
	}

	fn size_hint(&self) -> Option<usize> {
		self.inner.size_hint()
	}
}
