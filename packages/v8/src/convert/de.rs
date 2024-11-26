use num::ToPrimitive as _;
use serde::{de::Error as _, Deserialize as _, Deserializer as _};

pub struct Deserializer<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	value: v8::Local<'s, v8::Value>,
}

struct SeqAccess<'a, 's> {
	array: v8::Local<'s, v8::Array>,
	index: u32,
	length: u32,
	scope: &'a mut v8::HandleScope<'s>,
}

struct MapAccess<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	index: u32,
	keys: v8::Local<'s, v8::Array>,
	object: v8::Local<'s, v8::Object>,
}

struct EnumAccess<'a, 's> {
	content: v8::Local<'s, v8::Value>,
	scope: &'a mut v8::HandleScope<'s>,
	tag: v8::Local<'s, v8::Value>,
}

struct VariantAccess<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	value: v8::Local<'s, v8::Value>,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);

impl<'a, 's> Deserializer<'a, 's> {
	pub fn new(scope: &'a mut v8::HandleScope<'s>, value: v8::Local<'s, v8::Value>) -> Self {
		Self { scope, value }
	}
}

impl<'de, 'a, 's> serde::de::IntoDeserializer<'de, Error> for Deserializer<'a, 's> {
	type Deserializer = Self;

	fn into_deserializer(self) -> Self::Deserializer {
		self
	}
}

impl<'de, 'a, 's> serde::Deserializer<'de> for Deserializer<'a, 's> {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if self.value.is_boolean() {
			self.deserialize_bool(visitor)
		} else if self.value.is_uint32() {
			self.deserialize_u32(visitor)
		} else if self.value.is_int32() {
			self.deserialize_i32(visitor)
		} else if self.value.is_number() {
			self.deserialize_f64(visitor)
		} else if self.value.is_string() {
			self.deserialize_string(visitor)
		} else if self.value.is_uint8_array() {
			self.deserialize_byte_buf(visitor)
		} else if self.value.is_null_or_undefined() {
			self.deserialize_unit(visitor)
		} else if self.value.is_array() {
			self.deserialize_seq(visitor)
		} else if self.value.is_object() {
			self.deserialize_map(visitor)
		} else {
			Err(Error::custom("invalid value"))
		}
	}

	fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if !self.value.is_boolean() {
			return Err(Error::custom("expected a boolean"));
		}
		let value = self.value.boolean_value(self.scope);
		visitor.visit_bool(value)
	}

	fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_i64(visitor)
	}

	fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_i64(visitor)
	}

	fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_i64(visitor)
	}

	fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if let Ok(value) = v8::Local::<v8::Number>::try_from(self.value) {
			let value = value.number_value(self.scope).unwrap();
			let Some(value) = value.to_i64() else {
				return Err(Error::custom("invalid value"));
			};
			visitor.visit_i64(value)
		} else {
			Err(Error::custom("invalid value"))
		}
	}

	fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_u64(visitor)
	}

	fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_u64(visitor)
	}

	fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_u64(visitor)
	}

	fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if let Ok(value) = v8::Local::<v8::Number>::try_from(self.value) {
			let value = value.number_value(self.scope).unwrap();
			let Some(value) = value.to_u64() else {
				return Err(Error::custom("invalid value"));
			};
			visitor.visit_u64(value)
		} else {
			Err(Error::custom("invalid value"))
		}
	}

	fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_f64(visitor)
	}

	fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if let Ok(value) = v8::Local::<v8::Number>::try_from(self.value) {
			let value = value.number_value(self.scope).unwrap();
			visitor.visit_f64(value)
		} else {
			Err(Error::custom("invalid value"))
		}
	}

	fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_str(visitor)
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_string(visitor)
	}

	fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let value = self.value.to_rust_string_lossy(self.scope);
		visitor.visit_string(value)
	}

	fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_byte_buf(visitor)
	}

	fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let uint8_array = v8::Local::<v8::Uint8Array>::try_from(self.value).unwrap();
		let bytes = if let Some(data) = uint8_array
			.get_backing_store()
			.and_then(|backing_store| backing_store.data())
		{
			let offset = uint8_array.byte_offset();
			let length = uint8_array.byte_length();
			let slice = unsafe {
				std::slice::from_raw_parts(data.cast::<u8>().as_ptr().add(offset), length)
			};
			slice.to_owned()
		} else {
			let length = uint8_array.byte_length();
			if length > 0 {
				return Err(Error::custom("invalid uint8array"));
			}
			Vec::new()
		};
		visitor.visit_byte_buf(bytes)
	}

	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if self.value.is_null_or_undefined() {
			visitor.visit_none()
		} else {
			visitor.visit_some(self)
		}
	}

	fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if !self.value.is_null_or_undefined() {
			return Err(Error::custom("expected undefined"));
		}
		visitor.visit_unit()
	}

	fn deserialize_unit_struct<V>(
		self,
		_name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_unit(visitor)
	}

	fn deserialize_newtype_struct<V>(
		self,
		_name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		visitor.visit_newtype_struct(self)
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let array = v8::Local::<v8::Array>::try_from(self.value).unwrap();
		let length = array.length();
		visitor.visit_seq(SeqAccess {
			array,
			index: 0,
			length,
			scope: self.scope,
		})
	}

	fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_seq(visitor)
	}

	fn deserialize_tuple_struct<V>(
		self,
		_name: &'static str,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_tuple(len, visitor)
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		let object = v8::Local::<v8::Object>::try_from(self.value).unwrap();
		let keys = object
			.get_property_names(self.scope, v8::GetPropertyNamesArgs::default())
			.unwrap();
		visitor.visit_map(MapAccess {
			index: 0,
			keys,
			object,
			scope: self.scope,
		})
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
		self.deserialize_map(visitor)
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
		if self.value.is_string() {
			visitor.visit_enum(EnumAccess {
				content: v8::undefined(self.scope).into(),
				scope: self.scope,
				tag: self.value,
			})
		} else if self.value.is_object() {
			let object = v8::Local::<v8::Object>::try_from(self.value).unwrap();
			let keys = object
				.get_property_names(self.scope, v8::GetPropertyNamesArgs::default())
				.unwrap();
			let tag = keys.get_index(self.scope, 0).unwrap();
			let content = object.get(self.scope, tag).unwrap();
			visitor.visit_enum(EnumAccess {
				content,
				scope: self.scope,
				tag,
			})
		} else {
			Err(Error::custom("invalid value"))
		}
	}

	fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		self.deserialize_str(visitor)
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		visitor.visit_unit()
	}
}

impl<'de, 'a, 's> serde::de::SeqAccess<'de> for SeqAccess<'a, 's> {
	type Error = Error;

	fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: serde::de::DeserializeSeed<'de>,
	{
		if self.index >= self.length {
			return Ok(None);
		}
		let value = self.array.get_index(self.scope, self.index).unwrap();
		self.index += 1;
		let deserializer = Deserializer {
			scope: self.scope,
			value,
		};
		let value = seed.deserialize(deserializer)?;
		Ok(Some(value))
	}
}

impl<'de, 'a, 's> serde::de::MapAccess<'de> for MapAccess<'a, 's> {
	type Error = Error;

	fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
	where
		K: serde::de::DeserializeSeed<'de>,
	{
		if self.index >= self.keys.length() {
			return Ok(None);
		}
		let key = self.keys.get_index(self.scope, self.index).unwrap();
		let deserializer = Deserializer {
			scope: self.scope,
			value: key,
		};
		let key = seed.deserialize(deserializer)?;
		Ok(Some(key))
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::DeserializeSeed<'de>,
	{
		let key = self.keys.get_index(self.scope, self.index).unwrap();
		let value = self.object.get(self.scope, key).unwrap();
		self.index += 1;
		let deserializer = Deserializer {
			scope: self.scope,
			value,
		};
		let value = seed.deserialize(deserializer)?;
		Ok(value)
	}
}

impl<'de, 'a, 's> serde::de::EnumAccess<'de> for EnumAccess<'a, 's> {
	type Error = Error;

	type Variant = VariantAccess<'a, 's>;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
	where
		V: serde::de::DeserializeSeed<'de>,
	{
		let deserializer = Deserializer::new(self.scope, self.tag);
		let tag = seed.deserialize(deserializer)?;
		let content = Self::Variant {
			scope: self.scope,
			value: self.content,
		};
		Ok((tag, content))
	}
}

impl<'de, 'a, 's> serde::de::VariantAccess<'de> for VariantAccess<'a, 's> {
	type Error = Error;

	fn unit_variant(self) -> Result<(), Self::Error> {
		let deserializer = Deserializer::new(self.scope, self.value);
		<_>::deserialize(deserializer)
	}

	fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(
		self,
		seed: T,
	) -> Result<T::Value, Self::Error> {
		let deserializer = Deserializer::new(self.scope, self.value);
		seed.deserialize(deserializer)
	}

	fn tuple_variant<V: serde::de::Visitor<'de>>(
		self,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error> {
		let deserializer = Deserializer::new(self.scope, self.value);
		deserializer.deserialize_tuple(len, visitor)
	}

	fn struct_variant<V: serde::de::Visitor<'de>>(
		self,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error> {
		let deserializer = Deserializer::new(self.scope, self.value);
		deserializer.deserialize_struct("", fields, visitor)
	}
}

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self(msg.to_string().into())
	}
}
