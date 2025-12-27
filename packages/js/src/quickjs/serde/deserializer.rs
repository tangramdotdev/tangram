use {
	num::ToPrimitive as _,
	rquickjs as qjs,
	serde::{Deserialize as _, Deserializer as _, de::Error as _},
};

pub struct Deserializer<'js> {
	ctx: qjs::Ctx<'js>,
	value: qjs::Value<'js>,
}

struct SeqAccess<'js> {
	ctx: qjs::Ctx<'js>,
	array: qjs::Array<'js>,
	index: usize,
	length: usize,
}

struct MapAccess<'js> {
	ctx: qjs::Ctx<'js>,
	keys: Vec<qjs::Value<'js>>,
	index: usize,
	object: qjs::Object<'js>,
}

struct EnumAccess<'js> {
	ctx: qjs::Ctx<'js>,
	tag: qjs::Value<'js>,
	content: qjs::Value<'js>,
}

struct VariantAccess<'js> {
	ctx: qjs::Ctx<'js>,
	value: qjs::Value<'js>,
}

#[derive(Debug)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.0.source()
	}
}

impl<'js> Deserializer<'js> {
	pub fn new(ctx: qjs::Ctx<'js>, value: qjs::Value<'js>) -> Self {
		Self { ctx, value }
	}
}

impl serde::de::IntoDeserializer<'_, Error> for Deserializer<'_> {
	type Deserializer = Self;

	fn into_deserializer(self) -> Self::Deserializer {
		self
	}
}

impl<'de> serde::Deserializer<'de> for Deserializer<'_> {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if self.value.is_bool() {
			self.deserialize_bool(visitor)
		} else if self.value.is_int() {
			self.deserialize_i32(visitor)
		} else if self.value.is_float() {
			self.deserialize_f64(visitor)
		} else if self.value.is_string() {
			self.deserialize_string(visitor)
		} else if self.value.type_of() == qjs::Type::Undefined || self.value.is_null() {
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
		let value = self
			.value
			.as_bool()
			.ok_or_else(|| Error::custom("expected a boolean"))?;
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
		let value = if let Some(v) = self.value.as_int() {
			f64::from(v)
		} else if let Some(v) = self.value.as_float() {
			v
		} else {
			return Err(Error::custom("expected a number"));
		};
		let Some(value) = value.to_i64() else {
			return Err(Error::custom("invalid value"));
		};
		visitor.visit_i64(value)
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
		let value = if let Some(v) = self.value.as_int() {
			f64::from(v)
		} else if let Some(v) = self.value.as_float() {
			v
		} else {
			return Err(Error::custom("expected a number"));
		};
		let Some(value) = value.to_u64() else {
			return Err(Error::custom("invalid value"));
		};
		visitor.visit_u64(value)
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
		let value = if let Some(v) = self.value.as_int() {
			f64::from(v)
		} else if let Some(v) = self.value.as_float() {
			v
		} else {
			return Err(Error::custom("expected a number"));
		};
		visitor.visit_f64(value)
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
		let string = self
			.value
			.as_string()
			.ok_or_else(|| Error::custom("expected a string"))?;
		let value = string
			.to_string()
			.map_err(|error| Error::custom(error.to_string()))?;
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
		let typed_array: qjs::TypedArray<u8> = qjs::TypedArray::from_value(self.value)
			.map_err(|_| Error::custom("expected a Uint8Array"))?;
		let bytes = typed_array
			.as_bytes()
			.ok_or_else(|| Error::custom("failed to get bytes from Uint8Array"))?;
		visitor.visit_byte_buf(bytes.to_vec())
	}

	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if self.value.is_null() || self.value.type_of() == qjs::Type::Undefined {
			visitor.visit_none()
		} else {
			visitor.visit_some(self)
		}
	}

	fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::Visitor<'de>,
	{
		if !self.value.is_null() && self.value.type_of() != qjs::Type::Undefined {
			return Err(Error::custom("expected undefined or null"));
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
		let array = self
			.value
			.into_array()
			.ok_or_else(|| Error::custom("expected an array"))?;
		let length = array.len();
		visitor.visit_seq(SeqAccess {
			ctx: self.ctx,
			array,
			index: 0,
			length,
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
		let object = self
			.value
			.into_object()
			.ok_or_else(|| Error::custom("expected an object"))?;
		let keys: Vec<qjs::Value> = object
			.keys()
			.collect::<Result<Vec<_>, _>>()
			.map_err(|error| Error::custom(error.to_string()))?;
		visitor.visit_map(MapAccess {
			ctx: self.ctx,
			keys,
			index: 0,
			object,
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
				ctx: self.ctx.clone(),
				tag: self.value,
				content: qjs::Value::new_undefined(self.ctx),
			})
		} else if self.value.is_object() {
			let object = self.value.into_object().unwrap();
			let keys: Vec<qjs::Value> = object
				.keys()
				.collect::<Result<Vec<_>, _>>()
				.map_err(|error| Error::custom(error.to_string()))?;
			let tag = keys
				.first()
				.ok_or_else(|| Error::custom("expected at least one key"))?
				.clone();
			let content = object
				.get::<_, qjs::Value>(tag.clone())
				.map_err(|error| Error::custom(error.to_string()))?;
			visitor.visit_enum(EnumAccess {
				ctx: self.ctx,
				tag,
				content,
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

impl<'de> serde::de::SeqAccess<'de> for SeqAccess<'_> {
	type Error = Error;

	fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
	where
		T: serde::de::DeserializeSeed<'de>,
	{
		if self.index >= self.length {
			return Ok(None);
		}
		let value: qjs::Value = self
			.array
			.get(self.index)
			.map_err(|error| Error::custom(error.to_string()))?;
		self.index += 1;
		let deserializer = Deserializer::new(self.ctx.clone(), value);
		let value = seed.deserialize(deserializer)?;
		Ok(Some(value))
	}
}

impl<'de> serde::de::MapAccess<'de> for MapAccess<'_> {
	type Error = Error;

	fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
	where
		K: serde::de::DeserializeSeed<'de>,
	{
		if self.index >= self.keys.len() {
			return Ok(None);
		}
		let key = self.keys[self.index].clone();
		let deserializer = Deserializer::new(self.ctx.clone(), key);
		let key = seed.deserialize(deserializer)?;
		Ok(Some(key))
	}

	fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
	where
		V: serde::de::DeserializeSeed<'de>,
	{
		let key = self.keys[self.index].clone();
		let value: qjs::Value = self
			.object
			.get(key)
			.map_err(|error| Error::custom(error.to_string()))?;
		self.index += 1;
		let deserializer = Deserializer::new(self.ctx.clone(), value);
		let value = seed.deserialize(deserializer)?;
		Ok(value)
	}
}

impl<'de, 'js> serde::de::EnumAccess<'de> for EnumAccess<'js> {
	type Error = Error;
	type Variant = VariantAccess<'js>;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
	where
		V: serde::de::DeserializeSeed<'de>,
	{
		let deserializer = Deserializer::new(self.ctx.clone(), self.tag);
		let tag = seed.deserialize(deserializer)?;
		let content = VariantAccess {
			ctx: self.ctx,
			value: self.content,
		};
		Ok((tag, content))
	}
}

impl<'de> serde::de::VariantAccess<'de> for VariantAccess<'_> {
	type Error = Error;

	fn unit_variant(self) -> Result<(), Self::Error> {
		let deserializer = Deserializer::new(self.ctx, self.value);
		<_>::deserialize(deserializer)
	}

	fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(
		self,
		seed: T,
	) -> Result<T::Value, Self::Error> {
		let deserializer = Deserializer::new(self.ctx, self.value);
		seed.deserialize(deserializer)
	}

	fn tuple_variant<V: serde::de::Visitor<'de>>(
		self,
		len: usize,
		visitor: V,
	) -> Result<V::Value, Self::Error> {
		let deserializer = Deserializer::new(self.ctx, self.value);
		deserializer.deserialize_tuple(len, visitor)
	}

	fn struct_variant<V: serde::de::Visitor<'de>>(
		self,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error> {
		let deserializer = Deserializer::new(self.ctx, self.value);
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
