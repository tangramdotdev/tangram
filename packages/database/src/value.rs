use derive_more::{From, TryInto, TryUnwrap};
use std::collections::BTreeMap;
use tangram_error::{error, Error};

#[derive(Clone, Debug, From, serde::Deserialize, serde::Serialize, TryInto, TryUnwrap)]
#[serde(untagged)]
#[try_unwrap(ref)]
pub enum Value {
	Null,
	Integer(i64),
	Real(f64),
	Text(String),
	Blob(Vec<u8>),
}

pub struct Serializer;

pub struct SerializeSeq(Vec<serde_json::Value>);

pub struct SerializeTuple(Vec<serde_json::Value>);

pub struct SerializeTupleStruct(Vec<serde_json::Value>);

pub struct SerializeTupleVariant(Vec<serde_json::Value>);

pub struct SerializeMap {
	entries: serde_json::Map<String, serde_json::Value>,
	key: Option<String>,
}

pub struct SerializeStruct {
	fields: BTreeMap<&'static str, serde_json::Value>,
}

pub struct SerializeStructVariant {
	fields: BTreeMap<&'static str, serde_json::Value>,
}

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
		match value {
			serde_json::Value::Null => visitor.visit_unit(),
			serde_json::Value::Bool(value) => visitor.visit_bool(value),
			serde_json::Value::Number(value) => visitor.visit_f64(value.as_f64().unwrap()),
			serde_json::Value::String(value) => visitor.visit_string(value),
			serde_json::Value::Array(value) => visitor
				.visit_seq(serde::de::value::SeqDeserializer::new(value.into_iter()))
				.map_err(|source| error!(!source, "failed to deserialize the array")),
			serde_json::Value::Object(value) => visitor
				.visit_map(serde::de::value::MapDeserializer::new(
					value
						.into_iter()
						.map(|(key, value)| (serde_json::Value::String(key), value)),
				))
				.map_err(|source| error!(!source, "failed to deserialize the map")),
		}
	}
}

impl serde::Serializer for Serializer {
	type Ok = Value;

	type Error = Error;

	type SerializeSeq = SerializeSeq;

	type SerializeTuple = SerializeTuple;

	type SerializeTupleStruct = SerializeTupleStruct;

	type SerializeTupleVariant = SerializeTupleVariant;

	type SerializeMap = SerializeMap;

	type SerializeStruct = SerializeStruct;

	type SerializeStructVariant = SerializeStructVariant;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Integer(v.into()))
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Integer(v))
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(
			v.try_into()
				.map_err(|source| error!(!source, "failed to serialize u64"))?,
		)
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		self.serialize_f64(v.into())
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Real(v))
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Text(v.into()))
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Text(v.into()))
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Blob(v.into()))
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Null)
	}

	fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Null)
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Null)
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Null)
	}

	fn serialize_newtype_struct<T: ?Sized>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T: ?Sized>(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Ok(SerializeSeq(Vec::new()))
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeTuple(Vec::new()))
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		Ok(SerializeTupleStruct(Vec::new()))
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		Ok(SerializeTupleVariant(Vec::new()))
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		Ok(SerializeMap {
			entries: serde_json::Map::new(),
			key: None,
		})
	}

	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		Ok(SerializeStruct {
			fields: BTreeMap::default(),
		})
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		Ok(SerializeStructVariant {
			fields: BTreeMap::default(),
		})
	}
}

impl serde::ser::SerializeSeq for SerializeSeq {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.0.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.0)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTuple for SerializeTuple {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.0.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.0)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTupleStruct for SerializeTupleStruct {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.0.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.0)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.0.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.0)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeMap for SerializeMap {
	type Ok = Value;

	type Error = Error;

	fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let key = serde_json::to_value(key)
			.map_err(|source| error!(!source, "failed to serialize the key"))?
			.as_str()
			.ok_or_else(|| error!("expected a string"))?
			.to_owned();
		self.key.replace(key);
		Ok(())
	}

	fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let key = self.key.take().ok_or_else(|| error!("missing key"))?;
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.entries.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.entries)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeStruct for SerializeStruct {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.fields.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.fields)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_value(value)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		self.fields.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.fields)
			.map_err(|source| error!(!source, "failed to serialize the value"))?;
		Ok(Value::Text(json))
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
