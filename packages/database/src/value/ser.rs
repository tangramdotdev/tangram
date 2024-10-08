use super::Value;
use serde::ser::Error as _;
use std::collections::BTreeMap;

pub struct Serializer;

pub struct SerializeSeq {
	values: Vec<serde_json::Value>,
}

pub struct SerializeTuple {
	values: Vec<serde_json::Value>,
}

pub struct SerializeTupleStruct {
	values: Vec<serde_json::Value>,
}

pub struct SerializeTupleVariant {
	values: Vec<serde_json::Value>,
	variant: &'static str,
}

pub struct SerializeMap {
	entries: serde_json::Map<String, serde_json::Value>,
	key: Option<String>,
}

pub struct SerializeStruct {
	fields: BTreeMap<&'static str, serde_json::Value>,
}

pub struct SerializeStructVariant {
	fields: BTreeMap<&'static str, serde_json::Value>,
	variant: &'static str,
}

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Json(serde_json::Error),
	Other(Box<dyn std::error::Error + Send + Sync>),
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
				.map_err(|_| Error::custom("failed to serialize u64"))?,
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

	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Null)
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
		self.serialize_unit()
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(variant)
	}

	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let map = BTreeMap::from([(variant.to_owned(), value.serialize(self)?)]);
		let json = serde_json::to_string(&map)?;
		Ok(Value::Text(json))
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Ok(SerializeSeq { values: Vec::new() })
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeTuple { values: Vec::new() })
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		Ok(SerializeTupleStruct { values: Vec::new() })
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		Ok(SerializeTupleVariant {
			values: Vec::new(),
			variant,
		})
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
			fields: BTreeMap::new(),
		})
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		Ok(SerializeStructVariant {
			fields: BTreeMap::new(),
			variant,
		})
	}
}

impl serde::ser::SerializeSeq for SerializeSeq {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.values)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTuple for SerializeTuple {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.values)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTupleStruct for SerializeTupleStruct {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.values)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let map = BTreeMap::from([(self.variant, self.values)]);
		let json = serde_json::to_string(&map)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeMap for SerializeMap {
	type Ok = Value;

	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = serde_json::to_value(key)?
			.as_str()
			.ok_or_else(|| Error::custom("expected a string"))?
			.to_owned();
		self.key.replace(key);
		Ok(())
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = self
			.key
			.take()
			.ok_or_else(|| Error::custom("missing key"))?;
		let value = serde_json::to_value(value)?;
		self.entries.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.entries)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeStruct for SerializeStruct {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.fields.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let json = serde_json::to_string(&self.fields)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = serde_json::to_value(value)?;
		self.fields.insert(key, value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let map = BTreeMap::from([(self.variant, self.fields)]);
		let json = serde_json::to_string(&map)?;
		Ok(Value::Text(json))
	}
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self::Other(msg.to_string().into())
	}
}
