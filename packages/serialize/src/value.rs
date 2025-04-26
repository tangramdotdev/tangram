use crate::{Deserialize, Kind, Serialize};
use std::io::{Read, Result, Write};

#[derive(Clone, Debug)]
pub enum Value {
	Unit(()),

	Bool(bool),

	UVarint(u64),
	IVarint(i64),

	F32(f32),
	F64(f64),

	String(String),
	Bytes(Vec<u8>),

	Option(Option<Box<Value>>),
	Array(Vec<Value>),
	Map(Vec<(Value, Value)>),

	Struct(Struct),
	Enum(Enum),
}

#[derive(Clone, Debug)]
pub struct Struct {
	pub fields: Vec<Field>,
}

#[derive(Clone, Debug)]
pub struct Field {
	pub id: u8,
	pub value: Box<Value>,
}

#[derive(Clone, Debug)]
pub struct Enum {
	pub id: u8,
	pub value: Box<Value>,
}

impl Serialize for Value {
	fn serialize<W>(&self, serializer: &mut crate::Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		match self {
			Value::Unit(()) => {
				serializer.serialize_unit()?;
			},
			Value::Bool(value) => {
				serializer.serialize_bool(*value)?;
			},
			Value::UVarint(value) => {
				serializer.serialize_uvarint(*value)?;
			},
			Value::IVarint(value) => {
				serializer.serialize_ivarint(*value)?;
			},
			Value::F32(value) => {
				serializer.serialize_f32(*value)?;
			},
			Value::F64(value) => {
				serializer.serialize_f64(*value)?;
			},
			Value::String(value) => {
				serializer.serialize_string(value)?;
			},
			Value::Bytes(value) => {
				serializer.serialize_bytes(value)?;
			},
			Value::Option(value) => {
				serializer.serialize_option(value)?;
			},
			Value::Array(value) => {
				serializer.serialize_array(value.len(), value)?;
			},
			Value::Map(value) => {
				serializer
					.serialize_map(value.len(), value.iter().map(|(key, value)| (key, value)))?;
			},
			Value::Struct(value) => {
				serializer.write_kind(Kind::Struct)?;
				serializer.write_len(value.fields.len())?;
				for field in &value.fields {
					serializer.write_id(field.id)?;
					serializer.serialize(&field.value)?;
				}
			},
			Value::Enum(value) => {
				serializer.write_kind(Kind::Enum)?;
				serializer.write_id(value.id)?;
				serializer.serialize(&value.value)?;
			},
		}
		Ok(())
	}
}

impl Deserialize for Value {
	fn deserialize<R>(deserializer: &mut crate::Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let kind = deserializer.read_kind()?;
		let value = match kind {
			Kind::Unit => Value::Unit(deserializer.read_unit()?),
			Kind::Bool => Value::Bool(deserializer.read_bool()?),
			Kind::UVarint => Value::UVarint(deserializer.read_uvarint()?),
			Kind::IVarint => Value::IVarint(deserializer.read_ivarint()?),
			Kind::F32 => Value::F32(deserializer.read_f32()?),
			Kind::F64 => Value::F64(deserializer.read_f64()?),
			Kind::String => Value::String(deserializer.read_string()?),
			Kind::Bytes => Value::Bytes(deserializer.read_bytes()?),
			Kind::Option => Value::Option(deserializer.read_option()?),
			Kind::Array => Value::Array(deserializer.read_array()?),
			Kind::Map => Value::Map(deserializer.read_map()?),
			Kind::Struct => {
				deserializer.ensure_kind(Kind::Struct)?;
				let len = deserializer.read_len()?;
				let mut fields = Vec::with_capacity(len);
				for _ in 0..len {
					let id = deserializer.read_id()?;
					let value = deserializer.deserialize()?;
					fields.push(Field { id, value });
				}
				Value::Struct(Struct { fields })
			},
			Kind::Enum => {
				deserializer.ensure_kind(Kind::Enum)?;
				let id = deserializer.read_id()?;
				let value = deserializer.deserialize()?;
				Value::Enum(Enum { id, value })
			},
		};
		Ok(value)
	}
}
