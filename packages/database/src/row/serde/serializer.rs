use crate::Row;
use serde::ser::Error as _;

pub struct Serializer;

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl serde::Serializer for Serializer {
	type Ok = Row;

	type Error = Error;

	type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeTuple = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeTupleVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;

	type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

	fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		Err(Error::custom("invalid type"))
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		_value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		Err(Error::custom("invalid type"))
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		Err(Error::custom("invalid type"))
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		Err(Error::custom("invalid type"))
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		Err(Error::custom("invalid type"))
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
