use std::io::{Read, Result, Write};

pub use self::{
	deserialize::Deserialize, deserializer::Deserializer, kind::Kind, serialize::Serialize,
	serializer::Serializer, value::Value,
};
pub use tangram_serialize_macro::{Deserialize, Serialize};

pub mod deserialize;
pub mod deserializer;
pub mod kind;
pub mod serialize;
pub mod serializer;
pub mod value;

pub fn to_writer<T, W>(writer: &mut W, value: &T) -> Result<()>
where
	T: Serialize,
	W: Write,
{
	let mut serializer = Serializer::new(writer);
	serializer.serialize(value)?;
	Ok(())
}

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>>
where
	T: Serialize,
{
	let mut bytes = Vec::new();
	to_writer(&mut bytes, value)?;
	Ok(bytes)
}

pub fn from_reader<T, R>(reader: R) -> Result<T>
where
	T: Deserialize,
	R: Read,
{
	let mut deserializer = Deserializer::new(reader);
	deserializer.deserialize()
}

pub fn from_slice<T>(slice: &[u8]) -> Result<T>
where
	T: Deserialize,
{
	from_reader(slice)
}

pub fn serialize_display<T, W>(value: &T, serializer: &mut Serializer<W>) -> Result<()>
where
	T: std::fmt::Display,
	W: Write,
{
	serializer.serialize(&value.to_string())
}

pub fn deserialize_from_str<T, R>(deserializer: &mut Deserializer<R>) -> Result<T>
where
	T: std::str::FromStr,
	T::Err: std::fmt::Display,
	R: Read,
{
	deserializer
		.deserialize::<String>()?
		.parse()
		.map_err(|error| std::io::Error::other(format!("{error}")))
}
