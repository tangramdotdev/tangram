pub use self::{deserializer::Deserializer, kind::Kind, serializer::Serializer, value::Value};
use std::io::{Read, Result, Write};
pub use tangram_serialize_macro::{Deserialize, Serialize};

#[cfg(feature = "bytes")]
mod bytes;
pub mod deserializer;
pub mod kind;
pub mod serializer;
pub mod types;
#[cfg(feature = "url")]
mod url;
pub mod value;

pub trait Deserialize: Sized {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read;
}

pub trait Serialize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write;
}

pub fn from_reader<T, R>(reader: R) -> Result<T>
where
	T: Deserialize,
	R: Read,
{
	let mut deserializer = Deserializer::new(reader);
	deserializer.deserialize()
}

pub fn to_writer<T, W>(writer: &mut W, value: &T) -> Result<()>
where
	T: Serialize,
	W: Write,
{
	let mut serializer = Serializer::new(writer);
	serializer.serialize(value)?;
	Ok(())
}

pub fn from_slice<T>(slice: &[u8]) -> Result<T>
where
	T: Deserialize,
{
	from_reader(slice)
}

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>>
where
	T: Serialize,
{
	let mut bytes = Vec::new();
	to_writer(&mut bytes, value)?;
	Ok(bytes)
}

pub fn serialize_display<T, W>(value: &T, serializer: &mut Serializer<W>) -> Result<()>
where
	T: std::fmt::Display,
	W: Write,
{
	let display_string = value.to_string();
	serializer.serialize(&display_string)
}

pub fn deserialize_from_str<T, R>(deserializer: &mut Deserializer<R>) -> Result<T>
where
	T: std::str::FromStr,
	T::Err: std::fmt::Display,
	R: Read,
{
	let display_string: String = deserializer.deserialize()?;
	display_string
		.parse()
		.map_err(|error| std::io::Error::other(format!("{error}")))
}
