use std::io::Result;

pub use {
	self::{
		deserialize::Deserialize, deserializer::Deserializer, kind::Kind, serialize::Serialize,
		serializer::Serializer, value::Value,
	},
	tangram_serialize_macro::{Deserialize, Serialize},
};

pub mod deserialize;
pub mod deserializer;
pub mod kind;
pub mod serialize;
pub mod serializer;
pub mod value;

pub fn to_writer<T>(buffer: &mut Vec<u8>, value: &T) -> Result<()>
where
	T: Serialize,
{
	let mut serializer = Serializer::new(buffer);
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

pub fn from_slice<'de, T>(slice: &'de [u8]) -> Result<T>
where
	T: Deserialize<'de>,
{
	let mut deserializer = Deserializer::new(slice);
	deserializer.deserialize()
}

pub fn serialize_display<T>(value: &T, serializer: &mut Serializer<'_>) -> Result<()>
where
	T: std::fmt::Display,
{
	serializer.serialize(&value.to_string())
}

pub fn deserialize_from_str<T>(deserializer: &mut Deserializer<'_>) -> Result<T>
where
	T: std::str::FromStr,
	T::Err: std::fmt::Display,
{
	deserializer
		.deserialize::<String>()?
		.parse()
		.map_err(|error| std::io::Error::other(format!("{error}")))
}
