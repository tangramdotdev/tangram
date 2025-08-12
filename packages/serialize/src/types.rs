use crate::{Deserialize, Deserializer, Kind, Serialize, Serializer};
use std::{
	collections::{BTreeMap, BTreeSet},
	io::{Read, Result, Write},
	sync::Arc,
};

impl Serialize for () {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_unit()
	}
}

impl Deserialize for () {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_unit()
	}
}

impl Serialize for bool {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_bool(*self)
	}
}

impl Deserialize for bool {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_bool()
	}
}

macro_rules! uvarint {
	($t:ty) => {
		impl Serialize for $t {
			fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
			where
				W: Write,
			{
				serializer.serialize_uvarint(u64::from(*self))
			}
		}

		impl Deserialize for $t {
			fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
			where
				R: Read,
			{
				let value = deserializer.deserialize_uvarint()?;
				let value = value
					.try_into()
					.map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))?;
				Ok(value)
			}
		}
	};
}

uvarint!(u8);
uvarint!(u16);
uvarint!(u32);
uvarint!(u64);

macro_rules! ivarint {
	($t:ty) => {
		impl Serialize for $t {
			fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
			where
				W: Write,
			{
				serializer.serialize_ivarint(i64::from(*self))
			}
		}

		impl Deserialize for $t {
			fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
			where
				R: Read,
			{
				let value = deserializer.deserialize_ivarint()?;
				let value = value
					.try_into()
					.map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))?;
				Ok(value)
			}
		}
	};
}

ivarint!(i8);
ivarint!(i16);
ivarint!(i32);
ivarint!(i64);

impl Serialize for usize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_uvarint(*self as u64)
	}
}

impl Deserialize for usize {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let value = deserializer.deserialize_uvarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Serialize for isize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_ivarint(*self as i64)
	}
}

impl Deserialize for isize {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let value = deserializer.deserialize_ivarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Serialize for f32 {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_f32(*self)
	}
}

impl Deserialize for f32 {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_f32()
	}
}

impl Serialize for f64 {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_f64(*self)
	}
}

impl Deserialize for f64 {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_f64()
	}
}

impl<const N: usize> Serialize for [u8; N] {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_bytes(self)
	}
}

impl<const N: usize> Deserialize for [u8; N] {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer
			.deserialize_bytes()?
			.try_into()
			.map_err(|_| std::io::Error::other("Invalid length."))
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_option(self)
	}
}

impl<T> Deserialize for Option<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_option()
	}
}

impl<T, E> Serialize for std::result::Result<T, E>
where
	T: Serialize,
	E: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.write_kind(Kind::Enum)?;
		match self {
			Ok(value) => {
				serializer.write_id(0)?;
				serializer.serialize(value)?;
			},
			Err(value) => {
				serializer.write_id(1)?;
				serializer.serialize(value)?;
			},
		}
		Ok(())
	}
}

impl<T, E> Deserialize for std::result::Result<T, E>
where
	T: Deserialize,
	E: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.ensure_kind(Kind::Enum)?;
		let id = deserializer.read_id()?;
		match id {
			0 => {
				let value = deserializer.deserialize()?;
				Ok(Ok(value))
			},
			1 => {
				let value = deserializer.deserialize()?;
				Ok(Err(value))
			},
			_ => Err(std::io::Error::other("Unexpected variant ID.")),
		}
	}
}

impl Serialize for str {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_string(self)
	}
}

impl Serialize for String {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_string(self)
	}
}

impl Deserialize for String {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_string()
	}
}

impl Serialize for std::path::PathBuf {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		let path_str = self.to_string_lossy();
		serializer.serialize_string(&path_str)
	}
}

impl Deserialize for std::path::PathBuf {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let path_str = deserializer.deserialize_string()?;
		Ok(std::path::PathBuf::from(path_str))
	}
}

impl Serialize for Arc<str> {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_string(self)
	}
}

impl Deserialize for Arc<str> {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let value = deserializer.deserialize_string()?;
		let value = value.into();
		Ok(value)
	}
}

impl<T> Serialize for Box<T>
where
	T: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		self.as_ref().serialize(serializer)
	}
}

impl<T> Deserialize for Box<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<T> Serialize for Arc<T>
where
	T: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		self.as_ref().serialize(serializer)
	}
}

impl<T> Deserialize for Arc<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<T, U> Serialize for (T, U)
where
	T: Serialize,
	U: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize(&self.0)?;
		serializer.serialize(&self.1)?;
		Ok(())
	}
}

impl<T, U> Deserialize for (T, U)
where
	T: Deserialize,
	U: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let t = deserializer.deserialize()?;
		let u = deserializer.deserialize()?;
		Ok((t, u))
	}
}

impl<T> Serialize for Vec<T>
where
	T: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_array(self.len(), self)
	}
}

impl<T> Deserialize for Vec<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer.deserialize_array()
	}
}

impl<T> Serialize for BTreeSet<T>
where
	T: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_array(self.len(), self)
	}
}

impl<T> Deserialize for BTreeSet<T>
where
	T: Deserialize + Ord,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		deserializer
			.deserialize_array()
			.map(|array| array.into_iter().collect())
	}
}

impl<K, V> Serialize for BTreeMap<K, V>
where
	K: Serialize,
	V: Serialize,
{
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_map(self.len(), self.iter())
	}
}

impl<K, V> Deserialize for BTreeMap<K, V>
where
	K: Deserialize + Ord,
	V: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read,
	{
		let value = deserializer.deserialize_map()?;
		let value = value.into_iter().collect();
		Ok(value)
	}
}
