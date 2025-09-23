use {
	crate::{Kind, Serializer},
	std::{
		collections::{BTreeMap, BTreeSet},
		io::{Result, Write},
		sync::Arc,
	},
};

pub trait Serialize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write;
}

impl Serialize for () {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_unit()
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

impl Serialize for isize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_ivarint(*self as i64)
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

impl Serialize for f64 {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize_f64(*self)
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

impl Serialize for std::path::PathBuf {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		let path_str = self.to_string_lossy();
		serializer.serialize_string(&path_str)
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

#[cfg(feature = "bytes")]
impl Serialize for bytes::Bytes {
	fn serialize<W>(&self, serializer: &mut crate::Serializer<W>) -> std::io::Result<()>
	where
		W: std::io::Write,
	{
		serializer.serialize_bytes(self)
	}
}

#[cfg(feature = "url")]
impl Serialize for url::Url {
	fn serialize<W>(&self, serializer: &mut crate::Serializer<W>) -> Result<()>
	where
		W: Write,
	{
		serializer.serialize(self.as_str())
	}
}
