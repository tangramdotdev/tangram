use {
	crate::{Deserializer, Kind},
	std::{
		collections::{BTreeMap, BTreeSet},
		io::{Error, Read, Result, Seek},
		sync::Arc,
	},
};

pub trait Deserialize: Sized {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek;
}

impl Deserialize for () {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_null()
	}
}

impl Deserialize for bool {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_bool()
	}
}

macro_rules! uvarint {
	($t:ty) => {
		impl Deserialize for $t {
			fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
			where
				R: Read + Seek,
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
		impl Deserialize for $t {
			fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
			where
				R: Read + Seek,
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

impl Deserialize for usize {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let value = deserializer.deserialize_uvarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Deserialize for isize {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let value = deserializer.deserialize_ivarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Deserialize for f32 {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_f32()
	}
}

impl Deserialize for f64 {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_f64()
	}
}

impl<const N: usize> Deserialize for [u8; N] {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer
			.deserialize_bytes()?
			.try_into()
			.map_err(|_| std::io::Error::other("invalid length"))
	}
}

impl<T> Deserialize for Option<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let start = deserializer.position()?;
		let kind = deserializer.read_kind()?;
		if kind == Kind::Null {
			Ok(None)
		} else {
			deserializer.seek(start)?;
			let value = deserializer.deserialize()?;
			Ok(Some(value))
		}
	}
}

impl<T, E> Deserialize for std::result::Result<T, E>
where
	T: Deserialize,
	E: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
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
			_ => Err(std::io::Error::other("unexpected variant id")),
		}
	}
}

impl Deserialize for String {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_string()
	}
}

impl Deserialize for std::path::PathBuf {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let path_str = deserializer.deserialize_string()?;
		Ok(std::path::PathBuf::from(path_str))
	}
}

impl Deserialize for Arc<str> {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let value = deserializer.deserialize_string()?;
		let value = value.into();
		Ok(value)
	}
}

impl<T> Deserialize for Box<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<T> Deserialize for Arc<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<T, U> Deserialize for (T, U)
where
	T: Deserialize,
	U: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let t = deserializer.deserialize()?;
		let u = deserializer.deserialize()?;
		Ok((t, u))
	}
}

impl<T> Deserialize for Vec<T>
where
	T: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer.deserialize_array()
	}
}

impl<T> Deserialize for BTreeSet<T>
where
	T: Deserialize + Ord,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		deserializer
			.deserialize_array()
			.map(|array| array.into_iter().collect())
	}
}

impl<K, V> Deserialize for BTreeMap<K, V>
where
	K: Deserialize + Ord,
	V: Deserialize,
{
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let value = deserializer.deserialize_map()?;
		let value = value.into_iter().collect();
		Ok(value)
	}
}

#[cfg(feature = "bytes")]
impl Deserialize for bytes::Bytes {
	fn deserialize<R>(deserializer: &mut crate::Deserializer<R>) -> std::io::Result<Self>
	where
		R: std::io::Read + std::io::Seek,
	{
		Ok(bytes::Bytes::from(deserializer.deserialize_bytes()?))
	}
}

#[cfg(feature = "url")]
impl Deserialize for url::Url {
	fn deserialize<R>(deserializer: &mut crate::Deserializer<R>) -> Result<Self>
	where
		R: Read + Seek,
	{
		let value = deserializer.deserialize_string()?;
		let url = value.parse().map_err(Error::other)?;
		Ok(url)
	}
}
