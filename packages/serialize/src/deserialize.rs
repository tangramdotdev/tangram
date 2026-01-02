use {
	crate::{Deserializer, Kind},
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet},
		io::Result,
		sync::Arc,
	},
};

pub trait Deserialize<'de>: Sized {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self>;
}

impl Deserialize<'_> for () {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer.deserialize_null()
	}
}

impl Deserialize<'_> for bool {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer.deserialize_bool()
	}
}

macro_rules! uvarint {
	($t:ty) => {
		impl Deserialize<'_> for $t {
			fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
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
		impl Deserialize<'_> for $t {
			fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
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

impl Deserialize<'_> for usize {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		let value = deserializer.deserialize_uvarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Deserialize<'_> for isize {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		let value = deserializer.deserialize_ivarint()?;
		let value = value.try_into().map_err(std::io::Error::other)?;
		Ok(value)
	}
}

impl Deserialize<'_> for f32 {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer.deserialize_f32()
	}
}

impl Deserialize<'_> for f64 {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer.deserialize_f64()
	}
}

impl<const N: usize> Deserialize<'_> for [u8; N] {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer
			.deserialize_bytes()?
			.try_into()
			.map_err(|_| std::io::Error::other("invalid length"))
	}
}

impl<'de, T> Deserialize<'de> for Option<T>
where
	T: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		let start = deserializer.position();
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

impl<'de, T, E> Deserialize<'de> for std::result::Result<T, E>
where
	T: Deserialize<'de>,
	E: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
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

// Zero-copy string reference.
impl<'de> Deserialize<'de> for &'de str {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer.deserialize_str()
	}
}

// Owned String.
impl Deserialize<'_> for String {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		deserializer.deserialize_string()
	}
}

// Cow<str> - borrows when possible.
impl<'de> Deserialize<'de> for Cow<'de, str> {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer.deserialize_str().map(Cow::Borrowed)
	}
}

// Zero-copy byte slice reference.
impl<'de> Deserialize<'de> for &'de [u8] {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer.deserialize_byte_slice()
	}
}

// Cow<[u8]> - borrows when possible.
impl<'de> Deserialize<'de> for Cow<'de, [u8]> {
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer.deserialize_byte_slice().map(Cow::Borrowed)
	}
}

impl Deserialize<'_> for std::path::PathBuf {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		let path_str = deserializer.deserialize_string()?;
		Ok(std::path::PathBuf::from(path_str))
	}
}

impl Deserialize<'_> for Arc<str> {
	fn deserialize(deserializer: &mut Deserializer<'_>) -> Result<Self> {
		let value = deserializer.deserialize_string()?;
		Ok(value.into())
	}
}

impl<'de, T> Deserialize<'de> for Box<T>
where
	T: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<'de, T> Deserialize<'de> for Arc<T>
where
	T: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		Ok(Self::new(deserializer.deserialize()?))
	}
}

impl<'de, T, U> Deserialize<'de> for (T, U)
where
	T: Deserialize<'de>,
	U: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		let t = deserializer.deserialize()?;
		let u = deserializer.deserialize()?;
		Ok((t, u))
	}
}

impl<'de, T> Deserialize<'de> for Vec<T>
where
	T: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer.deserialize_array()
	}
}

impl<'de, T> Deserialize<'de> for BTreeSet<T>
where
	T: Deserialize<'de> + Ord,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		deserializer
			.deserialize_array()
			.map(|array| array.into_iter().collect())
	}
}

impl<'de, K, V> Deserialize<'de> for BTreeMap<K, V>
where
	K: Deserialize<'de> + Ord,
	V: Deserialize<'de>,
{
	fn deserialize(deserializer: &mut Deserializer<'de>) -> Result<Self> {
		let value = deserializer.deserialize_map()?;
		Ok(value.into_iter().collect())
	}
}

#[cfg(feature = "bytes")]
impl Deserialize<'_> for bytes::Bytes {
	fn deserialize(deserializer: &mut crate::Deserializer<'_>) -> std::io::Result<Self> {
		Ok(bytes::Bytes::from(deserializer.deserialize_bytes()?))
	}
}
