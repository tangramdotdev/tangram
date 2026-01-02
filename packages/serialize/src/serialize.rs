use {
	crate::{Kind, Serializer},
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet},
		io::Result,
		sync::Arc,
	},
};

pub trait Serialize {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()>;
}

impl Serialize for () {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_null()
	}
}

impl Serialize for bool {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_bool(*self)
	}
}

macro_rules! uvarint {
	($t:ty) => {
		impl Serialize for $t {
			fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
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
			fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
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
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_uvarint(*self as u64)
	}
}

impl Serialize for isize {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_ivarint(*self as i64)
	}
}

impl Serialize for f32 {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_f32(*self)
	}
}

impl Serialize for f64 {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_f64(*self)
	}
}

impl<const N: usize> Serialize for [u8; N] {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_bytes(self)
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		match self {
			Some(value) => serializer.serialize(value),
			None => serializer.serialize_null(),
		}
	}
}

impl<T, E> Serialize for std::result::Result<T, E>
where
	T: Serialize,
	E: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
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
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_string(self)
	}
}

impl Serialize for String {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_string(self)
	}
}

// Cow<str> serializes the same as str/String.
impl Serialize for Cow<'_, str> {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_string(self)
	}
}

impl Serialize for [u8] {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_bytes(self)
	}
}

// Cow<[u8]> serializes the same as [u8]/Vec<u8>.
impl Serialize for Cow<'_, [u8]> {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_bytes(self)
	}
}

impl Serialize for std::path::PathBuf {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		let path_str = self.to_string_lossy();
		serializer.serialize_string(&path_str)
	}
}

impl Serialize for Arc<str> {
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_string(self)
	}
}

impl<T> Serialize for Box<T>
where
	T: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		self.as_ref().serialize(serializer)
	}
}

impl<T> Serialize for Arc<T>
where
	T: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		self.as_ref().serialize(serializer)
	}
}

impl<T, U> Serialize for (T, U)
where
	T: Serialize,
	U: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize(&self.0)?;
		serializer.serialize(&self.1)?;
		Ok(())
	}
}

impl<T> Serialize for Vec<T>
where
	T: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_array(self.len(), self)
	}
}

impl<T> Serialize for BTreeSet<T>
where
	T: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_array(self.len(), self)
	}
}

impl<K, V> Serialize for BTreeMap<K, V>
where
	K: Serialize,
	V: Serialize,
{
	fn serialize(&self, serializer: &mut Serializer<'_>) -> Result<()> {
		serializer.serialize_map(self.len(), self.iter())
	}
}

#[cfg(feature = "bytes")]
impl Serialize for bytes::Bytes {
	fn serialize(&self, serializer: &mut crate::Serializer<'_>) -> std::io::Result<()> {
		serializer.serialize_bytes(self)
	}
}
