use {
	crate::{Deserialize, Kind},
	num::{FromPrimitive as _, ToPrimitive as _},
	std::io::{Error, Result},
};

pub struct Deserializer<'de> {
	data: &'de [u8],
	position: usize,
}

impl<'de> Deserializer<'de> {
	#[must_use]
	pub fn new(data: &'de [u8]) -> Deserializer<'de> {
		Deserializer { data, position: 0 }
	}

	#[must_use]
	pub fn data(&self) -> &'de [u8] {
		self.data
	}

	#[must_use]
	pub fn position(&self) -> usize {
		self.position
	}

	pub fn seek(&mut self, position: usize) -> Result<()> {
		if position > self.data.len() {
			return Err(Error::other("seek position out of bounds"));
		}
		self.position = position;
		Ok(())
	}

	#[must_use]
	pub fn remaining(&self) -> &'de [u8] {
		&self.data[self.position..]
	}

	fn read_byte(&mut self) -> Result<u8> {
		if self.position >= self.data.len() {
			return Err(Error::other("unexpected end of data"));
		}
		let byte = self.data[self.position];
		self.position += 1;
		Ok(byte)
	}

	fn read_exact(&mut self, len: usize) -> Result<&'de [u8]> {
		if self.position + len > self.data.len() {
			return Err(Error::other("unexpected end of data"));
		}
		let slice = &self.data[self.position..self.position + len];
		self.position += len;
		Ok(slice)
	}

	pub fn deserialize<T>(&mut self) -> Result<T>
	where
		T: Deserialize<'de>,
	{
		T::deserialize(self)
	}

	pub fn read_kind(&mut self) -> Result<Kind> {
		let value = self.read_byte()?;
		let kind = Kind::from_u8(value).ok_or_else(|| Error::other("invalid kind"))?;
		Ok(kind)
	}

	pub fn ensure_kind(&mut self, kind: Kind) -> Result<Kind> {
		let value = self.read_byte()?;
		let read =
			num::FromPrimitive::from_u8(value).ok_or_else(|| Error::other("invalid kind"))?;
		if read != kind {
			return Err(Error::other("incorrect kind"));
		}
		Ok(read)
	}

	pub fn read_id(&mut self) -> Result<u8> {
		self.read_byte()
	}

	pub fn read_len(&mut self) -> Result<usize> {
		let len = self.read_uvarint()?;
		let len = len
			.to_usize()
			.ok_or_else(|| Error::other("invalid length"))?;
		Ok(len)
	}

	pub fn deserialize_null(&mut self) -> Result<()> {
		self.ensure_kind(Kind::Null)?;
		Ok(())
	}

	pub fn deserialize_bool(&mut self) -> Result<bool> {
		self.ensure_kind(Kind::Bool)?;
		self.read_bool()
	}

	pub fn read_bool(&mut self) -> Result<bool> {
		let value = self.read_byte()?;
		Ok(value != 0)
	}

	pub fn deserialize_uvarint(&mut self) -> Result<u64> {
		self.ensure_kind(Kind::UVarint)?;
		self.read_uvarint()
	}

	#[expect(clippy::cast_lossless)]
	pub fn read_uvarint(&mut self) -> Result<u64> {
		let mut value: u64 = 0;
		let mut shift: u8 = 0;
		let mut success = false;
		for _ in 0..10 {
			let byte = self.read_byte()?;
			value |= ((byte & 0x7f) as u64) << shift;
			shift += 7;
			if byte & 0x80 == 0 {
				success = true;
				break;
			}
		}
		if !success {
			return Err(Error::other("invalid varint"));
		}
		Ok(value)
	}

	pub fn deserialize_ivarint(&mut self) -> Result<i64> {
		self.ensure_kind(Kind::IVarint)?;
		self.read_ivarint()
	}

	#[expect(clippy::cast_possible_wrap)]
	pub fn read_ivarint(&mut self) -> Result<i64> {
		// Read a uvarint.
		let value = self.read_uvarint()?;

		// ZigZag decode the value.
		let value = ((value >> 1) as i64) ^ (-((value & 1) as i64));

		Ok(value)
	}

	pub fn deserialize_f32(&mut self) -> Result<f32> {
		self.ensure_kind(Kind::F32)?;
		self.read_f32()
	}

	pub fn read_f32(&mut self) -> Result<f32> {
		let bytes = self.read_exact(4)?;
		Ok(f32::from_le_bytes(bytes.try_into().unwrap()))
	}

	pub fn deserialize_f64(&mut self) -> Result<f64> {
		self.ensure_kind(Kind::F64)?;
		self.read_f64()
	}

	pub fn read_f64(&mut self) -> Result<f64> {
		let bytes = self.read_exact(8)?;
		Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
	}

	// Zero-copy string methods.
	pub fn deserialize_str(&mut self) -> Result<&'de str> {
		self.ensure_kind(Kind::String)?;
		self.read_str()
	}

	pub fn read_str(&mut self) -> Result<&'de str> {
		let len = self.read_len()?;
		let bytes = self.read_exact(len)?;
		std::str::from_utf8(bytes).map_err(Error::other)
	}

	// Owned string (for backward compatibility).
	pub fn deserialize_string(&mut self) -> Result<String> {
		self.deserialize_str().map(String::from)
	}

	pub fn read_string(&mut self) -> Result<String> {
		self.read_str().map(String::from)
	}

	// Zero-copy bytes methods.
	pub fn deserialize_byte_slice(&mut self) -> Result<&'de [u8]> {
		self.ensure_kind(Kind::Bytes)?;
		self.read_byte_slice()
	}

	pub fn read_byte_slice(&mut self) -> Result<&'de [u8]> {
		let len = self.read_len()?;
		self.read_exact(len)
	}

	// Owned bytes (for backward compatibility).
	pub fn deserialize_bytes(&mut self) -> Result<Vec<u8>> {
		self.deserialize_byte_slice().map(Vec::from)
	}

	pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
		self.read_byte_slice().map(Vec::from)
	}

	pub fn deserialize_array<T>(&mut self) -> Result<Vec<T>>
	where
		T: Deserialize<'de>,
	{
		self.ensure_kind(Kind::Array)?;
		self.read_array()
	}

	pub fn read_array<T>(&mut self) -> Result<Vec<T>>
	where
		T: Deserialize<'de>,
	{
		let len = self.read_len()?;
		let mut array = Vec::with_capacity(len);
		for _ in 0..len {
			let value = self.deserialize()?;
			array.push(value);
		}
		Ok(array)
	}

	pub fn deserialize_map<K, V>(&mut self) -> Result<Vec<(K, V)>>
	where
		K: Deserialize<'de>,
		V: Deserialize<'de>,
	{
		self.ensure_kind(Kind::Map)?;
		self.read_map()
	}

	pub fn read_map<K, V>(&mut self) -> Result<Vec<(K, V)>>
	where
		K: Deserialize<'de>,
		V: Deserialize<'de>,
	{
		let len = self.read_len()?;
		let mut map = Vec::with_capacity(len);
		for _ in 0..len {
			let key = self.deserialize()?;
			let value = self.deserialize()?;
			map.push((key, value));
		}
		Ok(map)
	}
}
