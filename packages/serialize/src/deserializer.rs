use crate::{Deserialize, Kind};
use byteorder::{LittleEndian, ReadBytesExt as _};
use num::ToPrimitive as _;
use std::io::{Error, Read, Result};

pub struct Deserializer<R>(R)
where
	R: Read;

impl<R> Deserializer<R>
where
	R: Read,
{
	pub fn new(reader: R) -> Deserializer<R> {
		Deserializer(reader)
	}

	pub fn into_inner(self) -> R {
		self.0
	}

	pub fn deserialize<T>(&mut self) -> Result<T>
	where
		T: Deserialize,
	{
		T::deserialize(self)
	}

	pub fn read_kind(&mut self) -> Result<Kind> {
		let value = self.0.read_u8()?;
		let kind = num::FromPrimitive::from_u8(value)
			.ok_or_else(|| Error::other("Invalid value kind."))?;
		Ok(kind)
	}

	pub fn ensure_kind(&mut self, kind: Kind) -> Result<Kind> {
		let value = self.0.read_u8()?;
		let read = num::FromPrimitive::from_u8(value)
			.ok_or_else(|| Error::other("Invalid value kind."))?;
		if read != kind {
			return Err(Error::other("Incorrect kind."));
		}
		Ok(read)
	}

	pub fn read_id(&mut self) -> Result<u8> {
		let id = self.0.read_u8()?;
		Ok(id)
	}

	pub fn read_len(&mut self) -> Result<usize> {
		let len = self.read_uvarint()?;
		let len = len
			.to_usize()
			.ok_or_else(|| Error::other("The length is too long for this architecture."))?;
		Ok(len)
	}

	pub fn deserialize_unit(&mut self) -> Result<()> {
		self.ensure_kind(Kind::Unit)?;
		self.read_unit()?;
		Ok(())
	}

	pub fn read_unit(&mut self) -> Result<()> {
		Ok(())
	}

	pub fn deserialize_bool(&mut self) -> Result<bool> {
		self.ensure_kind(Kind::Bool)?;
		let value = self.read_bool()?;
		Ok(value)
	}

	pub fn read_bool(&mut self) -> Result<bool> {
		let value = self.0.read_u8()?;
		let value = value != 0;
		Ok(value)
	}

	pub fn deserialize_uvarint(&mut self) -> Result<u64> {
		self.ensure_kind(Kind::UVarint)?;
		let value = self.read_uvarint()?;
		Ok(value)
	}

	#[allow(clippy::cast_lossless)]
	pub fn read_uvarint(&mut self) -> Result<u64> {
		let mut value: u64 = 0;
		let mut shift: u8 = 0;
		let mut success = false;
		for _ in 0..10 {
			let byte = self.0.read_u8()?;
			value |= ((byte & 0x7f) as u64) << shift;
			shift += 7;
			if byte & 0x80 == 0 {
				success = true;
				break;
			}
		}
		if !success {
			return Err(Error::other("Invalid varint."));
		}
		Ok(value)
	}

	pub fn deserialize_ivarint(&mut self) -> Result<i64> {
		self.ensure_kind(Kind::IVarint)?;
		let value = self.read_ivarint()?;
		Ok(value)
	}

	#[allow(clippy::cast_possible_wrap)]
	pub fn read_ivarint(&mut self) -> Result<i64> {
		// Read a uvarint.
		let value = self.read_uvarint()?;

		// ZigZag decode the value.
		let value = ((value >> 1) as i64) ^ (-((value & 1) as i64));

		Ok(value)
	}

	pub fn deserialize_f32(&mut self) -> Result<f32> {
		self.ensure_kind(Kind::F32)?;
		let value = self.read_f32()?;
		Ok(value)
	}

	pub fn read_f32(&mut self) -> Result<f32> {
		let value = self.0.read_f32::<LittleEndian>()?;
		Ok(value)
	}

	pub fn deserialize_f64(&mut self) -> Result<f64> {
		self.ensure_kind(Kind::F64)?;
		let value = self.read_f64()?;
		Ok(value)
	}

	pub fn read_f64(&mut self) -> Result<f64> {
		let value = self.0.read_f64::<LittleEndian>()?;
		Ok(value)
	}

	pub fn deserialize_string(&mut self) -> Result<String> {
		self.ensure_kind(Kind::String)?;
		let value = self.read_string()?;
		Ok(value)
	}

	pub fn read_string(&mut self) -> Result<String> {
		let len = self.read_len()?;
		let mut value = vec![0u8; len];
		self.0.read_exact(&mut value)?;
		let value = String::from_utf8(value).map_err(Error::other)?;
		Ok(value)
	}

	pub fn deserialize_bytes(&mut self) -> Result<Vec<u8>> {
		self.ensure_kind(Kind::Bytes)?;
		let value = self.read_bytes()?;
		Ok(value)
	}

	pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
		let len = self.read_len()?;
		let mut value = vec![0u8; len];
		self.0.read_exact(&mut value)?;
		Ok(value)
	}

	pub fn deserialize_option<T>(&mut self) -> Result<Option<T>>
	where
		T: Deserialize,
	{
		self.ensure_kind(Kind::Option)?;
		let value = self.read_option()?;
		Ok(value)
	}

	pub fn read_option<T>(&mut self) -> Result<Option<T>>
	where
		T: Deserialize,
	{
		let is_some = self.0.read_u8()? != 0;
		let value = if is_some {
			Some(self.deserialize()?)
		} else {
			None
		};
		Ok(value)
	}

	pub fn deserialize_array<T>(&mut self) -> Result<Vec<T>>
	where
		T: Deserialize,
	{
		self.ensure_kind(Kind::Array)?;
		let value = self.read_array()?;
		Ok(value)
	}

	pub fn read_array<T>(&mut self) -> Result<Vec<T>>
	where
		T: Deserialize,
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
		K: Deserialize,
		V: Deserialize,
	{
		self.ensure_kind(Kind::Map)?;
		let value = self.read_map()?;
		Ok(value)
	}

	pub fn read_map<K, V>(&mut self) -> Result<Vec<(K, V)>>
	where
		K: Deserialize,
		V: Deserialize,
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
