use {
	crate::{Kind, Serialize},
	num::ToPrimitive as _,
	std::io::{Error, Result},
};

pub struct Serializer<'a>(&'a mut Vec<u8>);

impl<'a> Serializer<'a> {
	#[must_use]
	pub fn new(buffer: &'a mut Vec<u8>) -> Serializer<'a> {
		Serializer(buffer)
	}

	#[must_use]
	pub fn into_inner(self) -> &'a mut Vec<u8> {
		self.0
	}

	pub fn serialize<T>(&mut self, value: &T) -> Result<()>
	where
		T: Serialize + ?Sized,
	{
		value.serialize(self)
	}

	pub fn write_kind(&mut self, kind: Kind) -> Result<()> {
		self.0.push(kind as u8);
		Ok(())
	}

	pub fn write_id(&mut self, id: u8) -> Result<()> {
		self.0.push(id);
		Ok(())
	}

	#[expect(clippy::cast_possible_truncation)]
	pub fn write_uvarint(&mut self, value: u64) -> Result<()> {
		let mut value = value;
		while value > 0x7f {
			let byte = (value | 0x80) as u8;
			self.0.push(byte);
			value >>= 7;
		}
		self.0.push((value & 0x7f) as u8);
		Ok(())
	}

	#[expect(clippy::cast_sign_loss)]
	pub fn write_ivarint(&mut self, value: i64) -> Result<()> {
		// ZigZag encode the value.
		let value = ((value << 1) ^ (value >> 63)) as u64;

		// Write the uvarint.
		self.write_uvarint(value)?;

		Ok(())
	}

	pub fn write_len(&mut self, len: usize) -> Result<()> {
		let len = len.to_u64().ok_or_else(|| Error::other("invalid length"))?;
		self.write_uvarint(len)?;
		Ok(())
	}

	pub fn serialize_null(&mut self) -> Result<()> {
		self.write_kind(Kind::Null)?;
		Ok(())
	}

	pub fn serialize_bool(&mut self, value: bool) -> Result<()> {
		self.write_kind(Kind::Bool)?;
		self.0.push(u8::from(value));
		Ok(())
	}

	pub fn serialize_uvarint(&mut self, value: u64) -> Result<()> {
		self.write_kind(Kind::UVarint)?;
		self.write_uvarint(value)?;
		Ok(())
	}

	pub fn serialize_ivarint(&mut self, value: i64) -> Result<()> {
		self.write_kind(Kind::IVarint)?;
		self.write_ivarint(value)?;
		Ok(())
	}

	pub fn serialize_f32(&mut self, value: f32) -> Result<()> {
		self.write_kind(Kind::F32)?;
		self.0.extend_from_slice(&value.to_le_bytes());
		Ok(())
	}

	pub fn serialize_f64(&mut self, value: f64) -> Result<()> {
		self.write_kind(Kind::F64)?;
		self.0.extend_from_slice(&value.to_le_bytes());
		Ok(())
	}

	pub fn serialize_string(&mut self, value: &str) -> Result<()> {
		self.write_kind(Kind::String)?;
		self.write_len(value.len())?;
		self.0.extend_from_slice(value.as_bytes());
		Ok(())
	}

	pub fn serialize_bytes(&mut self, value: &[u8]) -> Result<()> {
		self.write_kind(Kind::Bytes)?;
		self.write_len(value.len())?;
		self.0.extend_from_slice(value);
		Ok(())
	}

	pub fn serialize_array<'b, T>(
		&mut self,
		len: usize,
		value: impl IntoIterator<Item = &'b T>,
	) -> Result<()>
	where
		T: 'b + Serialize,
	{
		self.write_kind(Kind::Array)?;
		self.write_len(len)?;
		let mut value = value.into_iter();
		for _ in 0..len {
			let value = value
				.next()
				.ok_or_else(|| Error::other("incorrect length"))?;
			self.serialize(value)?;
		}
		Ok(())
	}

	pub fn serialize_map<'b, K, V>(
		&mut self,
		len: usize,
		value: impl IntoIterator<Item = (&'b K, &'b V)>,
	) -> Result<()>
	where
		K: 'b + Serialize,
		V: 'b + Serialize,
	{
		self.write_kind(Kind::Map)?;
		self.write_len(len)?;
		let mut value = value.into_iter();
		for _ in 0..len {
			let (key, value) = value
				.next()
				.ok_or_else(|| Error::other("incorrect length"))?;
			self.serialize(key)?;
			self.serialize(value)?;
		}
		Ok(())
	}
}
