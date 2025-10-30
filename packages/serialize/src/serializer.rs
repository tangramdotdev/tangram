use {
	crate::{Kind, Serialize},
	byteorder::{LittleEndian, WriteBytesExt as _},
	num::ToPrimitive as _,
	std::io::{Error, Result, Write},
};

pub struct Serializer<W>(W)
where
	W: Write;

impl<W> Serializer<W>
where
	W: Write,
{
	pub fn new(writer: W) -> Serializer<W> {
		Serializer(writer)
	}

	pub fn into_inner(self) -> W {
		self.0
	}

	pub fn serialize<T>(&mut self, value: &T) -> Result<()>
	where
		T: Serialize + ?Sized,
	{
		value.serialize(self)
	}

	pub fn write_kind(&mut self, kind: Kind) -> Result<()> {
		self.0.write_u8(kind as u8)?;
		Ok(())
	}

	pub fn write_id(&mut self, id: u8) -> Result<()> {
		self.0.write_u8(id)?;
		Ok(())
	}

	#[expect(clippy::cast_possible_truncation)]
	pub fn write_uvarint(&mut self, value: u64) -> Result<()> {
		let mut value = value;
		while value > 0x7f {
			let byte = (value | 0x80) as u8;
			self.0.write_u8(byte)?;
			value >>= 7;
		}
		self.0.write_u8((value & 0x7f) as u8)?;
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
		let value = u8::from(value);
		self.0.write_u8(value)?;
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
		self.0.write_f32::<LittleEndian>(value)?;
		Ok(())
	}

	pub fn serialize_f64(&mut self, value: f64) -> Result<()> {
		self.write_kind(Kind::F64)?;
		self.0.write_f64::<LittleEndian>(value)?;
		Ok(())
	}

	pub fn serialize_string(&mut self, value: &str) -> Result<()> {
		self.write_kind(Kind::String)?;
		self.write_len(value.len())?;
		self.0.write_all(value.as_bytes())?;
		Ok(())
	}

	pub fn serialize_bytes(&mut self, value: &[u8]) -> Result<()> {
		self.write_kind(Kind::Bytes)?;
		self.write_len(value.len())?;
		self.0.write_all(value)?;
		Ok(())
	}

	pub fn serialize_array<'a, T>(
		&mut self,
		len: usize,
		value: impl IntoIterator<Item = &'a T>,
	) -> Result<()>
	where
		T: 'a + Serialize,
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

	pub fn serialize_map<'a, K, V>(
		&mut self,
		len: usize,
		value: impl IntoIterator<Item = (&'a K, &'a V)>,
	) -> Result<()>
	where
		K: 'a + Serialize,
		V: 'a + Serialize,
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
