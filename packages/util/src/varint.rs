pub trait ReadExt: std::io::Read {
	#[expect(clippy::cast_lossless)]
	fn read_uvarint(&mut self) -> std::io::Result<u64> {
		let mut value: u64 = 0;
		let mut shift: u8 = 0;
		let mut byte = [0u8; 1];
		loop {
			self.read_exact(&mut byte)?;
			if shift >= 64 {
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"varint overflow",
				));
			}
			value |= ((byte[0] & 0x7f) as u64) << shift;
			if byte[0] & 0x80 == 0 {
				return Ok(value);
			}
			shift += 7;
		}
	}

	#[expect(clippy::cast_possible_wrap)]
	fn read_ivarint(&mut self) -> std::io::Result<i64> {
		let zigzag = self.read_uvarint()?;
		Ok(((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64)))
	}
}

impl<R: std::io::Read + ?Sized> ReadExt for R {}

pub trait WriteExt: std::io::Write {
	#[expect(clippy::cast_possible_truncation)]
	fn write_uvarint(&mut self, mut value: u64) -> std::io::Result<()> {
		while value > 0x7f {
			self.write_all(&[(value | 0x80) as u8])?;
			value >>= 7;
		}
		self.write_all(&[(value & 0x7f) as u8])?;
		Ok(())
	}

	#[expect(clippy::cast_sign_loss)]
	fn write_ivarint(&mut self, value: i64) -> std::io::Result<()> {
		let zigzag = ((value << 1) ^ (value >> 63)) as u64;
		self.write_uvarint(zigzag)
	}
}

impl<W: std::io::Write + ?Sized> WriteExt for W {}

#[must_use]
pub fn encode_uvarint(value: u64) -> Vec<u8> {
	let mut buffer = Vec::with_capacity(10);
	buffer.write_uvarint(value).unwrap();
	buffer
}

#[must_use]
pub fn decode_uvarint(bytes: &[u8]) -> Option<u64> {
	let mut reader = bytes;
	reader.read_uvarint().ok()
}

#[must_use]
pub fn encode_ivarint(value: i64) -> Vec<u8> {
	let mut buffer = Vec::with_capacity(10);
	buffer.write_ivarint(value).unwrap();
	buffer
}

#[must_use]
pub fn decode_ivarint(bytes: &[u8]) -> Option<i64> {
	let mut reader = bytes;
	reader.read_ivarint().ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_uvarint_roundtrip() {
		for value in [0, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX] {
			let encoded = encode_uvarint(value);
			let decoded = decode_uvarint(&encoded);
			assert_eq!(decoded, Some(value), "failed for {value}");
		}
	}

	#[test]
	fn test_ivarint_roundtrip() {
		for value in [0, 1, -1, 63, -64, 64, -65, i64::MAX, i64::MIN] {
			let encoded = encode_ivarint(value);
			let decoded = decode_ivarint(&encoded);
			assert_eq!(decoded, Some(value), "failed for {value}");
		}
	}

	#[test]
	fn test_uvarint_size() {
		assert_eq!(encode_uvarint(0).len(), 1);
		assert_eq!(encode_uvarint(127).len(), 1);
		assert_eq!(encode_uvarint(128).len(), 2);
		assert_eq!(encode_uvarint(16383).len(), 2);
		assert_eq!(encode_uvarint(16384).len(), 3);
	}

	#[test]
	fn test_extension_traits() {
		// Test reading from a slice using the extension trait.
		let data = encode_uvarint(12345);
		let mut reader = data.as_slice();
		assert_eq!(reader.read_uvarint().unwrap(), 12345);

		// Test writing to a Vec using the extension trait.
		let mut buffer = Vec::new();
		buffer.write_uvarint(67890).unwrap();
		assert_eq!(decode_uvarint(&buffer), Some(67890));

		// Test reading multiple values.
		let mut data = Vec::new();
		data.write_uvarint(100).unwrap();
		data.write_uvarint(200).unwrap();
		data.write_ivarint(-50).unwrap();

		let mut reader = data.as_slice();
		assert_eq!(reader.read_uvarint().unwrap(), 100);
		assert_eq!(reader.read_uvarint().unwrap(), 200);
		assert_eq!(reader.read_ivarint().unwrap(), -50);
	}
}
