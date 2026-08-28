use {
	bytes::{BufMut as _, Bytes, BytesMut},
	tangram_client::prelude::*,
};

mod delete;
mod get;
mod put;

const FORMAT: u8 = 0;
pub(super) const HEADER_LENGTH: usize = 9;

pub(super) fn deserialize(bytes: &Bytes) -> tg::Result<Bytes> {
	deserialize_stored_at(bytes)?;
	let bytes = bytes.slice(HEADER_LENGTH..);

	Ok(bytes)
}

pub(super) fn deserialize_stored_at(bytes: &[u8]) -> tg::Result<i64> {
	if bytes.len() < HEADER_LENGTH {
		return Err(tg::error!("the S3 object header is too short"));
	}
	let format = bytes[0];
	if format != FORMAT {
		return Err(tg::error!(%format, "the S3 object format is invalid"));
	}
	let stored_at = i64::from_be_bytes(bytes[1..HEADER_LENGTH].try_into().unwrap());

	Ok(stored_at)
}

pub(super) fn serialize(stored_at: i64, bytes: &Bytes) -> Bytes {
	let mut output = BytesMut::with_capacity(HEADER_LENGTH + bytes.len());
	output.put_u8(FORMAT);
	output.put_i64(stored_at);
	output.extend_from_slice(bytes);

	output.freeze()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serialization() {
		let bytes = Bytes::from_static(b"object");
		let stored_at = 1_234_567_890;
		let serialized = serialize(stored_at, &bytes);
		assert_eq!(deserialize_stored_at(&serialized).unwrap(), stored_at);
		assert_eq!(deserialize(&serialized).unwrap(), bytes);
	}
}
