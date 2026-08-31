use {
	bytes::{BufMut as _, Bytes, BytesMut},
	tangram_client::prelude::*,
};

mod delete;
mod get;
mod put;

const FORMAT: u8 = 1;
pub(super) const HEADER_LENGTH: usize = 17;

pub(super) fn deserialize(bytes: &Bytes) -> tg::Result<crate::object::get::Object> {
	let put = deserialize_put(bytes)?;
	let bytes = bytes.slice(HEADER_LENGTH..);
	let object = crate::object::get::Object { bytes, put };

	Ok(object)
}

pub(super) fn deserialize_put(bytes: &[u8]) -> tg::Result<[u8; 16]> {
	if bytes.len() < HEADER_LENGTH {
		return Err(tg::error!("the S3 object header is too short"));
	}
	let format = bytes[0];
	if format != FORMAT {
		return Err(tg::error!(%format, "the S3 object format is invalid"));
	}
	let put = bytes[1..HEADER_LENGTH].try_into().unwrap();

	Ok(put)
}

pub(super) fn serialize(put: [u8; 16], bytes: &Bytes) -> Bytes {
	let mut output = BytesMut::with_capacity(HEADER_LENGTH + bytes.len());
	output.put_u8(FORMAT);
	output.extend_from_slice(&put);
	output.extend_from_slice(bytes);

	output.freeze()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serialization() {
		let bytes = Bytes::from_static(b"object");
		let put = [42; 16];
		let serialized = serialize(put, &bytes);
		assert_eq!(deserialize_put(&serialized).unwrap(), put);
		let object = deserialize(&serialized).unwrap();
		assert_eq!(object.bytes, bytes);
		assert_eq!(object.put, put);
	}
}
