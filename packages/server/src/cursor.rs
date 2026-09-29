use tangram_client::prelude::*;

pub const DEFAULT_LIMIT: u64 = 100;
pub const MAX_LIMIT: u64 = 1000;

pub fn serialize<T: serde::Serialize>(cursor: &T) -> tg::Result<String> {
	let bytes = serde_json::to_vec(cursor)
		.map_err(|error| tg::error!(!error, "failed to serialize the cursor"))?;
	let cursor = data_encoding::BASE64URL_NOPAD.encode(&bytes);
	Ok(cursor)
}

pub fn deserialize<T: serde::de::DeserializeOwned>(cursor: &str) -> tg::Result<T> {
	let bytes = data_encoding::BASE64URL_NOPAD
		.decode(cursor.as_bytes())
		.map_err(|error| tg::error!(!error, "failed to decode the cursor"))?;
	let cursor = serde_json::from_slice(&bytes)
		.map_err(|error| tg::error!(!error, "failed to deserialize the cursor"))?;
	Ok(cursor)
}
