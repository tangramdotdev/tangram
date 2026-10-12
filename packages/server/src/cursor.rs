use {crate::Session, tangram_client::prelude::*};

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

impl Session {
	pub(crate) fn parse_page_limit(&self, limit: Option<u64>) -> tg::Result<u64> {
		let pagination = &self.server.config.pagination;
		let limit = limit.unwrap_or(pagination.default_limit);
		let max_limit = pagination.max_limit;
		if !(1..=max_limit).contains(&limit) {
			return Err(tg::error!(
				"the page limit must be between 1 and {max_limit}"
			));
		}
		Ok(limit)
	}
}
