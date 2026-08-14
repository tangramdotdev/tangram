use {std::pin::pin, tangram_client::prelude::*, tokio::io::AsyncReadExt as _};

const BLOB_LENGTH_LIMIT: u64 = 1 << 20;

pub async fn format_blob(client: &tg::Client, blob: &tg::Blob) -> tg::Result<String> {
	let length = blob.length_with_handle(client).await?;
	if length > BLOB_LENGTH_LIMIT {
		return Err(tg::error!("cannot view blobs larger than 1 MiB"));
	}
	let options = tg::read::Options {
		length: Some(BLOB_LENGTH_LIMIT),
		..tg::read::Options::default()
	};
	let reader = blob
		.read_with_handle(client, options)
		.await
		.map_err(|error| tg::error!(!error, "failed to read the blob"))?;
	let mut reader = pin!(reader);
	let mut contents = Vec::with_capacity(length.try_into().unwrap_or_default());
	reader
		.read_to_end(&mut contents)
		.await
		.map_err(|error| tg::error!(!error, "failed to read the blob"))?;

	Ok(format_bytes(contents))
}

fn format_bytes(contents: Vec<u8>) -> String {
	match String::from_utf8(contents) {
		Ok(contents) => contents,
		Err(error) => error
			.into_bytes()
			.into_iter()
			.flat_map(std::ascii::escape_default)
			.map(char::from)
			.collect(),
	}
}

#[cfg(test)]
mod tests {
	use super::format_bytes;

	#[test]
	fn formats_binary_bytes() {
		assert_eq!(format_bytes(vec![b'a', 0, 0xff]), r"a\x00\xff");
	}

	#[test]
	fn formats_utf8() {
		assert_eq!(
			format_bytes("split 😀 text".as_bytes().to_vec()),
			"split 😀 text"
		);
	}
}
