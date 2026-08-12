use {futures::TryStreamExt as _, std::pin::pin, tangram_client::prelude::*};

const BLOB_LENGTH_LIMIT: u64 = 1 << 20;

pub async fn format_blob(client: &tg::Client, blob: &tg::Blob) -> tg::Result<String> {
	let length = blob.length_with_handle(client).await?;
	if length > BLOB_LENGTH_LIMIT {
		return Err(tg::error!("cannot view blobs larger than 1 MiB"));
	}
	let arg = tg::read::Arg {
		blob: blob.id(),
		tokens: blob.state().tokens(),
		options: tg::read::Options {
			length: Some(BLOB_LENGTH_LIMIT),
			..tg::read::Options::default()
		},
	};
	let stream = client
		.try_read(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to read the blob"))?
		.ok_or_else(|| tg::error!("blob not found"))?;
	let mut stream = pin!(stream);
	let mut contents = Vec::with_capacity(length.try_into().unwrap_or_default());
	while let Some(chunk) = stream.try_next().await? {
		contents.extend_from_slice(&chunk.bytes);
	}

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
