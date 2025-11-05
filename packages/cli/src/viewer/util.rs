use {
	futures::TryStreamExt as _,
	std::{io::Write as _, pin::pin},
	tangram_client::prelude::*,
};

const BLOB_LENGTH_LIMIT: u64 = 1 << 20;

pub async fn format_blob<H: tg::Handle>(handle: &H, blob: &tg::Blob) -> tg::Result<String> {
	let length = blob.length(handle).await?;
	if length > BLOB_LENGTH_LIMIT {
		return Err(tg::error!("cannot view blobs larger than 1Mib"));
	}
	let mut contents = Vec::new();
	let arg = tg::read::Arg {
		blob: blob.id(),
		options: tg::read::Options {
			length: Some(BLOB_LENGTH_LIMIT),
			..tg::read::Options::default()
		},
	};
	let stream = handle
		.try_read(arg)
		.await
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?
		.ok_or_else(|| tg::error!("blob not found"))?;
	let mut stream = pin!(stream);
	let mut is_utf8 = true;
	while let Some(chunk) = stream.try_next().await? {
		if let (true, Ok(chunk)) = (is_utf8, std::str::from_utf8(&chunk.bytes)) {
			contents.extend_from_slice(chunk.as_bytes());
			continue;
		}
		is_utf8 = false;
		contents.reserve(chunk.bytes.len());
		for byte in chunk.bytes {
			if byte.is_ascii_graphic() {
				contents.push(byte);
			} else {
				write!(&mut contents, " {byte:20x}").unwrap();
			}
		}
	}
	Ok(String::from_utf8(contents).unwrap())
}
