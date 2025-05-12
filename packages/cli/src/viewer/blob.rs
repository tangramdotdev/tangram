use futures::TryStreamExt;
use std::{io::Write as _, pin::pin};
use tangram_client::{self as tg, handle::Ext as _};

const ONE_MIB: u64 = 1 << 20;

pub async fn format<H: tg::Handle>(handle: &H, blob: &tg::Blob) -> tg::Result<String> {
	let length = blob.length(handle).await?;
	if length > ONE_MIB {
		return Err(tg::error!("cannot view blobs larger than 1Mib"));
	}
	let mut contents = Vec::new();
	let mut stream = handle
		.try_read_blob(
			&blob.id(),
			tg::blob::read::Arg {
				length: Some(ONE_MIB),
				..tg::blob::read::Arg::default()
			},
		)
		.await?
		.ok_or_else(|| tg::error!("failed to read the blob"))?;
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
