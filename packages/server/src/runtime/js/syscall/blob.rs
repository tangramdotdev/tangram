use super::State;
use bytes::Bytes;
use std::rc::Rc;
use tangram_client as tg;
use url::Url;

pub async fn compress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::compress::Format),
) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (blob, format) = args;
	let blob = server.compress_blob(&blob, format).await?;
	Ok(blob)
}

pub async fn checksum(
	state: Rc<State>,
	args: (tg::Blob, tg::checksum::Algorithm),
) -> tg::Result<tg::Checksum> {
	let server = &state.server;
	let (blob, algorithm) = args;
	let checksum = server.checksum_blob(&blob, algorithm).await?;
	Ok(checksum)
}

pub async fn decompress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::compress::Format),
) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (blob, format) = args;
	let blob = server.decompress_blob(&blob, format).await?;
	Ok(blob)
}

pub async fn download(state: Rc<State>, args: (Url, tg::Checksum)) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (url, checksum) = args;
	let blob = server.download_blob(&url, &checksum, &state.build).await?;
	Ok(blob)
}

pub async fn read(state: Rc<State>, args: (tg::Blob,)) -> tg::Result<Bytes> {
	let (blob,) = args;
	let bytes = blob
		.bytes(&state.server)
		.await
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(bytes.into())
}
