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
	let id = blob.id(server, None).await?;
	let arg = tg::blob::compress::Arg { format };
	let output = server.compress_blob(&id, arg).await?;
	let blob = tg::Blob::with_id(output.blob);
	Ok(blob)
}

pub async fn checksum(
	state: Rc<State>,
	args: (tg::Blob, tg::checksum::Algorithm),
) -> tg::Result<tg::Checksum> {
	let server = &state.server;
	let (blob, algorithm) = args;
	let blob = blob.id(server, None).await?;
	let arg = tangram_client::blob::checksum::Arg { algorithm };
	let checksum = server.checksum_blob(&blob, arg).await?;
	Ok(checksum)
}

pub async fn decompress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::compress::Format),
) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (blob, format) = args;
	let id = blob.id(server, None).await?;
	let arg = tg::blob::decompress::Arg { format };
	let output = server.decompress_blob(&id, arg).await?;
	let blob = tg::Blob::with_id(output.blob);
	Ok(blob)
}

pub async fn download(state: Rc<State>, args: (Url, tg::Checksum)) -> tg::Result<tg::Blob> {
	let server = &state.server;
	let (url, checksum) = args;
	let arg = tg::blob::download::Arg { url, checksum };
	let output = server.download_blob(arg).await?;
	let blob = tg::Blob::with_id(output.blob);
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
