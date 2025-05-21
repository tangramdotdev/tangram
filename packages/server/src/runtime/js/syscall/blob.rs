use super::State;
use bytes::Bytes;
use std::{io::Cursor, rc::Rc};
use tangram_client as tg;
use tangram_either::Either;
use tokio::io::AsyncReadExt;

pub async fn read(
	state: Rc<State>,
	args: (tg::Blob, Option<tg::blob::read::Arg>),
) -> tg::Result<Bytes> {
	let (blob, arg) = args;
	let arg = arg.unwrap_or_default();
	let server = state.server.clone();
	let bytes = state
		.main_runtime_handle
		.spawn(async move {
			let mut reader = blob.read(&server, arg).await?;
			let mut buffer = Vec::new();
			reader
				.read_to_end(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
			Ok::<_, tg::Error>(buffer.into())
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(bytes)
}

pub async fn create(state: Rc<State>, args: (Either<String, Bytes>,)) -> tg::Result<tg::Blob> {
	let (bytes,) = args;
	let reader = Cursor::new(bytes);
	let server = state.server.clone();
	let blob = state
		.main_runtime_handle
		.spawn(async move { tg::Blob::with_reader(&server, reader).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
	Ok(blob)
}
