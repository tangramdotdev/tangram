use super::State;
use bytes::Bytes;
use std::{io::Cursor, rc::Rc};
use tangram_client as tg;
use tangram_either::Either;

pub async fn read(state: Rc<State>, args: (tg::Blob,)) -> tg::Result<Bytes> {
	let (blob,) = args;
	let server = state.server.clone();
	let bytes = state
		.main_runtime_handle
		.spawn(async move { blob.bytes(&server).await })
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(bytes.into())
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
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(blob)
}
