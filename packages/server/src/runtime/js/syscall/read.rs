use super::State;
use bytes::Bytes;
use std::rc::Rc;
use tangram_client as tg;

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
