use super::State;
use bytes::Bytes;
use futures::TryStreamExt as _;
use std::{io::Cursor, pin::pin, rc::Rc};
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;
use tangram_v8::Serde;
use tokio::io::AsyncReadExt as _;
use tokio_util::io::StreamReader;

pub async fn read(
	state: Rc<State>,
	args: (Serde<tg::blob::Id>, Serde<tg::blob::read::Arg>),
) -> tg::Result<Bytes> {
	let (Serde(id), Serde(arg)) = args;
	let server = state.server.clone();
	let bytes = state
		.main_runtime_handle
		.spawn(async move {
			let stream = server.read_blob(&id, arg).await?;
			let reader = StreamReader::new(
				stream
					.map_ok(|chunk| chunk.bytes)
					.map_err(std::io::Error::other),
			);
			let mut buffer = Vec::new();
			pin!(reader)
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

pub async fn create(
	state: Rc<State>,
	args: (Either<String, Bytes>,),
) -> tg::Result<Serde<tg::blob::Id>> {
	let (bytes,) = args;
	let reader = Cursor::new(bytes);
	let server = state.server.clone();
	let blob = state
		.main_runtime_handle
		.spawn(async move {
			let tg::blob::create::Output { blob } = server.create_blob(reader).await?;
			Ok::<_, tg::Error>(blob)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
	Ok(Serde(blob))
}
