use {
	super::State,
	bytes::Bytes,
	futures::TryStreamExt as _,
	std::{io::Cursor, pin::pin, rc::Rc},
	tangram_client::{self as tg, prelude::*},
	tangram_either::Either,
	tangram_v8::Serde,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

pub async fn read(state: Rc<State>, args: (Serde<tg::read::Arg>,)) -> tg::Result<Bytes> {
	let (Serde(arg),) = args;
	let handle = state.handle.clone();
	let bytes = state
		.main_runtime_handle
		.spawn(async move {
			let stream = handle.read(arg).await?;
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

pub async fn write(
	state: Rc<State>,
	args: (Either<String, Bytes>,),
) -> tg::Result<Serde<tg::blob::Id>> {
	let (bytes,) = args;
	let reader = Cursor::new(bytes);
	let handle = state.handle.clone();
	let blob = state
		.main_runtime_handle
		.spawn(async move {
			let tg::write::Output { blob } = handle.write(reader).await?;
			Ok::<_, tg::Error>(blob)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
	Ok(Serde(blob))
}
