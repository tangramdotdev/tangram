use {
	super::Result,
	crate::quickjs::{
		StateHandle,
		serde::Serde,
		types::{Either, Uint8Array},
	},
	futures::TryStreamExt as _,
	rquickjs as qjs,
	std::{io::Cursor, pin::pin},
	tangram_client::prelude::*,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

pub async fn read(ctx: qjs::Ctx<'_>, arg: Serde<tg::read::Arg>) -> Result<Uint8Array> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result: tg::Result<_> = async {
		let bytes = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
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
					Ok::<_, tg::Error>(buffer)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
		Ok(Uint8Array::from(bytes))
	}
	.await;
	Result(result)
}

pub async fn write(
	ctx: qjs::Ctx<'_>,
	bytes: Either<String, Uint8Array>,
) -> Result<Serde<tg::blob::Id>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let bytes: Vec<u8> = match bytes {
		Either::Left(left) => left.into_bytes(),
		Either::Right(right) => right.0.to_vec(),
	};
	let result = async {
		let reader = Cursor::new(bytes);
		let blob_id = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let tg::write::Output { blob } = handle.write(reader).await?;
					Ok::<_, tg::Error>(blob)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
		Ok(Serde(blob_id))
	}
	.await;
	Result(result)
}
