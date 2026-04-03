use {
	crate::prelude::*,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	tokio::io::AsyncBufRead,
	tokio_util::io::StreamReader,
};

impl tg::Blob {
	pub async fn read<H>(
		&self,
		handle: &H,
		options: tg::read::Options,
	) -> tg::Result<impl AsyncBufRead + Send + use<H>>
	where
		H: tg::Handle,
	{
		let handle = handle.clone();
		let id = self.store(&handle).await?.clone();
		let arg = tg::read::Arg { blob: id, options };
		let stream = handle.read(arg).boxed().await?.boxed();
		let reader = StreamReader::new(
			stream
				.map_ok(|chunk| chunk.bytes)
				.map_err(std::io::Error::other),
		);
		Ok(reader)
	}
}
