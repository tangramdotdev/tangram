use {
	crate::prelude::*,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	tokio::io::AsyncBufRead,
	tokio_util::io::StreamReader,
};

impl tg::Blob {
	pub async fn read(&self, options: tg::read::Options) -> tg::Result<impl AsyncBufRead + Send> {
		let instance = tg::instance()?;
		self.read_with_instance(instance, options).await
	}

	pub async fn read_with_instance<I>(
		&self,
		instance: &I,
		options: tg::read::Options,
	) -> tg::Result<impl AsyncBufRead + Send + use<I>>
	where
		I: tg::Instance,
	{
		let instance = instance.clone();
		let id = self.store_with_instance(&instance).await?.clone();
		let tokens = self.state().tokens();
		let arg = tg::read::Arg {
			blob: id,
			options,
			tokens,
		};
		let stream = instance.read(arg).boxed().await?.boxed();
		let reader = StreamReader::new(
			stream
				.map_ok(|chunk| chunk.bytes)
				.map_err(std::io::Error::other),
		);
		Ok(reader)
	}
}
