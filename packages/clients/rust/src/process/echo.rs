use {crate::prelude::*, futures::TryStreamExt as _, tokio::io::AsyncWriteExt as _};

impl<O> tg::Process<O> {
	/// Echo the process's logged stdout and stderr streams to this process's stdout and stderr, from the beginning of its log until it ends.
	pub async fn echo(&self, streams: Vec<tg::process::stdio::Stream>) -> tg::Result<()> {
		let instance = tg::instance()?;
		self.echo_with_instance(instance, streams).await
	}

	/// Echo the process's logged stdout and stderr streams to this process's stdout and stderr, from the beginning of its log until it ends.
	pub async fn echo_with_instance<I>(
		&self,
		instance: &I,
		streams: Vec<tg::process::stdio::Stream>,
	) -> tg::Result<()>
	where
		I: tg::Instance,
	{
		if streams.is_empty() {
			return Ok(());
		}

		// Read the logged streams until the log ends.
		let options = tg::process::stdio::read::Options {
			length: None,
			location: None,
			position: None,
			size: None,
			streams,
			timeout: None,
		};
		let Some(mut stdio) = self.try_read_stdio_with_instance(instance, options).await? else {
			return Ok(());
		};

		// Write each chunk to the stream that it came from.
		let mut stdout = tokio::io::stdout();
		let mut stderr = tokio::io::stderr();
		while let Some(chunk) = stdio.try_next().await? {
			match chunk.stream {
				tg::process::stdio::Stream::Stderr => {
					stderr
						.write_all(&chunk.bytes)
						.await
						.map_err(|error| tg::error!(!error, "failed to write to stderr"))?;
					stderr
						.flush()
						.await
						.map_err(|error| tg::error!(!error, "failed to flush stderr"))?;
				},
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
				tg::process::stdio::Stream::Stdout => {
					stdout
						.write_all(&chunk.bytes)
						.await
						.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
					stdout
						.flush()
						.await
						.map_err(|error| tg::error!(!error, "failed to flush stdout"))?;
				},
			}
		}

		Ok(())
	}
}
