use {
	crate::Server,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	std::pin::pin,
	tangram_client::prelude::*,
	tokio_util::io::ReaderStream,
};

impl Server {
	pub(crate) async fn run_stdin_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		location: Option<tg::location::Location>,
		mode: tg::process::Stdio,
		blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		// Create a blob stream if necessary.
		let blob_stream = if let Some(blob) = blob {
			let reader = blob
				.read_with_handle(self, tg::read::Options::default())
				.await
				.map_err(|source| tg::error!(!source, "failed to read process stdin blob"))?;
			let stream = ReaderStream::new(reader)
				.map_ok(|bytes| {
					tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
						bytes,
						position: None,
						stream: tg::process::stdio::Stream::Stdin,
					})
				})
				.map_err(|error| tg::error!(!error, "failed to read from the blob"))
				.boxed();
			Some(stream)
		} else {
			None
		};

		// Create the stdio stream if necessary.
		let stream = if matches!(mode, tg::process::Stdio::Pipe | tg::process::Stdio::Tty) {
			let arg = tg::process::stdio::read::Arg {
				locations: location.clone().map_or_else(
					tg::location::Locations::default,
					tg::location::Locations::from,
				),
				streams: vec![tg::process::stdio::Stream::Stdin],
				..Default::default()
			};
			let stream = self
				.try_read_process_stdio_all(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to read process stdin stream"))?
				.ok_or_else(
					|| tg::error!(process = %id, "expected the process stdin stream to exist"),
				)?
				.boxed();
			Some(stream)
		} else {
			None
		};

		// Chain the blob stream and stdio stream if necessary.
		let input = match (blob_stream, stream) {
			(Some(blob_stream), Some(stream)) => Some(blob_stream.chain(stream).boxed()),
			(Some(blob_stream), None) => Some(
				blob_stream
					.chain(stream::once(future::ok(
						tg::process::stdio::read::Event::End,
					)))
					.boxed(),
			),
			(None, Some(stream)) => Some(stream),
			(None, None) => None,
		};

		// Write.
		if let Some(input) = input {
			let output = sandbox
				.write_stdio(
					sandbox_process,
					vec![tg::process::stdio::Stream::Stdin],
					input,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to forward process stdin"))?;
			let mut output = pin!(output);
			while let Some(event) = output.try_next().await? {
				match event {
					tg::process::stdio::write::Event::End => {
						break;
					},
					tg::process::stdio::write::Event::Stop => (),
				}
			}
		}

		Ok(())
	}

	pub(crate) async fn run_stdout_stderr_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		location: Option<tg::location::Location>,
	) -> tg::Result<()> {
		let arg = tg::process::stdio::write::Arg {
			location,
			streams: vec![
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			],
		};
		let stream = sandbox
			.read_stdio(
				sandbox_process,
				vec![
					tg::process::stdio::Stream::Stdout,
					tg::process::stdio::Stream::Stderr,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio from sandbox"))?
			.boxed();
		self.write_process_stdio_all(id, arg, stream)
			.await
			.map_err(|source| tg::error!(!source, "failed to write stdio"))?;
		Ok(())
	}
}
