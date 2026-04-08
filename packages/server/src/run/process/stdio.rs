use {
	crate::Server,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::ReaderStream,
};

impl Server {
	pub(crate) async fn run_stdin_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		remote: Option<String>,
		mode: tg::process::Stdio,
		blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		// Create a blob stream if necessary.
		let blob_stream = if let Some(blob) = blob {
			let reader = blob
				.read(self, tg::read::Options::default())
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
				local: None,
				remotes: remote.map(|remote| vec![remote]),
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
		remote: Option<String>,
		stdout: tg::process::Stdio,
		stderr: tg::process::Stdio,
	) -> tg::Result<()> {
		// Create the read stream.
		let stream = sandbox
			.read_stdio(
				sandbox_process,
				vec![
					tg::process::stdio::Stream::Stdout,
					tg::process::stdio::Stream::Stderr,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio from sandbox"))?;

		// Create the stdout task.
		let stdout = if matches!(
			stdout,
			tg::process::Stdio::Log | tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			let (sender, receiver) =
				tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
			let server = self.clone();
			let id = id.clone();
			let arg = tg::process::stdio::write::Arg {
				local: None,
				remotes: remote.clone().map(|remote| vec![remote]),
				streams: vec![tg::process::stdio::Stream::Stdout],
			};
			let task = Task::spawn(move |_| async move {
				let input = ReceiverStream::new(receiver).boxed();
				server
					.write_process_stdio_all(&id, arg, input)
					.await
					.map_err(|source| tg::error!(!source, "failed to write stdout"))
			});
			Some((sender, task))
		} else {
			None
		};

		// Create the stderr task.
		let stderr = if matches!(
			stderr,
			tg::process::Stdio::Log | tg::process::Stdio::Pipe | tg::process::Stdio::Tty
		) {
			let (sender, receiver) =
				tokio::sync::mpsc::channel::<tg::Result<tg::process::stdio::read::Event>>(1);
			let server = self.clone();
			let id = id.clone();
			let arg = tg::process::stdio::write::Arg {
				local: None,
				remotes: remote.clone().map(|remote| vec![remote]),
				streams: vec![tg::process::stdio::Stream::Stderr],
			};
			let task = Task::spawn(move |_| async move {
				let input = ReceiverStream::new(receiver).boxed();
				server
					.write_process_stdio_all(&id, arg, input)
					.await
					.map_err(|source| tg::error!(!source, "failed to write stderr"))
			});
			Some((sender, task))
		} else {
			None
		};

		// Consume the stream and send the events.
		let mut stream = pin!(stream);
		while let Some(event) = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio stream"))?
		{
			match event {
				tg::process::stdio::read::Event::Chunk(chunk) => {
					if chunk.bytes.is_empty() {
						continue;
					}
					let sender = match chunk.stream {
						tg::process::stdio::Stream::Stdin => {
							return Err(tg::error!("unexpected stdin chunk"));
						},
						tg::process::stdio::Stream::Stdout => {
							stdout.as_ref().map(|(sender, _)| sender)
						},
						tg::process::stdio::Stream::Stderr => {
							stderr.as_ref().map(|(sender, _)| sender)
						},
					};
					if let Some(sender) = sender {
						sender
							.send(Ok(tg::process::stdio::read::Event::Chunk(chunk)))
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to forward process stdio")
							})?;
					}
				},
				tg::process::stdio::read::Event::End => {
					break;
				},
			}
		}

		// Send the end events and await the tasks.
		if let Some((sender, task)) = stdout {
			sender
				.send(Ok(tg::process::stdio::read::Event::End))
				.await
				.map_err(|source| tg::error!(!source, "failed to close stdout"))?;
			drop(sender);
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stdout task panicked"))??;
		}
		if let Some((sender, task)) = stderr {
			sender
				.send(Ok(tg::process::stdio::read::Event::End))
				.await
				.map_err(|source| tg::error!(!source, "failed to close stderr"))?;
			drop(sender);
			task.wait()
				.await
				.map_err(|source| tg::error!(!source, "the stderr task panicked"))??;
		}

		Ok(())
	}
}
