use {
	crate::session::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{collections::BTreeSet, pin::pin},
	tangram_client::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::ReaderStream,
};

struct Reader {
	buffer: Bytes,
	offset: usize,
	eof: bool,
	stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
}

impl Session {
	pub(crate) async fn run_control_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		process: &tg::process::Id,
		_location: Option<&tg::Location>,
		_stdin: tg::process::Stdio,
		stdin_blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		// Dump stdin if necessary.
		if let Some(blob) = stdin_blob {
			// TODO: handle graceful shutdown here?
			let reader = blob
				.read_with_handle(self, tg::read::Options::default())
				.await
				.map_err(|error| tg::error!(!error, "failed to read process stdin blob"))?;
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
			let stream = sandbox
				.write_stdio(
					sandbox_process,
					vec![tg::process::stdio::Stream::Stdin],
					stream,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
			let mut stream = pin!(stream);
			let _event = stream
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
		}

		// Create the request/response channels and streams.
		let (sender, responses) = tokio::sync::mpsc::channel(512);
		let responses = ReceiverStream::new(responses).boxed();
		let mut requests = self
			.try_get_process_control_stream_all(process, responses)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
			.ok_or_else(|| tg::error!("expected a control stream"))?
			.boxed();

		// Cache of open readers.
		let mut stdout = None;
		let mut stderr = None;

		// Cache of requests to deduplicate requests.
		let mut previous = BTreeSet::new();

		// Handle events.
		while let Some(event) = requests
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the next control request"))?
		{
			match event {
				tg::process::control::RequestEvent::Request(request) => {
					if !previous.insert(request.id) {
						continue;
					}

					match request.kind {
						tg::process::control::RequestKind::Read(read) => {
							let result = Self::handle_process_control_read_request(
								sandbox,
								sandbox_process,
								read,
								&mut stdout,
								&mut stderr,
							)
							.await
							.map(|response| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Read(response),
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Write(write) => {
							let result = Self::handle_process_control_write_request(
								sandbox,
								sandbox_process,
								write,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Write,
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Signal(signal) => {
							let result = Self::handle_process_control_signal_request(
								sandbox,
								sandbox_process,
								signal,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Signal,
									},
								)
							});
							sender.send(result).await.ok();
						},
						tg::process::control::RequestKind::Tty(tty) => {
							let result = Self::handle_process_control_tty_request(
								sandbox,
								sandbox_process,
								tty,
							)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id: request.id,
										kind: tg::process::control::ResponseKind::Tty,
									},
								)
							});
							sender.send(result).await.ok();
						},
					}
				},
				tg::process::control::RequestEvent::Stop => break,
			}
			let stdout_done = stdout.as_ref().is_none_or(|reader| reader.eof);
			let stderr_done = stderr.as_ref().is_none_or(|reader| reader.eof);
			if stdout_done && stderr_done {
				break;
			}
		}

		sender
			.send(Ok(tg::process::control::ResponseEvent::Stop))
			.await
			.ok();
		Ok(())
	}

	async fn handle_process_control_read_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::ReadRequest,
		stdout: &mut Option<Reader>,
		stderr: &mut Option<Reader>,
	) -> tg::Result<tg::process::control::ReadResponse> {
		let options = tangram_futures::retry::Options::default();
		let mut retries = pin!(
			tangram_futures::retry::stream(options.clone()).take(options.max_retries as usize + 1)
		);

		// Cannot use tangram_futures::retry(...) here due to lifetime requirements of closure captures.
		'retry: loop {
			if retries.next().await.is_none() {
				return Err(tg::error!("lost connection to the sandbox i/o"));
			}

			let reader = match request.stream {
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("cannot read the stdin of a process"));
				},
				tg::process::stdio::Stream::Stdout => &mut *stdout,
				tg::process::stdio::Stream::Stderr => &mut *stderr,
			};

			// Create the stream if it does not exist.
			if reader.is_none() {
				let stream = sandbox
					.read_stdio(sandbox_process, vec![request.stream])
					.await
					.map_err(|source| tg::error!(!source, "failed to create the stream"))?
					.boxed();
				reader.replace(Reader {
					buffer: Bytes::new(),
					offset: 0,
					eof: false,
					stream,
				});
			}

			let r = reader.as_mut().unwrap();

			// If the buffer has been fully read, fetch a single chunk.
			if r.offset >= r.buffer.len() && !r.eof {
				let Some(event) = r
					.stream
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the next event"))?
				else {
					reader.take();
					continue 'retry;
				};
				match event {
					tg::process::stdio::read::Event::Chunk(chunk) => {
						r.offset = 0;
						r.buffer = chunk.bytes;
					},
					tg::process::stdio::read::Event::End => {
						r.eof = true;
					},
				}
			}

			// Take up to the requested length from the buffer.
			let amount = (r.buffer.len() - r.offset).min(request.len);
			let bytes = r.buffer.slice(r.offset..r.offset + amount);
			r.offset += amount;

			return Ok(tg::process::control::ReadResponse {
				stream: request.stream,
				bytes,
			});
		}
	}

	async fn handle_process_control_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		write: tg::process::control::WriteRequest,
	) -> tg::Result<()> {
		let chunk = tg::process::stdio::Chunk {
			bytes: write.bytes,
			position: None,
			stream: write.stream,
		};
		let input = stream::once(future::ok(tg::process::stdio::read::Event::Chunk(chunk)));
		let output = sandbox
			.write_stdio(sandbox_process, vec![write.stream], input)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the process stdio"))?;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			match event {
				tg::process::stdio::write::Event::End => {
					break;
				},
				tg::process::stdio::write::Event::Stop => (),
			}
		}
		Ok(())
	}

	async fn handle_process_control_signal_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		signal: tg::process::control::SignalRequest,
	) -> tg::Result<()> {
		sandbox
			.kill(sandbox_process, signal.signal)
			.await
			.map_err(|error| tg::error!(!error, "failed to signal the process"))?;
		Ok(())
	}

	async fn handle_process_control_tty_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		tty: tg::process::control::TtyRequest,
	) -> tg::Result<()> {
		sandbox
			.set_tty_size(sandbox_process, tty.size)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the tty size"))?;
		Ok(())
	}
}
