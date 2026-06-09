use {
	crate::session::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{collections::BTreeSet, pin::pin, sync::Arc},
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
		sandbox: tangram_sandbox::Sandbox,
		sandbox_process: Arc<tangram_sandbox::Process>,
		process: &tg::process::Id,
		_location: Option<&tg::Location>,
		stdin: tg::process::Stdio,
		stdout: tg::process::Stdio,
		stderr: tg::process::Stdio,
		stdin_blob: Option<tg::Blob>,
	) -> tg::Result<()> {
		let stdin_pipe = matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
		let stdout_pipe = matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
		let stderr_pipe = matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);

		// Create the request/response channels and streams.
		let (sender, responses) = tokio::sync::mpsc::channel(512);
		let responses = ReceiverStream::new(responses).boxed();
		let mut requests = self
			.try_get_process_control_stream_all(process, responses)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the control stream"))?
			.ok_or_else(|| tg::error!("expected a control stream"))?
			.boxed();

		let (stdout_sender, mut stdout_receiver) = tokio::sync::mpsc::channel::<(
			uuid::Uuid,
			tg::process::control::ReadRequest,
		)>(256);
		let stdout_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				let mut reader = None;
				while let Some((id, request)) = stdout_receiver.recv().await {
					let result = Self::handle_process_control_read_request(
						&sandbox,
						&sandbox_process,
						request,
						&mut reader,
					)
					.await
					.map(|response| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Read(response),
							},
						)
					});
					sender.send(result).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stderr_sender, mut stderr_receiver) = tokio::sync::mpsc::channel::<(
			uuid::Uuid,
			tg::process::control::ReadRequest,
		)>(256);
		let stderr_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				let mut reader = None;
				while let Some((id, request)) = stderr_receiver.recv().await {
					let result = Self::handle_process_control_read_request(
						&sandbox,
						&sandbox_process,
						request,
						&mut reader,
					)
					.await
					.map(|response| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Read(response),
							},
						)
					});
					sender.send(result).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stdin_sender, mut stdin_receiver) = tokio::sync::mpsc::channel::<(
			uuid::Uuid,
			tg::process::control::WriteRequest,
		)>(256);
		let stdin_task = tokio::spawn({
			let session = self.clone();
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				// Dump stdin if necessary.
				if let Some(blob) = stdin_blob {
					let reader = blob
						.read_with_handle(&session, tg::read::Options::default())
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
							&sandbox_process,
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

				// Service write requests.
				while let Some((id, request)) = stdin_receiver.recv().await {
					let result = Self::handle_process_control_write_request(
						&sandbox,
						&sandbox_process,
						request,
					)
					.await
					.map(|()| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Write,
							},
						)
					});
					sender.send(result).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (signal_sender, mut signal_receiver) = tokio::sync::mpsc::channel::<(
			uuid::Uuid,
			tg::process::control::SignalRequest,
		)>(256);
		let signal_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				while let Some((id, request)) = signal_receiver.recv().await {
					let result = Self::handle_process_control_signal_request(
						&sandbox,
						&sandbox_process,
						request,
					)
					.await
					.map(|()| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Signal,
							},
						)
					});
					sender.send(result).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (tty_sender, mut tty_receiver) = tokio::sync::mpsc::channel::<(
			uuid::Uuid,
			tg::process::control::TtyRequest,
		)>(256);
		let tty_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				while let Some((id, request)) = tty_receiver.recv().await {
					let result =
						Self::handle_process_control_tty_request(&sandbox, &sandbox_process, request)
							.await
							.map(|()| {
								tg::process::control::ResponseEvent::Response(
									tg::process::control::Response {
										id,
										kind: tg::process::control::ResponseKind::Tty,
									},
								)
							});
					sender.send(result).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Cache of requests to deduplicate requests.
		let mut previous = BTreeSet::new();

		// Handle events
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
					let id = request.id;
					match request.kind {
						tg::process::control::RequestKind::Read(read) => match read.stream {
							tg::process::stdio::Stream::Stdout if stdout_pipe => {
								stdout_sender.send((id, read)).await.ok();
							},
							tg::process::stdio::Stream::Stderr if stderr_pipe => {
								stderr_sender.send((id, read)).await.ok();
							},
							tg::process::stdio::Stream::Stdin => {
								sender
									.send(Err(tg::error!("cannot read the stdin of a process")))
									.await
									.ok();
							},
							tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr => {
								sender
									.send(Err(tg::error!("the stream is not a pipe")))
									.await
									.ok();
							},
						},
						tg::process::control::RequestKind::Write(write) => {
							if stdin_pipe {
								stdin_sender.send((id, write)).await.ok();
							} else {
								sender
									.send(Err(tg::error!("stdin is not a pipe")))
									.await
									.ok();
							}
						},
						tg::process::control::RequestKind::Signal(signal) => {
							signal_sender.send((id, signal)).await.ok();
						},
						tg::process::control::RequestKind::Tty(tty) => {
							tty_sender.send((id, tty)).await.ok();
						},
					}
				},
				tg::process::control::RequestEvent::End => break,
			}
		}

		// Close the task channels and wait for the tasks to finish.
		drop(stdout_sender);
		drop(stderr_sender);
		drop(stdin_sender);
		drop(signal_sender);
		drop(tty_sender);
		for result in future::join_all([
			stdout_task,
			stderr_task,
			stdin_task,
			signal_task,
			tty_task,
		])
		.await
		{
			result.map_err(|source| tg::error!(!source, "a control task panicked"))??;
		}

		sender
			.send(Ok(tg::process::control::ResponseEvent::End))
			.await
			.ok();
		Ok(())
	}

	async fn handle_process_control_read_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::ReadRequest,
		reader: &mut Option<Reader>,
	) -> tg::Result<tg::process::control::ReadResponse> {
		if matches!(request.stream, tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("cannot read the stdin of a process"));
		}

		let options = tangram_futures::retry::Options::default();
		let mut retries = pin!(
			tangram_futures::retry::stream(options.clone()).take(options.max_retries as usize + 1)
		);

		// Cannot use tangram_futures::retry(...) here due to lifetime requirements of closure captures.
		'retry: loop {
			if retries.next().await.is_none() {
				return Err(tg::error!("lost connection to the sandbox i/o"));
			}

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
