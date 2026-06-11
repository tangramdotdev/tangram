use {
	crate::session::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tokio_util::io::ReaderStream,
};

pub(crate) struct RunControlTaskArg {
	pub sandbox: tangram_sandbox::Sandbox,
	pub sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub requests: BoxStream<'static, tg::Result<tg::process::control::RequestEvent>>,
	pub sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::control::ResponseEvent>>,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub stderr: tg::process::Stdio,
	pub stdin_blob: Option<tg::Blob>,
}

struct Reader {
	buffer: Bytes,
	offset: usize,
	eof: bool,
	stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
}

impl Session {
	pub(crate) async fn run_control_task(&self, arg: RunControlTaskArg) -> tg::Result<()> {
		let RunControlTaskArg {
			sandbox,
			sandbox_process,
			mut requests,
			sender,
			stdin,
			stdout,
			stderr,
			stdin_blob,
		} = arg;
		let (stdout_sender, mut stdout_receiver) =
			tokio::sync::mpsc::channel::<(uuid::Uuid, tg::process::control::ReadRequest)>(256);
		let stdout_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let piped = matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			async move {
				if !piped {
					return Ok(());
				}

				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = wait_for_sandbox_process(&mut sandbox_process).await;

				let mut reader = None;
				while let Some((id, request)) = stdout_receiver.recv().await {
					let response = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_read_request(
							&sandbox,
							sandbox_process,
							request,
							&mut reader,
						)
						.await
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::ReadResponse {
							stream: request.stream,
							bytes: Bytes::new(),
						})
					};
					let eof = response
						.as_ref()
						.is_ok_and(|response| response.bytes.is_empty());
					let result = response.map(|response| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Read(response),
							},
						)
					});
					sender.send(result).await.ok();
					if eof {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stderr_sender, mut stderr_receiver) =
			tokio::sync::mpsc::channel::<(uuid::Uuid, tg::process::control::ReadRequest)>(256);
		let stderr_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let piped = matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			async move {
				if !piped {
					return Ok(());
				}

				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = wait_for_sandbox_process(&mut sandbox_process).await;

				let mut reader = None;
				while let Some((id, request)) = stderr_receiver.recv().await {
					let response = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_read_request(
							&sandbox,
							sandbox_process,
							request,
							&mut reader,
						)
						.await
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::ReadResponse {
							stream: request.stream,
							bytes: Bytes::new(),
						})
					};
					let eof = response
						.as_ref()
						.is_ok_and(|response| response.bytes.is_empty());
					let result = response.map(|response| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Read(response),
							},
						)
					});
					sender.send(result).await.ok();
					if eof {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stdin_sender, mut stdin_receiver) =
			tokio::sync::mpsc::channel::<(uuid::Uuid, tg::process::control::WriteRequest)>(256);
		let stdin_task = tokio::spawn({
			let session = self.clone();
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let piped = matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = wait_for_sandbox_process(&mut sandbox_process).await;

				// Dump stdin if necessary.
				if let Some(blob) = stdin_blob
					&& let Some(sandbox_process) = &sandbox_process
				{
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
							sandbox_process,
							vec![tg::process::stdio::Stream::Stdin],
							stream,
						)
						.await
						.map_err(|source| tg::error!(!source, "failed to write stdin"))?;
					let mut stream = pin!(stream);
					while let Some(event) = stream
						.try_next()
						.await
						.map_err(|source| tg::error!(!source, "failed to write stdin"))?
					{
						if matches!(event, tg::process::stdio::write::Event::End) {
							break;
						}
					}
				}
				if !piped {
					return Ok(());
				}
				// Service write requests.
				while let Some((id, request)) = stdin_receiver.recv().await {
					let response = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_write_request(
							&sandbox,
							sandbox_process,
							request,
						)
						.await
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::WriteResponse { len: 0 })
					};
					let eof = response.as_ref().is_ok_and(|response| response.len == 0);
					let result = response.map(|response| {
						tg::process::control::ResponseEvent::Response(
							tg::process::control::Response {
								id,
								kind: tg::process::control::ResponseKind::Write(response),
							},
						)
					});
					sender.send(result).await.ok();
					if eof {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (signal_sender, mut signal_receiver) =
			tokio::sync::mpsc::channel::<(uuid::Uuid, tg::process::control::SignalRequest)>(256);
		let signal_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = wait_for_sandbox_process(&mut sandbox_process).await;

				while let Some((id, request)) = signal_receiver.recv().await {
					let result = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_signal_request(
							&sandbox,
							sandbox_process,
							request,
						)
						.await
					} else {
						Err(tg::error!("the process was not spawned"))
					}
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

		let (tty_sender, mut tty_receiver) =
			tokio::sync::mpsc::channel::<(uuid::Uuid, tg::process::control::TtyRequest)>(256);
		let tty_task = tokio::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = wait_for_sandbox_process(&mut sandbox_process).await;

				while let Some((id, request)) = tty_receiver.recv().await {
					let result = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_tty_request(&sandbox, sandbox_process, request)
							.await
					} else {
						Err(tg::error!("the process was not spawned"))
					}
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

		// Spawn a task to handle events.
		let handler_task = tokio::spawn({
			let sender = sender.clone();
			async move {
				while let Some(event) = requests.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get the next control request")
				})? {
					match event {
						tg::process::control::RequestEvent::Request(request) => {
							let id = request.id;
							match request.kind {
								tg::process::control::RequestKind::Read(read) => {
									match read.stream {
										tg::process::stdio::Stream::Stdout => {
											stdout_sender.send((id, read)).await.ok();
										},
										tg::process::stdio::Stream::Stderr => {
											stderr_sender.send((id, read)).await.ok();
										},
										tg::process::stdio::Stream::Stdin => {
											sender
												.send(Err(tg::error!(
													"cannot read the stdin of a process"
												)))
												.await
												.ok();
										},
									}
								},
								tg::process::control::RequestKind::Write(write) => {
									stdin_sender.send((id, write)).await.ok();
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
				Ok::<_, tg::Error>(())
			}
		});

		// Join the i/o tasks.
		for result in future::join_all([stdout_task, stderr_task, stdin_task]).await {
			result.map_err(|source| tg::error!(!source, "an i/o task panicked"))??;
		}

		// Abort the other tasks.
		signal_task.abort();
		tty_task.abort();
		handler_task.abort();

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
		let mut retries = pin!(tangram_futures::retry::stream(options.clone()));

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
	) -> tg::Result<tg::process::control::WriteResponse> {
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

		// Accumulate the lengths of the writes. A total length of zero indicates that
		// the stream has reached EOF.
		let mut len = 0;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			match event {
				tg::process::stdio::write::Event::Write(n) => {
					len += n;
				},
				tg::process::stdio::write::Event::End => {
					break;
				},
				tg::process::stdio::write::Event::Stop => (),
			}
		}
		Ok(tg::process::control::WriteResponse { len })
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

async fn wait_for_sandbox_process(
	receiver: &mut tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
) -> Option<Arc<tangram_sandbox::Process>> {
	receiver
		.wait_for(Option::is_some)
		.await
		.ok()
		.and_then(|sandbox_process| sandbox_process.as_ref().cloned())
}
