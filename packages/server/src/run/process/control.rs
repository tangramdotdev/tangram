use {
	crate::session::Session,
	bytes::Bytes,
	dashmap::DashMap,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tokio_util::io::ReaderStream,
};

pub(crate) struct RunControlTaskArg {
	pub exited: tokio::sync::oneshot::Receiver<()>,
	pub requests: BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
	pub sandbox: tangram_sandbox::Sandbox,
	pub sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::control::ClientMessage>>,
	pub stderr: tg::process::Stdio,
	pub stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub stdin: tg::process::Stdio,
	pub stdin_blob: Option<tg::Blob>,
	pub stdout: tg::process::Stdio,
}

struct Reader {
	buffer: Bytes,
	eof: bool,
	offset: usize,
	stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
}

#[derive(Clone, Debug)]
enum CachedProcessResponse {
	Pending,
	Ready(tg::process::control::ClientResponse),
}

impl Session {
	pub(crate) async fn run_control_task(&self, arg: RunControlTaskArg) -> tg::Result<()> {
		let RunControlTaskArg {
			exited,
			mut requests,
			sandbox,
			sandbox_process,
			sender,
			stderr,
			stderr_progress,
			stdin,
			stdin_blob,
			stdout,
		} = arg;

		// Get the TTL after which an acknowledged response is removed from the cache.
		let control_ttl = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(std::time::Duration::from_mins(1), |runner| {
				runner.control_ttl
			});

		let responses: Arc<DashMap<String, CachedProcessResponse>> = Arc::new(DashMap::new());

		let (stdout_sender, mut stdout_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequest)>(256);
		let stdout_task = Task::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let responses = responses.clone();
			let piped = matches!(stdout, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			move |_| async move {
				if !piped {
					return Ok(());
				}

				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = sandbox_process
					.wait_for(Option::is_some)
					.await
					.ok()
					.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

				let mut writes = None;
				let mut reader = None;
				while let Some((id, request)) = stdout_receiver.recv().await {
					let response = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_read_request(
							&sandbox,
							sandbox_process,
							request,
							&mut reader,
							&mut writes,
						)
						.await
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::ReadClientResponse {
							id: String::new(),
							stream: request.stream,
							bytes: Bytes::new(),
						})
					};
					let eof = response
						.as_ref()
						.is_ok_and(|response| response.bytes.is_empty());
					let response = match response {
						Ok(mut response) => {
							response.id = id.clone();
							tg::process::control::ClientResponse::Read(response)
						},
						Err(error) => tg::process::control::ClientResponse::Error(
							tg::process::control::ErrorClientResponse {
								id: id.clone(),
								error: error.to_data_or_id(),
							},
						),
					};
					responses.insert(id, CachedProcessResponse::Ready(response.clone()));
					sender
						.send(Ok(tg::process::control::ClientMessage::Response(response)))
						.await
						.ok();
					if eof {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stderr_sender, mut stderr_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequest)>(256);
		let stderr_task = Task::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let responses = responses.clone();
			let piped = matches!(stderr, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			move |_| async move {
				if !piped {
					return Ok(());
				}

				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = sandbox_process
					.wait_for(Option::is_some)
					.await
					.ok()
					.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

				// Serve the progress stream to readers along with the output of the sandbox process.
				let mut writes = stderr_progress.map(|progress| {
					progress
						.map_ok(|bytes| {
							tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
								bytes,
								position: None,
								stream: tg::process::stdio::Stream::Stderr,
							})
						})
						.boxed()
				});

				let mut reader = None;
				while let Some((id, request)) = stderr_receiver.recv().await {
					let response = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_read_request(
							&sandbox,
							sandbox_process,
							request,
							&mut reader,
							&mut writes,
						)
						.await
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::ReadClientResponse {
							id: String::new(),
							stream: request.stream,
							bytes: Bytes::new(),
						})
					};
					let eof = response
						.as_ref()
						.is_ok_and(|response| response.bytes.is_empty());
					let response = match response {
						Ok(mut response) => {
							response.id = id.clone();
							tg::process::control::ClientResponse::Read(response)
						},
						Err(error) => tg::process::control::ClientResponse::Error(
							tg::process::control::ErrorClientResponse {
								id: id.clone(),
								error: error.to_data_or_id(),
							},
						),
					};
					responses.insert(id, CachedProcessResponse::Ready(response.clone()));
					sender
						.send(Ok(tg::process::control::ClientMessage::Response(response)))
						.await
						.ok();
					if eof {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (stdin_sender, mut stdin_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::WriteServerRequest)>(256);
		let stdin_task = Task::spawn({
			let session = self.clone();
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let responses = responses.clone();
			let piped = matches!(stdin, tg::process::Stdio::Pipe | tg::process::Stdio::Tty);
			move |_| async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = sandbox_process
					.wait_for(Option::is_some)
					.await
					.ok()
					.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

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
						if request.bytes.is_empty() {
							// Empty bytes means EOF, so close stdin.
							Self::handle_process_control_stdin_close_request(
								&sandbox,
								sandbox_process,
							)
							.await
							.map(|()| tg::process::control::WriteClientResponse {
								id: String::new(),
								length: 0,
							})
						} else {
							Self::handle_process_control_write_request(
								&sandbox,
								sandbox_process,
								request,
							)
							.await
						}
					} else {
						// If the sandbox process was never spawned, then respond with EOF.
						Ok(tg::process::control::WriteClientResponse {
							id: String::new(),
							length: 0,
						})
					};
					let eof = response.as_ref().is_ok_and(|response| response.length == 0);
					let response = match response {
						Ok(mut response) => {
							response.id = id.clone();
							tg::process::control::ClientResponse::Write(response)
						},
						Err(error) => tg::process::control::ClientResponse::Error(
							tg::process::control::ErrorClientResponse {
								id: id.clone(),
								error: error.to_data_or_id(),
							},
						),
					};
					responses.insert(id, CachedProcessResponse::Ready(response.clone()));
					sender
						.send(Ok(tg::process::control::ClientMessage::Response(response)))
						.await
						.ok();
					if eof {
						break;
					}
				}

				Ok::<_, tg::Error>(())
			}
		});

		let (signal_sender, mut signal_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::SignalServerRequest)>(256);
		let signal_task = Task::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let responses = responses.clone();
			|_| async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = sandbox_process
					.wait_for(Option::is_some)
					.await
					.ok()
					.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

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
					};
					let response = match result {
						Ok(()) => tg::process::control::ClientResponse::Signal(
							tg::process::control::SignalClientResponse { id: id.clone() },
						),
						Err(error) => tg::process::control::ClientResponse::Error(
							tg::process::control::ErrorClientResponse {
								id: id.clone(),
								error: error.to_data_or_id(),
							},
						),
					};
					responses.insert(id, CachedProcessResponse::Ready(response.clone()));
					sender
						.send(Ok(tg::process::control::ClientMessage::Response(response)))
						.await
						.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		let (tty_sender, mut tty_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::TtyServerRequest)>(256);
		let tty_task = Task::spawn({
			let sandbox = sandbox.clone();
			let mut sandbox_process = sandbox_process.clone();
			let sender = sender.clone();
			let responses = responses.clone();
			|_| async move {
				// Wait until the sandbox process has spawned or it is known that it never will.
				let sandbox_process = sandbox_process
					.wait_for(Option::is_some)
					.await
					.ok()
					.and_then(|sandbox_process| sandbox_process.as_ref().cloned());

				while let Some((id, request)) = tty_receiver.recv().await {
					let result = if let Some(sandbox_process) = &sandbox_process {
						Self::handle_process_control_tty_request(&sandbox, sandbox_process, request)
							.await
					} else {
						Err(tg::error!("the process was not spawned"))
					};
					let response = match result {
						Ok(()) => tg::process::control::ClientResponse::Tty(
							tg::process::control::TtyClientResponse { id: id.clone() },
						),
						Err(error) => tg::process::control::ClientResponse::Error(
							tg::process::control::ErrorClientResponse {
								id: id.clone(),
								error: error.to_data_or_id(),
							},
						),
					};
					responses.insert(id, CachedProcessResponse::Ready(response.clone()));
					sender
						.send(Ok(tg::process::control::ClientMessage::Response(response)))
						.await
						.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn a task to handle messages.
		let handler_task = Task::spawn({
			let sender = sender.clone();
			let responses = responses.clone();
			|_| async move {
				while let Some(message) = requests.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get the next control request")
				})? {
					match message {
						tg::process::control::ServerMessage::Request(request) => {
							let id = request.id().to_owned();

							// If there is already an entry for the request, then it is a duplicate. Resend the cached response if there is one.
							let cached = responses.get(&id).map(|entry| entry.value().clone());
							if let Some(cached) = cached {
								// Return the cached response.
								if let CachedProcessResponse::Ready(response) = cached {
									sender
										.send(Ok(tg::process::control::ClientMessage::Response(
											response,
										)))
										.await
										.ok();
								}

								// Otherwise drop the request since the request is in flight or ack'd.
								continue;
							}

							// Mark the request as in flight so that duplicates are dropped until the response is ready.
							responses.insert(id.clone(), CachedProcessResponse::Pending);

							match request {
								tg::process::control::ServerRequest::Read(read) => {
									match read.stream {
										tg::process::stdio::Stream::Stdout => {
											stdout_sender.send((id, read)).await.ok();
										},
										tg::process::stdio::Stream::Stderr => {
											stderr_sender.send((id, read)).await.ok();
										},
										tg::process::stdio::Stream::Stdin => {
											// Reading stdin is forbidden. Send a correlated error response and cache it so that a retry is answered rather than suppressed by the in-flight entry.
											let error =
												tg::error!("cannot read the stdin of a process");
											let response =
												tg::process::control::ClientResponse::Error(
													tg::process::control::ErrorClientResponse {
														id: id.clone(),
														error: error.to_data_or_id(),
													},
												);
											responses.insert(
												id,
												CachedProcessResponse::Ready(response.clone()),
											);
											sender
												.send(Ok(
													tg::process::control::ClientMessage::Response(
														response,
													),
												))
												.await
												.ok();
										},
									}
								},
								tg::process::control::ServerRequest::Write(write) => {
									match write.stream {
										tg::process::stdio::Stream::Stdin => {
											stdin_sender.send((id, write)).await.ok();
										},
										tg::process::stdio::Stream::Stdout
										| tg::process::stdio::Stream::Stderr => {
											// Writes to stdout and stderr are forbidden. The progress stream is served to readers directly.
											let error = tg::error!(
												"cannot write to the stdout or stderr of a process"
											);
											let response =
												tg::process::control::ClientResponse::Error(
													tg::process::control::ErrorClientResponse {
														id: id.clone(),
														error: error.to_data_or_id(),
													},
												);
											responses.insert(
												id,
												CachedProcessResponse::Ready(response.clone()),
											);
											sender
												.send(Ok(
													tg::process::control::ClientMessage::Response(
														response,
													),
												))
												.await
												.ok();
										},
									}
								},
								tg::process::control::ServerRequest::Signal(signal) => {
									signal_sender.send((id, signal)).await.ok();
								},
								tg::process::control::ServerRequest::Tty(tty) => {
									tty_sender.send((id, tty)).await.ok();
								},
							}
						},
						tg::process::control::ServerMessage::Ack(ack) => {
							let id = ack.id;
							responses.insert(id.clone(), CachedProcessResponse::Pending);
							tokio::spawn({
								let responses = responses.clone();
								async move {
									tokio::time::sleep(control_ttl).await;
									responses.remove(&id);
								}
							});
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Wait until the sandbox process has exited or will never be spawned.
		exited.await.ok();

		// Join the output tasks, which finish when the process's stdout and stderr reach EOF.
		for result in future::join_all([stdout_task.wait(), stderr_task.wait()]).await {
			result.map_err(|source| tg::error!(!source, "an i/o task panicked"))??;
		}

		// Abort the other tasks.
		stdin_task.abort();
		signal_task.abort();
		tty_task.abort();
		handler_task.abort();

		drop(sender);
		Ok(())
	}

	async fn handle_process_control_read_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::ReadServerRequest,
		reader: &mut Option<Reader>,
		writes: &mut Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	) -> tg::Result<tg::process::control::ReadClientResponse> {
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

			// Create the stream if it does not exist, merging in the buffered writes if they have not been consumed.
			if reader.is_none() {
				let stream = sandbox
					.read_stdio(sandbox_process, vec![request.stream])
					.await
					.map_err(|source| tg::error!(!source, "failed to create the stream"))?
					.boxed();
				let stream = match writes.take() {
					Some(writes) => stream::select(stream, writes).boxed(),
					None => stream,
				};
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
			let amount = (r.buffer.len() - r.offset).min(request.length);
			let bytes = r.buffer.slice(r.offset..r.offset + amount);
			r.offset += amount;

			return Ok(tg::process::control::ReadClientResponse {
				id: String::new(),
				stream: request.stream,
				bytes,
			});
		}
	}

	async fn handle_process_control_write_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		write: tg::process::control::WriteServerRequest,
	) -> tg::Result<tg::process::control::WriteClientResponse> {
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

		// Accumulate the lengths of the writes. A total length of zero indicates that the stream has reached EOF.
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
		Ok(tg::process::control::WriteClientResponse {
			id: String::new(),
			length: len,
		})
	}

	async fn handle_process_control_stdin_close_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
	) -> tg::Result<()> {
		let input = stream::once(future::ok(tg::process::stdio::read::Event::End));
		let output = sandbox
			.write_stdio(
				sandbox_process,
				vec![tg::process::stdio::Stream::Stdin],
				input,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to close the process stdin"))?;
		let mut output = pin!(output);
		while let Some(event) = output.try_next().await? {
			if matches!(event, tg::process::stdio::write::Event::End) {
				break;
			}
		}
		Ok(())
	}

	async fn handle_process_control_signal_request(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		signal: tg::process::control::SignalServerRequest,
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
		tty: tg::process::control::TtyServerRequest,
	) -> tg::Result<()> {
		sandbox
			.set_tty_size(sandbox_process, tty.size)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the tty size"))?;
		Ok(())
	}
}
