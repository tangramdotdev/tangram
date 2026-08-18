use {
	self::{
		signal::RunProcessControlSignalTaskArg, stderr::RunProcessControlStderrTaskArg,
		stdin::RunProcessControlStdinTaskArg, stdout::RunProcessControlStdoutTaskArg,
		tty::RunProcessControlTtyTaskArg,
	},
	crate::session::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tokio_stream::wrappers::UnboundedReceiverStream,
};

mod signal;
mod stderr;
mod stdin;
mod stdout;
mod tty;

pub(super) type ProcessControlSender = crate::control::Sender<
	tg::process::control::ServerMessage,
	tg::process::control::ClientMessage,
>;

const PROCESS_CONTROL_READER_BUFFER_CAPACITY: usize = 4096;

pub(crate) struct RunProcessControlTaskArg {
	pub finish: tokio::sync::oneshot::Receiver<tg::process::Data>,
	pub requests: BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
	pub retention_stopper: Stopper,
	pub sandbox: tangram_sandbox::Sandbox,
	pub sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub sender: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub stderr: tg::process::Stdio,
	pub stderr_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub stdin: tg::process::Stdio,
	pub stdin_blob: Option<tg::Blob>,
	pub stdout: tg::process::Stdio,
	pub stdout_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

pub(super) struct Reader {
	pub(super) buffer: Bytes,
	pub(super) eof: bool,
	pub(super) offset: usize,
	permit: Option<tokio::sync::OwnedSemaphorePermit>,
	stream: BoxStream<'static, tg::Result<BufferedChunk>>,
}

struct BufferedChunk {
	bytes: Bytes,
	permit: tokio::sync::OwnedSemaphorePermit,
}

struct RunProcessControlHandlerTaskArg {
	control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	response_sender: tokio::sync::mpsc::Sender<tg::process::control::ServerResponse>,
	sender: ProcessControlSender,
	shared_tty: bool,
	signal_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::SignalServerRequestArg)>,
	stderr_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::ReadServerRequestArg)>,
	stdin_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::WriteServerRequestArg)>,
	stdout_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::ReadServerRequestArg)>,
	tty_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::TtyServerRequestArg)>,
}

impl Session {
	fn acquire_process_lease(
		&self,
	) -> tg::Result<tg::process::control::AcquireLeaseClientResponseOutput> {
		let tg::Principal::Process(id) = &self.context.principal else {
			return Err(tg::error!("expected a process principal"));
		};
		self.server
			.runner
			.state
			.try_update_process(id, |process| {
				let lease = if process.data.status.is_finished() || process.stopper.stopped() {
					None
				} else {
					let lease = Self::create_process_lease();
					process.leases.insert(lease.clone());
					Some(lease)
				};
				tg::process::control::AcquireLeaseClientResponseOutput {
					data: process.data(),
					lease,
				}
			})
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))
	}

	fn release_process_lease(
		&self,
		arg: &tg::process::control::ReleaseLeaseServerRequestArg,
	) -> tg::Result<tg::process::control::ReleaseLeaseClientResponseOutput> {
		let tg::Principal::Process(id) = &self.context.principal else {
			return Err(tg::error!("expected a process principal"));
		};
		self.server
			.runner
			.state
			.try_update_process(id, |process| {
				if process.data.status.is_finished() {
					return tg::process::control::ReleaseLeaseClientResponseOutput {
						released: false,
					};
				}
				let released = process.leases.remove(&arg.lease);
				if released && process.leases.is_empty() {
					process.stopper.stop();
				}
				tg::process::control::ReleaseLeaseClientResponseOutput { released }
			})
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))
	}

	#[must_use]
	pub(super) fn process_control_response(
		id: String,
		result: tg::Result<tg::process::control::ClientResponseOutput>,
	) -> tg::process::control::ClientMessage {
		let (error, output) = match result {
			Ok(output) => {
				let error = None;
				let output = Some(output);
				(error, output)
			},
			Err(error) => {
				let error = Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				});
				let output = None;
				(error, output)
			},
		};
		tg::process::control::ClientMessage::Response(tg::process::control::ClientResponse {
			error,
			id,
			output,
		})
	}

	pub(crate) async fn run_process_control_task(
		&self,
		arg: RunProcessControlTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlTaskArg {
			finish,
			requests,
			retention_stopper,
			sandbox,
			sandbox_process,
			sender,
			stderr,
			stderr_buffered,
			stderr_progress,
			stdin,
			stdin_blob,
			stdout,
			stdout_buffered,
		} = arg;
		let shared_tty =
			matches!(stderr, tg::process::Stdio::Tty) && matches!(stdout, tg::process::Stdio::Tty);
		let (stderr, stderr_progress, stdout_progress) = if shared_tty {
			(tg::process::Stdio::Null, None, stderr_progress)
		} else {
			(stderr, stderr_progress, None)
		};

		let control =
			crate::control::Stream::new(requests, sender, crate::control::stream_options());
		let sender = control.sender();
		let (response_sender, mut response_receiver) = tokio::sync::mpsc::channel(16);

		let (stdout_sender, stdout_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequestArg)>(256);
		let stdout_task = self.spawn_process_control_stdout_task(RunProcessControlStdoutTaskArg {
			buffered: stdout_buffered,
			progress: stdout_progress,
			receiver: stdout_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
			stdout,
		});

		let (stderr_sender, stderr_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequestArg)>(256);
		let stderr_task = self.spawn_process_control_stderr_task(RunProcessControlStderrTaskArg {
			buffered: stderr_buffered,
			receiver: stderr_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
			stderr,
			stderr_progress,
		});

		let (stdin_sender, stdin_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::WriteServerRequestArg,
		)>(256);
		let stdin_task = self.spawn_process_control_stdin_task(RunProcessControlStdinTaskArg {
			receiver: stdin_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
			stdin,
			stdin_blob,
		});

		let (signal_sender, signal_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::SignalServerRequestArg,
		)>(256);
		let signal_task = self.spawn_process_control_signal_task(RunProcessControlSignalTaskArg {
			receiver: signal_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
		});

		let (tty_sender, tty_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::TtyServerRequestArg)>(256);
		let tty_task = self.spawn_process_control_tty_task(RunProcessControlTtyTaskArg {
			receiver: tty_receiver,
			sandbox,
			sandbox_process,
			sender: sender.clone(),
		});

		let handler_task =
			self.spawn_process_control_handler_task(RunProcessControlHandlerTaskArg {
				control,
				response_sender,
				sender: sender.clone(),
				shared_tty,
				signal_sender,
				stderr_sender,
				stdin_sender,
				stdout_sender,
				tty_sender,
			});

		let data = finish
			.await
			.map_err(|_| tg::error!("failed to receive the finished process data"))?;
		let request_id = crate::control::id();
		let request =
			tg::process::control::ClientMessage::Request(tg::process::control::ClientRequest {
				arg: tg::process::control::ClientRequestArg::Finish(
					tg::process::control::FinishClientRequestArg { data },
				),
				id: request_id.clone(),
			});
		sender
			.send(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the finish process request"))?;
		let response = loop {
			let response = response_receiver
				.recv()
				.await
				.ok_or_else(|| tg::error!("the process control response stream ended"))?;
			if response.id == request_id {
				break response;
			}
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
			return Err(tg::error!(!error, "the finish process request failed"));
		}
		let output = response
			.output
			.ok_or_else(|| tg::error!("missing finish process response output"))?;
		output
			.try_unwrap_finish()
			.map_err(|_| tg::error!("expected a finish process response"))?;

		let stdio_tasks = async {
			let (stderr_result, stdout_result) =
				tokio::join!(stderr_task.wait(), stdout_task.wait());
			stderr_result.map_err(|error| {
				tg::error!(!error, "the process control stderr task panicked")
			})??;
			stdout_result.map_err(|error| {
				tg::error!(!error, "the process control stdout task panicked")
			})??;
			sender.wait_for_empty().await;

			Ok::<_, tg::Error>(())
		};
		let retention_ttl = self.server.config.runner.process_state_ttl;
		tokio::select! {
			result = stdio_tasks => result?,
			() = retention_stopper.wait() => {},
			() = tokio::time::sleep(retention_ttl) => {},
		}

		// Abort the other tasks.
		handler_task.abort();
		stdin_task.abort();
		signal_task.abort();
		tty_task.abort();

		Ok(())
	}

	fn spawn_process_control_handler_task(
		&self,
		arg: RunProcessControlHandlerTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_handler_task(arg).await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control handler task failed"),
			)
		})
	}

	async fn run_process_control_handler_task(
		&self,
		arg: RunProcessControlHandlerTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlHandlerTaskArg {
			mut control,
			response_sender,
			sender,
			shared_tty,
			signal_sender,
			stderr_sender,
			stdin_sender,
			stdout_sender,
			tty_sender,
		} = arg;

		while let Some(message) = control
			.recv()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the next control request"))?
		{
			match message {
				tg::process::control::ServerMessage::Request(message) => {
					let request_id = message.id;

					match message.arg {
						tg::process::control::ServerRequestArg::AcquireLease(_) => {
							let result = self
								.acquire_process_lease()
								.map(tg::process::control::ClientResponseOutput::AcquireLease);
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await.ok();
						},
						tg::process::control::ServerRequestArg::Finish(finish) => {
							let result = (|| match &self.context.principal {
								tg::Principal::Process(process_id) => {
									let sandbox = self
										.server
										.runner
										.state
										.try_get_process_sandbox(process_id)
										.ok_or_else(
											|| tg::error!(%process_id, "failed to find the process sandbox"),
										)?;
									let mut sandbox = self
										.server
										.runner
										.state
										.sandboxes
										.get_mut_by_id(&sandbox)
										.ok_or_else(
											|| tg::error!(%process_id, "failed to find the sandbox"),
										)?;
									let process =
										sandbox.processes.get_mut(process_id).ok_or_else(
											|| tg::error!(%process_id, "failed to find the process"),
										)?;
									if !process.data.status.is_finished() {
										process.finish.get_or_insert(finish);
										process.stopper.stop();
									}
									Ok(tg::process::control::ClientResponseOutput::Finish(
										tg::process::control::FinishClientResponseOutput {},
									))
								},
								_ => Err(tg::error!("expected a process principal")),
							})();
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await.ok();
						},
						tg::process::control::ServerRequestArg::Get(_) => {
							let result = match &self.context.principal {
								tg::Principal::Process(process) => self
									.server
									.runner
									.state
									.try_get_process(process)
									.map(|data| {
										tg::process::control::ClientResponseOutput::Get(
											tg::process::control::GetClientResponseOutput { data },
										)
									})
									.ok_or_else(
										|| tg::error!(%process, "failed to find the process"),
									),
								_ => Err(tg::error!("expected a process principal")),
							};
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await.ok();
						},
						tg::process::control::ServerRequestArg::GetChildren(arg) => {
							let result = match &self.context.principal {
								tg::Principal::Process(process) => self
									.server
									.runner
									.state
									.try_get_process_children(process, arg.position, arg.length)
									.map(tg::process::control::ClientResponseOutput::GetChildren)
									.ok_or_else(
										|| tg::error!(%process, "failed to find the process"),
									),
								_ => Err(tg::error!("expected a process principal")),
							};
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await.ok();
						},
						tg::process::control::ServerRequestArg::Read(read) => match read.stream {
							tg::process::stdio::Stream::Stdout => {
								stdout_sender.send((request_id, read)).await.ok();
							},
							tg::process::stdio::Stream::Stderr => {
								if shared_tty {
									stdout_sender.send((request_id, read)).await.ok();
								} else {
									stderr_sender.send((request_id, read)).await.ok();
								}
							},
							tg::process::stdio::Stream::Stdin => {
								let error = tg::error!("cannot read the stdin of a process");
								sender
									.send(Self::process_control_response(request_id, Err(error)))
									.await
									.ok();
							},
						},
						tg::process::control::ServerRequestArg::ReleaseLease(arg) => {
							let result = self
								.release_process_lease(&arg)
								.map(tg::process::control::ClientResponseOutput::ReleaseLease);
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await.ok();
						},
						tg::process::control::ServerRequestArg::Write(write) => {
							match write.stream {
								tg::process::stdio::Stream::Stdin => {
									stdin_sender.send((request_id, write)).await.ok();
								},
								tg::process::stdio::Stream::Stdout
								| tg::process::stdio::Stream::Stderr => {
									let error = tg::error!(
										"cannot write to the stdout or stderr of a process"
									);
									sender
										.send(Self::process_control_response(
											request_id,
											Err(error),
										))
										.await
										.ok();
								},
							}
						},
						tg::process::control::ServerRequestArg::Signal(signal) => {
							signal_sender.send((request_id, signal)).await.ok();
						},
						tg::process::control::ServerRequestArg::Tty(tty) => {
							tty_sender.send((request_id, tty)).await.ok();
						},
					}
				},
				tg::process::control::ServerMessage::Response(response) => {
					response_sender.send(response).await.ok();
				},
				tg::process::control::ServerMessage::Ack(_) => unreachable!(),
				tg::process::control::ServerMessage::Notification(notification) => {
					match notification {}
				},
			}
		}

		Ok(())
	}

	pub(super) async fn handle_process_control_read_request(
		request: tg::process::control::ReadServerRequestArg,
		reader: &mut Reader,
	) -> tg::Result<tg::process::control::ReadClientResponseOutput> {
		if matches!(request.stream, tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("cannot read the stdin of a process"));
		}

		let options = tangram_futures::retry::Options::default();
		let mut retries = pin!(tangram_futures::retry::stream(options.clone()));

		// Cannot use tangram_futures::retry(...) here due to lifetime requirements of closure captures.
		loop {
			if retries.next().await.is_none() {
				return Err(tg::error!("lost connection to the sandbox i/o"));
			}

			if reader.offset >= reader.buffer.len() {
				reader.permit = None;
				if reader.eof {
					return Ok(tg::process::control::ReadClientResponseOutput {
						stream: request.stream,
						bytes: Bytes::new(),
					});
				}

				let chunk = reader
					.stream
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the next event"))?;
				let Some(chunk) = chunk else {
					reader.eof = true;
					continue;
				};
				reader.buffer = chunk.bytes;
				reader.offset = 0;
				reader.permit = Some(chunk.permit);
			}

			let amount = (reader.buffer.len() - reader.offset).min(request.length);
			let bytes = reader.buffer.slice(reader.offset..reader.offset + amount);
			reader.offset += amount;
			let permit = reader
				.permit
				.as_mut()
				.and_then(|permit| permit.split(amount))
				.ok_or_else(|| tg::error!("the process stdio buffer is inconsistent"))?;
			drop(permit);

			return Ok(tg::process::control::ReadClientResponseOutput {
				stream: request.stream,
				bytes,
			});
		}
	}

	pub(super) async fn create_process_control_reader(
		&self,
		buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: Option<&tangram_sandbox::Process>,
		stream: tg::process::stdio::Stream,
		writes: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	) -> tg::Result<Reader> {
		crate::checkpoint!(
			self.server,
			"runner.process.control.reader.create",
			stream = %stream,
		)
		.await;

		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let semaphore = Arc::new(tokio::sync::Semaphore::new(
			PROCESS_CONTROL_READER_BUFFER_CAPACITY,
		));
		let mut tasks = Vec::new();

		// Drain the sandbox stream into the bounded buffer.
		if let Some(sandbox_process) = sandbox_process {
			let input = match sandbox.read_stdio(sandbox_process, vec![stream]).await {
				Ok(input) => input.boxed(),
				Err(source) => {
					let error = tg::error!(!source, "failed to create the stream");
					buffered.send(Err(error.clone())).ok();

					return Err(error);
				},
			};
			let sender = sender.clone();
			let semaphore = semaphore.clone();
			let task = Task::spawn(move |_| async move {
				let result =
					Self::run_process_control_reader_task(input, sender.clone(), semaphore).await;
				if let Err(error) = &result {
					sender.send(Err(error.clone())).ok();
				}
				buffered.send(result).ok();
			});
			tasks.push(task);
		} else {
			// A process that was never spawned produces no sandbox output.
			buffered.send(Ok(())).ok();
		}

		// Buffer progress independently because it can outlive the sandbox stream.
		if let Some(mut writes) = writes {
			let sender = sender.clone();
			let semaphore = semaphore.clone();
			let task = Task::spawn(move |_| async move {
				while let Some(event) = writes.next().await {
					match event {
						Ok(tg::process::stdio::read::Event::Chunk(chunk)) => {
							if Self::buffer_process_control_chunk(chunk.bytes, &sender, &semaphore)
								.await
								.is_err()
							{
								break;
							}
						},
						Ok(tg::process::stdio::read::Event::End) => break,
						Err(error) => {
							sender.send(Err(error)).ok();

							break;
						},
					}
				}
			});
			tasks.push(task);
		}
		drop(sender);

		let stream = UnboundedReceiverStream::new(receiver).attach(tasks).boxed();
		Ok(Reader {
			buffer: Bytes::new(),
			eof: false,
			offset: 0,
			permit: None,
			stream,
		})
	}

	async fn run_process_control_reader_task(
		mut input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		sender: tokio::sync::mpsc::UnboundedSender<tg::Result<BufferedChunk>>,
		semaphore: Arc<tokio::sync::Semaphore>,
	) -> tg::Result<()> {
		loop {
			let event = input
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the next event"))?
				.ok_or_else(|| tg::error!("the sandbox stdio stream ended unexpectedly"))?;
			match event {
				tg::process::stdio::read::Event::Chunk(chunk) => {
					Self::buffer_process_control_chunk(chunk.bytes, &sender, &semaphore).await?;
				},
				tg::process::stdio::read::Event::End => break,
			}
		}

		Ok(())
	}

	async fn buffer_process_control_chunk(
		mut bytes: Bytes,
		sender: &tokio::sync::mpsc::UnboundedSender<tg::Result<BufferedChunk>>,
		semaphore: &Arc<tokio::sync::Semaphore>,
	) -> tg::Result<()> {
		while !bytes.is_empty() {
			let length = bytes.len().min(PROCESS_CONTROL_READER_BUFFER_CAPACITY);
			let permit = semaphore
				.clone()
				.acquire_many_owned(length.try_into().unwrap())
				.await
				.map_err(|_| tg::error!("the process stdio buffer was closed"))?;
			let bytes = bytes.split_to(length);
			sender
				.send(Ok(BufferedChunk { bytes, permit }))
				.map_err(|_| tg::error!("failed to buffer process stdio"))?;
		}

		Ok(())
	}
}
