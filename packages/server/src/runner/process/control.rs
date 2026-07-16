use {
	self::{
		signal::RunProcessControlSignalTaskArg, stderr::RunProcessControlStderrTaskArg,
		stdin::RunProcessControlStdinTaskArg, stdout::RunProcessControlStdoutTaskArg,
		tty::RunProcessControlTtyTaskArg,
	},
	crate::session::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream},
	},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
};

mod signal;
mod stderr;
mod stdin;
mod stdout;
mod tty;

pub(crate) struct RunProcessControlTaskArg {
	pub finish: tokio::sync::oneshot::Receiver<tg::process::Data>,
	pub requests: BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
	pub retention_stopper: Stopper,
	pub sandbox: tangram_sandbox::Sandbox,
	pub sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub sender: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub stderr: tg::process::Stdio,
	pub stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub stdin: tg::process::Stdio,
	pub stdin_blob: Option<tg::Blob>,
	pub stdout: tg::process::Stdio,
}

pub(super) struct Reader {
	pub(super) buffer: Bytes,
	pub(super) eof: bool,
	pub(super) offset: usize,
	pub(super) stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
}

pub(super) type ProcessControlSender = crate::control::Sender<
	tg::process::control::ServerMessage,
	tg::process::control::ClientMessage,
>;

struct RunProcessControlHandlerTaskArg {
	control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	response_sender: tokio::sync::mpsc::Sender<tg::process::control::ServerResponse>,
	sender: ProcessControlSender,
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
				let lease = if process.data.status.is_finished() {
					None
				} else {
					let lease = Self::create_process_lease();
					process.leases.insert(lease.clone());
					Some(lease)
				};
				tg::process::control::AcquireLeaseClientResponseOutput {
					data: process.data.clone(),
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
					return Ok(tg::process::control::ReleaseLeaseClientResponseOutput {});
				}
				if !process.leases.remove(&arg.lease) {
					return Err(tg::error!("the process lease was not found"));
				}
				if process.leases.is_empty() {
					process.stopper.stop();
				}
				Ok(tg::process::control::ReleaseLeaseClientResponseOutput {})
			})
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?
	}

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
			stderr_progress,
			stdin,
			stdin_blob,
			stdout,
		} = arg;

		let control =
			crate::control::Stream::new(requests, sender, crate::control::stream_options());
		let sender = control.sender();
		let (response_sender, mut response_receiver) = tokio::sync::mpsc::channel(16);

		let (stdout_sender, stdout_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequestArg)>(256);
		let stdout_task = self.spawn_process_control_stdout_task(RunProcessControlStdoutTaskArg {
			receiver: stdout_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
			stdout,
		});

		let (stderr_sender, stderr_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequestArg)>(256);
		let stderr_task = self.spawn_process_control_stderr_task(RunProcessControlStderrTaskArg {
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

		let retention_ttl = self
			.server
			.config
			.runner
			.as_ref()
			.unwrap()
			.process_state_ttl;
		tokio::select! {
			() = retention_stopper.wait() => {},
			() = tokio::time::sleep(retention_ttl) => {},
		}

		// Abort the other tasks.
		handler_task.abort();
		stdout_task.abort();
		stderr_task.abort();
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
		Task::spawn(move |_| async move { session.run_process_control_handler_task(arg).await })
	}

	async fn run_process_control_handler_task(
		&self,
		arg: RunProcessControlHandlerTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlHandlerTaskArg {
			mut control,
			response_sender,
			sender,
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
										.get_mut(&sandbox)
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
								stderr_sender.send((request_id, read)).await.ok();
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
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: tg::process::control::ReadServerRequestArg,
		reader: &mut Option<Reader>,
		writes: &mut Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
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

			if reader.is_none() {
				reader.replace(
					Self::create_process_control_reader(sandbox, sandbox_process, &request, writes)
						.await?,
				);
			}

			let reader_mut = reader.as_mut().unwrap();

			if reader_mut.offset >= reader_mut.buffer.len() {
				if reader_mut.eof {
					return Ok(tg::process::control::ReadClientResponseOutput {
						stream: request.stream,
						bytes: Bytes::new(),
					});
				}

				let event = reader_mut
					.stream
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the next event"))?;
				let Some(event) = event else {
					reader.take();
					continue;
				};
				match event {
					tg::process::stdio::read::Event::Chunk(chunk) => {
						reader_mut.offset = 0;
						reader_mut.buffer = chunk.bytes;
					},
					tg::process::stdio::read::Event::End => {
						reader_mut.eof = true;
					},
				}
			}

			let amount = (reader_mut.buffer.len() - reader_mut.offset).min(request.length);
			let bytes = reader_mut
				.buffer
				.slice(reader_mut.offset..reader_mut.offset + amount);
			reader_mut.offset += amount;

			return Ok(tg::process::control::ReadClientResponseOutput {
				stream: request.stream,
				bytes,
			});
		}
	}

	async fn create_process_control_reader(
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		request: &tg::process::control::ReadServerRequestArg,
		writes: &mut Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
	) -> tg::Result<Reader> {
		let stream = sandbox
			.read_stdio(sandbox_process, vec![request.stream])
			.await
			.map_err(|source| tg::error!(!source, "failed to create the stream"))?
			.boxed();
		let stream = match writes.take() {
			Some(writes) => stream::select(stream, writes).boxed(),
			None => stream,
		};
		Ok(Reader {
			buffer: Bytes::new(),
			eof: false,
			offset: 0,
			stream,
		})
	}
}
