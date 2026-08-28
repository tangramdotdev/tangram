use {
	self::{
		output::RunProcessControlOutputTaskArg, signal::RunProcessControlSignalTaskArg,
		stdin::RunProcessControlStdinTaskArg, tty::RunProcessControlTtyTaskArg,
	},
	crate::session::Session,
	bytes::Bytes,
	futures::{TryFutureExt as _, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
};

mod output;
mod signal;
mod stdin;
mod tty;

pub(super) type ProcessControlSender = crate::control::Sender<
	tg::process::control::ServerMessage,
	tg::process::control::ClientMessage,
>;

pub(crate) struct RunProcessControlTaskArg {
	pub exited: Stopper,
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

struct RunProcessControlHandlerTaskArg {
	control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	output_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::ReadServerRequestArg)>,
	response_sender: tokio::sync::mpsc::Sender<tg::process::control::ServerResponse>,
	sender: ProcessControlSender,
	signal_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::SignalServerRequestArg)>,
	stdin_sender: tokio::sync::mpsc::Sender<(String, tg::process::control::WriteServerRequestArg)>,
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
			exited,
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
		let control =
			crate::control::Stream::new(requests, sender, crate::control::stream_options());
		let sender = control.sender();
		let (response_sender, mut response_receiver) = tokio::sync::mpsc::channel(16);

		let (output_sender, output_receiver) =
			tokio::sync::mpsc::channel::<(String, tg::process::control::ReadServerRequestArg)>(256);
		let output_task = self.spawn_process_control_output_task(RunProcessControlOutputTaskArg {
			receiver: output_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			sender: sender.clone(),
			stderr,
			stderr_buffered,
			stderr_progress,
			stdout,
			stdout_buffered,
		});

		let (stdin_sender, stdin_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::WriteServerRequestArg,
		)>(256);
		let stdin_task = self.spawn_process_control_stdin_task(RunProcessControlStdinTaskArg {
			exited,
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
				output_sender,
				response_sender,
				sender: sender.clone(),
				signal_sender,
				stdin_sender,
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

		let stdio_task = async {
			output_task.wait().await.map_err(|error| {
				tg::error!(!error, "the process control output task panicked")
			})??;
			sender.wait_for_empty().await;

			Ok::<_, tg::Error>(())
		};
		let retention_ttl = self.server.config.runner.process_state_ttl;
		tokio::select! {
			result = stdio_task => result?,
			() = retention_stopper.wait() => {},
			() = tokio::time::sleep(retention_ttl) => {},
		}

		crate::checkpoint!(self.server, "runner.process.control.retention.finished").await;

		// Abort the other tasks.
		handler_task.abort();
		stdin_task.abort();
		signal_task.abort();
		tty_task.abort();

		crate::checkpoint!(self.server, "runner.process.control.finished").await;

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
			output_sender,
			response_sender,
			sender,
			signal_sender,
			stdin_sender,
			tty_sender,
		} = arg;

		while let Some(message) = control
			.recv_with_ack()
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
							sender.send(response).await?;
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
									let sandbox = self
										.server
										.runner
										.state
										.sandboxes
										.get_by_id(&sandbox)
										.ok_or_else(
											|| tg::error!(%process_id, "failed to find the sandbox"),
										)?;
									let mut process =
										sandbox.processes.get_mut_by_id(process_id).ok_or_else(
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
							sender.send(response).await?;
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
							sender.send(response).await?;
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
							sender.send(response).await?;
						},
						tg::process::control::ServerRequestArg::Read(read) => {
							if read.streams.contains(&tg::process::stdio::Stream::Stdin) {
								let error = tg::error!("cannot read the stdin of a process");
								sender
									.send(Self::process_control_response(request_id, Err(error)))
									.await?;
							} else {
								output_sender.send((request_id, read)).await.map_err(|_| {
									tg::error!("failed to queue the process output request")
								})?;
							}
						},
						tg::process::control::ServerRequestArg::ReleaseLease(arg) => {
							let result = self
								.release_process_lease(&arg)
								.map(tg::process::control::ClientResponseOutput::ReleaseLease);
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await?;
						},
						tg::process::control::ServerRequestArg::Write(write) => {
							match write.chunk.stream {
								tg::process::stdio::Stream::Stdin => {
									stdin_sender.send((request_id, write)).await.map_err(|_| {
										tg::error!("failed to queue the process stdin request")
									})?;
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
										.await?;
								},
							}
						},
						tg::process::control::ServerRequestArg::Signal(signal) => {
							signal_sender
								.send((request_id, signal))
								.await
								.map_err(|_| {
									tg::error!("failed to queue the process signal request")
								})?;
						},
						tg::process::control::ServerRequestArg::Tty(tty) => {
							tty_sender.send((request_id, tty)).await.map_err(|_| {
								tg::error!("failed to queue the process tty request")
							})?;
						},
					}
				},
				tg::process::control::ServerMessage::Response(response) => {
					response_sender
						.send(response)
						.await
						.map_err(|_| tg::error!("failed to queue the process control response"))?;
				},
				tg::process::control::ServerMessage::Ack(_) => unreachable!(),
				tg::process::control::ServerMessage::Notification(notification) => {
					match notification {}
				},
			}
		}

		Ok(())
	}
}
