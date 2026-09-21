use {
	self::{
		output::RunProcessControlOutputTaskArg, signal::RunProcessControlSignalTaskArg,
		stdin::RunProcessControlStdinTaskArg, tty::RunProcessControlTtyTaskArg,
	},
	crate::{
		process::control::local::{self, Reply},
		session::Session,
	},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream,
	},
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

pub(super) type ProcessControlResponseReceiver = crate::control::Response<
	tg::process::control::ServerMessage,
	tg::process::control::ClientMessage,
>;

pub(super) struct RunProcessControlTaskArg {
	pub control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	pub exited: Stopper,
	pub finish: tokio::sync::oneshot::Receiver<ProcessControlResponseReceiver>,
	pub local: tokio::sync::mpsc::Receiver<local::Message>,
	pub log: Option<super::WriteProcessLogTaskArg>,
	pub push: tokio::sync::oneshot::Receiver<()>,
	pub retention_stopper: Stopper,
	pub sandbox: tangram_sandbox::Sandbox,
	pub sandbox_process: tokio::sync::watch::Receiver<Option<Arc<tangram_sandbox::Process>>>,
	pub stderr: tg::process::Stdio,
	pub stderr_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
	pub stderr_progress: Option<BoxStream<'static, tg::Result<Bytes>>>,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub stdout_buffered: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct RunProcessControlHandlerTaskArg {
	control: crate::control::Stream<
		tg::process::control::ServerMessage,
		tg::process::control::ClientMessage,
	>,
	local: tokio::sync::mpsc::Receiver<local::Message>,
	output_sender: tokio::sync::mpsc::Sender<output::Message>,
	sender: ProcessControlSender,
	signal_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::SignalServerRequestArg, Reply)>,
	stdin_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::WriteServerRequestArg, Reply)>,
	tty_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::TtyServerRequestArg, Reply)>,
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

	pub(super) async fn run_process_control_task(
		&self,
		arg: RunProcessControlTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlTaskArg {
			control,
			exited,
			finish,
			local,
			log,
			push,
			retention_stopper,
			sandbox,
			sandbox_process,
			stderr,
			stderr_buffered,
			stderr_progress,
			stdin,
			stdout,
			stdout_buffered,
		} = arg;
		let sender = control.sender();
		let (finished_sender, finished_receiver) = tokio::sync::oneshot::channel();
		let log_task = log.map(|arg| {
			let session = self.clone();
			let sender = sender.clone();
			Task::spawn(move |_| async move {
				session
					.write_process_log_task(arg, finished_receiver, sender)
					.boxed()
					.await
			})
		});

		let (output_sender, output_receiver) = tokio::sync::mpsc::channel::<output::Message>(256);
		let output_task = self.spawn_process_control_output_task(RunProcessControlOutputTaskArg {
			receiver: output_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			stderr,
			stderr_buffered,
			stderr_progress,
			stdout,
			stdout_buffered,
		});

		let (stdin_sender, stdin_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::WriteServerRequestArg,
			Reply,
		)>(256);
		let stdin_task = self.spawn_process_control_stdin_task(RunProcessControlStdinTaskArg {
			exited,
			receiver: stdin_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			stdin,
		});

		let (signal_sender, signal_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::SignalServerRequestArg,
			Reply,
		)>(256);
		let signal_task = self.spawn_process_control_signal_task(RunProcessControlSignalTaskArg {
			receiver: signal_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
		});

		let (tty_sender, tty_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::TtyServerRequestArg,
			Reply,
		)>(256);
		let tty_task = self.spawn_process_control_tty_task(RunProcessControlTtyTaskArg {
			receiver: tty_receiver,
			sandbox,
			sandbox_process,
		});

		let handler_task =
			self.spawn_process_control_handler_task(RunProcessControlHandlerTaskArg {
				control,
				local,
				output_sender,
				sender: sender.clone(),
				signal_sender,
				stdin_sender,
				tty_sender,
			});

		let receiver = finish
			.await
			.map_err(|_| tg::error!("failed to receive the process finish response receiver"))?;
		let output = Self::receive_process_control_client_response(receiver)
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the finish process response"))?;
		output
			.try_unwrap_finish()
			.map_err(|_| tg::error!("expected a finish process response"))?;
		finished_sender.send(()).ok();
		let log_result = if let Some(log_task) = log_task {
			match log_task.wait().await {
				Ok(result) => result,
				Err(error) => Err(tg::error!(!error, "the process control log task panicked")),
			}
		} else {
			Ok(())
		};

		// Retain control so waits can obtain the result sync token while its objects are in transit.
		push.await.ok();

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
		log_result?;

		Ok(())
	}

	pub(super) async fn send_process_control_client_request(
		sender: &ProcessControlSender,
		arg: tg::process::control::ClientRequestArg,
		priority: crate::control::Priority,
	) -> tg::Result<tg::process::control::ServerResponseOutput> {
		let receiver =
			Self::send_process_control_client_request_inner(sender, arg, priority).await?;
		Self::receive_process_control_client_response(receiver).await
	}

	pub(super) fn send_process_control_client_request_inner(
		sender: &ProcessControlSender,
		arg: tg::process::control::ClientRequestArg,
		priority: crate::control::Priority,
	) -> impl Future<Output = tg::Result<ProcessControlResponseReceiver>> + Send {
		let id = crate::control::id();
		let request =
			tg::process::control::ClientMessage::Request(tg::process::control::ClientRequest {
				arg,
				id,
			});
		sender.request(request, priority)
	}

	pub(super) async fn receive_process_control_client_response(
		response_receiver: ProcessControlResponseReceiver,
	) -> tg::Result<tg::process::control::ServerResponseOutput> {
		// Receive the response.
		let response = response_receiver
			.await
			.map_err(|_| tg::error!("the process control response stream ended"))?;
		let tg::process::control::ServerMessage::Response(response) = response else {
			return Err(tg::error!("expected a control response"));
		};
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error)
				.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;

			return Err(error);
		}
		let output = response
			.output
			.ok_or_else(|| tg::error!("missing the process control response output"))?;

		Ok(output)
	}

	fn spawn_process_control_handler_task(
		&self,
		arg: RunProcessControlHandlerTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| {
			async move { session.run_process_control_handler_task(arg).boxed().await }.inspect_err(
				|error| tracing::error!(error = %error.trace(), "the process control handler task failed"),
			)
		})
	}

	async fn run_process_control_handler_task(
		&self,
		arg: RunProcessControlHandlerTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlHandlerTaskArg {
			control,
			mut local,
			output_sender,
			sender,
			signal_sender,
			stdin_sender,
			tty_sender,
		} = arg;

		// Retain an in-flight receive across local requests because acknowledging a remote message can yield.
		let mut control = futures::stream::try_unfold(control, |mut control| async move {
			let event = control.recv_event_with_ack().await?;
			Ok::<_, tg::Error>(event.map(|event| (event, control)))
		})
		.boxed();
		let mut local_open = true;
		loop {
			let (message, sender) = tokio::select! {
				event = control.try_next() => {
					let Some(event) = event.map_err(|source| tg::error!(!source, "failed to get the next control request"))? else { break; };
					match event {
						tg::control::Event::Message(message) => (message, Reply::Remote(sender.clone())),
						tg::control::Event::Reconnect => {
							output_sender.send(output::Message::Reconnect).await.ok();
							continue;
						},
					}
				},
				message = local.recv(), if local_open => {
					match message {
						Some(local::Message::Close(id)) => { output_sender.send(output::Message::Close(id)).await.ok(); continue; },
						Some(local::Message::Progress(progress)) => { output_sender.send(output::Message::Progress(progress)).await.ok(); continue; },
						Some(local::Message::Request { request, sender }) => (tg::process::control::ServerMessage::Request(request), sender),
						None => { local_open = false; continue; },
					}
				},
			};
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
						tg::process::control::ServerRequestArg::Close(id) => {
							output_sender.send(output::Message::Close(id)).await.ok();
							let response = Self::process_control_response(
								request_id,
								Ok(tg::process::control::ClientResponseOutput::Close),
							);
							sender.send_low(response).await?;
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
											tg::process::control::GetClientResponseOutput {
												data,
												sync: None,
											},
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
									.send_low(Self::process_control_response(
										request_id,
										Err(error),
									))
									.await?;
							} else {
								let message = output::Message::Read {
									arg: read,
									id: request_id.clone(),
									sender: sender.clone(),
								};
								if output_sender.send(message).await.is_err() {
									let error =
										tg::error!("the process output reader has finished");
									let response =
										Self::process_control_response(request_id, Err(error));
									sender.send_low(response).await?;
								}
							}
						},
						tg::process::control::ServerRequestArg::ReleaseLease(arg) => {
							let result = self
								.release_process_lease(&arg)
								.map(tg::process::control::ClientResponseOutput::ReleaseLease);
							let response = Self::process_control_response(request_id, result);
							sender.send(response).await?;
						},
						tg::process::control::ServerRequestArg::Signal(signal) => {
							signal_sender
								.send((request_id, signal, sender))
								.await
								.map_err(|_| {
									tg::error!("failed to queue the process signal request")
								})?;
						},
						tg::process::control::ServerRequestArg::Tty(tty) => {
							tty_sender
								.send((request_id, tty, sender))
								.await
								.map_err(|_| {
									tg::error!("failed to queue the process tty request")
								})?;
						},
						tg::process::control::ServerRequestArg::Write(write) => {
							stdin_sender
								.send((request_id, write, sender))
								.await
								.map_err(|_| {
									tg::error!("failed to queue the process stdin request")
								})?;
						},
					}
				},
				tg::process::control::ServerMessage::Response(_) => {},
				tg::process::control::ServerMessage::Ack(_) => unreachable!(),
				tg::process::control::ServerMessage::Notification(notification) => {
					match notification {
						tg::process::control::ServerNotification::Read(notification) => {
							// Consumption progress may arrive after the reader has sent its terminal response.
							output_sender
								.send(output::Message::Progress(notification))
								.await
								.ok();
						},
					}
				},
			}
		}

		Ok(())
	}
}
