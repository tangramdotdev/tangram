use {
	self::{
		signal::RunProcessControlSignalTaskArg, stdin::RunProcessControlStdinTaskArg,
		tty::RunProcessControlTtyTaskArg,
	},
	super::stdio::{self, RunProcessStdioTaskArg},
	crate::{
		process::control::local::{self, Reply},
		session::Session,
	},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream,
	},
	std::{collections::VecDeque, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tracing::Instrument as _,
};

mod signal;
mod stdin;
mod tty;

pub(super) type ProcessControlSender = tg::process::control::Sender;

pub(super) type ProcessControlResponseReceiver = tg::process::control::Response;

pub(super) struct RunProcessControlTaskArg {
	pub control: tg::process::control::Connection,
	pub exited: Stopper,
	pub finish: tokio::sync::oneshot::Receiver<ProcessControlResponseReceiver>,
	pub local: tokio::sync::mpsc::Receiver<local::Message>,
	pub location: tg::Location,
	pub outcome_sync: tokio::sync::oneshot::Receiver<()>,
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
	control: tg::process::control::Connection,
	local: tokio::sync::mpsc::Receiver<local::Message>,
	location: tg::Location,
	sender: ProcessControlSender,
	signal_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::SignalServerRequestArg, Reply)>,
	stdin_sender: tokio::sync::mpsc::Sender<stdin::Write>,
	stdio_sender: tokio::sync::mpsc::Sender<stdio::Message>,
	tty_sender:
		tokio::sync::mpsc::Sender<(String, tg::process::control::TtyServerRequestArg, Reply)>,
}

/// The data written to a relay, which the reader acknowledges.
#[derive(Default)]
struct RelaySent {
	ended: bool,
	items: VecDeque<RelaySentItem>,
	position: Option<u64>,
}

/// A written chunk's end in combined coordinates, which the write reports, and in the read's coordinates, which the kept chunks are released by.
struct RelaySentItem {
	combined: u64,
	end: u64,
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
					let lease = tg::process::Lease::new();
					process.leases.insert(lease);
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
					code: error.code(),
					kind: error.kind(),
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

	#[tracing::instrument(name = "process.control", level = "debug", skip_all, fields(process = %self.context.principal), err(level = "debug"))]
	pub(super) async fn run_process_control_task(
		&self,
		arg: RunProcessControlTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlTaskArg {
			control,
			exited,
			finish,
			local,
			location,
			outcome_sync,
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
		let capacity = self.server.config.runner.process_control_channel_capacity;

		let (stdio_sender, stdio_receiver) = tokio::sync::mpsc::channel::<stdio::Message>(capacity);
		let stdio_task = self.spawn_process_stdio_task(RunProcessStdioTaskArg {
			receiver: stdio_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
			stderr,
			stderr_buffered,
			stderr_progress,
			stdout,
			stdout_buffered,
		});

		let (stdin_sender, stdin_receiver) = tokio::sync::mpsc::channel::<stdin::Write>(capacity);
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
		)>(capacity);
		let signal_task = self.spawn_process_control_signal_task(RunProcessControlSignalTaskArg {
			receiver: signal_receiver,
			sandbox: sandbox.clone(),
			sandbox_process: sandbox_process.clone(),
		});

		let (tty_sender, tty_receiver) = tokio::sync::mpsc::channel::<(
			String,
			tg::process::control::TtyServerRequestArg,
			Reply,
		)>(capacity);
		let tty_task = self.spawn_process_control_tty_task(RunProcessControlTtyTaskArg {
			receiver: tty_receiver,
			sandbox,
			sandbox_process,
		});

		let handler_task =
			self.spawn_process_control_handler_task(RunProcessControlHandlerTaskArg {
				control,
				local,
				location,
				sender: sender.clone(),
				signal_sender,
				stdin_sender,
				stdio_sender,
				tty_sender,
			});

		let receiver = finish
			.await
			.map_err(|_| tg::error!("failed to receive the process finish response receiver"))?;
		let started = std::time::Instant::now();
		tracing::debug!(process = %self.context.principal, "waiting for the process finish response");
		let output = Self::receive_process_control_client_response(receiver)
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the finish process response"))?;
		output
			.try_unwrap_finish()
			.map_err(|_| tg::error!("expected a finish process response"))?;
		tracing::info!(elapsed = ?started.elapsed(), process = %self.context.principal, "received the process finish response");

		crate::checkpoint!(self.server, "runner.process.control.finish.succeeded").await;

		// Retain control until the command and outcome pushes complete.
		outcome_sync.await.ok();

		let stdio_task = async {
			stdio_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the process stdio task panicked"))??;
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

	pub(super) fn send_process_control_client_request_inner(
		sender: &ProcessControlSender,
		arg: tg::process::control::ClientRequestArg,
		priority: tg::process::control::Priority,
	) -> impl Future<Output = tg::Result<ProcessControlResponseReceiver>> + Send {
		let id = crate::control::id();
		let request =
			tg::process::control::ClientMessage::Request(tg::process::control::ClientRequest {
				arg,
				id,
			});
		sender.request(request, priority).boxed()
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
			async move { session.run_process_control_handler_task(arg).boxed().await }
				.in_current_span()
				.inspect_err(
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
			location,
			sender,
			signal_sender,
			stdin_sender,
			stdio_sender,
			tty_sender,
		} = arg;

		// Retain an in-flight receive across local requests because acknowledging a remote message can yield.
		let mut control = futures::stream::try_unfold(control, |mut control| async move {
			let event = control.recv_event_with_ack().await?;
			Ok::<_, tg::Error>(event.map(|event| (event, control)))
		})
		.boxed();
		let mut local_open = true;
		let mut relays = tokio::task::JoinSet::new();
		loop {
			let (message, sender) = tokio::select! {
				Some(_) = relays.join_next(), if !relays.is_empty() => continue,
				event = control.try_next() => {
					let Some(event) = event.map_err(|source| tg::error!(!source, "failed to get the next control request"))? else { break; };
					match event {
						tg::control::Event::Message(message) => (message, Reply::Remote(sender.clone())),
						tg::control::Event::Reconnect => continue,
					}
				},
				message = local.recv(), if local_open => {
					match message {
						Some(local::Message::Ack { position, streams }) => {
							let message = stdio::Message::Ack { position, streams };
							stdio_sender.send(message).await.ok();
							continue;
						},
						Some(local::Message::Read { arg, sender }) => {
							Self::open_process_stdio_read(&stdio_sender, arg, sender).await;
							continue;
						},
						Some(local::Message::Request { request, sender }) => (tg::process::control::ServerMessage::Request(request), sender),
						Some(local::Message::Write { data, sender }) => {
							let write = stdin::Write { data, sender };
							if let Err(tokio::sync::mpsc::error::SendError(write)) = stdin_sender.send(write).await {
								let error = tg::error!("the process stdin task has finished");
								write.sender.send(Err(error)).ok();
							}
							continue;
						},
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
						tg::process::control::ServerRequestArg::Open(open)
							if open
								.arg
								.streams
								.contains(&tg::process::stdio::Stream::Stdin) =>
						{
							self.open_process_stdin_relay(
								&mut relays,
								&stdin_sender,
								&location,
								request_id,
								open.arg,
								sender,
							);
						},
						tg::process::control::ServerRequestArg::Open(open) => {
							self.open_process_stdio_relay(
								&mut relays,
								&stdio_sender,
								&location,
								request_id,
								open.arg,
								sender,
							)
							.await;
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
					}
				},
				tg::process::control::ServerMessage::Notification(
					tg::process::control::ServerNotification::Sync(_),
				)
				| tg::process::control::ServerMessage::Response(_) => {},
				tg::process::control::ServerMessage::Ack(_) => unreachable!(),
			}
		}

		// Wait for the relays to finish, so that they are aborted only with this task.
		while relays.join_next().await.is_some() {}

		Ok(())
	}

	async fn open_process_stdio_read(
		stdio_sender: &tokio::sync::mpsc::Sender<stdio::Message>,
		arg: tg::process::stdio::read::Arg,
		sender: stdio::Sender,
	) {
		let message = stdio::Message::Read { arg, sender };
		if let Err(error) = stdio_sender.send(message).await
			&& let stdio::Message::Read { sender, .. } = error.0
		{
			let error = tg::error!("the process output reader has finished");
			sender.try_send(Err(error)).ok();
		}
	}

	async fn open_process_stdio_relay(
		&self,
		relays: &mut tokio::task::JoinSet<()>,
		stdio_sender: &tokio::sync::mpsc::Sender<stdio::Message>,
		location: &tg::Location,
		id: String,
		arg: tg::process::stdio::read::Arg,
		reply: Reply,
	) {
		// Write the read's bytes to the relay, and answer the open request when the write ends.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let streams = arg.streams.clone();
		Self::open_process_stdio_read(stdio_sender, arg, sender).await;
		let session = self.clone();
		let location = location.clone();
		let stdio_sender = stdio_sender.clone();
		relays.spawn(
			async move {
				let result = session
					.write_process_stdio_relay(
						&location,
						id.clone(),
						streams,
						receiver,
						&stdio_sender,
					)
					.await
					.map(|()| {
						tg::process::control::ClientResponseOutput::Open(
							tg::process::control::OpenClientResponseOutput {},
						)
					});
				let response = Self::process_control_response(id, result);
				reply.send_low(response).await.ok();
			}
			.in_current_span(),
		);
	}

	async fn write_process_stdio_relay(
		&self,
		location: &tg::Location,
		open: String,
		streams: Vec<tg::process::stdio::Stream>,
		receiver: tokio::sync::mpsc::Receiver<tg::Result<tg::process::stdio::read::ServerMessage>>,
		stdio_sender: &tokio::sync::mpsc::Sender<stdio::Message>,
	) -> tg::Result<()> {
		let tg::Principal::Process(id) = &self.context.principal else {
			return Err(tg::error!("expected a process principal"));
		};

		// Convert the read's messages to the write's data, ending the body at the read's limit or timeout. Record each item's combined end, which the write reports, with its end in the read's coordinates, which the kept chunks are released by.
		let combined = streams.len() > 1;
		let sent = Arc::new(std::sync::Mutex::new(RelaySent::default()));
		let input = futures::stream::unfold(Some(receiver), |receiver| async move {
			let mut receiver = receiver?;
			loop {
				let message = receiver.recv().await.unwrap_or_else(|| {
					Err(tg::error!(
						code = tg::error::Code::Cancellation,
						"the process read was superseded"
					))
				});
				match message {
					Ok(tg::process::stdio::read::ServerMessage::Notification(
						tg::process::stdio::read::Event::Chunk(chunk),
					)) => {
						let data = tg::process::stdio::write::Data::Chunk(chunk);
						return Some((Ok(data), Some(receiver)));
					},
					Ok(tg::process::stdio::read::ServerMessage::Notification(_)) => (),
					Ok(tg::process::stdio::read::ServerMessage::Response(
						tg::process::stdio::read::Output::End(end),
					)) => {
						let data = tg::process::stdio::write::Data::End(end);
						return Some((Ok(data), None));
					},
					Ok(tg::process::stdio::read::ServerMessage::Response(_)) => return None,
					Err(error) => return Some((Err(error), None)),
				}
			}
		})
		.inspect({
			let sent = sent.clone();
			move |result| {
				let mut sent = sent.lock().unwrap();
				match result {
					Ok(tg::process::stdio::write::Data::Chunk(chunk)) => {
						let length = chunk.bytes.len() as u64;
						let end = if combined {
							chunk.combined_position + length
						} else {
							chunk.stream_position + length
						};
						let item = RelaySentItem {
							combined: chunk.combined_position + length,
							end,
						};
						sent.items.push_back(item);
						sent.position = Some(chunk.combined_position + length);
					},
					Ok(tg::process::stdio::write::Data::End(end)) => {
						sent.ended = true;
						sent.position = Some(end.combined_position);
					},
					Err(_) => (),
				}
			}
		})
		.fuse()
		.boxed();

		// Write to the relay and wait for the write to complete.
		let arg = tg::process::stdio::write::stream::Arg {
			id: Some(open),
			location: Some(location.clone().into()),
			streams: streams.clone(),
			tokens: tg::authorization::Tokens::default(),
		};
		let mut output = self
			.try_write_process_stdio(&tg::Either::Right(id.clone()), arg, input)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		// Release the kept chunks as the reader acknowledges them.
		let mut acknowledged = None::<tg::process::stdio::write::Output>;
		while let Some(value) = output.try_next().await? {
			let mut position = None;
			{
				let mut sent = sent.lock().unwrap();
				while sent
					.items
					.front()
					.is_some_and(|item| item.combined <= value.position)
				{
					position = sent.items.pop_front().map(|item| item.end);
				}
			}
			if let Some(position) = position {
				let message = stdio::Message::Ack {
					position,
					streams: streams.clone(),
				};
				stdio_sender.send(message).await.ok();
			}

			acknowledged = Some(value);
		}

		// The write succeeded only if the reader acknowledged everything sent.
		let (position, ended) = {
			let sent = sent.lock().unwrap();
			(sent.position, sent.ended)
		};
		let complete = match (position, acknowledged) {
			(None, _) => true,
			(Some(_), Some(output)) if ended => output.closed,
			(Some(position), Some(output)) => output.position >= position,
			(Some(_), None) => false,
		};
		if !complete {
			return Err(tg::error!(
				unavailable,
				"the stdio relay write ended before it was acknowledged"
			));
		}

		Ok(())
	}

	fn open_process_stdin_relay(
		&self,
		relays: &mut tokio::task::JoinSet<()>,
		stdin_sender: &tokio::sync::mpsc::Sender<stdin::Write>,
		location: &tg::Location,
		id: String,
		arg: tg::process::stdio::read::Arg,
		reply: Reply,
	) {
		// Read the stdin bytes from the relay, and answer the open request when stdin closes.
		let session = self.clone();
		let stdin_sender = stdin_sender.clone();
		let location = location.clone();
		relays.spawn(
			async move {
				let result = if arg.streams.as_slice() == [tg::process::stdio::Stream::Stdin] {
					session
						.read_process_stdin_relay(&location, id.clone(), &stdin_sender)
						.await
				} else {
					Err(tg::error!(
						"cannot read stdin and stdout or stderr in a single request"
					))
				};
				let result = result.map(|()| {
					tg::process::control::ClientResponseOutput::Open(
						tg::process::control::OpenClientResponseOutput {},
					)
				});
				let response = Self::process_control_response(id, result);
				reply.send_low(response).await.ok();
			}
			.in_current_span(),
		);
	}

	async fn read_process_stdin_relay(
		&self,
		location: &tg::Location,
		open: String,
		stdin_sender: &tokio::sync::mpsc::Sender<stdin::Write>,
	) -> tg::Result<()> {
		let tg::Principal::Process(id) = &self.context.principal else {
			return Err(tg::error!("expected a process principal"));
		};

		// Read the relay, reporting each written position on the request body.
		let (written_sender, written_receiver) = tokio::sync::watch::channel(0);
		let input = tokio_stream::wrappers::WatchStream::from_changes(written_receiver)
			.map(|position| Ok(tg::process::stdio::read::ClientMessage::Ack { position }))
			.boxed();
		let arg = tg::process::stdio::read::Arg {
			id: Some(open),
			location: Some(location.clone().into()),
			streams: vec![tg::process::stdio::Stream::Stdin],
			..Default::default()
		};
		let mut output = self
			.try_read_process_stdio(&tg::Either::Right(id.clone()), arg, input)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		// Write each chunk into the process.
		let mut written_sender = Some(written_sender);
		let mut ended = false;
		while let Some(message) = output.try_next().await? {
			let data = match message {
				tg::process::stdio::read::ServerMessage::Notification(
					tg::process::stdio::read::Event::Chunk(chunk),
				) => tg::process::stdio::write::Data::Chunk(chunk),
				tg::process::stdio::read::ServerMessage::Notification(_) => continue,
				tg::process::stdio::read::ServerMessage::Response(
					tg::process::stdio::read::Output::End(end),
				) => tg::process::stdio::write::Data::End(end),
				tg::process::stdio::read::ServerMessage::Response(_) => break,
			};
			let chunk = matches!(data, tg::process::stdio::write::Data::Chunk(_));
			let (sender, receiver) = tokio::sync::oneshot::channel();
			let write = stdin::Write { data, sender };
			stdin_sender
				.send(write)
				.await
				.map_err(|_| tg::error!("the process stdin task has finished"))?;
			let written = receiver
				.await
				.map_err(|_| tg::error!("the process stdin task has finished"))??;
			match chunk {
				true if !written.closed => {
					if let Some(written_sender) = &written_sender {
						written_sender.send_replace(written.position);
					}
				},
				true => {
					// Report the bytes written before stdin closed, and read until the acknowledgments finish, so that the position reaches the server before the open request is answered.
					if let Some(written_sender) = written_sender.take() {
						written_sender.send_replace(written.position);
					}
					while output.try_next().await?.is_some() {}
					return Ok(());
				},
				// Close stdin and end the request body, so that the server can acknowledge EOF.
				false => {
					ended = true;
					written_sender.take();
				},
			}
		}

		// Fail a read that ends before EOF, so that the writer's server reopens it.
		if !ended {
			return Err(tg::error!(unavailable, "the stdin relay ended before EOF"));
		}

		Ok(())
	}
}
