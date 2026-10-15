use {
	crate::Session,
	futures::{
		StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::BoxStream,
	},
	std::{
		collections::VecDeque,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tg::process::stdio::{
		read::ClientMessage,
		write::{Data, Output},
	},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

/// A write item that the reader has not yet acknowledged, with its end in combined coordinates, which the write reports, and in the read's coordinates, which the reader acknowledges.
struct Pending {
	combined: u64,
	end: bool,
	position: u64,
}

impl Session {
	pub(super) fn write_process_stdio_relay_local(
		&self,
		id: &tg::process::Id,
		open: &str,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<Data>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<Output>>>> {
		// The relay subjects are scoped to the process, so only the process may feed its own reads.
		match &self.context.principal {
			tg::Principal::Process(process) if process == id => (),
			_ => return Err(tg::error!("unauthorized")),
		}

		// Relay the write's data, ending after EOF, and record each item's ends. A body that ends before EOF ends the relay too, which tells the reader that the read reached its limit or timeout.
		let combined = streams.len() > 1;
		let pending = Arc::new(Mutex::new(VecDeque::<Pending>::new()));
		let notify = Arc::new(tokio::sync::Notify::new());
		let items = input
			.scan(false, {
				let pending = pending.clone();
				let notify = notify.clone();
				move |ended, result| {
					if *ended {
						return future::ready(None);
					}
					*ended = matches!(result, Ok(Data::End(_)) | Err(_));
					if let Ok(data) = &result {
						let entry = match data {
							Data::Chunk(chunk) => {
								let length = chunk.bytes.len() as u64;
								let start = if combined {
									chunk.combined_position
								} else {
									chunk.stream_position
								};
								Pending {
									combined: chunk.combined_position + length,
									end: false,
									position: start + length,
								}
							},
							Data::End(end) => {
								let position = if combined {
									end.combined_position
								} else {
									end.stream_positions
										.values()
										.copied()
										.next()
										.unwrap_or_default()
								};
								Pending {
									combined: end.combined_position,
									end: true,
									position,
								}
							},
						};
						pending.lock().unwrap().push_back(entry);
						notify.notify_one();
					}
					future::ready(Some(result))
				}
			})
			.fuse()
			.then({
				let server = self.server.clone();
				let open = open.to_owned();
				move |result| {
					let server = server.clone();
					let open = open.clone();
					async move {
						crate::checkpoint!(server, "process.stdio.write.relay.data", open = %open)
							.await;
						result
							.map(super::super::relay::Data)
							.map_err(|error| super::super::relay::abort(&error))
					}
				}
			})
			.boxed();

		// Send the data on the relay, and report the positions that the reader acknowledges.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let session = self.clone();
		let id = id.clone();
		let open = open.to_owned();
		let task = Task::spawn(move |_| async move {
			let result = session
				.write_process_stdio_relay_local_task(&id, &open, items, &pending, &notify, &sender)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn write_process_stdio_relay_local_task(
		&self,
		id: &tg::process::Id,
		open: &str,
		items: BoxStream<'static, Result<super::super::relay::Data, bytes::Bytes>>,
		pending: &Mutex<VecDeque<Pending>>,
		notify: &tokio::sync::Notify,
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
	) -> tg::Result<()> {
		// Open the relay, which sends the data while its window allows and receives the reader's acknowledgments.
		let send_subject = super::super::relay::data_subject(id, open);
		let receive_subject = super::super::relay::ack_subject(id, open);
		let options = self.server.config().process.stdio_relay;
		let (relay_sender, receiver) = tangram_messenger::relay::open::<
			super::super::relay::Data,
			super::super::relay::Ack,
			_,
		>(
			&self.server.messenger,
			send_subject,
			receive_subject,
			options,
		)
		.await
		.map_err(super::super::relay::error)?;
		let mut acks = receiver;
		let mut relay = AbortOnDropHandle::new(tokio::spawn(relay_sender.send_all(items)));
		let mut relayed = false;
		let mut acked = None::<u64>;
		loop {
			// Report the position through which the reader has acknowledged the write.
			if let Some(acked) = acked
				&& let Some(output) = Self::acknowledged_process_stdio_relay_output(pending, acked)
			{
				sender
					.send(Ok(output))
					.await
					.map_err(|_| tg::error!("the stdio write output closed"))?;
				if output.closed {
					// Wait for the relay to deliver the data's end while reading the acknowledgments to their end, so that both directions acknowledge their ends.
					let relay = async {
						if !relayed {
							(&mut relay)
								.await
								.map_err(|error| {
									tg::error!(unavailable, !error, "the stdio relay task panicked")
								})?
								.map_err(super::super::relay::error)?;
						}
						Ok::<_, tg::Error>(())
					};
					let drain = acks.by_ref().for_each(|_| future::ready(()));
					let (result, ()) = future::join(relay, drain).await;
					result?;
					return Ok(());
				}
			}

			tokio::select! {
				result = &mut relay, if !relayed => {
					result
						.map_err(|error| tg::error!(unavailable, !error, "the stdio relay task panicked"))?
						.map_err(super::super::relay::error)?;
					relayed = true;
				},
				message = acks.next() => match message {
					Some(Ok(super::super::relay::Ack(ClientMessage::Ack { position }))) => {
						acked = Some(acked.map_or(position, |acked| acked.max(position)));
					},
					Some(Err(error)) => return Err(super::super::relay::error(error)),
					None => return Ok(()),
				},
				() = notify.notified() => {},
			}
		}
	}

	fn acknowledged_process_stdio_relay_output(
		pending: &Mutex<VecDeque<Pending>>,
		acked: u64,
	) -> Option<Output> {
		let mut pending = pending.lock().unwrap();
		let mut output = None;
		while pending.front().is_some_and(|entry| entry.position <= acked) {
			let entry = pending.pop_front().unwrap();
			output = Some(Output {
				closed: entry.end,
				position: entry.combined,
			});
		}
		output
	}

	pub(super) async fn write_process_stdio_relay_task(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		mut input: BoxStream<'static, tg::Result<Data>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
		wait: future::Shared<BoxFuture<'static, tg::Result<()>>>,
	) -> tg::Result<()> {
		// Keep the client's data until the runner has written it, so a failed relay can be replaced by one that resends it.
		let mut kept = VecDeque::<(Data, Pending)>::new();
		let mut ended = false;
		let mut position = 0;
		let options: tangram_futures::retry::Options = self
			.server
			.config()
			.process
			.stdio_relay_retry
			.clone()
			.into();
		let mut retries = tangram_futures::retry::stream(options.clone()).boxed();
		let mut error = None;
		loop {
			if retries.next().await.is_none() {
				return Err(error.unwrap());
			}
			let start = position;
			let result = self
				.write_process_stdio_relay_attempt(
					id,
					streams,
					&mut input,
					sender,
					wait.clone(),
					&mut kept,
					&mut ended,
					&mut position,
				)
				.await;
			let attempt_error = match result {
				Ok(()) => return Ok(()),
				Err(error) => error,
			};

			// Return the error unless the relay failed, so that only a failed relay is replaced.
			if attempt_error.kind() != Some(tg::error::Kind::Unavailable) || sender.is_closed() {
				return Err(attempt_error);
			}

			// Restart the retries if the attempt wrote bytes, so that only consecutive failures exhaust them.
			if position > start {
				retries = tangram_futures::retry::stream(options.clone()).boxed();
			}
			tracing::warn!(error = %attempt_error.trace(), %id, "reopening the stdin relay");
			crate::checkpoint!(self.server, "process.stdio.write.reopen", process = %id).await;
			error = Some(attempt_error);
		}
	}

	#[expect(clippy::too_many_arguments)]
	async fn write_process_stdio_relay_attempt(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: &mut BoxStream<'static, tg::Result<Data>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
		wait: future::Shared<BoxFuture<'static, tg::Result<()>>>,
		kept: &mut VecDeque<(Data, Pending)>,
		ended: &mut bool,
		position: &mut u64,
	) -> tg::Result<()> {
		// Open the relay, which sends the data and receives the runner's acknowledgments.
		let open = crate::control::id();
		let send_subject = super::super::relay::data_subject(id, &open);
		let receive_subject = super::super::relay::ack_subject(id, &open);
		let options = self.server.config().process.stdio_relay;
		let (relay_sender, receiver) = tangram_messenger::relay::open::<
			super::super::relay::Data,
			super::super::relay::Ack,
			_,
		>(
			&self.server.messenger,
			send_subject,
			receive_subject,
			options,
		)
		.await
		.map_err(super::super::relay::error)?;
		let mut acks = receiver;
		let (items_sender, items_receiver) = tokio::sync::mpsc::channel(1);
		let items = ReceiverStream::new(items_receiver);
		let mut relay = AbortOnDropHandle::new(tokio::spawn(relay_sender.send_all(items)));

		// Open the runner's stdin read.
		let arg = tg::process::stdio::read::Arg {
			streams: vec![tg::process::stdio::Stream::Stdin],
			..tg::process::stdio::read::Arg::new(id.clone())
		};
		let open = tg::process::control::ServerRequest {
			arg: tg::process::control::ServerRequestArg::Open(
				tg::process::control::OpenServerRequestArg { arg },
			),
			id: open,
		};
		let mut response = self.send_process_control_open(id, open).await?;

		// Resend the kept data on the new relay, then queue new data behind it. The runner skips the bytes it already wrote.
		let mut queue = kept
			.iter()
			.map(|(data, _)| data.clone())
			.collect::<VecDeque<_>>();
		let mut items_sender = Some(items_sender);
		if queue.is_empty() && *ended {
			items_sender.take();
		}

		// Report each position once the runner has written it, or close once stdin closes.
		let mut acks_open = true;
		let mut relayed = false;
		loop {
			let reading = queue.is_empty() && !*ended;
			let channel = items_sender.clone();
			tokio::select! {
				permit = async { channel.unwrap().reserve_owned().await }, if items_sender.is_some() && !queue.is_empty() => {
					let Ok(permit) = permit else {
						items_sender.take();
						continue;
					};
					let data = queue.pop_front().unwrap();
					permit.send(Ok(super::super::relay::Data(data)));
					if queue.is_empty() && *ended {
						items_sender.take();
					}
				},
				data = input.try_next(), if reading => {
					let Some(data) = data? else {
						return Ok(());
					};
					super::validate_write(&data, streams)?;
					crate::checkpoint!(self.server, "process.stdio.write.request", close = %matches!(&data, Data::End(_)), stream = %tg::process::stdio::Stream::Stdin).await;
					let entry = match &data {
						Data::Chunk(chunk) => {
							let length = chunk.bytes.len() as u64;
							Pending {
								combined: chunk.combined_position + length,
								end: false,
								position: chunk.stream_position + length,
							}
						},
						Data::End(end) => Pending {
							combined: end.combined_position,
							end: true,
							position: end.combined_position,
						},
					};
					*ended = entry.end;
					kept.push_back((data.clone(), entry));
					if items_sender.is_some() {
						queue.push_back(data);
					}
				},
				message = acks.next(), if acks_open => match message {
					Some(Ok(super::super::relay::Ack(ClientMessage::Ack { position: acked }))) => {
						Self::answer_process_stdio_relay_writes(sender, kept, position, acked).await?;
					},
					Some(Err(error)) => return Err(super::super::relay::error(error)),
					None => acks_open = false,
				},
				result = &mut relay, if !relayed => {
					relayed = true;
					let result = result.map_err(|error| tg::error!(unavailable, !error, "the stdio relay task panicked"))?;
					result.map_err(super::super::relay::error)?;
				},
				result = &mut response => {
					// An error means the runner's read failed, and success means stdin closed.
					result?;

					// Answer the writes that the runner reported written before stdin closed.
					while acks_open
						&& let Some(Ok(super::super::relay::Ack(ClientMessage::Ack { position: acked }))) = acks.next().await
					{
						Self::answer_process_stdio_relay_writes(sender, kept, position, acked).await?;
					}

					return Self::close_process_stdio_relay(sender, *position).await;
				},
				result = wait.clone() => {
					result?;
					return Self::close_process_stdio_relay(sender, *position).await;
				},
			}
		}
	}

	async fn answer_process_stdio_relay_writes(
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
		kept: &mut VecDeque<(Data, Pending)>,
		position: &mut u64,
		acked: u64,
	) -> tg::Result<()> {
		// Answer the kept writes through the acknowledged position, leaving the end for the runner's read to acknowledge when it completes.
		let mut answered = false;
		while kept
			.front()
			.is_some_and(|(_, entry)| !entry.end && entry.position <= acked)
		{
			let (_, entry) = kept.pop_front().unwrap();
			*position = entry.position;
			answered = true;
		}
		if answered {
			let output = Output {
				closed: false,
				position: *position,
			};
			sender
				.send(Ok(output))
				.await
				.map_err(|_| tg::error!("the stdio write output closed"))?;
		}

		Ok(())
	}

	async fn close_process_stdio_relay(
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
		position: u64,
	) -> tg::Result<()> {
		let output = Output {
			closed: true,
			position,
		};
		sender
			.send(Ok(output))
			.await
			.map_err(|_| tg::error!("the stdio write output closed"))?;

		Ok(())
	}
}
