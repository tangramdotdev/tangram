use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::{
		prelude::*,
		process::connect::{
			Ack, ClientMessage, ClientNotification, ClientRequest, ClientRequestArg,
			ErrorServerNotification, Mode, ReadServerNotification, ServerMessage,
			ServerNotification, ServerResponse, ServerResponseOutput, TANGRAM_CONTENT_TYPE, Target,
			WriteServerNotification,
		},
	},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed, request::Ext as _},
	tokio::sync::mpsc,
	tokio_stream::wrappers::ReceiverStream,
};

const MAX_OPERATIONS: usize = 64;

type Sender = mpsc::Sender<tg::Result<ServerMessage>>;
type Operation = BoxFuture<'static, (u64, tg::Result<()>)>;

struct Streams {
	reads: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>>,
	tasks: FuturesUnordered<Operation>,
	writes: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::write::ClientMessage>>>,
}

impl Session {
	pub async fn connect_process(
		&self,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<ServerMessage>>> {
		let (high, receiver_high) = mpsc::channel(64);
		let (low, receiver_low) = mpsc::channel(16);
		let session = self.clone();
		let task = Task::spawn(move |_| async move {
			if let Err(error) = session
				.connect_process_task(input, &high, &low)
				.boxed()
				.await
			{
				high.send(Err(error)).await.ok();
			}
		});
		let stream = crate::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed();
		Ok(stream)
	}

	async fn connect_process_task(
		&self,
		mut input: BoxStream<'static, tg::Result<ClientMessage>>,
		high: &Sender,
		low: &Sender,
	) -> tg::Result<()> {
		// Select the process before opening any operations against it.
		let Some(ClientMessage::Request(ClientRequest {
			arg: ClientRequestArg::Connect(arg),
			id: request_id,
		})) = input.try_next().await?
		else {
			return Err(tg::error!("expected a connect request"));
		};
		Self::send_connect_ack(high, request_id).await?;
		if arg.reads.len() > MAX_OPERATIONS || arg.reads.contains_key(&request_id) {
			return Err(tg::error!("invalid initial process reads"));
		}
		if matches!(
			&arg.target,
			Target::Spawn {
				mode: Mode::Spawn,
				..
			}
		) && !arg.reads.is_empty()
		{
			return Err(tg::error!("spawn mode does not support initial reads"));
		}
		let mut pending = VecDeque::new();
		let (output, mode, location) = match arg.target {
			Target::Existing { id, options } => {
				let location = options.location.clone();
				let output = tg::process::spawn::Output {
					cached: false,
					lease: options.lease,
					location: options.location.and_then(|location| location.to_location()),
					process: tg::Either::Right(id),
					tokens: options.tokens,
					wait: None,
				};
				(output, Mode::Run, location)
			},
			Target::Spawn { arg, mode } => {
				let mut progress = self.try_spawn_process(*arg).await?.boxed();
				let mut input_open = true;
				let output = loop {
					tokio::select! {
						message = input.try_next(), if input_open && pending.len() < MAX_OPERATIONS => {
							match message? {
								Some(message) => pending.push_back(message),
								None if mode == Mode::Spawn => input_open = false,
								None => return Err(tg::error!("the process connection closed while spawning")),
							}
						},
						event = progress.try_next() => {
							let event = event?.ok_or_else(|| tg::error!("the spawn stream ended without an output"))?;
							if let tg::progress::Event::Output(output) = event {
								break output.ok_or_else(|| tg::error!("expected a process"))?;
							}
							let message = ServerMessage::Notification(ServerNotification::Progress(event.map_output(|_| ())));
							high.send(Ok(message)).await.map_err(|_| tg::error!("the process connection closed"))?;
						},
					}
				};
				{
					let location = output.location.clone().map(Into::into);
					(output, mode, location)
				}
			},
		};
		if mode == Mode::Spawn {
			Self::send_connect_response(
				high,
				request_id,
				Ok(ServerResponseOutput::Connect(output)),
			)
			.await?;
			Self::finish_connect_response(&mut input, request_id).await?;
			return Ok(());
		}
		let id = output
			.process
			.as_ref()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("expected a sandboxed process"))?;
		let tokens = output.tokens.clone();
		let cancel = Arc::new(AtomicBool::new(true));
		let mut lease_guard = output
			.wait
			.is_none()
			.then(|| super::spawn::lease::LeaseGuard::new(self, &output))
			.flatten();
		let wait_arg = tg::process::wait::Arg {
			lease: output.lease.clone(),
			location: location.clone(),
			tokens: tokens.clone(),
		};
		let mut wait = if let Some(output) = output.wait.clone() {
			futures::future::ready(Ok(Some(output))).boxed()
		} else {
			self.try_wait_process_future_with_cancel(&id, wait_arg, cancel.clone())
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))?
		};
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}
		let mut streams = Streams {
			reads: BTreeMap::new(),
			tasks: FuturesUnordered::new(),
			writes: BTreeMap::new(),
		};
		let mut ids = BTreeSet::from([request_id]);
		let mut responses = BTreeSet::from([request_id]);
		for (request_id, mut arg) in arg.reads {
			ids.insert(request_id);
			arg.location = location.clone();
			arg.tokens.inherit(&tokens);
			self.connect_process_read(&id, request_id, arg, &mut streams, low)
				.await?;
		}
		Self::send_connect_response(high, request_id, Ok(ServerResponseOutput::Connect(output)))
			.await?;

		// Keep completion independent of subscribed output and its EOF handshakes.
		let mut finished = false;
		let mut operations = FuturesUnordered::<Operation>::new();
		loop {
			if finished && streams.reads.is_empty() && operations.is_empty() && responses.is_empty()
			{
				return Ok(());
			}
			let message = if let Some(message) = pending.pop_front() {
				message
			} else {
				tokio::select! {
					biased;
					output = &mut wait, if !finished => {
						let output = output?.ok_or_else(|| tg::error!("the process wait ended before completion"))?;
						cancel.store(false, Ordering::SeqCst);
						finished = true;
						high.send(Ok(ServerMessage::Notification(ServerNotification::Wait(output)))).await.map_err(|_| tg::error!("the process connection closed"))?;
						continue;
					},
					result = streams.tasks.next(), if !streams.tasks.is_empty() => {
						let (request_id, result) = result.unwrap();
						let read = streams.reads.remove(&request_id).is_some();
						let active = read | streams.writes.remove(&request_id).is_some();
						ids.remove(&request_id);
						if active && let Err(error) = result {
							let notification = ErrorServerNotification { error: Self::connect_process_error(&error), id: request_id };
							(if read { low } else { high }).send(Ok(ServerMessage::Notification(ServerNotification::Error(notification)))).await.map_err(|_| tg::error!("the process connection closed"))?;
						}
						continue;
					},
					result = operations.next(), if !operations.is_empty() => { let (id, result) = result.unwrap(); ids.remove(&id); result?; continue; },
					message = input.try_next() => message?.ok_or_else(|| tg::error!("the process connection closed before completion"))?,
				}
			};
			match message {
				ClientMessage::Ack(ack) => {
					responses.remove(&ack.id);
				},
				ClientMessage::Notification(ClientNotification::Read(notification)) => {
					let sender = streams
						.reads
						.get(&notification.id)
						.ok_or_else(|| tg::error!("unknown process read"))?;
					sender.try_send(Ok(notification.message)).map_err(|error| {
						tg::error!(!error, "failed to deliver the read message")
					})?;
				},
				ClientMessage::Notification(ClientNotification::Write(notification)) => {
					let sender = streams
						.writes
						.get(&notification.id)
						.ok_or_else(|| tg::error!("unknown process write"))?;
					sender.try_send(Ok(notification.message)).map_err(|error| {
						tg::error!(!error, "failed to deliver the write message")
					})?;
				},
				ClientMessage::Request(request) => {
					Self::send_connect_ack(high, request.id).await?;
					if responses.contains(&request.id) || !ids.insert(request.id) {
						return Err(tg::error!("duplicate process request id"));
					}
					if responses.len() >= MAX_OPERATIONS {
						return Err(tg::error!("too many unacknowledged process responses"));
					}
					responses.insert(request.id);
					if matches!(
						&request.arg,
						ClientRequestArg::Read(_) | ClientRequestArg::Write(_)
					) && streams.tasks.len() >= MAX_OPERATIONS
						|| !matches!(
							&request.arg,
							ClientRequestArg::Close(_) | ClientRequestArg::Detach
						) && operations.len() >= MAX_OPERATIONS
					{
						Self::send_connect_response(
							high,
							request.id,
							Err(tg::error!("too many process operations")),
						)
						.await?;
						ids.remove(&request.id);
						continue;
					}
					let result = match request.arg {
						ClientRequestArg::Close(id) => {
							streams.reads.remove(&id);
							streams.writes.remove(&id);
							Ok(ServerResponseOutput::Close)
						},
						ClientRequestArg::Connect(_) => {
							Err(tg::error!("the process is already connected"))
						},
						ClientRequestArg::Detach => {
							cancel.store(false, Ordering::SeqCst);
							Self::send_connect_response(
								high,
								request.id,
								Ok(ServerResponseOutput::Detach),
							)
							.await?;
							Self::finish_connect_response(&mut input, request.id).await?;
							return Ok(());
						},
						ClientRequestArg::Read(mut arg) => {
							arg.location = arg.location.or_else(|| location.clone());
							arg.tokens.inherit(&tokens);
							self.connect_process_read(&id, request.id, arg, &mut streams, low)
								.await
								.map(|()| ServerResponseOutput::Read)
						},
						ClientRequestArg::Write(mut arg) => {
							arg.location = arg.location.or_else(|| location.clone());
							arg.tokens.inherit(&tokens);
							self.connect_process_write(&id, request.id, arg, &mut streams, high)
								.await
								.map(|()| ServerResponseOutput::Write)
						},
						arg => {
							let session = self.clone();
							let id = id.clone();
							let location = location.clone();
							let tokens = tokens.clone();
							let high = high.clone();
							operations.push(
								async move {
									let result = session
										.connect_process_operation(&id, arg, location, tokens)
										.await;
									let result =
										Self::send_connect_response(&high, request.id, result)
											.await;
									(request.id, result)
								}
								.boxed(),
							);
							continue;
						},
					};
					Self::send_connect_response(high, request.id, result).await?;
					if !streams.reads.contains_key(&request.id)
						&& !streams.writes.contains_key(&request.id)
					{
						ids.remove(&request.id);
					}
				},
			}
		}
	}

	async fn finish_connect_response(
		input: &mut BoxStream<'static, tg::Result<ClientMessage>>,
		id: u64,
	) -> tg::Result<()> {
		// Keep the request body open until the final response is received or the peer closes it.
		while let Some(message) = input.try_next().await? {
			if matches!(message, ClientMessage::Ack(ack) if ack.id == id) {
				break;
			}
		}
		Ok(())
	}

	async fn connect_process_read(
		&self,
		id: &tg::process::Id,
		request_id: u64,
		arg: tg::process::stdio::read::Arg,
		streams: &mut Streams,
		sender: &Sender,
	) -> tg::Result<()> {
		let (input, receiver) = mpsc::channel(4);
		let mut output = self
			.try_read_process_stdio(id, arg, ReceiverStream::new(receiver).boxed())
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;
		streams.reads.insert(request_id, input);
		let sender = sender.clone();
		streams.tasks.push(
			async move {
				while let Some(message) = output.try_next().await? {
					let notification = ReadServerNotification {
						id: request_id,
						message,
					};
					sender
						.send(Ok(ServerMessage::Notification(ServerNotification::Read(
							notification,
						))))
						.await
						.map_err(|_| tg::error!("the process connection closed"))?;
				}
				Ok(())
			}
			.map(move |result| (request_id, result))
			.boxed(),
		);
		Ok(())
	}

	async fn connect_process_write(
		&self,
		id: &tg::process::Id,
		request_id: u64,
		arg: tg::process::stdio::write::Arg,
		streams: &mut Streams,
		sender: &Sender,
	) -> tg::Result<()> {
		let (input, receiver) = mpsc::channel(4);
		let mut output = self
			.try_write_process_stdio(id, arg, ReceiverStream::new(receiver).boxed())
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;
		streams.writes.insert(request_id, input);
		let sender = sender.clone();
		streams.tasks.push(
			async move {
				while let Some(message) = output.try_next().await? {
					let notification = WriteServerNotification {
						id: request_id,
						message,
					};
					sender
						.send(Ok(ServerMessage::Notification(ServerNotification::Write(
							notification,
						))))
						.await
						.map_err(|_| tg::error!("the process connection closed"))?;
				}
				Ok(())
			}
			.map(move |result| (request_id, result))
			.boxed(),
		);
		Ok(())
	}

	async fn connect_process_operation(
		&self,
		id: &tg::process::Id,
		arg: ClientRequestArg,
		location: Option<tg::location::Arg>,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<ServerResponseOutput> {
		let output = match arg {
			ClientRequestArg::Cancel(mut arg) => {
				arg.location = arg.location.or(location);
				let output = self.cancel_process(id, arg).await?;
				ServerResponseOutput::Cancel(output)
			},
			ClientRequestArg::Signal(mut arg) => {
				arg.location = arg.location.or(location);
				arg.tokens.inherit(&tokens);
				self.try_signal_process(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				ServerResponseOutput::Signal
			},
			ClientRequestArg::Tty(mut arg) => {
				arg.location = arg.location.or(location);
				arg.tokens.inherit(&tokens);
				self.try_set_process_tty_size(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				ServerResponseOutput::Tty
			},
			ClientRequestArg::Close(_)
			| ClientRequestArg::Connect(_)
			| ClientRequestArg::Detach
			| ClientRequestArg::Read(_)
			| ClientRequestArg::Write(_) => unreachable!(),
		};
		Ok(output)
	}

	async fn send_connect_ack(sender: &Sender, id: u64) -> tg::Result<()> {
		let message = ServerMessage::Ack(Ack { id });
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	async fn send_connect_response(
		sender: &Sender,
		id: u64,
		result: tg::Result<ServerResponseOutput>,
	) -> tg::Result<()> {
		let (error, output) = match result {
			Ok(output) => (None, Some(output)),
			Err(error) => (Some(Self::connect_process_error(&error)), None),
		};
		let response = ServerResponse { error, id, output };
		sender
			.send(Ok(ServerMessage::Response(response)))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	fn connect_process_error(error: &tg::Error) -> tg::error::Data {
		tg::error::Data {
			message: Some(error.to_string()),
			source: Some(tg::Referent::new(
				error.to_data_or_id().map_left(Box::new),
				tg::referent::Options::default(),
			)),
			..Default::default()
		}
	}

	pub(crate) async fn connect_process_request(
		&self,
		request: http::Request<Boxed>,
	) -> tg::Result<http::Response<Boxed>> {
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;
		let input_encoding = super::stdio::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!("missing the content type"))?,
			TANGRAM_CONTENT_TYPE,
		)?;
		let output_encoding =
			super::stdio::Encoding::from_accept(accept.as_ref(), TANGRAM_CONTENT_TYPE)?;
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::stdio::decode(request, input_encoding, max_frame_size);
		let output = self.connect_process(input).await?;
		let body = super::stdio::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				output_encoding
					.content_type(TANGRAM_CONTENT_TYPE)
					.to_string(),
			)
			.body(body)
			.unwrap();
		Ok(response)
	}
}
