use super::*;

struct State<'a> {
	cancel: Arc<AtomicBool>,
	high: &'a Sender,
	id: tg::process::Id,
	location: Option<tg::location::Arg>,
	low: &'a Sender,
	operations: FuturesUnordered<Operation>,
	ready: bool,
	requests: BTreeSet<u64>,
	responses: BTreeSet<u64>,
	streams: Streams,
	tokens: tg::authorization::Tokens,
	writer: Option<Writer>,
	write_bytes: u64,
	writes: BTreeMap<u64, u64>,
}

struct Streams {
	aborts: BTreeMap<u64, AbortHandle>,
	reads: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>>,
	tasks: FuturesUnordered<Operation>,
}

struct Writer {
	closed: bool,
	input: mpsc::Sender<tg::Result<tg::process::stdio::write::ClientMessage>>,
	output: BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>,
}

impl Session {
	pub(super) fn get_process_connect_stream_local(
		&self,
		options: Options,
		input: Input,
	) -> Output {
		let (high, receiver_high) = mpsc::channel(64);
		let (low, receiver_low) = mpsc::channel(16);
		// The connection owns its lease until completion, detachment, or client disconnect.
		let mut session = self.clone();
		session.context.stopper = None;
		let task = Task::spawn(move |_| async move {
			if let Err(error) = session
				.run_process_connect_local_task(options, input, &high, &low)
				.boxed()
				.await
			{
				high.send(Err(error)).await.ok();
			}
		});
		crate::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed()
	}

	pub(super) async fn run_process_connect_local_task(
		&self,
		options: Options,
		mut input: Input,
		high: &Sender,
		low: &Sender,
	) -> tg::Result<()> {
		// Select the process.
		let Options {
			mut arg,
			prepare_output,
			wait_future,
		} = options;
		let mut sync_task = None;
		if arg.sync {
			let tg::Either::Left(spawn) = &arg.process else {
				return Err(tg::error!("command sync requires a spawn"));
			};
			let destination = self
				.connect_process_command_sync_destination(&spawn.command, input, high, low)
				.await?;
			input = destination.input;
			// Attach the destination-minted tokens to the ephemeral command; process storage strips them.
			let tg::Either::Left(spawn) = &mut arg.process else {
				return Err(tg::error!("command sync requires a spawn"));
			};
			spawn
				.command
				.options
				.tokens
				.inherit(&destination.sync.options.tokens);
			sync_task = Some(destination.task);
		}

		// Buffer requests while command sync progresses on the same connection.
		let mut pending = VecDeque::new();
		if self.server.config.process.await_push {
			Self::connect_process_await_command_sync(
				self.server.config().process.stdio,
				&mut sync_task,
				&mut input,
				&mut pending,
			)
			.await?;
		}

		let mode = arg.mode;
		let (output, location) = match arg.process {
			tg::Either::Left(spawn) => {
				let output = self
					.connect_process_spawn_local(
						*spawn,
						mode,
						prepare_output.unwrap(),
						&mut input,
						&mut pending,
						high,
					)
					.await?;
				let location = output.location.clone().map(Into::into);
				(output, location)
			},
			tg::Either::Right(id) => {
				let location = wait_future.as_ref().unwrap().1.clone();
				let output = tg::process::spawn::Output {
					cached: false,
					command: None,
					lease: arg.lease,
					location: Some(location.clone()),
					outcome: None,
					process: tg::Either::Right(id),
					tokens: arg.tokens,
				};
				(output, Some(location.into()))
			},
		};

		// Complete a spawn-only connection.
		if mode == tg::process::connect::Mode::Spawn {
			Self::send_process_connect_output(high, output).await?;
			// Keep the transfer alive after responding when command sync runs concurrently.
			Self::connect_process_finish_command_sync(&mut sync_task).await?;
			while let Some(message) = input.try_next().await? {
				if matches!(
					message,
					tg::process::connect::ClientMessage::Notification(
						tg::process::connect::ClientNotification::Ready
					)
				) {
					break;
				}
			}
			return Ok(());
		}

		// Follow a cached process to its selected location using the same connection routing.
		if wait_future.is_none()
			&& !matches!(
				self.server.location(location.as_ref())?,
				tg::Location::Local(tg::location::Local { region })
				if region.as_deref().is_none_or(|region| Some(region) == self.server.config.region.as_deref())
			) {
			let arg = tg::process::connect::Arg {
				lease: output.lease.clone(),
				location,
				mode,
				process: tg::Either::Right(output.process.as_ref().unwrap_right().clone()),
				reads: arg.reads,
				sync: false,
				tokens: output.tokens.clone(),
			};
			if let Some(task) = sync_task.take() {
				task.abort();
			}
			let input = futures::stream::iter(pending.into_iter().map(Ok))
				.chain(input)
				.boxed();
			self.connect_process_cached_task(arg, output, input, low)
				.await?;
			return Ok(());
		}

		// Attach the wait and transfer cancellation to its guard.
		let id = output
			.process
			.as_ref()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("expected a sandboxed process"))?;
		let tokens = output.tokens.clone();
		let cancel = Arc::new(AtomicBool::new(true));
		let mut lease_guard = output
			.outcome
			.is_none()
			.then(|| spawn::lease::LeaseGuard::new(self, &output))
			.flatten();
		let wait_future = if let Some(outcome) = output.outcome.clone() {
			futures::future::ready(Ok(Some(outcome))).boxed()
		} else {
			let mut wait_arg = tg::process::wait::Arg {
				lease: None,
				location: location.clone(),
				source: tg::process::Source::Auto,
				tokens: tokens.clone(),
			};

			// Prefer the runner over the local wait because the runner's outcome retains the result tokens.
			let future = if let Some((future, _)) = wait_future {
				future
			} else if let Some((future, _)) = self.try_wait_process_runner(&id, &wait_arg).await? {
				future
			} else {
				self.try_wait_process_local(
					&id,
					wait_arg.tokens.local_authorization().to_vec(),
					tg::process::Source::Auto,
				)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))?
			};
			wait_arg.lease = output.lease.clone();
			self.attach_wait_process_guard(&id, &wait_arg, location.clone(), cancel.clone(), future)
		};
		if let Some(guard) = &mut lease_guard {
			guard.disarm();
		}

		// Start the initial reads and return the selected process.
		let streams = Streams {
			aborts: BTreeMap::new(),
			reads: BTreeMap::new(),
			tasks: FuturesUnordered::new(),
		};
		let mut state = State {
			cancel,
			high,
			id,
			location,
			low,
			operations: FuturesUnordered::new(),
			ready: false,
			requests: BTreeSet::new(),
			responses: BTreeSet::new(),
			streams,
			tokens,
			writer: None,
			write_bytes: 0,
			writes: BTreeMap::new(),
		};
		for (request_id, arg) in arg.reads {
			state.requests.insert(request_id);
			state.responses.insert(request_id);
			self.read_process_connect(&mut state, request_id, arg)
				.await?;
		}
		Self::send_process_connect_output(high, output).await?;

		// Run the connection until completion or detachment.
		let finished = self
			.run_process_connect_task(state, wait_future, pending, input)
			.boxed()
			.await?;
		if finished {
			if let Some(task) = sync_task.take() {
				task.abort();
			}
		} else {
			Self::connect_process_finish_command_sync(&mut sync_task).await?;
		}

		Ok(())
	}

	pub(super) async fn connect_process_spawn_local(
		&self,
		arg: tg::process::spawn::Arg,
		mode: tg::process::connect::Mode,
		prepare_output: spawn::PrepareOutput,
		input: &mut Input,
		pending: &mut VecDeque<tg::process::connect::ClientMessage>,
		sender: &Sender,
	) -> tg::Result<tg::process::spawn::Output> {
		// Buffer requests while spawning so the client can send stdio immediately.
		let mut progress = self
			.try_spawn_process_inner(arg, prepare_output)
			.await?
			.boxed();
		let mut input_open = true;
		let output = loop {
			tokio::select! {
				message = input.try_next(), if input_open => {
					match message? {
						Some(message) => Self::connect_process_buffer_message(self.server.config().process.stdio, pending, message)?,
						None if mode == tg::process::connect::Mode::Spawn => input_open = false,
						None => return Err(tg::error!("the process connection closed while spawning")),
					}
				},
				event = progress.try_next() => {
					let event = event?.ok_or_else(|| tg::error!("the spawn stream ended without an output"))?;
					if let tg::progress::Event::Output(output) = event {
						break output.ok_or_else(|| tg::error!("expected a process"))?;
					}
					let message = tg::process::connect::ServerMessage::Notification(tg::process::connect::ServerNotification::Progress(event.map_output(Option::unwrap)));
					sender.send(Ok(message)).await.map_err(|_| tg::error!("the process connection closed"))?;
				},
			}
		};

		Ok(output)
	}

	pub(super) async fn connect_process_cached_task(
		&self,
		arg: tg::process::connect::Arg,
		output: tg::process::spawn::Output,
		input: Input,
		sender: &Sender,
	) -> tg::Result<()> {
		// Transfer the lease to a connection at the selected cache location.
		let mut guard = spawn::lease::LeaseGuard::new(self, &output);
		let (_, mut stream) = self.get_process_connect_stream(arg, input).boxed().await?;
		if let Some(guard) = &mut guard {
			guard.disarm();
		}

		// Preserve the upstream ordering so terminal responses cannot overtake their read chunks.
		while let Some(mut message) = stream.try_next().await? {
			if let tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Progress(tg::progress::Event::Output(
					selected,
				)),
			) = &mut message
			{
				selected.cached = output.cached;
				selected.outcome = output.outcome.clone();
			}
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		Ok(())
	}

	async fn run_process_connect_task(
		&self,
		mut state: State<'_>,
		mut wait_future: WaitFuture,
		mut pending: VecDeque<tg::process::connect::ClientMessage>,
		mut input: Input,
	) -> tg::Result<bool> {
		// Keep completion independent of subscribed output and its EOF handshakes.
		let mut finished = false;
		loop {
			if finished
				&& state.ready
				&& pending.is_empty()
				&& state.streams.tasks.is_empty()
				&& state.operations.is_empty()
				&& state.writer.as_ref().is_none_or(|writer| writer.closed)
				&& state.writes.is_empty()
				&& state.responses.is_empty()
			{
				return Ok(true);
			}
			let message = if let Some(message) = pending.pop_front() {
				message
			} else {
				tokio::select! {
					biased;
					outcome = &mut wait_future, if !finished => {
						let outcome = outcome?.ok_or_else(|| tg::error!("the process wait ended before completion"))?;
						Self::handle_process_connect_outcome(&mut state, outcome).await?;
						finished = true;
						continue;
					},
					result = state.streams.tasks.next(), if !state.streams.tasks.is_empty() => {
						let (id, result) = result.unwrap();
						Self::handle_process_connect_stream(&mut state, id, result).await?;
						continue;
					},
					result = state.operations.next(), if !state.operations.is_empty() => {
						let (id, result) = result.unwrap();
						state.requests.remove(&id);
						result?;
						continue;
					},
					message = async { state.writer.as_mut().unwrap().output.next().await }, if state.writer.is_some() => {
						Self::handle_process_connect_write(&mut state, message).await?;
						continue;
					},
					message = input.try_next() => message?.ok_or_else(|| tg::error!("the process connection closed before completion"))?,
				}
			};
			if let ControlFlow::Break(id) = self
				.handle_process_connect_message(&mut state, message)
				.await?
			{
				Self::finish_process_connect_response(&mut input, id).await?;
				return Ok(false);
			}
		}
	}

	async fn handle_process_connect_outcome(
		state: &mut State<'_>,
		outcome: tg::process::outcome::Data,
	) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		let notification = tg::process::connect::ServerNotification::Outcome(outcome);
		let message = tg::process::connect::ServerMessage::Notification(notification);
		state
			.high
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	async fn handle_process_connect_stream(
		state: &mut State<'_>,
		id: u64,
		result: tg::Result<()>,
	) -> tg::Result<()> {
		// Release the completed read.
		state.streams.aborts.remove(&id);
		let active = state.streams.reads.remove(&id).is_some();
		state.requests.remove(&id);
		if active && let Err(error) = result {
			Self::send_process_connect_response(state.low, id, Err(error)).await?;
		}

		Ok(())
	}

	async fn handle_process_connect_message(
		&self,
		state: &mut State<'_>,
		message: tg::process::connect::ClientMessage,
	) -> tg::Result<ControlFlow<u64>> {
		match message {
			tg::process::connect::ClientMessage::Ack(ack) => {
				state.responses.remove(&ack.id);
				if let Some(sender) = state.streams.reads.get(&ack.id) {
					sender
						.try_send(Ok(tg::process::stdio::read::ClientMessage::Ack))
						.map_err(|error| {
							tg::error!(!error, "failed to acknowledge the read response")
						})?;
				}
			},
			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::ReadConsumption(notification),
			) => {
				Self::handle_process_connect_read(state, &notification)?;
			},

			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::Ready,
			) => state.ready = true,
			tg::process::connect::ClientMessage::Request(request) => {
				return self.handle_process_connect_request(state, request).await;
			},
			tg::process::connect::ClientMessage::Sync(_)
			| tg::process::connect::ClientMessage::SyncConfig(_)
			| tg::process::connect::ClientMessage::SyncConsumption(_) => {
				return Err(tg::error!("unexpected process sync message"));
			},
		}
		Ok(ControlFlow::Continue(()))
	}

	fn handle_process_connect_read(
		state: &State<'_>,
		notification: &tg::process::connect::ReadConsumption,
	) -> tg::Result<()> {
		// Progress already in transit may arrive after an error or cancellation ends the read.
		let Some(sender) = state.streams.reads.get(&notification.id) else {
			return Ok(());
		};
		sender
			.try_send(Ok(tg::process::stdio::read::ClientMessage::Notification(
				notification.consumption,
			)))
			.map_err(|error| tg::error!(!error, "failed to deliver the read message"))?;
		Ok(())
	}

	async fn handle_process_connect_write(
		state: &mut State<'_>,
		message: Option<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		let error = match message {
			Some(Ok(write::ServerMessage::Ack(_))) => return Ok(()),
			Some(Ok(write::ServerMessage::Response(response))) => {
				let id = response.id;
				// Keep the connection open between writes until stdin is confirmed closed.
				state.writer.as_mut().unwrap().closed |= response.error.is_some()
					|| response.output.as_ref().is_some_and(|output| output.closed);
				state
					.writer
					.as_ref()
					.unwrap()
					.input
					.try_send(Ok(write::ClientMessage::Ack(
						tg::process::stdio::write::Ack { id },
					)))
					.map_err(|error| {
						tg::error!(!error, "failed to acknowledge the stdio write response")
					})?;
				state.requests.remove(&id);
				if let Some(length) = state.writes.remove(&id) {
					state.write_bytes -= length;
				}
				let response = tg::process::connect::ServerResponse {
					error: response.error,
					id,
					output: response
						.output
						.map(tg::process::connect::ServerResponseOutput::Write),
				};
				state
					.high
					.send(Ok(tg::process::connect::ServerMessage::Response(response)))
					.await
					.map_err(|_| tg::error!("the process connection closed"))?;
				return Ok(());
			},
			Some(Err(error)) => error,
			None => tg::error!("the stdio write stream closed before completion"),
		};
		state.writer = None;
		state.write_bytes = 0;
		for (id, _) in std::mem::take(&mut state.writes) {
			state.requests.remove(&id);
			Self::send_process_connect_response(state.high, id, Err(error.clone())).await?;
		}
		Ok(())
	}

	async fn handle_process_connect_request(
		&self,
		state: &mut State<'_>,
		request: tg::process::connect::ClientRequest,
	) -> tg::Result<ControlFlow<u64>> {
		// Acknowledge receipt and register the request.
		Self::send_process_connect_ack(state.high, request.id).await?;
		if state.responses.contains(&request.id) || !state.requests.insert(request.id) {
			return Err(tg::error!("duplicate process request id"));
		}
		if state.responses.len()
			>= self.server.config().process.stdio.message_capacity()
				+ self.server.config().process.stdio.max_reads
				+ MAX_OPERATIONS * 3
				+ 4
		{
			return Err(tg::error!("too many unacknowledged process responses"));
		}
		state.responses.insert(request.id);

		// Keep close and detach available when the operation limit is reached.
		let limit = match &request.arg {
			tg::process::connect::ClientRequestArg::Cancel(_)
			| tg::process::connect::ClientRequestArg::Signal(_)
			| tg::process::connect::ClientRequestArg::Tty(_) => state.operations.len() >= MAX_OPERATIONS,
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Detach => false,
			tg::process::connect::ClientRequestArg::Read(_) => {
				state.streams.tasks.len() >= self.server.config().process.stdio.max_reads
			},
			tg::process::connect::ClientRequestArg::Write(arg) => {
				let limit = self.server.config().process.stdio.message_capacity()
					+ usize::from(matches!(arg.data, write::Data::End(_)));
				state.writes.len() >= limit
			},
		};
		if limit {
			Self::send_process_connect_response(
				state.high,
				request.id,
				Err(tg::error!("too many process operations")),
			)
			.await?;
			state.requests.remove(&request.id);
			return Ok(ControlFlow::Continue(()));
		}

		// Dispatch the request without blocking the connection on independent operations.
		let result = match request.arg {
			arg @ (tg::process::connect::ClientRequestArg::Cancel(_)
			| tg::process::connect::ClientRequestArg::Signal(_)
			| tg::process::connect::ClientRequestArg::Tty(_)) => {
				self.start_process_connect_operation(state, request.id, &arg);
				return Ok(ControlFlow::Continue(()));
			},
			tg::process::connect::ClientRequestArg::Close(id) => {
				Self::close_process_connect_read(state, id);
				Ok(tg::process::connect::ServerResponseOutput::Close)
			},
			tg::process::connect::ClientRequestArg::Detach => {
				Self::detach_process_connect(state, request.id).await?;
				return Ok(ControlFlow::Break(request.id));
			},
			tg::process::connect::ClientRequestArg::Read(arg) => {
				match self.read_process_connect(state, request.id, arg).await {
					Ok(()) => return Ok(ControlFlow::Continue(())),
					Err(error) => Err(error),
				}
			},
			tg::process::connect::ClientRequestArg::Write(arg) => {
				match self.write_process_connect(state, request.id, arg).await {
					Ok(()) => return Ok(ControlFlow::Continue(())),
					Err(error) => Err(error),
				}
			},
		};

		Self::send_process_connect_response(state.high, request.id, result).await?;
		if !state.streams.reads.contains_key(&request.id) && !state.writes.contains_key(&request.id)
		{
			state.requests.remove(&request.id);
		}

		Ok(ControlFlow::Continue(()))
	}

	fn start_process_connect_operation(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		arg: &tg::process::connect::ClientRequestArg,
	) {
		let session = self.clone();
		let id = state.id.clone();
		let location = state.location.clone();
		let tokens = state.tokens.clone();
		let arg = arg.clone();
		let sender = state.high.clone();
		let future = async move {
			let result = session
				.run_process_connect_operation(&id, arg, location, tokens)
				.await;
			let result = Self::send_process_connect_response(&sender, request_id, result).await;
			(request_id, result)
		}
		.boxed();
		state.operations.push(future);
	}

	pub(super) async fn run_process_connect_operation(
		&self,
		id: &tg::process::Id,
		arg: tg::process::connect::ClientRequestArg,
		location: Option<tg::location::Arg>,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<tg::process::connect::ServerResponseOutput> {
		let output = match arg {
			tg::process::connect::ClientRequestArg::Cancel(mut arg) => {
				arg.location = location;
				let output = self
					.try_cancel_process(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Cancel(output)
			},
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Detach
			| tg::process::connect::ClientRequestArg::Read(_)
			| tg::process::connect::ClientRequestArg::Write(_) => unreachable!(),
			tg::process::connect::ClientRequestArg::Signal(mut arg) => {
				arg.location = location;
				arg.tokens.inherit(&tokens);
				self.try_post_process_signal(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Signal
			},
			tg::process::connect::ClientRequestArg::Tty(mut arg) => {
				arg.location = location;
				arg.tokens.inherit(&tokens);
				self.try_set_process_tty_size(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Tty
			},
		};
		Ok(output)
	}

	fn close_process_connect_read(state: &mut State<'_>, id: u64) {
		state.streams.reads.remove(&id);
		state.responses.remove(&id);
		if let Some(abort) = state.streams.aborts.get(&id) {
			abort.abort();
		}
	}

	async fn detach_process_connect(state: &mut State<'_>, id: u64) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		// Drop the subscribed reads before acknowledging detach so fallback readers can take over.
		state.streams.tasks.clear();
		state.streams.reads.clear();
		state.streams.aborts.clear();
		Self::send_process_connect_response(
			state.high,
			id,
			Ok(tg::process::connect::ServerResponseOutput::Detach),
		)
		.await?;
		Ok(())
	}

	async fn read_process_connect(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		mut arg: tg::process::stdio::read::Arg,
	) -> tg::Result<()> {
		// Open the local stdio stream.
		self.server
			.config()
			.process
			.stdio
			.validate_receiver(arg.flow)?;
		if state.streams.reads.len() >= self.server.config().process.stdio.max_reads {
			return Err(tg::error!("too many process reads"));
		}
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		arg.tokens.inherit(&state.tokens);
		arg.location = state.location.clone();
		let (input, receiver) = mpsc::channel(4);
		let output = self
			.try_read_process_stdio_source(&state.id, arg.clone())
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;
		let mut output =
			self.read_process_stdio_protocol(&arg, ReceiverStream::new(receiver).boxed(), output);

		// Register the read request and return its messages.
		state.streams.reads.insert(request_id, input);
		let sender = state.low.clone();
		state.streams.insert(request_id, async move {
			while let Some(message) = output.try_next().await? {
				match message {
					tg::process::stdio::read::ServerMessage::Notification(event) => {
						let notification = tg::process::connect::ReadServerNotification {
							event,
							id: request_id,
						};
						let message = tg::process::connect::ServerMessage::Notification(
							tg::process::connect::ServerNotification::Read(notification),
						);
						sender
							.send(Ok(message))
							.await
							.map_err(|_| tg::error!("the process connection closed"))?;
					},
					tg::process::stdio::read::ServerMessage::Response(output) => {
						Self::send_process_connect_response(
							&sender,
							request_id,
							Ok(tg::process::connect::ServerResponseOutput::Read(output)),
						)
						.await?;
					},
				}
			}

			Ok(())
		});

		Ok(())
	}

	async fn write_process_connect(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		mut arg: tg::process::stdio::write::Arg,
	) -> tg::Result<()> {
		// Prepare stdin once for the connection, then reuse the standalone write implementation.
		if state.writer.is_none() {
			arg.tokens.inherit(&state.tokens);
			let (input, receiver) =
				mpsc::channel(self.server.config().process.stdio.channel_capacity());
			let write_arg = tg::process::stdio::write::stream::Arg {
				location: state.location.clone(),
				streams: vec![Stream::Stdin],
				tokens: arg.tokens.clone(),
			};
			let output = self
				.try_write_process_stdio(
					&state.id,
					write_arg,
					ReceiverStream::new(receiver).boxed(),
				)
				.await?
				.ok_or_else(|| tg::error!("failed to find process stdio"))?;
			state.writer = Some(Writer {
				closed: false,
				input,
				output,
			});
		}
		match &arg.data {
			write::Data::Chunk(chunk) if chunk.stream != Stream::Stdin => {
				return Err(tg::error!("cannot write process stdout or stderr"));
			},
			write::Data::End(end)
				if end.stream_positions.len() != 1
					|| !end.stream_positions.contains_key(&Stream::Stdin) =>
			{
				return Err(tg::error!("invalid stdin end positions"));
			},
			write::Data::Chunk(_) | write::Data::End(_) => {},
		}
		let length = match &arg.data {
			write::Data::Chunk(chunk) => chunk.bytes.len() as u64,
			write::Data::End(_) => 0,
		};
		if length > self.server.config().process.stdio.limits.bytes - state.write_bytes {
			return Err(tg::error!("the stdio byte window was exceeded"));
		}
		let request = write::Request {
			arg: arg.data,
			id: request_id,
		};
		state
			.writer
			.as_ref()
			.unwrap()
			.input
			.try_send(Ok(write::ClientMessage::Request(request)))
			.map_err(|error| tg::error!(!error, "failed to queue the stdio write"))?;
		state.writes.insert(request_id, length);
		state.write_bytes += length;
		Ok(())
	}

	pub(super) async fn send_process_connect_ack(sender: &Sender, id: u64) -> tg::Result<()> {
		let message = tg::process::connect::ServerMessage::Ack(tg::process::connect::Ack { id });
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	pub(super) async fn send_process_connect_response(
		sender: &Sender,
		id: u64,
		result: tg::Result<tg::process::connect::ServerResponseOutput>,
	) -> tg::Result<()> {
		let (error, output) = match result {
			Err(error) => (Some(Self::process_connect_error(&error)), None),
			Ok(output) => (None, Some(output)),
		};
		let response = tg::process::connect::ServerResponse { error, id, output };
		sender
			.send(Ok(tg::process::connect::ServerMessage::Response(response)))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	#[must_use]
	pub(super) fn process_connect_error(error: &tg::Error) -> tg::error::Data {
		tg::error::Data {
			message: Some(error.to_string()),
			source: Some(tg::Referent::new(
				error.to_data_or_id().map_left(Box::new),
				tg::referent::Options::default(),
			)),
			..Default::default()
		}
	}

	async fn send_process_connect_output(
		sender: &Sender,
		output: tg::process::spawn::Output,
	) -> tg::Result<()> {
		let message = tg::process::connect::ServerMessage::Notification(
			tg::process::connect::ServerNotification::Progress(tg::progress::Event::Output(output)),
		);
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	pub(super) async fn finish_process_connect_response(
		input: &mut Input,
		id: u64,
	) -> tg::Result<()> {
		// Keep the request body open until the final response is received or the peer closes it.
		while let Some(message) = input.try_next().await? {
			if matches!(message, tg::process::connect::ClientMessage::Ack(ack) if ack.id == id) {
				break;
			}
		}
		Ok(())
	}
}

impl Streams {
	fn insert(&mut self, id: u64, future: impl Future<Output = tg::Result<()>> + Send + 'static) {
		let (abort, registration) = AbortHandle::new_pair();
		self.aborts.insert(id, abort);
		let future = Abortable::new(future, registration)
			.map(move |result| (id, result.unwrap_or(Ok(()))))
			.boxed();
		self.tasks.push(future);
	}
}
