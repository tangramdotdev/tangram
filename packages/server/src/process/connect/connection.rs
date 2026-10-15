use super::*;

#[cfg(test)]
mod tests;

struct State<'a> {
	cancel: Arc<AtomicBool>,
	high: &'a Sender,
	id: tg::process::Id,
	location: Option<tg::location::Arg>,
	operations: FuturesUnordered<Operation>,
	ready: bool,
	requests: BTreeSet<u64>,
	responses: BTreeSet<u64>,
	tokens: tg::authorization::Tokens,
}

impl Session {
	pub(super) fn get_process_connect_stream_local(
		&self,
		mut options: Options,
		input: Input,
	) -> tg::Result<(tg::process::connect::Header, Output)> {
		// Create a sync for the command objects when an upstream hop will push them.
		let sync = if options.arg.push
			&& let tg::Either::Left(spawn) = &mut options.arg.process
			&& !Self::spawn_process_command_nodes(&spawn.command)?.is_empty()
		{
			let sync = self.prepare_sync(None)?;
			// Attach the sync's tokens to the command, so that the upstream hop can push its objects.
			spawn.command.options.tokens.inherit(&sync.options.tokens);
			Some(sync)
		} else {
			None
		};
		let header = tg::process::connect::Header { sync };

		let config = &self.server.config.process.connect;
		let (high, receiver_high) = mpsc::channel(config.high_priority_channel_capacity);
		let (low, receiver_low) = mpsc::channel(config.low_priority_channel_capacity);
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
		let output = crate::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed();

		Ok((header, output))
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
			arg,
			prepare_output,
			wait_future,
		} = options;

		// Buffer requests while spawning.
		let mut pending = VecDeque::new();

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
				push: false,
				tokens: output.tokens.clone(),
			};
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
				process: id.clone(),
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

		// Return the selected process.
		let state = State {
			cancel,
			high,
			id,
			location,
			operations: FuturesUnordered::new(),
			ready: false,
			requests: BTreeSet::new(),
			responses: BTreeSet::new(),
			tokens,
		};
		Self::send_process_connect_output(high, output).await?;

		// Run the connection until completion or detachment.
		self.run_process_connect_task(state, wait_future, pending, input)
			.boxed()
			.await?;

		Ok(())
	}

	pub(super) fn connect_process_buffer_message(
		pending: &mut VecDeque<tg::process::connect::ClientMessage>,
		message: tg::process::connect::ClientMessage,
		max_pending_messages: usize,
	) -> tg::Result<()> {
		if pending.len() >= max_pending_messages {
			return Err(tg::error!("too many buffered process messages"));
		}
		pending.push_back(message);
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
		// Buffer requests while spawning so the client can send operations immediately.
		let mut progress = self
			.try_spawn_process_inner(arg, prepare_output)
			.await?
			.boxed();
		let mut input_open = true;
		let output = loop {
			tokio::select! {
				message = input.try_next(), if input_open => {
					match message? {
						Some(message) => {
							let max_pending_messages = self.server.config.process.connect.max_pending_messages;
							Self::connect_process_buffer_message(pending, message, max_pending_messages)?;
						},
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

		// Preserve the upstream ordering.
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
				&& state.operations.is_empty()
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
					result = state.operations.next(), if !state.operations.is_empty() => {
						let (id, result) = result.unwrap();
						state.requests.remove(&id);
						result?;
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

	async fn handle_process_connect_message(
		&self,
		state: &mut State<'_>,
		message: tg::process::connect::ClientMessage,
	) -> tg::Result<ControlFlow<u64>> {
		match message {
			tg::process::connect::ClientMessage::Ack(ack) => {
				state.responses.remove(&ack.id);
			},
			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::Ready,
			) => state.ready = true,
			tg::process::connect::ClientMessage::Request(request) => {
				return self.handle_process_connect_request(state, request).await;
			},
		}
		Ok(ControlFlow::Continue(()))
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
		let config = &self.server.config.process.connect;
		if state.responses.len() >= config.max_unacknowledged_responses {
			return Err(tg::error!("too many unacknowledged process responses"));
		}
		state.responses.insert(request.id);

		// Keep detach available when the operation limit is reached.
		let limit = match &request.arg {
			tg::process::connect::ClientRequestArg::Cancel(_)
			| tg::process::connect::ClientRequestArg::Signal(_)
			| tg::process::connect::ClientRequestArg::Tty(_) => {
				state.operations.len() >= config.max_operations
			},
			tg::process::connect::ClientRequestArg::Detach => false,
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
		match request.arg {
			arg @ (tg::process::connect::ClientRequestArg::Cancel(_)
			| tg::process::connect::ClientRequestArg::Signal(_)
			| tg::process::connect::ClientRequestArg::Tty(_)) => {
				self.start_process_connect_operation(state, request.id, &arg);
				Ok(ControlFlow::Continue(()))
			},
			tg::process::connect::ClientRequestArg::Detach => {
				Self::detach_process_connect(state, request.id).await?;
				Ok(ControlFlow::Break(request.id))
			},
		}
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

	/// Run an operation that a client requested on its connection, which always acts on the connection's process.
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
				arg.process = Some(id.clone());
				let output = self
					.try_cancel_process(arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Cancel(output)
			},
			tg::process::connect::ClientRequestArg::Detach => unreachable!(),
			tg::process::connect::ClientRequestArg::Signal(mut arg) => {
				arg.location = location;
				arg.process = Some(id.clone());
				arg.tokens.inherit(&tokens);
				self.try_post_process_signal(arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Signal
			},
			tg::process::connect::ClientRequestArg::Tty(mut arg) => {
				arg.location = location;
				arg.process = Some(id.clone());
				arg.tokens.inherit(&tokens);
				self.try_set_process_tty_size(arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Tty
			},
		};
		Ok(output)
	}

	async fn detach_process_connect(state: &mut State<'_>, id: u64) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		Self::send_process_connect_response(
			state.high,
			id,
			Ok(tg::process::connect::ServerResponseOutput::Detach),
		)
		.await?;
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
			code: error.code(),
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
