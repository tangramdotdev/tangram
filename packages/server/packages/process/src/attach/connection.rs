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

pub(super) struct AttachProcessSpawnLocalArg<'a> {
	input: &'a mut Input,
	mode: tg::process::attach::Mode,
	pending: &'a mut VecDeque<tg::process::attach::ClientMessage>,
	prepare_output: spawn::PrepareOutput,
	sender: &'a Sender,
	spawn_arg: tg::process::spawn::Arg,
}

impl Component {
	pub(super) fn get_process_attach_stream_local(
		&self,
		session: &Session,
		mut options: Options,
		input: Input,
	) -> tg::Result<(tg::process::attach::Header, Output)> {
		// Create a sync for the command objects when an upstream hop will push them.
		let sync = if options.arg.push
			&& let tg::Either::Left(spawn) = &mut options.arg.process
			&& !Self::spawn_process_command_nodes(&spawn.command)?.is_empty()
		{
			let sync = session.server.components.sync.prepare_sync(session, None)?;
			// Attach the sync's tokens to the command, so that the upstream hop can push its objects.
			spawn.command.options.tokens.inherit(&sync.options.tokens);
			Some(sync)
		} else {
			None
		};
		let header = tg::process::attach::Header { sync };

		let config = &session.server.config.process.attach;
		let (high, receiver_high) = mpsc::channel(config.high_priority_channel_capacity);
		let (low, receiver_low) = mpsc::channel(config.low_priority_channel_capacity);
		// The connection owns its lease until completion, detachment, or client disconnect.
		let component = self.clone();
		let mut session = session.clone();
		session.context.stopper = None;
		let task = Task::spawn(move |_| async move {
			if let Err(error) = component
				.run_process_attach_local_task(&session, options, input, &high, &low)
				.boxed()
				.await
			{
				high.send(Err(error)).await.ok();
			}
		});
		let output = tangram_server_core::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed();

		Ok((header, output))
	}

	pub(super) async fn run_process_attach_local_task(
		&self,
		session: &Session,
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
				let arg = AttachProcessSpawnLocalArg {
					input: &mut input,
					mode,
					pending: &mut pending,
					prepare_output: prepare_output.unwrap(),
					sender: high,
					spawn_arg: *spawn,
				};
				let output = self.attach_process_spawn_local(session, arg).await?;
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
		if mode == tg::process::attach::Mode::Spawn {
			Self::send_process_attach_output(high, output).await?;
			while let Some(message) = input.try_next().await? {
				if matches!(
					message,
					tg::process::attach::ClientMessage::Notification(
						tg::process::attach::ClientNotification::Ready
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
				session.server.location(location.as_ref())?,
				tg::Location::Local(tg::location::Local { region })
				if region.as_deref().is_none_or(|region| Some(region) == session.server.config.region.as_deref())
			) {
			let arg = tg::process::attach::Arg {
				lease: output.lease,
				location,
				mode,
				nonce: None,
				open: Vec::new(),
				process: tg::Either::Right(output.process.as_ref().unwrap_right().clone()),
				push: false,
				tokens: output.tokens.clone(),
			};
			let input = futures::stream::iter(pending.into_iter().map(Ok))
				.chain(input)
				.boxed();
			self.attach_process_cached_task(session, arg, output, input, low)
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
			.then(|| spawn::lease::LeaseGuard::new(session, &output))
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
			} else if let Some((future, _)) = self
				.try_wait_process_runner(session, &id, &wait_arg)
				.await?
			{
				future
			} else {
				self.try_wait_process_local(
					session,
					&id,
					wait_arg.tokens.local_authorization().to_vec(),
					tg::process::Source::Auto,
				)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))?
			};
			wait_arg.lease = output.lease;
			Self::attach_wait_process_guard(
				session,
				&id,
				&wait_arg,
				location.clone(),
				cancel.clone(),
				future,
			)
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
		Self::send_process_attach_output(high, output).await?;

		// Run the connection until completion or detachment.
		self.run_process_attach_task(session, state, wait_future, pending, input)
			.boxed()
			.await?;

		Ok(())
	}

	pub(super) fn attach_process_buffer_message(
		pending: &mut VecDeque<tg::process::attach::ClientMessage>,
		message: tg::process::attach::ClientMessage,
		max_pending_messages: usize,
	) -> tg::Result<()> {
		if pending.len() >= max_pending_messages {
			return Err(tg::error!("too many buffered process messages"));
		}
		pending.push_back(message);
		Ok(())
	}

	pub(super) async fn attach_process_spawn_local(
		&self,
		session: &Session,
		arg: AttachProcessSpawnLocalArg<'_>,
	) -> tg::Result<tg::process::spawn::Output> {
		let AttachProcessSpawnLocalArg {
			input,
			mode,
			pending,
			prepare_output,
			sender,
			spawn_arg,
		} = arg;

		// Buffer requests while spawning so the client can send operations immediately.
		let mut progress = self
			.try_spawn_process_inner(session, spawn_arg, prepare_output)
			.await?
			.boxed();
		let mut input_open = true;
		let output = loop {
			tokio::select! {
				message = input.try_next(), if input_open => {
					match message? {
						Some(message) => {
							let max_pending_messages = session.server.config.process.attach.max_pending_messages;
							Self::attach_process_buffer_message(pending, message, max_pending_messages)?;
						},
						None if mode == tg::process::attach::Mode::Spawn => input_open = false,
						None => return Err(tg::error!("the process connection closed while spawning")),
					}
				},
				event = progress.try_next() => {
					let event = event?.ok_or_else(|| tg::error!("the spawn stream ended without an output"))?;
					if let tg::progress::Event::Output(output) = event {
						break output.ok_or_else(|| tg::error!("expected a process"))?;
					}
					let message = tg::process::attach::ServerMessage::Notification(tg::process::attach::ServerNotification::Progress(event.map_output(Option::unwrap)));
					sender.send(Ok(message)).await.map_err(|_| tg::error!("the process connection closed"))?;
				},
			}
		};

		Ok(output)
	}

	pub(super) async fn attach_process_cached_task(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		output: tg::process::spawn::Output,
		input: Input,
		sender: &Sender,
	) -> tg::Result<()> {
		// Transfer the lease to a connection at the selected cache location.
		let mut guard = spawn::lease::LeaseGuard::new(session, &output);
		let (_, mut stream) = session
			.get_process_attach_stream(arg, input)
			.boxed()
			.await?;
		if let Some(guard) = &mut guard {
			guard.disarm();
		}

		// Preserve the upstream ordering.
		while let Some(mut message) = stream.try_next().await? {
			if let tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
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

	async fn run_process_attach_task(
		&self,
		session: &Session,
		mut state: State<'_>,
		mut wait_future: WaitFuture,
		mut pending: VecDeque<tg::process::attach::ClientMessage>,
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
						Self::handle_process_attach_outcome(&mut state, outcome).await?;
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
				.handle_process_attach_message(session, &mut state, message)
				.await?
			{
				Self::finish_process_attach_response(&mut input, id).await?;
				return Ok(false);
			}
		}
	}

	async fn handle_process_attach_outcome(
		state: &mut State<'_>,
		outcome: tg::process::outcome::Data,
	) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		let notification = tg::process::attach::ServerNotification::Outcome(outcome);
		let message = tg::process::attach::ServerMessage::Notification(notification);
		state
			.high
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	async fn handle_process_attach_message(
		&self,
		session: &Session,
		state: &mut State<'_>,
		message: tg::process::attach::ClientMessage,
	) -> tg::Result<ControlFlow<u64>> {
		match message {
			tg::process::attach::ClientMessage::Ack(ack) => {
				state.responses.remove(&ack.id);
			},
			tg::process::attach::ClientMessage::Notification(
				tg::process::attach::ClientNotification::Ready,
			) => state.ready = true,
			tg::process::attach::ClientMessage::Request(request) => {
				return self
					.handle_process_attach_request(session, state, request)
					.await;
			},
		}
		Ok(ControlFlow::Continue(()))
	}

	async fn handle_process_attach_request(
		&self,
		session: &Session,
		state: &mut State<'_>,
		request: tg::process::attach::ClientRequest,
	) -> tg::Result<ControlFlow<u64>> {
		// Acknowledge receipt and register the request.
		Self::send_process_attach_ack(state.high, request.id).await?;
		if state.responses.contains(&request.id) || !state.requests.insert(request.id) {
			return Err(tg::error!("duplicate process request id"));
		}
		let config = &session.server.config.process.attach;
		if state.responses.len() >= config.max_unacknowledged_responses {
			return Err(tg::error!("too many unacknowledged process responses"));
		}
		state.responses.insert(request.id);

		// Keep detach available when the operation limit is reached.
		let limit = match &request.arg {
			tg::process::attach::ClientRequestArg::Cancel(_)
			| tg::process::attach::ClientRequestArg::Signal(_)
			| tg::process::attach::ClientRequestArg::Tty(_) => {
				state.operations.len() >= config.max_operations
			},
			tg::process::attach::ClientRequestArg::Detach => false,
		};
		if limit {
			Self::send_process_attach_response(
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
			arg @ (tg::process::attach::ClientRequestArg::Cancel(_)
			| tg::process::attach::ClientRequestArg::Signal(_)
			| tg::process::attach::ClientRequestArg::Tty(_)) => {
				self.start_process_attach_operation(session, state, request.id, &arg);
				Ok(ControlFlow::Continue(()))
			},
			tg::process::attach::ClientRequestArg::Detach => {
				Self::detach_process_attach(state, request.id).await?;
				Ok(ControlFlow::Break(request.id))
			},
		}
	}

	fn start_process_attach_operation(
		&self,
		session: &Session,
		state: &mut State<'_>,
		request_id: u64,
		arg: &tg::process::attach::ClientRequestArg,
	) {
		let component = self.clone();
		let session = session.clone();
		let id = state.id.clone();
		let location = state.location.clone();
		let tokens = state.tokens.clone();
		let arg = arg.clone();
		let sender = state.high.clone();
		let future = async move {
			let result = component
				.run_process_attach_operation(&session, &id, arg, location, tokens)
				.await;
			let result = Self::send_process_attach_response(&sender, request_id, result).await;
			(request_id, result)
		}
		.boxed();
		state.operations.push(future);
	}

	pub(super) async fn run_process_attach_operation(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::attach::ClientRequestArg,
		location: Option<tg::location::Arg>,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<tg::process::attach::ServerResponseOutput> {
		let output = match arg {
			tg::process::attach::ClientRequestArg::Cancel(mut arg) => {
				arg.location = location;
				let output = self
					.try_cancel_process(session, &tg::Either::Right(id.clone()), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::attach::ServerResponseOutput::Cancel(output)
			},
			tg::process::attach::ClientRequestArg::Detach => unreachable!(),
			tg::process::attach::ClientRequestArg::Signal(mut arg) => {
				arg.location = location;
				arg.tokens.inherit(&tokens);
				self.try_post_process_signal(session, &tg::Either::Right(id.clone()), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::attach::ServerResponseOutput::Signal
			},
			tg::process::attach::ClientRequestArg::Tty(mut arg) => {
				arg.location = location;
				arg.tokens.inherit(&tokens);
				self.try_set_process_tty_size(session, &tg::Either::Right(id.clone()), arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::attach::ServerResponseOutput::Tty
			},
		};
		Ok(output)
	}

	async fn detach_process_attach(state: &mut State<'_>, id: u64) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		Self::send_process_attach_response(
			state.high,
			id,
			Ok(tg::process::attach::ServerResponseOutput::Detach),
		)
		.await?;
		Ok(())
	}

	pub(super) async fn send_process_attach_ack(sender: &Sender, id: u64) -> tg::Result<()> {
		let message = tg::process::attach::ServerMessage::Ack(tg::process::attach::Ack { id });
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	pub(super) async fn send_process_attach_response(
		sender: &Sender,
		id: u64,
		result: tg::Result<tg::process::attach::ServerResponseOutput>,
	) -> tg::Result<()> {
		let (error, output) = match result {
			Err(error) => (Some(Self::process_attach_error(&error)), None),
			Ok(output) => (None, Some(output)),
		};
		let response = tg::process::attach::ServerResponse { error, id, output };
		sender
			.send(Ok(tg::process::attach::ServerMessage::Response(response)))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	#[must_use]
	pub(super) fn process_attach_error(error: &tg::Error) -> tg::error::Data {
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

	async fn send_process_attach_output(
		sender: &Sender,
		output: tg::process::spawn::Output,
	) -> tg::Result<()> {
		let message = tg::process::attach::ServerMessage::Notification(
			tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(output)),
		);
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	pub(super) async fn finish_process_attach_response(
		input: &mut Input,
		id: u64,
	) -> tg::Result<()> {
		// Keep the request body open until the final response is received or the peer closes it.
		while let Some(message) = input.try_next().await? {
			if matches!(message, tg::process::attach::ClientMessage::Ack(ack) if ack.id == id) {
				break;
			}
		}
		Ok(())
	}
}
