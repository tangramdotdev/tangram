use {
	crate::{Session, process::spawn},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{AbortHandle, Abortable, BoxFuture},
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		ops::ControlFlow,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tokio::sync::{mpsc, oneshot},
	tokio_stream::wrappers::ReceiverStream,
};

const MAX_OPERATIONS: usize = 64;

type Input = BoxStream<'static, tg::Result<tg::process::connect::ClientMessage>>;
type Operation = BoxFuture<'static, (u64, tg::Result<()>)>;
type Output = BoxStream<'static, tg::Result<tg::process::connect::ServerMessage>>;
type Sender = mpsc::Sender<tg::Result<tg::process::connect::ServerMessage>>;
type Wait = BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>;

struct Options {
	arg: tg::process::connect::Arg,
	id: u64,
	prepared: Option<spawn::Prepared>,
	wait: Option<Wait>,
}

struct State<'a> {
	cancel: Arc<AtomicBool>,
	high: &'a Sender,
	id: tg::process::Id,
	low: &'a Sender,
	operations: FuturesUnordered<Operation>,
	requests: BTreeSet<u64>,
	responses: BTreeSet<u64>,
	streams: Streams,
	tokens: tg::authorization::Tokens,
}

struct Streams {
	aborts: BTreeMap<u64, AbortHandle>,
	reads: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>>,
	tasks: FuturesUnordered<Operation>,
	writes: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::write::ClientMessage>>>,
}

impl Session {
	pub fn try_connect_process(
		&self,
		mut input: Input,
	) -> BoxFuture<'_, tg::Result<Option<Output>>> {
		async move {
			// Read the opening request.
			let Some(tg::process::connect::ClientMessage::Request(
				tg::process::connect::ClientRequest {
					arg: tg::process::connect::ClientRequestArg::Connect(mut arg),
					id: request_id,
				},
			)) = input.try_next().await?
			else {
				return Err(tg::error!("expected a connect request"));
			};

			// Validate the initial reads.
			if arg.reads.len() > MAX_OPERATIONS || arg.reads.contains_key(&request_id) {
				return Err(tg::error!("invalid initial process reads"));
			}
			if matches!(
				&arg.target,
				tg::process::connect::Target::Spawn {
					mode: tg::process::connect::Mode::Spawn,
					..
				}
			) && !arg.reads.is_empty()
			{
				return Err(tg::error!("spawn mode does not support initial reads"));
			}

			// Resolve the destination.
			let mut input = Some(input);
			if matches!(&arg.target, tg::process::connect::Target::Existing { .. }) {
				return self
					.try_connect_process_inner(arg, request_id, &mut input)
					.await;
			}

			// A spawn selects one destination, using the same preparation and routing as spawn.
			let tg::process::connect::Target::Spawn { arg: spawn, .. } = &mut arg.target else {
				unreachable!()
			};
			let prepared = self.prepare_spawn_process(spawn).await?;
			let location = self.server.location(spawn.location.as_ref())?;
			if matches!(
				location,
				tg::Location::Local(tg::location::Local { region: None })
			) {
				return self
					.try_connect_process_local(arg, request_id, &mut input, Some(prepared))
					.await;
			}

			let input = input.take().unwrap();
			let (sender, receiver) = mpsc::channel(64);
			let session = self.clone();
			let task = Task::spawn(move |_| async move {
				if let Err(error) = session
					.connect_process_spawn_task(arg, request_id, input, prepared, &sender)
					.await
				{
					sender.send(Err(error)).await.ok();
				}
			});
			let output = ReceiverStream::new(receiver).attach(task).boxed();

			Ok(Some(output))
		}
		.boxed()
	}

	async fn connect_process_spawn_task(
		&self,
		mut arg: tg::process::connect::Arg,
		id: u64,
		input: Input,
		prepared: spawn::Prepared,
		sender: &Sender,
	) -> tg::Result<()> {
		let tg::process::connect::Target::Spawn { arg: spawn_arg, .. } = &mut arg.target else {
			unreachable!()
		};
		let spawn::Prepared {
			command,
			parent_sandbox,
		} = prepared;
		let location = self.server.location(spawn_arg.location.as_ref())?;
		let mut notify = self
			.try_prepare_spawn_process_for_location(spawn_arg, &location, parent_sandbox.as_ref())
			.await;
		let spawn_arg = spawn_arg.clone();

		// Push the command and report progress.
		let progress = crate::progress::Handle::new();
		let mut events = progress.stream().boxed();
		let mut push = self
			.spawn_process_push_command(&command, Some(location.clone()), &progress)
			.boxed();
		loop {
			tokio::select! {
				result = &mut push => {
					result?;
					break;
				},
				event = events.try_next() => {
					let event = event?.ok_or_else(|| tg::error!("the command transfer ended"))?;
					let notification = tg::process::connect::ServerNotification::Progress(event.map_output(|_| ()));
					let message = tg::process::connect::ServerMessage::Notification(notification);
					sender.send(Ok(message)).await.map_err(|_| tg::error!("the process connection closed"))?;
				},
			}
		}
		drop(push);

		// Connect to the destination.
		let mut input = Some(input);
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_connect_process_region(arg, id, &mut input, &region)
					.await?
			},
			tg::Location::Local(tg::location::Local { region: None }) => unreachable!(),
			tg::Location::Remote(remote) => {
				let remote = crate::location::Remote {
					name: remote.name,
					regions: remote.region.map(|region| vec![region]),
				};
				self.try_connect_process_remote(arg, id, &mut input, &remote)
					.await?
			},
		}
		.ok_or_else(|| tg::error!("failed to find the process"))?;

		// Register the child and return the connection messages.
		let mut output = output;
		loop {
			let message = tokio::select! {
				message = output.try_next() => message?,
				() = async { notify.as_mut().unwrap().await }, if notify.is_some() => {
					notify = None;
					continue;
				},
			};
			let Some(message) = message else {
				break;
			};
			if let tg::process::connect::ServerMessage::Response(response) = &message
				&& let Some(tg::process::connect::ServerResponseOutput::Connect(output)) =
					&response.output
			{
				notify = None;
				self.spawn_process_add_child(&spawn_arg, &command, output)
					.await?;
			}
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		Ok(())
	}

	async fn try_connect_process_inner(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
	) -> tg::Result<Option<Output>> {
		let tg::process::connect::Target::Existing { options, .. } = &arg.target else {
			unreachable!()
		};
		let locations = self
			.locations(options.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_connect_process_local(arg.clone(), id, input, None)
					.await
					.map_err(|error| tg::error!(!error, "failed to connect to the local process"))?
			{
				return Ok(Some(output));
			}
			if let Some(output) = self
				.try_connect_process_regions(arg.clone(), id, input, &local.regions)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to connect to the process in another region")
				})? {
				return Ok(Some(output));
			}
		}
		if let Some(output) = self
			.try_connect_process_remotes(arg, id, input, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, "failed to connect to the process on a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_connect_process_local(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
		prepared: Option<spawn::Prepared>,
	) -> tg::Result<Option<Output>> {
		let wait = if let tg::process::connect::Target::Existing { id, options } = &arg.target {
			let Some(wait) = self
				.try_wait_process_local(id, options.tokens.local().to_vec())
				.await?
			else {
				return Ok(None);
			};
			Some(wait)
		} else {
			None
		};
		let options = Options {
			arg,
			id,
			prepared,
			wait,
		};
		let input = input.take().unwrap();
		let (high, receiver_high) = mpsc::channel(64);
		let (low, receiver_low) = mpsc::channel(16);
		let session = self.clone();
		let task = Task::spawn(move |_| async move {
			if let Err(error) = session
				.connect_process_local_task(options, input, &high, &low)
				.boxed()
				.await
			{
				high.send(Err(error)).await.ok();
			}
		});
		let output = crate::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed();
		Ok(Some(output))
	}

	async fn connect_process_local_task(
		&self,
		options: Options,
		mut input: Input,
		high: &Sender,
		low: &Sender,
	) -> tg::Result<()> {
		// Select the process.
		let Options {
			arg,
			id: request_id,
			prepared,
			wait,
		} = options;
		Self::send_connect_ack(high, request_id).await?;
		let mut pending = VecDeque::new();
		let (output, mode, location) = match arg.target {
			tg::process::connect::Target::Existing { id, options } => {
				let location = tg::Location::Local(tg::location::Local {
					region: self.server.config.region.clone(),
				});
				let output = tg::process::spawn::Output {
					cached: false,
					lease: options.lease,
					location: Some(location.clone()),
					process: tg::Either::Right(id),
					tokens: options.tokens,
					wait: None,
				};
				(
					output,
					tg::process::connect::Mode::Run,
					Some(location.into()),
				)
			},
			tg::process::connect::Target::Spawn { arg, mode } => {
				let output = self
					.connect_process_spawn_local(
						*arg,
						mode,
						prepared.unwrap(),
						&mut input,
						&mut pending,
						high,
					)
					.await?;
				let location = output.location.clone().map(Into::into);
				(output, mode, location)
			},
		};

		// Complete a spawn-only connection.
		if mode == tg::process::connect::Mode::Spawn {
			Self::send_connect_response(
				high,
				request_id,
				Ok(tg::process::connect::ServerResponseOutput::Connect(output)),
			)
			.await?;
			Self::finish_connect_response(&mut input, request_id).await?;
			return Ok(());
		}

		// Follow a cached process to its selected location using the same connection routing.
		if !matches!(
			self.server.location(location.as_ref())?,
			tg::Location::Local(tg::location::Local { region: None })
		) {
			let options = tg::process::wait::Arg {
				lease: output.lease.clone(),
				location,
				tokens: output.tokens.clone(),
			};
			let arg = tg::process::connect::Arg {
				reads: arg.reads,
				target: tg::process::connect::Target::Existing {
					id: output.process.as_ref().unwrap_right().clone(),
					options,
				},
			};
			let request = tg::process::connect::ClientRequest {
				arg: tg::process::connect::ClientRequestArg::Connect(arg),
				id: request_id,
			};
			let input = futures::stream::once(futures::future::ok(
				tg::process::connect::ClientMessage::Request(request),
			))
			.chain(futures::stream::iter(pending.into_iter().map(Ok)))
			.chain(input)
			.boxed();
			self.connect_process_cached_task(output, input, high, low)
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
			.wait
			.is_none()
			.then(|| spawn::lease::LeaseGuard::new(self, &output))
			.flatten();
		let wait_arg = tg::process::wait::Arg {
			lease: output.lease.clone(),
			location: location.clone(),
			tokens: tokens.clone(),
		};
		let wait = if let Some(output) = output.wait.clone() {
			futures::future::ready(Ok(Some(output))).boxed()
		} else {
			let future = match wait {
				Some(wait) => wait,
				None => self
					.try_wait_process_local(&id, wait_arg.tokens.local().to_vec())
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?,
			};
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
			writes: BTreeMap::new(),
		};
		let mut state = State {
			cancel,
			high,
			id,
			low,
			operations: FuturesUnordered::new(),
			requests: BTreeSet::from([request_id]),
			responses: BTreeSet::from([request_id]),
			streams,
			tokens,
		};
		for (request_id, arg) in arg.reads {
			state.requests.insert(request_id);
			self.connect_process_read(&mut state, request_id, arg)
				.await?;
		}
		Self::send_connect_response(
			high,
			request_id,
			Ok(tg::process::connect::ServerResponseOutput::Connect(output)),
		)
		.await?;

		// Run the connection until completion or detachment.
		self.connect_process_run_task(state, wait, pending, input)
			.await?;

		Ok(())
	}

	async fn connect_process_spawn_local(
		&self,
		arg: tg::process::spawn::Arg,
		mode: tg::process::connect::Mode,
		prepared: spawn::Prepared,
		input: &mut Input,
		pending: &mut VecDeque<tg::process::connect::ClientMessage>,
		sender: &Sender,
	) -> tg::Result<tg::process::spawn::Output> {
		// Buffer requests while spawning so the client can send stdio immediately.
		let mut progress = self.try_spawn_process_inner(arg, prepared).await?.boxed();
		let mut input_open = true;
		let output = loop {
			tokio::select! {
				message = input.try_next(), if input_open && pending.len() < MAX_OPERATIONS => {
					match message? {
						Some(message) => pending.push_back(message),
						None if mode == tg::process::connect::Mode::Spawn => input_open = false,
						None => return Err(tg::error!("the process connection closed while spawning")),
					}
				},
				event = progress.try_next() => {
					let event = event?.ok_or_else(|| tg::error!("the spawn stream ended without an output"))?;
					if let tg::progress::Event::Output(output) = event {
						break output.ok_or_else(|| tg::error!("expected a process"))?;
					}
					let message = tg::process::connect::ServerMessage::Notification(tg::process::connect::ServerNotification::Progress(event.map_output(|_| ())));
					sender.send(Ok(message)).await.map_err(|_| tg::error!("the process connection closed"))?;
				},
			}
		};

		Ok(output)
	}

	async fn connect_process_cached_task(
		&self,
		output: tg::process::spawn::Output,
		input: Input,
		high: &Sender,
		low: &Sender,
	) -> tg::Result<()> {
		// Transfer the lease to a connection at the selected cache location.
		let mut guard = spawn::lease::LeaseGuard::new(self, &output);
		let mut stream = self.connect_process(input).boxed().await?;
		if let Some(guard) = &mut guard {
			guard.disarm();
		}

		// Preserve the spawn result and the message priorities.
		while let Some(mut message) = stream.try_next().await? {
			if let tg::process::connect::ServerMessage::Response(response) = &mut message
				&& let Some(tg::process::connect::ServerResponseOutput::Connect(selected)) =
					&mut response.output
			{
				selected.cached = output.cached;
				selected.wait = output.wait.clone();
			}
			let sender = if matches!(
				message,
				tg::process::connect::ServerMessage::Notification(
					tg::process::connect::ServerNotification::Read(_)
				)
			) {
				low
			} else {
				high
			};
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		Ok(())
	}

	async fn connect_process_run_task(
		&self,
		mut state: State<'_>,
		mut wait: Wait,
		mut pending: VecDeque<tg::process::connect::ClientMessage>,
		mut input: Input,
	) -> tg::Result<()> {
		// Keep completion independent of subscribed output and its EOF handshakes.
		let mut finished = false;
		loop {
			if finished
				&& pending.is_empty()
				&& state.streams.tasks.is_empty()
				&& state.operations.is_empty()
				&& state.responses.is_empty()
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
						Self::connect_process_handle_wait(&mut state, output).await?;
						finished = true;
						continue;
					},
					result = state.streams.tasks.next(), if !state.streams.tasks.is_empty() => {
						let (id, result) = result.unwrap();
						Self::connect_process_handle_stream(&mut state, id, result).await?;
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
				.connect_process_handle_message(&mut state, message)
				.await?
			{
				Self::finish_connect_response(&mut input, id).await?;
				return Ok(());
			}
		}
	}

	async fn connect_process_handle_wait(
		state: &mut State<'_>,
		output: tg::process::wait::Output,
	) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		let notification = tg::process::connect::ServerNotification::Wait(output);
		let message = tg::process::connect::ServerMessage::Notification(notification);
		state
			.high
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	async fn connect_process_handle_stream(
		state: &mut State<'_>,
		id: u64,
		result: tg::Result<()>,
	) -> tg::Result<()> {
		// Release the completed subscription.
		state.streams.aborts.remove(&id);
		let read = state.streams.reads.remove(&id).is_some();
		let active = read | state.streams.writes.remove(&id).is_some();
		state.requests.remove(&id);

		// Report errors only for subscriptions that the client has not closed.
		if active && let Err(error) = result {
			let error = Self::connect_process_error(&error);
			let notification = tg::process::connect::ErrorServerNotification { error, id };
			let notification = tg::process::connect::ServerNotification::Error(notification);
			let message = tg::process::connect::ServerMessage::Notification(notification);
			let sender = if read { state.low } else { state.high };
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		Ok(())
	}

	async fn connect_process_handle_message(
		&self,
		state: &mut State<'_>,
		message: tg::process::connect::ClientMessage,
	) -> tg::Result<ControlFlow<u64>> {
		match message {
			tg::process::connect::ClientMessage::Ack(ack) => {
				state.responses.remove(&ack.id);
			},
			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::Read(notification),
			) => {
				Self::connect_process_handle_read(state, notification)?;
			},
			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::Write(notification),
			) => {
				Self::connect_process_handle_write(state, notification)?;
			},
			tg::process::connect::ClientMessage::Request(request) => {
				return self.connect_process_handle_request(state, request).await;
			},
		}
		Ok(ControlFlow::Continue(()))
	}

	fn connect_process_handle_read(
		state: &State<'_>,
		notification: tg::process::connect::ReadClientNotification,
	) -> tg::Result<()> {
		let sender = state
			.streams
			.reads
			.get(&notification.id)
			.ok_or_else(|| tg::error!("unknown process read"))?;
		sender
			.try_send(Ok(notification.message))
			.map_err(|error| tg::error!(!error, "failed to deliver the read message"))?;
		Ok(())
	}

	fn connect_process_handle_write(
		state: &State<'_>,
		notification: tg::process::connect::WriteClientNotification,
	) -> tg::Result<()> {
		let sender = state
			.streams
			.writes
			.get(&notification.id)
			.ok_or_else(|| tg::error!("unknown process write"))?;
		sender
			.try_send(Ok(notification.message))
			.map_err(|error| tg::error!(!error, "failed to deliver the write message"))?;
		Ok(())
	}

	async fn connect_process_handle_request(
		&self,
		state: &mut State<'_>,
		request: tg::process::connect::ClientRequest,
	) -> tg::Result<ControlFlow<u64>> {
		// Acknowledge receipt and register the request.
		Self::send_connect_ack(state.high, request.id).await?;
		if state.responses.contains(&request.id) || !state.requests.insert(request.id) {
			return Err(tg::error!("duplicate process request id"));
		}
		if state.responses.len() >= MAX_OPERATIONS {
			return Err(tg::error!("too many unacknowledged process responses"));
		}
		state.responses.insert(request.id);

		// Keep close and detach available when the operation limit is reached.
		let limit = match &request.arg {
			tg::process::connect::ClientRequestArg::Cancel(_)
			| tg::process::connect::ClientRequestArg::Connect(_)
			| tg::process::connect::ClientRequestArg::Signal(_)
			| tg::process::connect::ClientRequestArg::Tty(_) => state.operations.len() >= MAX_OPERATIONS,
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Detach => false,
			tg::process::connect::ClientRequestArg::Read(_)
			| tg::process::connect::ClientRequestArg::Write(_) => {
				state.streams.tasks.len() >= MAX_OPERATIONS
					|| state.operations.len() >= MAX_OPERATIONS
			},
		};
		if limit {
			Self::send_connect_response(
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
				self.connect_process_start_operation(state, request.id, arg);
				return Ok(ControlFlow::Continue(()));
			},
			tg::process::connect::ClientRequestArg::Close(id) => {
				Self::connect_process_close(state, id);
				Ok(tg::process::connect::ServerResponseOutput::Close)
			},
			tg::process::connect::ClientRequestArg::Connect(_) => {
				Err(tg::error!("the process is already connected"))
			},
			tg::process::connect::ClientRequestArg::Detach => {
				Self::connect_process_detach(state, request.id).await?;
				return Ok(ControlFlow::Break(request.id));
			},
			tg::process::connect::ClientRequestArg::Read(arg) => self
				.connect_process_read(state, request.id, arg)
				.await
				.map(|()| tg::process::connect::ServerResponseOutput::Read),
			tg::process::connect::ClientRequestArg::Write(arg) => self
				.connect_process_write(state, request.id, arg)
				.await
				.map(|()| tg::process::connect::ServerResponseOutput::Write),
		};

		// Retain subscription IDs until their streams finish.
		Self::send_connect_response(state.high, request.id, result).await?;
		if !state.streams.reads.contains_key(&request.id)
			&& !state.streams.writes.contains_key(&request.id)
		{
			state.requests.remove(&request.id);
		}

		Ok(ControlFlow::Continue(()))
	}

	fn connect_process_start_operation(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		arg: tg::process::connect::ClientRequestArg,
	) {
		let session = self.clone();
		let id = state.id.clone();
		let sender = state.high.clone();
		let tokens = state.tokens.clone();
		let future = async move {
			let result = session.connect_process_operation(&id, arg, tokens).await;
			let result = Self::send_connect_response(&sender, request_id, result).await;
			(request_id, result)
		}
		.boxed();
		state.operations.push(future);
	}

	async fn connect_process_operation(
		&self,
		id: &tg::process::Id,
		arg: tg::process::connect::ClientRequestArg,
		tokens: tg::authorization::Tokens,
	) -> tg::Result<tg::process::connect::ServerResponseOutput> {
		let output = match arg {
			tg::process::connect::ClientRequestArg::Cancel(arg) => {
				let output = self
					.try_cancel_process_local(id, arg)
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Cancel(output)
			},
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Connect(_)
			| tg::process::connect::ClientRequestArg::Detach
			| tg::process::connect::ClientRequestArg::Read(_)
			| tg::process::connect::ClientRequestArg::Write(_) => unreachable!(),
			tg::process::connect::ClientRequestArg::Signal(mut arg) => {
				arg.tokens.inherit(&tokens);
				self.try_post_process_signal_local(id, arg.signal, arg.tokens.local())
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Signal
			},
			tg::process::connect::ClientRequestArg::Tty(mut arg) => {
				arg.tokens.inherit(&tokens);
				self.try_set_process_tty_size_local(id, arg.size, arg.tokens.local())
					.await?
					.ok_or_else(|| tg::error!("failed to find the process"))?;
				tg::process::connect::ServerResponseOutput::Tty
			},
		};
		Ok(output)
	}

	fn connect_process_close(state: &mut State<'_>, id: u64) {
		state.streams.reads.remove(&id);
		state.streams.writes.remove(&id);
		if let Some(abort) = state.streams.aborts.get(&id) {
			abort.abort();
		}
	}

	async fn connect_process_detach(state: &mut State<'_>, id: u64) -> tg::Result<()> {
		state.cancel.store(false, Ordering::SeqCst);
		Self::send_connect_response(
			state.high,
			id,
			Ok(tg::process::connect::ServerResponseOutput::Detach),
		)
		.await?;
		Ok(())
	}

	async fn connect_process_read(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		mut arg: tg::process::stdio::read::Arg,
	) -> tg::Result<()> {
		// Open the local stdio stream.
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		arg.tokens.inherit(&state.tokens);
		let (input, receiver) = mpsc::channel(4);
		let output = self
			.try_read_process_stdio_local(&state.id, arg.clone())
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;
		let mut output =
			self.read_process_stdio_protocol(arg, ReceiverStream::new(receiver).boxed(), output);

		// Register the subscription and return its messages.
		state.streams.reads.insert(request_id, input);
		let sender = state.low.clone();
		state.streams.insert(request_id, async move {
			while let Some(message) = output.try_next().await? {
				let notification = tg::process::connect::ReadServerNotification {
					id: request_id,
					message,
				};
				sender
					.send(Ok(tg::process::connect::ServerMessage::Notification(
						tg::process::connect::ServerNotification::Read(notification),
					)))
					.await
					.map_err(|_| tg::error!("the process connection closed"))?;
			}
			Ok(())
		});

		Ok(())
	}

	async fn connect_process_write(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		mut arg: tg::process::stdio::write::Arg,
	) -> tg::Result<()> {
		// Open the local stdio stream.
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		arg.tokens.inherit(&state.tokens);
		let (input, receiver) = mpsc::channel(4);
		let mut output = self
			.try_write_process_stdio_local(
				&state.id,
				&arg.streams,
				ReceiverStream::new(receiver).boxed(),
				self.context.stopper.clone(),
				arg.tokens.local(),
			)
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;

		// Register the subscription and return its messages.
		state.streams.writes.insert(request_id, input);
		let sender = state.high.clone();
		state.streams.insert(request_id, async move {
			while let Some(message) = output.try_next().await? {
				let notification = tg::process::connect::WriteServerNotification {
					id: request_id,
					message,
				};
				sender
					.send(Ok(tg::process::connect::ServerMessage::Notification(
						tg::process::connect::ServerNotification::Write(notification),
					)))
					.await
					.map_err(|_| tg::error!("the process connection closed"))?;
			}
			Ok(())
		});

		Ok(())
	}

	async fn send_connect_ack(sender: &Sender, id: u64) -> tg::Result<()> {
		let message = tg::process::connect::ServerMessage::Ack(tg::process::connect::Ack { id });
		sender
			.send(Ok(message))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		Ok(())
	}

	async fn send_connect_response(
		sender: &Sender,
		id: u64,
		result: tg::Result<tg::process::connect::ServerResponseOutput>,
	) -> tg::Result<()> {
		let (error, output) = match result {
			Err(error) => (Some(Self::connect_process_error(&error)), None),
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

	async fn finish_connect_response(input: &mut Input, id: u64) -> tg::Result<()> {
		// Keep the request body open until the final response is received or the peer closes it.
		while let Some(message) = input.try_next().await? {
			if matches!(message, tg::process::connect::ClientMessage::Ack(ack) if ack.id == id) {
				break;
			}
		}
		Ok(())
	}

	async fn try_connect_process_regions(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
		regions: &[String],
	) -> tg::Result<Option<Output>> {
		// A connection owns its lease, so only one successful attempt may remain open.
		let mut result = Ok(None);
		for region in regions {
			match self
				.try_connect_process_region(arg.clone(), id, input, region)
				.await
			{
				Err(error) => result = Err(error),
				Ok(None) => (),
				Ok(Some(output)) => return Ok(Some(output)),
			}
		}
		let output = result?;
		Ok(output)
	}

	async fn try_connect_process_region(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
		region: &str,
	) -> tg::Result<Option<Output>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let mut location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let (stream, sender) =
			Self::connect_process_input(arg, id, location.clone(), location.clone().into());
		let Some(output) = client.try_connect_process(stream).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to connect to the process"),
		)?
		else {
			return Ok(None);
		};
		if let Err(stream) = sender.send(input.take().unwrap()) {
			*input = Some(stream);
			return Err(tg::error!("the process connection closed"));
		}
		let trusted = client.trusted();
		let mut process = None;
		let session = self.clone();
		let output = output
			.map(move |message| {
				let mut message = message?;
				session.update_connect_process_message_referents_for_location(
					&mut message,
					&mut location,
					trusted,
				)?;
				if let tg::process::connect::ServerMessage::Response(response) = &message
					&& let Some(tg::process::connect::ServerResponseOutput::Connect(output)) =
						&response.output
				{
					process = output.process.as_ref().right().cloned();
				}
				if matches!(
					&message,
					tg::process::connect::ServerMessage::Notification(
						tg::process::connect::ServerNotification::Wait(_)
					)
				) && let Some(process) = &process
				{
					session.remove_finished_process_child_lease(process);
				}
				Ok(message)
			})
			.with_stopper(self.context.stopper.clone())
			.boxed();

		Ok(Some(output))
	}

	async fn try_connect_process_remotes(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<Output>> {
		let mut result = Ok(None);
		for remote in remotes {
			match self
				.try_connect_process_remote(arg.clone(), id, input, remote)
				.await
			{
				Err(error) => result = Err(error),
				Ok(None) => (),
				Ok(Some(output)) => return Ok(Some(output)),
			}
		}
		let output = result?;
		Ok(output)
	}

	async fn try_connect_process_remote(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<Output>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let mut location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg_location = tg::location::Arg(vec![tg::location::arg::Component::Local(
			tg::location::arg::LocalComponent {
				regions: remote.regions.clone(),
			},
		)]);
		let (stream, sender) = Self::connect_process_input(arg, id, location.clone(), arg_location);
		let Some(output) = client.try_connect_process(stream).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to connect to the process"),
		)?
		else {
			return Ok(None);
		};
		if let Err(stream) = sender.send(input.take().unwrap()) {
			*input = Some(stream);
			return Err(tg::error!("the process connection closed"));
		}
		let trusted = client.trusted();
		let mut process = None;
		let session = self.clone();
		let output = output
			.map(move |message| {
				let mut message = message?;
				session.update_connect_process_message_referents_for_location(
					&mut message,
					&mut location,
					trusted,
				)?;
				if let tg::process::connect::ServerMessage::Response(response) = &message
					&& let Some(tg::process::connect::ServerResponseOutput::Connect(output)) =
						&response.output
				{
					process = output.process.as_ref().right().cloned();
				}
				if matches!(
					&message,
					tg::process::connect::ServerMessage::Notification(
						tg::process::connect::ServerNotification::Wait(_)
					)
				) && let Some(process) = &process
				{
					session.remove_finished_process_child_lease(process);
				}
				Ok(message)
			})
			.with_stopper(self.context.stopper.clone())
			.boxed();

		Ok(Some(output))
	}

	fn connect_process_input(
		arg: tg::process::connect::Arg,
		id: u64,
		destination: tg::Location,
		location: tg::location::Arg,
	) -> (Input, oneshot::Sender<Input>) {
		// Keep the operation stream until the endpoint confirms that it found the process.
		let (sender, receiver) = oneshot::channel::<Input>();
		let input = futures::stream::once(async move {
			receiver
				.await
				.map_err(|_| tg::error!("the process connection was not selected"))
		})
		.try_flatten();
		let request = tg::process::connect::ClientRequest {
			arg: tg::process::connect::ClientRequestArg::Connect(arg),
			id,
		};
		let input = futures::stream::once(futures::future::ok(
			tg::process::connect::ClientMessage::Request(request),
		))
		.chain(input)
		.map_ok(move |mut message| {
			Self::update_connect_process_request_for_location(
				&mut message,
				&destination,
				&location,
			);
			message
		})
		.boxed();
		(input, sender)
	}

	fn update_connect_process_message_referents_for_location(
		&self,
		message: &mut tg::process::connect::ServerMessage,
		location: &mut tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match message {
			tg::process::connect::ServerMessage::Ack(_)
			| tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Progress(
					tg::progress::Event::Indicators(_)
					| tg::progress::Event::Log(_)
					| tg::progress::Event::Output(()),
				)
				| tg::process::connect::ServerNotification::Read(_)
				| tg::process::connect::ServerNotification::Write(_),
			) => (),
			tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Error(notification),
			) => {
				self.update_error_data_referents_for_location(
					&mut notification.error,
					location,
					trusted,
				)?;
			},
			tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Progress(
					tg::progress::Event::Diagnostic(diagnostic),
				),
			) => {
				if let Some(data) = &mut diagnostic.location {
					self.update_tokens_and_location(
						&mut data.module.referent.options.tokens,
						Some(&mut data.module.referent.options.location),
						location,
						trusted,
					)?;
				}
			},
			tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Wait(output),
			) => {
				self.update_wait_output_referents_for_location(output, location, trusted)?;
			},
			tg::process::connect::ServerMessage::Response(response) => {
				if let Some(error) = &mut response.error {
					self.update_error_data_referents_for_location(error, location, trusted)?;
				}
				if let Some(tg::process::connect::ServerResponseOutput::Connect(output)) =
					&mut response.output
				{
					if let tg::Location::Remote(remote) = location {
						remote.region = output
							.location
							.as_ref()
							.and_then(|location| match location {
								tg::Location::Local(local) => local.region.clone(),
								tg::Location::Remote(remote) => remote.region.clone(),
							})
							.or_else(|| remote.region.clone());
					}
					self.update_spawn_process_output_referents_for_location(
						output, location, trusted,
					)?;
				}
			},
		}
		Ok(())
	}

	fn update_connect_process_request_for_location(
		message: &mut tg::process::connect::ClientMessage,
		destination: &tg::Location,
		location: &tg::location::Arg,
	) {
		let tg::process::connect::ClientMessage::Request(request) = message else {
			return;
		};
		let location = Some(location.clone());
		match &mut request.arg {
			tg::process::connect::ClientRequestArg::Cancel(arg) => arg.location = location,
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Detach => (),
			tg::process::connect::ClientRequestArg::Connect(arg) => {
				for read in arg.reads.values_mut() {
					read.location = location.clone();
					read.tokens = read.tokens.for_location(destination);
				}
				match &mut arg.target {
					tg::process::connect::Target::Existing { options, .. } => {
						options.location = location;
						options.tokens = options.tokens.for_location(destination);
					},
					tg::process::connect::Target::Spawn { arg, .. } => {
						arg.location = location;
						arg.command.options.tokens =
							arg.command.options.tokens.for_location(destination);
					},
				}
			},
			tg::process::connect::ClientRequestArg::Read(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
			tg::process::connect::ClientRequestArg::Signal(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
			tg::process::connect::ClientRequestArg::Tty(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
			tg::process::connect::ClientRequestArg::Write(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
		}
	}

	pub(crate) async fn try_connect_process_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the headers.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the content type header"))?;
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let input_encoding = super::stdio::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!("missing the content type"))?,
			tg::process::connect::TANGRAM_CONTENT_TYPE,
		)?;
		let output_encoding = super::stdio::Encoding::from_accept(
			accept.as_ref(),
			tg::process::connect::TANGRAM_CONTENT_TYPE,
		)?;

		// Connect the process.
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::stdio::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_connect_process(input).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		let body = super::stdio::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				output_encoding
					.content_type(tg::process::connect::TANGRAM_CONTENT_TYPE)
					.to_string(),
			)
			.body(body)
			.unwrap();

		Ok(response)
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
