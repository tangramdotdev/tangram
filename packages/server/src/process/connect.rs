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
	tangram_client::{
		prelude::*,
		process::stdio::{Stream, flow, write},
	},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tokio::sync::{mpsc, oneshot},
	tokio_stream::wrappers::ReceiverStream,
};

mod sync;

const MAX_OPERATIONS: usize = 64;

type Input = BoxStream<'static, tg::Result<tg::process::connect::ClientMessage>>;
type Operation = BoxFuture<'static, (u64, tg::Result<()>)>;
type Output = BoxStream<'static, tg::Result<tg::process::connect::ServerMessage>>;
type Sender = mpsc::Sender<tg::Result<tg::process::connect::ServerMessage>>;
type Wait = BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>;

struct Options {
	arg: tg::process::connect::Arg,
	id: u64,
	prepare_output: Option<spawn::PrepareOutput>,
	wait: Option<(Wait, tg::Location)>,
}

struct State<'a> {
	cancel: Arc<AtomicBool>,
	high: &'a Sender,
	id: tg::process::Id,
	location: Option<tg::location::Arg>,
	low: &'a Sender,
	operations: FuturesUnordered<Operation>,
	requests: BTreeSet<u64>,
	responses: BTreeSet<u64>,
	streams: Streams,
	tokens: tg::Tokens,
	writer: Option<Writer>,
	writes: BTreeSet<u64>,
}

struct Streams {
	aborts: BTreeMap<u64, AbortHandle>,
	reads: BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>>,
	tasks: FuturesUnordered<Operation>,
}

struct Writer {
	input: mpsc::Sender<tg::Result<tg::process::stdio::write::ClientMessage>>,
	output: BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>,
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
			if arg.mode == tg::process::connect::Mode::Spawn && !arg.reads.is_empty() {
				return Err(tg::error!("spawn mode does not support initial reads"));
			}

			// Resolve the destination.
			let mut input = Some(input);
			if arg.process.is_right() {
				return self
					.try_connect_process_inner(arg, request_id, &mut input)
					.await;
			}

			// A spawn selects one destination, using the same preparation and routing as spawn.
			let tg::Either::Left(spawn) = &mut arg.process else {
				unreachable!()
			};
			spawn.location = arg.location.take().or_else(|| spawn.location.take());
			let prepare_output = self.spawn_process_prepare(spawn).await?;
			arg.location = spawn.location.clone();
			let location = self.server.location(arg.location.as_ref())?;
			if matches!(
				location,
				tg::Location::Local(tg::location::Local { region: None })
			) || self.spawn_process_runner_matches_location(&location)
			{
				return self
					.try_connect_process_local(arg, request_id, &mut input, Some(prepare_output))
					.await;
			}

			let input = input.take().unwrap();
			let (sender, receiver) = mpsc::channel(64);
			let session = self.clone();
			let task = Task::spawn(move |_| async move {
				if let Err(error) = session
					.connect_process_spawn_task(arg, request_id, input, prepare_output, &sender)
					.boxed()
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
		prepare_output: spawn::PrepareOutput,
		sender: &Sender,
	) -> tg::Result<()> {
		let tg::Either::Left(spawn_arg) = &mut arg.process else {
			unreachable!()
		};
		let spawn::PrepareOutput {
			command,
			parent_sandbox,
		} = prepare_output;
		let location = self.server.location(arg.location.as_ref())?;
		let mut notify = self
			.try_prepare_spawn_process_for_location(spawn_arg, &location, parent_sandbox.as_ref())
			.await;
		let spawn_arg = spawn_arg.clone();

		// Start a command sync when this is the first routing hop.
		let mut input = Some(input);
		let mut sync_sender = None;
		let start_command_sync = !arg.command_sync;
		if start_command_sync {
			crate::checkpoint!(
				self.server,
				"process.connect.command.push.started",
				command = %command.node,
			)
			.await;
			let source = self
				.connect_process_command_sync_source(&spawn_arg.command, input.take().unwrap())
				.await?;
			arg.command_sync = true;
			input = Some(source.input);
			sync_sender = Some(source.sender);
		}

		// Connect to the destination.
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
		let mut pending_wait = None;
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
			if let tg::process::connect::ServerMessage::Sync(message) = &message
				&& sync_sender.is_some()
			{
				let sync_message = Self::connect_process_decode_sync_message(message)?;
				if matches!(sync_message, tg::sync::Message::End) {
					sync_sender = None;
					crate::checkpoint!(
						self.server,
						"process.connect.command.push.finished",
						command = %command.node,
					)
					.await;
					if let Some(message) = pending_wait.take() {
						sender
							.send(Ok(message))
							.await
							.map_err(|_| tg::error!("the process connection closed"))?;
					}
				} else {
					sync_sender
						.as_ref()
						.unwrap()
						.send(Ok(sync_message))
						.await
						.map_err(|_| tg::error!("the command sync closed"))?;
				}
				continue;
			}
			if start_command_sync
				&& sync_sender.is_some()
				&& matches!(
					message,
					tg::process::connect::ServerMessage::Notification(
						tg::process::connect::ServerNotification::Wait(_)
					)
				) {
				pending_wait = Some(message);
				continue;
			}
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
		if sync_sender.is_some() {
			return Err(tg::error!("the command sync ended unexpectedly"));
		}
		if pending_wait.is_some() {
			return Err(tg::error!("the process wait was not forwarded"));
		}

		Ok(())
	}

	async fn try_connect_process_inner(
		&self,
		arg: tg::process::connect::Arg,
		id: u64,
		input: &mut Option<Input>,
	) -> tg::Result<Option<Output>> {
		if let tg::Either::Right(process) = &arg.process {
			let wait_arg = tg::process::wait::Arg {
				lease: None,
				location: arg.location.clone(),
				tokens: arg.tokens.clone(),
			};
			if let Some(wait) = self.try_wait_process_runner(process, &wait_arg).await? {
				let options = Options {
					arg,
					id,
					prepare_output: None,
					wait: Some(wait),
				};
				let output = self.connect_process_local(options, input.take().unwrap());
				return Ok(Some(output));
			}
		}
		let locations = self
			.locations(arg.location.as_ref())
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
		prepare_output: Option<spawn::PrepareOutput>,
	) -> tg::Result<Option<Output>> {
		let wait = if let tg::Either::Right(id) = &arg.process {
			let Some(wait) = self
				.try_wait_process_local(id, arg.tokens.local_authorization().to_vec())
				.await?
			else {
				return Ok(None);
			};
			let location = tg::Location::Local(tg::location::Local {
				region: self.server.config.region.clone(),
			});
			Some((wait, location))
		} else {
			None
		};
		let options = Options {
			arg,
			id,
			prepare_output,
			wait,
		};
		let output = self.connect_process_local(options, input.take().unwrap());
		Ok(Some(output))
	}

	fn connect_process_local(&self, options: Options, input: Input) -> Output {
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
		crate::control::priority_stream(receiver_high, receiver_low)
			.attach(task)
			.boxed()
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
			mut arg,
			id: request_id,
			mut prepare_output,
			wait,
		} = options;
		let mut sync_task = None;
		if arg.command_sync {
			let prepare_output = prepare_output
				.as_mut()
				.ok_or_else(|| tg::error!("command sync requires a spawn"))?;
			let tg::Either::Left(spawn) = &arg.process else {
				return Err(tg::error!("command sync requires a spawn"));
			};
			let destination = self
				.connect_process_command_sync_destination(&spawn.command, input, high)
				.await?;
			input = destination.input;
			// Attach the destination-minted token to the ephemeral command; process storage strips it.
			let location = tg::Location::Local(tg::location::Local::default());
			prepare_output
				.command
				.options
				.tokens
				.insert_authorization(location.clone(), destination.token.clone());
			let tg::Either::Left(spawn) = &mut arg.process else {
				return Err(tg::error!("command sync requires a spawn"));
			};
			spawn
				.command
				.options
				.tokens
				.insert_authorization(location, destination.token);
			sync_task = Some(destination.task);
		}
		Self::send_connect_ack(high, request_id).await?;
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
				let location = wait.as_ref().unwrap().1.clone();
				let output = tg::process::spawn::Output {
					cached: false,
					lease: arg.lease,
					location: Some(location.clone()),
					process: tg::Either::Right(id),
					tokens: arg.tokens,
					wait: None,
				};
				(output, Some(location.into()))
			},
		};

		// Complete a spawn-only connection.
		if mode == tg::process::connect::Mode::Spawn {
			Self::connect_process_finish_command_sync(&mut sync_task).await?;
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
		if wait.is_none()
			&& !matches!(
				self.server.location(location.as_ref())?,
				tg::Location::Local(tg::location::Local { region: None })
			) {
			let arg = tg::process::connect::Arg {
				command_sync: false,
				lease: output.lease.clone(),
				location,
				mode,
				process: tg::Either::Right(output.process.as_ref().unwrap_right().clone()),
				reads: arg.reads,
				tokens: output.tokens.clone(),
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
			self.connect_process_cached_task(output, input, low).await?;
			Self::connect_process_finish_command_sync(&mut sync_task).await?;
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
				Some((wait, _)) => wait,
				None => self
					.try_wait_process_local(&id, wait_arg.tokens.local_authorization().to_vec())
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
		};
		let mut state = State {
			cancel,
			high,
			id,
			location,
			low,
			operations: FuturesUnordered::new(),
			requests: BTreeSet::from([request_id]),
			responses: BTreeSet::from([request_id]),
			streams,
			tokens,
			writer: None,
			writes: BTreeSet::new(),
		};
		for (request_id, arg) in arg.reads {
			state.requests.insert(request_id);
			state.responses.insert(request_id);
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
			.boxed()
			.await?;
		Self::connect_process_finish_command_sync(&mut sync_task).await?;

		Ok(())
	}

	async fn connect_process_spawn_local(
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
		sender: &Sender,
	) -> tg::Result<()> {
		// Transfer the lease to a connection at the selected cache location.
		let mut guard = spawn::lease::LeaseGuard::new(self, &output);
		let mut stream = self.connect_process(input).boxed().await?;
		if let Some(guard) = &mut guard {
			guard.disarm();
		}

		// Preserve the upstream ordering so terminal responses cannot overtake their read chunks.
		while let Some(mut message) = stream.try_next().await? {
			if let tg::process::connect::ServerMessage::Response(response) = &mut message
				&& let Some(tg::process::connect::ServerResponseOutput::Connect(selected)) =
					&mut response.output
			{
				selected.cached = output.cached;
				selected.wait = output.wait.clone();
			}
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
				&& state.writes.is_empty()
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
					message = async { state.writer.as_mut().unwrap().output.next().await }, if state.writer.is_some() => {
						Self::connect_process_handle_write(&mut state, message).await?;
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
		// Release the completed read.
		state.streams.aborts.remove(&id);
		let active = state.streams.reads.remove(&id).is_some();
		state.requests.remove(&id);
		if active && let Err(error) = result {
			Self::send_connect_response(state.low, id, Err(error)).await?;
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
				if let Some(sender) = state.streams.reads.get(&ack.id) {
					sender
						.try_send(Ok(tg::process::stdio::read::ClientMessage::Ack))
						.map_err(|error| {
							tg::error!(!error, "failed to acknowledge the read response")
						})?;
				}
			},
			tg::process::connect::ClientMessage::Notification(
				tg::process::connect::ClientNotification::Read(notification),
			) => {
				Self::connect_process_handle_read(state, &notification)?;
			},

			tg::process::connect::ClientMessage::Request(request) => {
				return self.connect_process_handle_request(state, request).await;
			},
			tg::process::connect::ClientMessage::Sync(_) => {
				return Err(tg::error!("unexpected process sync message"));
			},
		}
		Ok(ControlFlow::Continue(()))
	}

	fn connect_process_handle_read(
		state: &State<'_>,
		notification: &tg::process::connect::ReadClientNotification,
	) -> tg::Result<()> {
		// Progress already in transit may arrive after an error or cancellation ends the read.
		let Some(sender) = state.streams.reads.get(&notification.id) else {
			return Ok(());
		};
		sender
			.try_send(Ok(tg::process::stdio::read::ClientMessage::Notification(
				notification.progress,
			)))
			.map_err(|error| tg::error!(!error, "failed to deliver the read message"))?;
		Ok(())
	}

	async fn connect_process_handle_write(
		state: &mut State<'_>,
		message: Option<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		let error = match message {
			Some(Ok(write::ServerMessage::Ack(_))) => return Ok(()),
			Some(Ok(write::ServerMessage::Response(response))) => {
				let id = response.id;
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
				state.writes.remove(&id);
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
		for id in std::mem::take(&mut state.writes) {
			state.requests.remove(&id);
			Self::send_connect_response(state.high, id, Err(error.clone())).await?;
		}
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
		if state.responses.len() >= MAX_OPERATIONS * 3 + 4 {
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
			tg::process::connect::ClientRequestArg::Read(_) => {
				state.streams.tasks.len() >= MAX_OPERATIONS
					|| state.operations.len() >= MAX_OPERATIONS
			},
			tg::process::connect::ClientRequestArg::Write(_) => {
				state.writes.len() >= MAX_OPERATIONS
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
			tg::process::connect::ClientRequestArg::Read(arg) => {
				match self.connect_process_read(state, request.id, arg).await {
					Ok(()) => return Ok(ControlFlow::Continue(())),
					Err(error) => Err(error),
				}
			},
			tg::process::connect::ClientRequestArg::Write(arg) => {
				match self.connect_process_write(state, request.id, arg).await {
					Ok(()) => return Ok(ControlFlow::Continue(())),
					Err(error) => Err(error),
				}
			},
		};

		Self::send_connect_response(state.high, request.id, result).await?;
		if !state.streams.reads.contains_key(&request.id) && !state.writes.contains(&request.id) {
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
		let location = state.location.clone();
		let sender = state.high.clone();
		let tokens = state.tokens.clone();
		let future = async move {
			let result = session
				.connect_process_operation(&id, arg, location, tokens)
				.boxed()
				.await;
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
		location: Option<tg::location::Arg>,
		tokens: tg::Tokens,
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
			| tg::process::connect::ClientRequestArg::Connect(_)
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

	fn connect_process_close(state: &mut State<'_>, id: u64) {
		state.streams.reads.remove(&id);
		state.responses.remove(&id);
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
		arg.location = state.location.clone();
		let (input, receiver) = mpsc::channel(4);
		let output = self
			.try_read_process_stdio_source(&state.id, arg.clone())
			.await?
			.ok_or_else(|| tg::error!("failed to find process stdio"))?;
		let mut output =
			self.read_process_stdio_protocol(arg, ReceiverStream::new(receiver).boxed(), output);

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
						Self::send_connect_response(
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

	async fn connect_process_write(
		&self,
		state: &mut State<'_>,
		request_id: u64,
		mut arg: tg::process::stdio::write::Arg,
	) -> tg::Result<()> {
		// Prepare stdin once for the connection, then reuse the standalone write implementation.
		if state.writer.is_none() {
			arg.tokens.inherit(&state.tokens);
			let (input, receiver) = mpsc::channel(flow::CHANNEL_CAPACITY);
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
			state.writer = Some(Writer { input, output });
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
		state.writes.insert(request_id);
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
		.and_then(move |mut message| {
			let result = Self::update_connect_process_request_for_location(
				&mut message,
				&destination,
				&location,
			)
			.map(|()| message);
			futures::future::ready(result)
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
			| tg::process::connect::ServerMessage::Sync(_)
			| tg::process::connect::ServerMessage::Notification(
				tg::process::connect::ServerNotification::Progress(
					tg::progress::Event::Indicators(_)
					| tg::progress::Event::Log(_)
					| tg::progress::Event::Output(()),
				)
				| tg::process::connect::ServerNotification::Read(_),
			) => (),

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
	) -> tg::Result<()> {
		let tg::process::connect::ClientMessage::Request(request) = message else {
			return Ok(());
		};
		let location = Some(location.clone());
		match &mut request.arg {
			tg::process::connect::ClientRequestArg::Cancel(arg) => arg.location = location,
			tg::process::connect::ClientRequestArg::Close(_)
			| tg::process::connect::ClientRequestArg::Detach => (),
			tg::process::connect::ClientRequestArg::Connect(arg) => {
				arg.location = location.clone();
				arg.tokens = arg.tokens.for_location(destination);
				for read in arg.reads.values_mut() {
					read.location = location.clone();
					read.tokens = read.tokens.for_location(destination);
				}
				if let tg::Either::Left(spawn) = &mut arg.process {
					spawn.location = location;
					Self::update_spawn_process_command_for_location(
						&mut spawn.command,
						destination,
					)?;
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

		Ok(())
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
