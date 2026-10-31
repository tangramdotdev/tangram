use {
	crate::{Component, spawn},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::BoxFuture,
		stream::{BoxStream, FuturesUnordered},
	},
	std::{
		collections::{BTreeSet, VecDeque},
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
	tangram_server_core::Session,
	tokio::sync::{mpsc, oneshot},
	tokio_stream::wrappers::ReceiverStream,
};

mod connection;

pub mod service;

type Input = BoxStream<'static, tg::Result<tg::process::attach::ClientMessage>>;
type Operation = BoxFuture<'static, (u64, tg::Result<()>)>;
type Output = BoxStream<'static, tg::Result<tg::process::attach::ServerMessage>>;
type Sender = mpsc::Sender<tg::Result<tg::process::attach::ServerMessage>>;
type WaitFuture = BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>;

struct Options {
	arg: tg::process::attach::Arg,
	prepare_output: Option<spawn::PrepareOutput>,
	wait_future: Option<(WaitFuture, tg::Location)>,
}

struct AttachProcessSpawnTaskArg<'a> {
	notify: Option<BoxFuture<'static, ()>>,
	output: Output,
	push: Option<oneshot::Receiver<tg::Result<()>>>,
	spawn_arg: &'a tg::process::spawn::Arg,
	spawned: &'a AtomicBool,
}

impl Component {
	pub fn try_get_process_attach_stream<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::process::attach::Arg,
		input: Input,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>>,
	> {
		async move {
			// Start answering the requests that locate the process by the attach's nonce.
			let sender = match &arg.nonce {
				Some(nonce) => Some(
					self.spawn_process_attach_service_task(session, nonce)
						.await?,
				),
				None => None,
			};
			if let (Some(sender), tg::Either::Right(id)) = (&sender, &arg.process) {
				sender.set_process(tg::Referent::with_node(id.clone()));
			}

			// Attach.
			tangram_server_core::checkpoint!(session.server, "process.attach.start").await;
			let (header, output) = match self
				.try_get_process_attach_stream_destination(session, arg, input, sender.as_ref())
				.await?
			{
				ControlFlow::Break(Some(output)) => output,
				ControlFlow::Break(None) => return Ok(ControlFlow::Break(None)),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

			// Tell the attach service the attach's command sync and the process it selects, and finish it when the connection ends.
			if let (Some(sender), Some(sync)) = (&sender, &header.sync) {
				sender.set_sync(sync.clone());
			}
			let output = match sender {
				Some(sender) => output
					.inspect_ok(move |message| {
						if let tg::process::attach::ServerMessage::Notification(
							tg::process::attach::ServerNotification::Progress(
								tg::progress::Event::Output(output),
							),
						) = message && let tg::Either::Right(id) = &output.process
						{
							let options = tg::referent::Options {
								location: output.location.clone(),
								tokens: output.tokens.clone(),
								..Default::default()
							};
							sender.set_process(tg::Referent::new(id.clone(), options));
						}
					})
					.boxed(),
				None => output,
			};

			Ok(ControlFlow::Break(Some((header, output))))
		}
		.boxed()
	}

	async fn try_get_process_attach_stream_destination(
		&self,
		session: &Session,
		mut arg: tg::process::attach::Arg,
		input: Input,
		sender: Option<&crate::attach::service::Sender>,
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		// Resolve the destination.
		let mut input = Some(input);
		if arg.process.is_right() {
			return self
				.try_get_process_attach_stream_inner(session, arg, &mut input)
				.await;
		}

		// A spawn selects one destination, using the same preparation and routing as spawn.
		let tg::Either::Left(spawn) = &mut arg.process else {
			unreachable!()
		};
		spawn.location = arg.location.take().or_else(|| spawn.location.take());
		let mut prepare_output = self.spawn_process_prepare(session, spawn).await?;
		arg.location = spawn.location.clone();
		let location = session.server.location(arg.location.as_ref())?;
		if matches!(
			&location,
			tg::Location::Local(tg::location::Local { region })
			if region.as_deref().is_none_or(|region| Some(region) == session.server.config.region.as_deref())
		) || Self::spawn_process_runner_matches_location(session, &location)
		{
			// Give the spawn the attach's open, so that a scheduled process's runner opens the declared streams when the process starts.
			if let (Some(sender), Some(nonce)) = (sender, arg.nonce.as_ref())
				&& !arg.open.is_empty()
			{
				prepare_output.open = Some(sender.open(nonce, &arg.open));
			}
			let output = self
				.try_get_process_attach_stream_local(session, arg, &mut input, Some(prepare_output))
				.await?;
			return Ok(ControlFlow::Break(output));
		}
		// Tell the attach service the location of the server that the attach goes to, whose attach service answers for the nonce in its place.
		if let Some(sender) = sender {
			sender.set_location(location.clone());
		}
		let output = self
			.attach_process_spawn(session, arg, input.take().unwrap(), prepare_output)
			.boxed()
			.await?;

		Ok(ControlFlow::Break(Some(output)))
	}

	async fn attach_process_spawn(
		&self,
		session: &Session,
		mut arg: tg::process::attach::Arg,
		input: Input,
		prepare_output: spawn::PrepareOutput,
	) -> tg::Result<(tg::process::attach::Header, Output)> {
		let tg::Either::Left(spawn_arg) = &mut arg.process else {
			unreachable!()
		};
		let spawn::PrepareOutput { parent_sandbox, .. } = prepare_output;
		let location = session.server.location(arg.location.as_ref())?;
		let notify = self
			.try_prepare_spawn_process_for_location(
				session,
				spawn_arg,
				&location,
				parent_sandbox.as_ref(),
			)
			.await;
		let spawn_arg = spawn_arg.clone();

		let spawned = Arc::new(AtomicBool::new(false));

		// A spawn-only client can disconnect after receiving the selected process.
		let input = if arg.mode == tg::process::attach::Mode::Spawn {
			let spawned = spawned.clone();
			futures::stream::unfold(
				(input, false, spawned),
				move |(mut input, finished, spawned)| async move {
					if finished {
						return None;
					}
					let message = input.next().await?;
					if message.is_err() && spawned.load(Ordering::SeqCst) {
						return None;
					}
					let finished = matches!(
						&message,
						Ok(tg::process::attach::ClientMessage::Notification(
							tg::process::attach::ClientNotification::Ready
						))
					);
					Some((message, (input, finished, spawned)))
				},
			)
			.boxed()
		} else {
			input
		};

		// The first routing hop pushes the command objects, so later hops and the destination do not.
		let push = !arg.push;
		arg.push = true;

		// Push the command objects concurrently with the attach, joining the destination's command sync by the attach's nonce.
		let push = push.then(|| {
			let nonce = *arg.nonce.get_or_insert_with(tg::process::Nonce::new);
			self.push_process_attach_command(session, &spawn_arg.command, &location, nonce)
		});

		// Attach to the destination.
		let mut input = Some(input);
		let (header, output) = match location.clone() {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				let result = self
					.try_get_process_attach_stream_region(session, arg, &mut input, &region)
					.await;
				tangram_futures::retry::once(result)?
			},
			tg::Location::Local(tg::location::Local { region: None }) => unreachable!(),
			tg::Location::Remote(remote) => {
				let remote = tangram_server_core::location::Remote {
					name: remote.name,
					regions: remote.region.map(|region| vec![region]),
				};
				let result = self
					.try_get_process_attach_stream_remote(session, arg, &mut input, &remote)
					.await;
				tangram_futures::retry::once(result)?
			},
		}
		.ok_or_else(|| tg::error!("failed to find the process"))?;

		// Register the child and return the connection messages, failing the connection if the push fails.
		let capacity = session.server.config.process.attach.spawn_channel_capacity;
		let (sender, receiver) = mpsc::channel(capacity);
		let component = self.clone();
		let session = session.clone();
		let task = Task::spawn(move |_| async move {
			let arg = AttachProcessSpawnTaskArg {
				notify,
				output,
				push,
				spawn_arg: &spawn_arg,
				spawned: &spawned,
			};
			let result = component
				.attach_process_spawn_task(&session, arg, &sender)
				.boxed()
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		let output = ReceiverStream::new(receiver).attach(task).boxed();

		Ok((header, output))
	}

	fn push_process_attach_command(
		&self,
		session: &Session,
		command: &tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		location: &tg::Location,
		nonce: tg::process::Nonce,
	) -> oneshot::Receiver<tg::Result<()>> {
		// Keep transferring after the connection has been dropped.
		let (sender, receiver) = oneshot::channel();
		let component = self.clone();
		let session = session.clone();
		let command = command.clone();
		let location = location.clone();
		let mut task = Task::spawn(move |_| async move {
			let result = component
				.push_process_attach_command_task(&session, &command, &location, nonce)
				.await;
			if let Err(error) = &result {
				tracing::error!(error = %error.trace(), "failed to push the process command");
			}
			sender.send(result).ok();
		});
		task.detach();
		receiver
	}

	async fn push_process_attach_command_task(
		&self,
		session: &Session,
		command: &tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		location: &tg::Location,
		nonce: tg::process::Nonce,
	) -> tg::Result<()> {
		let nodes = Self::spawn_process_command_nodes(command)?;
		if nodes.is_empty() {
			return Ok(());
		}
		tangram_server_core::checkpoint!(session.server, "process.attach.command.push.started")
			.await;
		let arg = crate::PushProcessObjectsArg {
			join: Some(tg::sync::Join::Attach(nonce)),
			location: location.clone(),
			nodes,
			process_command_objects: true,
			sync: None,
		};
		let output = self.push_process_objects(session, arg).await?;
		let result = output
			.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the command push task panicked"))
			.and_then(|result| result);
		if result.is_ok() {
			tangram_server_core::checkpoint!(
				session.server,
				"process.attach.command.push.finished"
			)
			.await;
		}
		if let tg::Location::Remote(remote) = location {
			session
				.server
				.components
				.remote
				.invalidate_remote_cache(session, &remote.name)
				.await;
		}
		result
	}

	async fn attach_process_spawn_task(
		&self,
		session: &Session,
		arg: AttachProcessSpawnTaskArg<'_>,
		sender: &Sender,
	) -> tg::Result<()> {
		let AttachProcessSpawnTaskArg {
			mut notify,
			mut output,
			mut push,
			spawn_arg,
			spawned,
		} = arg;
		loop {
			let message = tokio::select! {
				message = output.try_next() => message?,
				() = async { notify.as_mut().unwrap().await }, if notify.is_some() => {
					notify = None;
					continue;
				},
				result = async { push.as_mut().unwrap().await }, if push.is_some() => {
					push = None;
					result.map_err(|_| tg::error!("the command push task panicked"))??;
					continue;
				},
			};
			let Some(message) = message else {
				break;
			};
			if let tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
					output,
				)),
			) = &message
			{
				notify = None;
				self.spawn_process_add_child(session, spawn_arg, output)
					.await?;
				spawned.store(true, Ordering::SeqCst);
			}
			sender
				.send(Ok(message))
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		// Wait for the command push after the output ends, so that its failure fails the stream.
		if let Some(push) = push {
			push.await
				.map_err(|_| tg::error!("the command push task panicked"))??;
		}

		Ok(())
	}

	async fn try_get_process_attach_stream_inner(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		if let tg::Either::Right(process) = &arg.process {
			let wait_arg = tg::process::wait::Arg {
				lease: None,
				location: arg.location.clone(),
				source: tg::process::Source::Auto,
				tokens: arg.tokens.clone(),
			};
			if let Some(wait_future) = self
				.try_wait_process_runner(session, process, &wait_arg)
				.await?
			{
				let options = Options {
					arg,
					prepare_output: None,
					wait_future: Some(wait_future),
				};
				let output =
					self.get_process_attach_stream_local(session, options, input.take().unwrap())?;
				return Ok(ControlFlow::Break(Some(output)));
			}
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		let mut unavailable = None;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_get_process_attach_stream_local(session, arg.clone(), input, None)
					.await
					.map_err(|error| tg::error!(!error, "failed to attach to the local process"))?
			{
				return Ok(ControlFlow::Break(Some(output)));
			}
			match self
				.try_get_process_attach_stream_regions(session, arg.clone(), input, &local.regions)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to attach to the process in another region")
				})? {
				ControlFlow::Break(Some(output)) => return Ok(ControlFlow::Break(Some(output))),
				ControlFlow::Break(None) => (),
				ControlFlow::Continue(error) => unavailable = Some(error),
			}
		}
		match self
			.try_get_process_attach_stream_remotes(session, arg, input, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, "failed to attach to the process on a remote"))?
		{
			ControlFlow::Break(Some(output)) => return Ok(ControlFlow::Break(Some(output))),
			ControlFlow::Break(None) => (),
			ControlFlow::Continue(error) => unavailable = Some(error),
		}

		// Continue if a location that could have the process could not be reached.
		let output = match unavailable {
			Some(error) => ControlFlow::Continue(error),
			None => ControlFlow::Break(None),
		};

		Ok(output)
	}

	async fn try_get_process_attach_stream_local(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
		prepare_output: Option<spawn::PrepareOutput>,
	) -> tg::Result<Option<(tg::process::attach::Header, Output)>> {
		let mut wait_session = session.clone();
		wait_session.context.stopper = None;
		let wait_future = if let tg::Either::Right(id) = &arg.process {
			let Some(wait_future) = self
				.try_wait_process_local(
					&wait_session,
					id,
					arg.tokens.local_authorization().to_vec(),
					tg::process::Source::Auto,
				)
				.await?
			else {
				return Ok(None);
			};
			let location = tg::Location::Local(tg::location::Local {
				region: wait_session.server.config.region.clone(),
			});
			Some((wait_future, location))
		} else {
			None
		};
		let options = Options {
			arg,
			prepare_output,
			wait_future,
		};
		let output =
			self.get_process_attach_stream_local(session, options, input.take().unwrap())?;
		Ok(Some(output))
	}

	async fn try_get_process_attach_stream_regions(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
		regions: &[String],
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		// A connection owns its lease, so only one successful attempt may remain open.
		let mut result = Ok(ControlFlow::Break(None));
		for region in regions {
			match self
				.try_get_process_attach_stream_region(session, arg.clone(), input, region)
				.await
			{
				Err(error) => result = Err(error),
				Ok(ControlFlow::Break(None)) => (),
				Ok(ControlFlow::Break(Some(output))) => {
					return Ok(ControlFlow::Break(Some(output)));
				},
				Ok(ControlFlow::Continue(error)) => {
					if result.is_ok() {
						result = Ok(ControlFlow::Continue(error));
					}
				},
			}
		}
		let output = result?;
		Ok(output)
	}

	async fn try_get_process_attach_stream_region(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
		region: &str,
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let mut location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let (arg, stream, sender) =
			Self::attach_process_input(arg, location.clone(), location.clone().into());
		let (header, output) = match client
			.try_get_process_attach_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to attach to the process"),
			)? {
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => return Ok(ControlFlow::Break(None)),
			ControlFlow::Continue(error) => {
				let error = tg::error!(!error, region = %region, "failed to attach to the process");
				return Ok(ControlFlow::Continue(error));
			},
		};
		if let Err(stream) = sender.send(input.take().unwrap()) {
			*input = Some(stream);
			return Err(tg::error!("the process connection closed"));
		}
		let trusted = client.trusted();
		let mut process = None;
		let stopper = session.context.stopper.clone();
		let session = session.clone();
		let output = output
			.map(move |message| {
				let mut message = message?;
				Self::update_attach_process_message_referents_for_location(
					&session,
					&mut message,
					&mut location,
					trusted,
				)?;
				if let tg::process::attach::ServerMessage::Notification(
					tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
						output,
					)),
				) = &message
				{
					process = output.process.as_ref().right().cloned();
				}
				if matches!(
					&message,
					tg::process::attach::ServerMessage::Notification(
						tg::process::attach::ServerNotification::Outcome(_)
					)
				) && let Some(process) = &process
				{
					Self::remove_finished_process_child_lease(&session, process);
				}
				Ok(message)
			})
			.with_stopper(stopper)
			.boxed();

		Ok(ControlFlow::Break(Some((header, output))))
	}

	async fn try_get_process_attach_stream_remotes(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		let mut result = Ok(ControlFlow::Break(None));
		for remote in remotes {
			match self
				.try_get_process_attach_stream_remote(session, arg.clone(), input, remote)
				.await
			{
				Err(error) => result = Err(error),
				Ok(ControlFlow::Break(None)) => (),
				Ok(ControlFlow::Break(Some(output))) => {
					return Ok(ControlFlow::Break(Some(output)));
				},
				Ok(ControlFlow::Continue(error)) => {
					if result.is_ok() {
						result = Ok(ControlFlow::Continue(error));
					}
				},
			}
		}
		let output = result?;
		Ok(output)
	}

	async fn try_get_process_attach_stream_remote(
		&self,
		session: &Session,
		arg: tg::process::attach::Arg,
		input: &mut Option<Input>,
		remote: &tangram_server_core::location::Remote,
	) -> tg::Result<ControlFlow<Option<(tg::process::attach::Header, Output)>, tg::Error>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
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
		let (arg, stream, sender) = Self::attach_process_input(arg, location.clone(), arg_location);
		let (header, output) = match client
			.try_get_process_attach_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to attach to the process"),
			)? {
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => return Ok(ControlFlow::Break(None)),
			ControlFlow::Continue(error) => {
				let error =
					tg::error!(!error, remote = %remote.name, "failed to attach to the process");
				return Ok(ControlFlow::Continue(error));
			},
		};
		if let Err(stream) = sender.send(input.take().unwrap()) {
			*input = Some(stream);
			return Err(tg::error!("the process connection closed"));
		}
		let trusted = client.trusted();
		let mut process = None;
		let stopper = session.context.stopper.clone();
		let session = session.clone();
		let output = output
			.map(move |message| {
				let mut message = message?;
				Self::update_attach_process_message_referents_for_location(
					&session,
					&mut message,
					&mut location,
					trusted,
				)?;
				if let tg::process::attach::ServerMessage::Notification(
					tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
						output,
					)),
				) = &message
				{
					process = output.process.as_ref().right().cloned();
				}
				if matches!(
					&message,
					tg::process::attach::ServerMessage::Notification(
						tg::process::attach::ServerNotification::Outcome(_)
					)
				) && let Some(process) = &process
				{
					Self::remove_finished_process_child_lease(&session, process);
				}
				Ok(message)
			})
			.with_stopper(stopper)
			.boxed();

		Ok(ControlFlow::Break(Some((header, output))))
	}

	fn attach_process_input(
		arg: tg::process::attach::Arg,
		destination: tg::Location,
		location: tg::location::Arg,
	) -> (tg::process::attach::Arg, Input, oneshot::Sender<Input>) {
		// Keep the operation stream until the endpoint confirms that it found the process.
		let (sender, receiver) = oneshot::channel::<Input>();
		let input = futures::stream::once(async move {
			receiver
				.await
				.map_err(|_| tg::error!("the process connection was not selected"))
		})
		.try_flatten();
		let mut arg = arg;
		Self::update_attach_process_arg_for_location(&mut arg, &destination, &location);
		let input = input
			.map_ok(move |mut message| {
				Self::update_attach_process_request_for_location(
					&mut message,
					&destination,
					&location,
				);
				message
			})
			.boxed();
		(arg, input, sender)
	}

	fn update_attach_process_message_referents_for_location(
		session: &Session,
		message: &mut tg::process::attach::ServerMessage,
		location: &mut tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match message {
			tg::process::attach::ServerMessage::Ack(_)
			| tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(
					tg::progress::Event::Indicators(_) | tg::progress::Event::Log(_),
				),
			) => (),

			tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Outcome(outcome),
			) => {
				Self::update_outcome_referents_for_location(session, outcome, location, trusted)?;
			},
			tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(tg::progress::Event::Diagnostic(
					diagnostic,
				)),
			) => {
				if let Some(data) = &mut diagnostic.location {
					session
						.server
						.components
						.authorization
						.update_tokens_and_location(
							session,
							&mut data.module.referent.options.tokens,
							Some(&mut data.module.referent.options.location),
							location,
							trusted,
						)?;
				}
			},
			tg::process::attach::ServerMessage::Notification(
				tg::process::attach::ServerNotification::Progress(tg::progress::Event::Output(
					output,
				)),
			) => {
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
				Self::update_spawn_process_output_referents_for_location(
					session, output, location, trusted,
				)?;
			},
			tg::process::attach::ServerMessage::Response(response) => {
				if let Some(error) = &mut response.error {
					session
						.server
						.components
						.authorization
						.update_error_data_referents_for_location(
							session, error, location, trusted,
						)?;
				}
			},
		}
		Ok(())
	}

	fn update_attach_process_arg_for_location(
		arg: &mut tg::process::attach::Arg,
		destination: &tg::Location,
		location: &tg::location::Arg,
	) {
		let location = Some(location.clone());

		arg.location = location.clone();
		arg.tokens = arg.tokens.for_location(destination);
		if let tg::Either::Left(spawn) = &mut arg.process {
			if let Some(tg::Either::Right(sandbox)) = &mut spawn.sandbox {
				sandbox.options.tokens = sandbox.options.tokens.for_location(destination);
				sandbox.options.location =
					location.as_ref().and_then(tg::location::Arg::to_location);
			}
			spawn.location = location;
			Self::update_spawn_process_command_for_location(&mut spawn.command, destination);
		}
	}

	fn update_attach_process_request_for_location(
		message: &mut tg::process::attach::ClientMessage,
		destination: &tg::Location,
		location: &tg::location::Arg,
	) {
		let tg::process::attach::ClientMessage::Request(request) = message else {
			return;
		};
		let location = Some(location.clone());
		match &mut request.arg {
			tg::process::attach::ClientRequestArg::Cancel(arg) => arg.location = location,
			tg::process::attach::ClientRequestArg::Detach => (),
			tg::process::attach::ClientRequestArg::Signal(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
			tg::process::attach::ClientRequestArg::Tty(arg) => {
				arg.location = location;
				arg.tokens = arg.tokens.for_location(destination);
			},
		}
	}

	pub(crate) async fn try_get_process_attach_stream_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the headers.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| {
				tg::error!(argument, !error, "failed to parse the content type header")
			})?;
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let input_encoding = tangram_client::frame::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tg::process::attach::TANGRAM_CONTENT_TYPE,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;
		let output_encoding = tangram_client::frame::Encoding::from_accept(
			accept.as_ref(),
			tg::process::attach::TANGRAM_CONTENT_TYPE,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;

		// Attach to the process.
		let (arg, request) = request
			.arg_with_tangram::<tg::process::attach::Arg>()
			.await
			.map_err(|error| tg::error!(argument, !error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;
		let max_frame_size = session.server.config.sync.max_frame_size;
		let input = input_encoding.decode(request, max_frame_size);
		let (header, output) = match self
			.try_get_process_attach_stream(session, arg, input)
			.await?
		{
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => {
				return Ok(http::Response::builder()
					.not_found()
					.empty()
					.unwrap()
					.boxed_body());
			},
			ControlFlow::Continue(error) => {
				return Err(tg::error!(
					unavailable,
					!error,
					"failed to attach to the process"
				));
			},
		};

		// Create the response.
		let body = output_encoding.encode(output, max_frame_size);
		let body = tangram_http::body::header::set(body, &header, output_encoding.serialization())
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				output_encoding
					.content_type(tg::process::attach::TANGRAM_CONTENT_TYPE)
					.to_string(),
			)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
