use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tokio::sync::{mpsc, oneshot, watch},
	tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream},
};

impl Session {
	pub(crate) async fn process_control_sync_source(
		&self,
		mut arg: tg::process::control::Arg,
		input: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		completion: watch::Sender<bool>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		completion.send_replace(false);
		let shortcut = matches!(arg.mode, tg::process::control::Mode::Wait);
		let destination = self.server.location(arg.location.as_ref())?;
		if let tg::process::control::Mode::Start(start) = &mut arg.mode {
			Self::inherit_process_control_tokens(&mut start.data, &destination);
		}
		let (outcome_sender, outcome_receiver) = watch::channel(false);
		let (initialization_sender, initialization_receiver) = oneshot::channel();
		let mut initialization_sender = Some(initialization_sender);
		let data = match &arg.mode {
			tg::process::control::Mode::Resume { .. } | tg::process::control::Mode::Wait => arg
				.id
				.as_ref()
				.and_then(|id| self.server.runner.state().try_get_process(id)),
			tg::process::control::Mode::Start(start) => Some(start.data.clone()),
		};
		if let Some(mut data) = data {
			Self::inherit_process_control_tokens(&mut data, &destination);
			let mut objects = data.command.objects();
			objects.extend(Self::process_control_outcome_objects(&data));
			outcome_sender.send_replace(data.status.is_finished());
			initialization_sender
				.take()
				.unwrap()
				.send((self.clone(), objects))
				.ok();
		}
		let (additional_sender, additional_receiver) = mpsc::unbounded_channel();
		let session = self.clone();
		let input = input.map(move |message| {
			let mut message = message?;
			if let tg::process::control::ClientMessage::Request(request) = &mut message {
				match &mut request.arg {
					tg::process::control::ClientRequestArg::Finish(finish) => {
						Self::inherit_process_control_tokens(&mut finish.data, &destination);
						outcome_sender.send_replace(true);
						for node in Self::process_control_outcome_objects(&finish.data) {
							let message = tg::sync::GetNodeMessage {
								descendants: true,
								eager: true,
								selector: tg::Selector::Id(node.node.into()),
								tokens: node.options.tokens,
							};
							additional_sender
								.send(Ok(tg::sync::Message::Get(tg::sync::GetMessage::Node(
									message,
								))))
								.ok();
						}
					},
					tg::process::control::ClientRequestArg::Start(start) => {
						Self::inherit_process_control_tokens(&mut start.data, &destination);
						if let Some(sender) = initialization_sender.take() {
							let parent = start.parent.as_ref().ok_or_else(|| {
								tg::error!("a process on the shortcut path must have a parent")
							})?;
							let data = &start.data;
							let session =
								session.try_get_process_session(parent).ok_or_else(|| {
									tg::error!("failed to find the parent process session")
								})?;
							sender.send((session, data.command.objects())).ok();
						}
					},
					tg::process::control::ClientRequestArg::Write(_) => {},
				}
			}
			Ok(message)
		});
		let (sync_sender, sync_receiver) = mpsc::channel(16);
		let config = self.server.config().sync.flow;
		let (sync_input_sender, sync_input, consumption) = crate::sync::flow::Input::new(config)?;
		let updates = crate::sync::flow::Updates::new();
		let (end_sender, end_receiver) = oneshot::channel();
		let sync_task = Task::spawn({
			let updates = updates.clone();
			move |_| async move {
				let error_sender = sync_sender.clone();
				let future = async move {
					let (mut session, objects): (Session, Vec<tg::Referent<tg::object::Id>>) =
						initialization_receiver.await.map_err(|_| {
							tg::error!("the process control stream ended before initialization")
						})?;
					session.context.stopper = None;
					if shortcut {
						crate::checkpoint!(session.server, "runner.process.command.sync.started")
							.await;
					}
					let put = objects
						.into_iter()
						.map(|node| node.map(Into::into))
						.collect();
					let arg = crate::sync::InnerArg {
						arg: tg::sync::Arg {
							eager: true,
							put,
							..Default::default()
						},
						..Default::default()
					};
					let input = futures::stream::select(
						sync_input,
						UnboundedReceiverStream::new(additional_receiver),
					)
					.boxed();
					let (_, output) = session.sync_inner(arg, input).await?;
					let mut output = updates.send_sync_messages(output);
					let mut outcome_started = false;
					while let Some(message) = output.next().await {
						if !outcome_started && *outcome_receiver.borrow() {
							outcome_started = true;
							crate::checkpoint!(
								session.server,
								"runner.process.outcome.sync.started"
							)
							.await;
						}
						let message = message.map(tg::process::control::ClientMessage::Sync);
						sync_sender
							.send(message)
							.await
							.map_err(|_| tg::error!("the process control stream closed"))?;
					}
					end_receiver
						.await
						.map_err(|_| tg::error!("the process sync ended unexpectedly"))?;
					crate::checkpoint!(session.server, "runner.process.outcome.sync.finished")
						.await;
					completion.send_replace(true);
					Ok::<_, tg::Error>(())
				};
				if let Err(error) = future.await {
					error_sender.send(Err(error)).await.ok();
				}
			}
		});
		let notifications = futures::stream::once(async move {
			Ok(tg::process::control::ClientMessage::SyncConfig(config))
		})
		.chain(consumption.map(|consumption| {
			Ok(tg::process::control::ClientMessage::SyncConsumption(
				consumption,
			))
		}))
		.boxed();
		let input = futures::stream::select_with_strategy(notifications, input, |(): &mut ()| {
			futures::stream::PollNext::Left
		})
		.boxed();
		let input = futures::stream::select_with_strategy(
			input,
			ReceiverStream::new(sync_receiver),
			|(): &mut ()| futures::stream::PollNext::Left,
		)
		.boxed();
		let mut session = self.clone();
		session.process_control_sync = None;
		let Some((header, mut output)) = session
			.try_get_process_control_stream_inner(arg, input)
			.boxed()
			.await?
		else {
			return Ok(None);
		};
		let (sender, receiver) =
			mpsc::channel(self.server.config().process.stdio.connection_capacity());
		let output_task = Task::spawn(move |_| async move {
			let error_sender = sender.clone();
			let future = async move {
				let mut end_sender = Some(end_sender);
				while let Some(message) = output.next().await {
					match message {
						Ok(tg::process::control::ServerMessage::Sync(message)) => {
							if matches!(message, tg::sync::Message::End)
								&& let Some(sender) = end_sender.take()
							{
								sender.send(()).ok();
							}
							sync_input_sender.receive_sync_message(message)?;
						},
						Ok(tg::process::control::ServerMessage::SyncConfig(config)) => {
							updates.set_sync_config(config)?;
						},
						Ok(tg::process::control::ServerMessage::SyncConsumption(consumption)) => {
							updates.update_sync_consumption(consumption)?;
						},
						message => {
							if sender.send(message).await.is_err() {
								break;
							}
						},
					}
				}
				Ok::<_, tg::Error>(())
			};
			if let Err(error) = future.await {
				error_sender.send(Err(error)).await.ok();
			}
		});
		let output = ReceiverStream::new(receiver)
			.attach(output_task)
			.attach(sync_task)
			.boxed();
		Ok(Some((header, output)))
	}

	fn inherit_process_control_tokens(data: &mut tg::process::Data, destination: &tg::Location) {
		if !destination.is_remote() {
			return;
		}
		let inherit = |tokens: &mut tg::authorization::Tokens| {
			let Some(entry) = tokens.get(destination).cloned() else {
				return;
			};
			let mut local = tokens.local_entry();
			local.inherit(&entry);
			tokens.set(tg::Location::Local(tg::location::Local::default()), local);
		};
		inherit(&mut data.command.options.tokens);
		if let tg::Either::Left(command) = &mut data.command.node {
			inherit(&mut command.executable.options.tokens);
			for value in command.args.iter_mut().chain(command.env.values_mut()) {
				let (tg::command::data::Value::String(value)
				| tg::command::data::Value::Value(value)) = value;
				Self::update_process_value_tokens(value, &mut |tokens, _| inherit(tokens));
			}
			if let Some(stdin) = &mut command.stdin {
				inherit(&mut stdin.options.tokens);
			}
		}
		if let Some(output) = &mut data.output {
			Self::update_process_value_tokens(output, &mut |tokens, _| inherit(tokens));
		}
		if let Some(tg::Either::Right(error)) = &mut data.error {
			inherit(&mut error.options.tokens);
		}
		if let Some(log) = &mut data.log {
			inherit(&mut log.options.tokens);
		}
	}
}
