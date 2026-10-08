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
		arg: tg::process::control::Arg,
		input: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		completion: watch::Sender<bool>,
	) -> tg::Result<
		Option<(
			tg::process::control::Header,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		completion.send_replace(false);
		let shortcut = !arg.start;
		let (outcome_sender, outcome_receiver) = watch::channel(false);
		let (initialization_sender, initialization_receiver) = oneshot::channel();
		let mut initialization_sender = Some(initialization_sender);
		let data = arg.data.clone().or_else(|| {
			arg.id
				.as_ref()
				.and_then(|id| self.server.runner.state().try_get_process(id))
		});
		if let Some(data) = data {
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
			let message = message?;
			if let tg::process::control::ClientMessage::Request(request) = &message {
				match &request.arg {
					tg::process::control::ClientRequestArg::Finish(finish) => {
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
						if let Some(sender) = initialization_sender.take() {
							let session =
								session.try_get_process_session(&start.parent).ok_or_else(
									|| tg::error!("failed to find the parent process session"),
								)?;
							sender.send((session, start.data.command.objects())).ok();
						}
					},
					tg::process::control::ClientRequestArg::Write(_) => {},
				}
			}
			Ok(message)
		});
		let (sync_sender, sync_receiver) = mpsc::channel(16);
		let (sync_input_sender, sync_input_receiver) = mpsc::channel(16);
		let (end_sender, end_receiver) = oneshot::channel();
		let sync_task = Task::spawn(move |_| async move {
			let error_sender = sync_sender.clone();
			let future = async move {
				let (mut session, objects): (Session, Vec<tg::Referent<tg::object::Id>>) =
					initialization_receiver.await.map_err(|_| {
						tg::error!("the process control stream ended before initialization")
					})?;
				session.context.stopper = None;
				if shortcut {
					crate::checkpoint!(session.server, "runner.process.command.sync.started").await;
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
					ReceiverStream::new(sync_input_receiver),
					UnboundedReceiverStream::new(additional_receiver),
				)
				.boxed();
				let (_, mut output) = session.sync_inner(arg, input).await?;
				let mut outcome_started = false;
				while let Some(message) = output.next().await {
					if !outcome_started && *outcome_receiver.borrow() {
						outcome_started = true;
						crate::checkpoint!(session.server, "runner.process.outcome.sync.started")
							.await;
					}
					let message = message
						.and_then(|message| {
							tangram_serialize::to_vec(&message).map_err(|error| {
								tg::error!(!error, "failed to serialize the sync message")
							})
						})
						.map(tg::process::control::ClientMessage::Sync);
					sync_sender
						.send(message)
						.await
						.map_err(|_| tg::error!("the process control stream closed"))?;
				}
				end_receiver
					.await
					.map_err(|_| tg::error!("the process sync ended unexpectedly"))?;
				crate::checkpoint!(session.server, "runner.process.outcome.sync.finished").await;
				completion.send_replace(true);
				Ok::<_, tg::Error>(())
			};
			if let Err(error) = future.await {
				error_sender.send(Err(error)).await.ok();
			}
		});
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
		let (sender, receiver) = mpsc::channel(512);
		let output_task = Task::spawn(move |_| async move {
			let error_sender = sender.clone();
			let future = async move {
				let mut end_sender = Some(end_sender);
				while let Some(message) = output.next().await {
					match message {
						Ok(tg::process::control::ServerMessage::Sync(bytes)) => {
							let message: tg::sync::Message = tangram_serialize::from_slice(&bytes)
								.map_err(|error| {
									tg::error!(!error, "failed to deserialize the sync message")
								})?;
							if matches!(message, tg::sync::Message::End)
								&& let Some(sender) = end_sender.take()
							{
								sender.send(()).ok();
							}
							sync_input_sender.send(Ok(message)).await.ok();
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
}
