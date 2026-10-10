use {
	crate::Session,
	futures::{StreamExt as _, stream::BoxStream},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::{Ext as _, TryExt as _},
		task::Task,
	},
	tokio::sync::mpsc,
	tokio_stream::wrappers::ReceiverStream,
};

#[derive(Clone)]
pub(super) struct Destination {
	get: Arc<Mutex<Option<mpsc::UnboundedSender<tg::Referent<tg::Selector<tg::Id>>>>>>,
	pub referent: tg::Referent<tg::sync::Id>,
}

pub(super) fn merge<T: Send + 'static>(
	high: BoxStream<'static, T>,
	low: BoxStream<'static, T>,
) -> BoxStream<'static, T> {
	futures::stream::select_with_strategy(high, low, |(): &mut ()| futures::stream::PollNext::Left)
		.boxed()
}

impl Session {
	pub(super) async fn process_control_sync_destination(
		&self,
		id: &tg::process::Id,
		data: Option<&tg::process::Data>,
		assign: bool,
		input: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<(
		Destination,
		BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
	)> {
		let (get_sender, mut get_receiver) = mpsc::unbounded_channel();
		let (sender, receiver) = mpsc::channel(256);
		let get_task = Task::spawn(move |_| async move {
			while let Some(node) = get_receiver.recv().await {
				if sender.send(node).await.is_err() {
					break;
				}
			}
		});
		let config = self.server.config().sync.flow;
		let (sync_sender, sync_input, consumption) = crate::sync::flow::Input::new(config)?;
		let updates = crate::sync::flow::Updates::new();
		let arg = crate::sync::InnerArg {
			arg: tg::sync::Arg {
				eager: true,
				..Default::default()
			},
			get: Some(receiver),
			..Default::default()
		};
		let (header, output) = self.sync_inner(arg, sync_input).await?;
		let sync = Destination {
			get: Arc::new(Mutex::new(Some(get_sender))),
			referent: header.sync.ok_or_else(|| tg::error!("missing the sync"))?,
		};
		if let Some(data) = data {
			sync.add(data)?;
		} else if !assign {
			// Finish may have been acknowledged while its index write is still queued.
			self.index().await?.try_last().await?;
			let arg = tg::process::get::Arg {
				location: Some(tg::Location::Local(tg::location::Local::default()).into()),
				source: tg::process::Source::Index,
				..Default::default()
			};
			if let Some(process) = self.try_get_process(id, arg).await? {
				sync.add(&process.data)?;
			}
		}
		let (sender, receiver) =
			mpsc::channel(self.server.config().process.stdio.connection_capacity());
		let input_task = Task::spawn({
			let updates = updates.clone();
			let sync = sync.clone();
			move |_| async move {
				let error_sender = sender.clone();
				let future = async move {
					let mut input = input;
					while let Some(message) = input.next().await {
						let message = match message {
							Ok(tg::process::control::ClientMessage::Sync(message)) => {
								sync_sender.receive_sync_message(message)?;
								continue;
							},
							Ok(tg::process::control::ClientMessage::SyncConfig(config)) => {
								updates.set_sync_config(config)?;
								continue;
							},
							Ok(tg::process::control::ClientMessage::SyncConsumption(
								consumption,
							)) => {
								updates.update_sync_consumption(consumption)?;
								continue;
							},
							Ok(tg::process::control::ClientMessage::Request(mut request)) => {
								match &mut request.arg {
									tg::process::control::ClientRequestArg::Start(start) => {
										let data = &mut start.data;
										sync.add(data)?;
										data.command
											.options
											.tokens
											.inherit(&sync.referent.options.tokens);
									},
									tg::process::control::ClientRequestArg::Finish(finish) => {
										sync.add(&finish.data)?;
									},
									tg::process::control::ClientRequestArg::Write(_) => {},
								}
								Ok(tg::process::control::ClientMessage::Request(request))
							},
							message => message,
						};
						if sender.send(message).await.is_err() {
							break;
						}
					}
					Ok::<_, tg::Error>(())
				};
				if let Err(error) = future.await {
					error_sender.send(Err(error)).await.ok();
				}
			}
		});
		let input = ReceiverStream::new(receiver).attach(input_task).boxed();
		let notifications = futures::stream::once(async move {
			Ok(tg::process::control::ServerMessage::SyncConfig(config))
		})
		.chain(consumption.map(|consumption| {
			Ok(tg::process::control::ServerMessage::SyncConsumption(
				consumption,
			))
		}))
		.boxed();
		let output = updates
			.send_sync_messages(output)
			.map(|message| message.map(tg::process::control::ServerMessage::Sync))
			.attach(get_task)
			.boxed();
		let output = merge(notifications, output);
		Ok((sync, input, output))
	}

	pub(crate) fn process_control_outcome_objects(
		data: &tg::process::Data,
	) -> Vec<tg::Referent<tg::object::Id>> {
		let mut objects = Vec::new();
		if let Some(output) = &data.output {
			output.children_with_tokens(&mut objects);
		}
		if let Some(tg::Either::Right(error)) = &data.error {
			objects.push(error.clone().map(tg::object::Id::Error));
		}
		if let Some(log) = &data.log {
			objects.push(log.clone().map(tg::object::Id::from));
		}
		objects
	}
}

impl Destination {
	fn add(&self, data: &tg::process::Data) -> tg::Result<()> {
		let mut get = self.get.lock().unwrap();
		let Some(sender) = get.as_ref() else {
			return Ok(());
		};
		let nodes = data
			.command
			.objects()
			.into_iter()
			.chain(Session::process_control_outcome_objects(data));
		for node in nodes {
			sender
				.send(node.map(|id| tg::Selector::Id(id.into())))
				.map_err(|_| tg::error!("the process sync closed"))?;
		}
		if data.status.is_finished() {
			get.take();
		}
		Ok(())
	}
}
