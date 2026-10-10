use {
	super::{Input, Sender},
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, stream},
	std::collections::VecDeque,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tokio::sync::mpsc,
	tokio_stream::wrappers::ReceiverStream,
};

#[cfg(test)]
mod tests;

pub(super) struct Destination {
	pub input: Input,
	pub sync: tg::Referent<tg::sync::Id>,
	pub task: Task<tg::Result<()>>,
}

pub(super) struct Source {
	pub input: Input,
	pub sender: crate::sync::flow::Input,
	pub updates: crate::sync::flow::Updates,
}

impl Session {
	pub(super) async fn connect_process_await_command_sync(
		flow: tg::process::stdio::Config,
		task: &mut Option<Task<tg::Result<()>>>,
		input: &mut Input,
		pending: &mut VecDeque<tg::process::connect::ClientMessage>,
	) -> tg::Result<()> {
		let finish = Self::connect_process_finish_command_sync(task);
		tokio::pin!(finish);
		let mut input_open = true;
		loop {
			tokio::select! {
				result = &mut finish => {
					result?;
					return Ok(());
				},
				message = input.try_next(), if input_open => {
					match message? {
						Some(message) => Self::connect_process_buffer_message(flow, pending, message)?,
						None => input_open = false,
					}
				},
			}
		}
	}

	pub(super) fn connect_process_buffer_message(
		flow: tg::process::stdio::Config,
		pending: &mut VecDeque<tg::process::connect::ClientMessage>,
		message: tg::process::connect::ClientMessage,
	) -> tg::Result<()> {
		if pending.len() >= flow.connection_capacity() {
			return Err(tg::error!("too many buffered process messages"));
		}
		if let tg::process::connect::ClientMessage::Request(request) = &message
			&& let tg::process::connect::ClientRequestArg::Write(arg) = &request.arg
			&& let tg::process::stdio::write::Data::Chunk(chunk) = &arg.data
			&& chunk.bytes.len() > flow.max_message_size
		{
			return Err(tg::error!("invalid stdio chunk size"));
		}
		pending.push_back(message);
		Ok(())
	}

	pub(super) async fn connect_process_finish_command_sync(
		task: &mut Option<Task<tg::Result<()>>>,
	) -> tg::Result<()> {
		let Some(task) = task.take() else {
			return Ok(());
		};
		task.wait()
			.await
			.map_err(|error| tg::error!(!error, "the command sync task panicked"))??;

		Ok(())
	}

	pub(super) async fn connect_process_command_sync_destination(
		&self,
		command: &tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		input: Input,
		high: &Sender,
		low: &Sender,
	) -> tg::Result<Destination> {
		// Split the process and sync messages.
		let (process_sender, process_receiver) =
			mpsc::channel(self.server.config().process.stdio.connection_capacity());
		let config = self.server.config().sync.flow;
		let (sync_sender, sync_input, consumption) = crate::sync::flow::Input::new(config)?;
		let updates = crate::sync::flow::Updates::new();
		let input_task = Task::spawn({
			let updates = updates.clone();
			move |_| async move {
				Self::connect_process_split_command_sync_input(
					input,
					process_sender,
					sync_sender,
					updates,
				)
				.await
			}
		});
		let input = ReceiverStream::new(process_receiver)
			.attach(input_task)
			.boxed();

		// Start the destination sync and obtain its referent.
		let get = Self::spawn_process_command_nodes(command)?
			.into_iter()
			.map(|node| node.map(tg::Selector::Id))
			.collect();
		let arg = tg::sync::Arg {
			eager: true,
			get,
			location: Some(tg::Location::Local(tg::location::Local::default()).into()),
			..Default::default()
		};
		let arg = crate::sync::InnerArg {
			arg,
			process: true,
			..Default::default()
		};
		let (output, sync_output) = self.sync_inner(arg, sync_input).await?;
		let mut sync_output = updates.send_sync_messages(sync_output);
		let sync = output
			.sync
			.ok_or_else(|| tg::error!("the command sync did not produce a sync"))?;

		// Send consumption separately from the sync producer so a full window does not delay credit.
		let notifications = futures::stream::once(async move {
			Ok(tg::process::connect::ServerMessage::SyncConfig(config))
		})
		.chain(consumption.map(|consumption| {
			Ok(tg::process::connect::ServerMessage::SyncConsumption(
				consumption,
			))
		}))
		.boxed();
		let notification_sender = high.clone();
		let notifications = Task::spawn(move |_| async move {
			let mut notifications = notifications;
			while let Some(message) = notifications.next().await {
				if notification_sender.send(message).await.is_err() {
					break;
				}
			}
		});

		// Forward the destination sync messages over the process connection.
		let sender = low.clone();
		let task = Task::spawn(move |_| async move {
			let _notifications = notifications;
			while let Some(message) = sync_output.next().await {
				let message = message.map(tg::process::connect::ServerMessage::Sync);
				let failed = message.is_err();
				sender
					.send(message)
					.await
					.map_err(|_| tg::error!("the process connection closed"))?;
				if failed {
					return Err(tg::error!("the command sync failed"));
				}
			}

			Ok(())
		});

		let destination = Destination { input, sync, task };

		Ok(destination)
	}

	pub(super) async fn connect_process_command_sync_source(
		&self,
		command: &tg::Referent<tg::Either<tg::process::spawn::CommandArg, tg::command::Id>>,
		input: Input,
	) -> tg::Result<Source> {
		// Start the source sync.
		let location = tg::Location::Local(tg::location::Local::default());
		let put = Self::spawn_process_command_nodes(command)?
			.into_iter()
			.map(|mut node| {
				node.options.tokens = node.options.tokens.for_location(&location);
				node
			})
			.collect();
		let arg = tg::sync::Arg {
			eager: true,
			location: Some(location.into()),
			put,
			..Default::default()
		};
		let config = self.server.config().sync.flow;
		let (sender, sync_input, consumption) = crate::sync::flow::Input::new(config)?;
		let updates = crate::sync::flow::Updates::new();
		let arg = crate::sync::InnerArg {
			arg,
			process: true,
			..Default::default()
		};
		let (_, sync_output) = self.sync_inner(arg, sync_input).await?;

		// Add the source sync messages to the process connection.
		let sync_output = updates
			.send_sync_messages(sync_output)
			.map(|message| message.map(tg::process::connect::ClientMessage::Sync));
		let notifications =
			stream::once(
				async move { Ok(tg::process::connect::ClientMessage::SyncConfig(config)) },
			)
			.chain(consumption.map(|consumption| {
				Ok(tg::process::connect::ClientMessage::SyncConsumption(
					consumption,
				))
			}))
			.boxed();
		let input = stream::select_with_strategy(input, sync_output.boxed(), |(): &mut ()| {
			stream::PollNext::Left
		})
		.boxed();
		let input = stream::select_with_strategy(notifications, input, |(): &mut ()| {
			stream::PollNext::Left
		})
		.boxed();
		let source = Source {
			input,
			sender,
			updates,
		};

		Ok(source)
	}

	async fn connect_process_split_command_sync_input(
		mut input: Input,
		process_sender: mpsc::Sender<tg::Result<tg::process::connect::ClientMessage>>,
		sync_sender: crate::sync::flow::Input,
		updates: crate::sync::flow::Updates,
	) -> tg::Result<()> {
		let mut sync_sender = Some(sync_sender);
		while let Some(message) = input.next().await {
			let message = match message {
				Ok(tg::process::connect::ClientMessage::Sync(message)) => {
					if matches!(message, tg::sync::Message::End) {
						sync_sender = None;
					} else {
						let sender = sync_sender
							.as_ref()
							.ok_or_else(|| tg::error!("received a sync message after the end"))?;
						sender.receive_sync_message(message)?;
					}
					continue;
				},
				Ok(tg::process::connect::ClientMessage::SyncConfig(config)) => {
					updates.set_sync_config(config)?;
					continue;
				},
				Ok(tg::process::connect::ClientMessage::SyncConsumption(consumption)) => {
					updates.update_sync_consumption(consumption)?;
					continue;
				},
				message => message,
			};
			process_sender.try_send(message).map_err(|source| {
				tg::error!(!source, "the process input closed or exceeded its window")
			})?;
		}

		Ok(())
	}
}
