use {
	super::{Input, Sender},
	crate::Session,
	futures::{StreamExt as _, stream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tokio::sync::mpsc,
	tokio_stream::wrappers::ReceiverStream,
};

pub(super) struct Destination {
	pub input: Input,
	pub sync: tg::Referent<tg::sync::Id>,
	pub task: Task<tg::Result<()>>,
}

pub(super) struct Source {
	pub input: Input,
	pub sender: mpsc::Sender<tg::Result<tg::sync::Message>>,
}

impl Session {
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
		sender: &Sender,
	) -> tg::Result<Destination> {
		// Split the process and sync messages.
		let (process_sender, process_receiver) = mpsc::channel(64);
		let (sync_sender, sync_receiver) = mpsc::channel(1024);
		let input_task = Task::spawn(move |_| async move {
			Self::connect_process_split_command_sync_input(input, process_sender, sync_sender).await
		});
		let input = ReceiverStream::new(process_receiver)
			.attach(input_task)
			.boxed();
		let sync_input = ReceiverStream::new(sync_receiver).boxed();

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
		let (output, mut sync_output) = self.sync_for_process(arg, sync_input).await?;
		let sync = output
			.sync
			.ok_or_else(|| tg::error!("the command sync did not produce a sync"))?;

		// Forward the destination sync messages over the process connection.
		let sender = sender.clone();
		let task = Task::spawn(move |_| async move {
			while let Some(message) = sync_output.next().await {
				let message = message
					.as_ref()
					.map_err(Clone::clone)
					.and_then(Self::connect_process_encode_sync_message);
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
		let (sender, receiver) = mpsc::channel(1024);
		let sync_input = ReceiverStream::new(receiver).boxed();
		let (_, sync_output) = self.sync_for_process(arg, sync_input).await?;

		// Add the source sync messages to the process connection.
		let sync_output = sync_output.map(|message| {
			message
				.as_ref()
				.map_err(Clone::clone)
				.and_then(Self::connect_process_encode_sync_message)
				.map(tg::process::connect::ClientMessage::Sync)
		});
		let input = stream::select(input, sync_output).boxed();
		let source = Source { input, sender };

		Ok(source)
	}

	async fn connect_process_split_command_sync_input(
		mut input: Input,
		process_sender: mpsc::Sender<tg::Result<tg::process::connect::ClientMessage>>,
		sync_sender: mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		let mut sync_sender = Some(sync_sender);
		while let Some(message) = input.next().await {
			let message = match message {
				Ok(tg::process::connect::ClientMessage::Sync(message)) => {
					let message = Self::connect_process_decode_sync_message(&message)?;
					if matches!(message, tg::sync::Message::End) {
						sync_sender = None;
					} else {
						let sender = sync_sender
							.as_ref()
							.ok_or_else(|| tg::error!("received a sync message after the end"))?;
						sender
							.send(Ok(message))
							.await
							.map_err(|_| tg::error!("the command sync closed"))?;
					}
					continue;
				},
				message => message,
			};
			process_sender
				.send(message)
				.await
				.map_err(|_| tg::error!("the process connection closed"))?;
		}

		Ok(())
	}

	pub(super) fn connect_process_decode_sync_message(
		message: &[u8],
	) -> tg::Result<tg::sync::Message> {
		let message = tangram_serialize::from_slice(message)
			.map_err(|error| tg::error!(!error, "failed to deserialize the sync message"))?;

		Ok(message)
	}

	fn connect_process_encode_sync_message(message: &tg::sync::Message) -> tg::Result<Vec<u8>> {
		let message = tangram_serialize::to_vec(message)
			.map_err(|error| tg::error!(!error, "failed to serialize the sync message"))?;

		Ok(message)
	}
}
