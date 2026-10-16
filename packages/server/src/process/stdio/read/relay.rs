use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tg::process::stdio::{
		read::{ClientMessage, Event, Output, ServerMessage},
		write::Data,
	},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

impl Session {
	pub(super) async fn try_read_process_stdio_relay(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<ServerMessage>>>> {
		if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
			return Err(tg::error!("expected only the stdin stream"));
		}
		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				let client = self.get_region_session_for_process(&region).await.map_err(
					|error| tg::error!(!error, region = %region, "failed to get the region client"),
				)?;
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region.clone()),
				});
				let arg = tg::process::stdio::read::Arg {
					location: Some(location.clone().into()),
					tokens: arg.tokens.for_location(&location),
					..arg
				};
				client
					.try_read_process_stdio(&tg::Either::Right(id.clone()), arg, input)
					.await
					.map_err(|error| tg::error!(!error, region = %region, "failed to read stdin"))?
					.map(futures::StreamExt::boxed)
			},
			tg::Location::Local(_) => {
				let open = arg.id.unwrap();
				Some(self.read_process_stdio_relay_local(id, open, input)?)
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				let client = self.get_remote_session_for_process(&remote).await.map_err(
					|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
				)?;
				let location = tg::Location::Remote(tg::location::Remote {
					name: remote.clone(),
					region: region.clone(),
				});
				let arg = tg::process::stdio::read::Arg {
					location: Some(tg::Location::Local(tg::location::Local { region }).into()),
					tokens: arg.tokens.for_location(&location),
					..arg
				};
				client
					.try_read_process_stdio(&tg::Either::Right(id.clone()), arg, input)
					.await
					.map_err(|error| tg::error!(!error, remote = %remote, "failed to read stdin"))?
					.map(futures::StreamExt::boxed)
			},
		};

		Ok(output)
	}

	fn read_process_stdio_relay_local(
		&self,
		id: &tg::process::Id,
		open: String,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<ServerMessage>>> {
		// The relay subjects are scoped to the process, so only the process may read its own stdin.
		match &self.context.principal {
			tg::Principal::Process(process) if process == id => (),
			_ => return Err(tg::error!("unauthorized")),
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			move |_| async move {
				let result = session
					.read_process_stdio_relay_local_task(&id, &open, input, &sender)
					.boxed()
					.await;
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(stream)
	}

	async fn read_process_stdio_relay_local_task(
		&self,
		id: &tg::process::Id,
		open: &str,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<ServerMessage>>,
	) -> tg::Result<()> {
		// Open the relay, which receives the writer's data and sends the runner's acknowledgments back, finishing once the runner's read completes after closing stdin.
		let send_subject = super::super::relay::ack_subject(id, open);
		let receive_subject = super::super::relay::data_subject(id, open);
		let options = self.server.config().process.stdio_relay;
		let (relay_sender, receiver) = tangram_messenger::relay::open::<
			super::super::relay::Ack,
			super::super::relay::Data,
			_,
		>(
			&self.server.messenger,
			send_subject,
			receive_subject,
			options,
		)
		.await
		.map_err(super::super::relay::error)?;
		let mut items = receiver;
		let acks = input.map(|message| {
			message
				.map(super::super::relay::Ack)
				.map_err(|error| super::super::relay::abort(&error))
		});
		let mut ack_task = AbortOnDropHandle::new(tokio::spawn(relay_sender.send_all(acks)));

		// Forward the relayed data to the runner's read.
		let mut ended = false;
		loop {
			tokio::select! {
				item = items.next(), if !ended => {
					let Some(item) = item else {
						ended = true;
						continue;
					};
					let super::super::relay::Data(data) = item.map_err(super::super::relay::error)?;
					crate::checkpoint!(self.server, "process.stdio.read.relay.data", open = %open).await;
					let message = match data {
						Data::Chunk(chunk) => ServerMessage::Notification(Event::Chunk(chunk)),
						Data::End(data) => ServerMessage::Response(Output::End(data)),
					};
					sender
						.send(Ok(message))
						.await
						.map_err(|_| tg::error!("the stdin read output closed"))?;
				},
				result = &mut ack_task => {
					result
						.map_err(|error| tg::error!(unavailable, !error, "the stdio acknowledgment task panicked"))?
						.map_err(super::super::relay::error)?;
					return Ok(());
				},
			}
		}
	}
}
