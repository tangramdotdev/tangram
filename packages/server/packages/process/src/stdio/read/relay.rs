use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_server_core::Session,
	tg::process::stdio::{
		read::{ClientMessage, Event, Output, ServerMessage},
		write::Data,
	},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

impl Component {
	pub(super) async fn try_read_process_stdio_relay(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<ControlFlow<Option<BoxStream<'static, tg::Result<ServerMessage>>>, tg::Error>>
	{
		if arg.streams.as_slice() != [tg::process::stdio::Stream::Stdin] {
			return Err(tg::error!("expected only the stdin stream"));
		}
		let location = session.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session
					.get_region_session_for_process(&region)
					.await
					.map_err(
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
					.map_break(|output| output.map(futures::StreamExt::boxed))
					.map_continue(
						|error| tg::error!(!error, region = %region, "failed to read stdin"),
					)
			},
			tg::Location::Local(_) => {
				let open = arg.id.unwrap();
				ControlFlow::Break(Some(
					self.read_process_stdio_relay_local(session, id, open, input)?,
				))
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session_for_process(session, &remote)
					.await
					.map_err(
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
					.map_break(|output| output.map(futures::StreamExt::boxed))
					.map_continue(
						|error| tg::error!(!error, remote = %remote, "failed to read stdin"),
					)
			},
		};

		Ok(output)
	}

	fn read_process_stdio_relay_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		open: String,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<ServerMessage>>> {
		// The relay subjects are scoped to the process, so only the process may read its own stdin.
		match &session.context.principal {
			tg::Principal::Process(process) if process == id => (),
			_ => return Err(tg::error!("unauthorized")),
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let id = id.clone();
			move |_| async move {
				let result = component
					.read_process_stdio_relay_local_task(&session, &id, &open, input, &sender)
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
		session: &Session,
		id: &tg::process::Id,
		open: &str,
		input: BoxStream<'static, tg::Result<ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<ServerMessage>>,
	) -> tg::Result<()> {
		// Open the relay, which receives the writer's data and sends the runner's acknowledgments back, finishing once the runner's read completes after closing stdin.
		let send_subject = super::super::relay::ack_subject(id, open);
		let receive_subject = super::super::relay::data_subject(id, open);
		let options = session.server.config.process.stdio_relay;
		let (relay_sender, receiver) = tangram_messenger::relay::open::<
			super::super::relay::Ack,
			super::super::relay::Data,
			_,
		>(
			&session.server.messenger,
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
					tangram_server_core::checkpoint!(session.server, "process.stdio.read.relay.data", open = %open).await;
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
						.map_err(|error| tg::error!(!error, "the stdio acknowledgment task panicked"))?
						.map_err(super::super::relay::error)?;
					return Ok(());
				},
			}
		}
	}
}
