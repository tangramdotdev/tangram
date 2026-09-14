use {
	super::{ClientMessage, ServerMessage},
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	std::time::Duration,
	tangram_client::{
		prelude::*,
		process::{
			control,
			stdio::{flow, read},
		},
	},
	tangram_messenger::Messenger as _,
};

impl Session {
	pub(crate) fn send_process_control_read(
		&self,
		id: &tg::process::Id,
		arg: read::Arg,
	) -> BoxStream<'static, tg::Result<read::ServerMessage>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(flow::CHANNEL_CAPACITY);
		let (progress_sender, progress_receiver) = tokio::sync::mpsc::channel(4);
		let session = self.clone();
		let id = id.clone();
		tokio::spawn(async move {
			let result = session
				.send_process_control_read_task(&id, arg, sender.clone(), progress_receiver)
				.boxed()
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		let output = tokio_stream::wrappers::ReceiverStream::new(receiver);
		let state = (output, progress_sender, flow::Receiver::default(), 0);
		futures::stream::try_unfold(
			state,
			|(mut output, sender, mut window, pending)| async move {
				if let Some(progress) = window.consume(pending)? {
					sender.send(progress).await.ok();
				}
				let Some(message) = output.try_next().await? else {
					return Ok(None);
				};
				let pending = match &message {
					read::ServerMessage::Notification(read::Event::Chunk(chunk)) => {
						chunk.bytes.len()
					},
					_ => 0,
				};
				Ok(Some((message, (output, sender, window, pending))))
			},
		)
		.boxed()
	}

	async fn send_process_control_read_task(
		&self,
		id: &tg::process::Id,
		arg: read::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<read::ServerMessage>>,
		mut progress: tokio::sync::mpsc::Receiver<read::Progress>,
	) -> tg::Result<()> {
		for stream in &arg.streams {
			crate::checkpoint!(self.server, "process.stdio.read.request", process = %id, stream = %stream).await;
		}
		let request_id = crate::control::id();
		let request = control::ServerRequest {
			arg: control::ServerRequestArg::Read(arg),
			id: request_id.clone(),
		};
		let request = ServerMessage(control::ServerMessage::Request(request));
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			},
			timeout: if self
				.server
				.config
				.roles
				.contains(&crate::config::Role::Runner)
			{
				self.server.config.runner.stdio_drain_timeout
			} else {
				Duration::from_secs(10)
			},
		};
		let arg = crate::control::SendControlRequestArg {
			ack: |id| ServerMessage(control::ServerMessage::Ack(control::ServerAck { id })),
			client_subject: format!("processes.{id}.control.client.{request_id}"),
			is_ack: |message: &ClientMessage| matches!(message.0, control::ClientMessage::Ack(_)),
			marker: std::marker::PhantomData,
			options,
			request,
			response: {
				let sender = sender.clone();
				move |message: ClientMessage| match message.0 {
					control::ClientMessage::Notification(control::ClientNotification::Read(
						notification,
					)) => {
						if sender.is_closed() {
							return Ok(None);
						}
						sender
							.try_send(Ok(read::ServerMessage::Notification(notification.event)))
							.map_err(|_| tg::error!("the process read window was exceeded"))?;
						Ok(None)
					},
					control::ClientMessage::Response(response) => {
						let output = if let Some(error) = response.error {
							Err(tg::Error::try_from(error)?)
						} else {
							response
								.output
								.ok_or_else(|| tg::error!("missing the process read response"))?
								.try_unwrap_read()
								.map_err(|_| tg::error!("expected a process read response"))
						};
						Ok(Some((response.id, output)))
					},
					_ => Ok(None),
				}
			},
			server_subject: format!("processes.{id}.control.server"),
		};
		let response = self.server.start_control_request(arg).await?;
		let mut response = std::pin::pin!(response);
		loop {
			tokio::select! {
				result = &mut response => {
					let result = result?.map(read::ServerMessage::Response);
					sender.send(result).await.ok();
					return Ok(());
				},
				value = progress.recv() => {
					let Some(progress) = value else { break; };
					let notification = control::ReadServerNotification { id: request_id.clone(), progress };
					let message = ServerMessage(control::ServerMessage::Notification(control::ServerNotification::Read(notification)));
					self.server.messenger.publish(format!("processes.{id}.control.server"), message).await
						.map_err(|source| tg::error!(!source, "failed to publish the process read progress"))?;
				},
				() = sender.closed() => break,
			}
		}
		// Cancel the runner read when the downstream reader closes, including while it is idle.
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: Duration::from_secs(1),
		};
		let close = self.send_process_control_request(
			id,
			control::ServerRequestArg::Close(request_id),
			options,
		);
		tokio::time::timeout(Duration::from_secs(10), async {
			let _ = futures::join!(close, response);
		})
		.await
		.ok();
		Ok(())
	}
}
