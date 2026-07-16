use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::runner::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::runner::control::ServerMessage);

impl Session {
	pub(crate) async fn get_runner_control_stream_with_context(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>> {
		match &self.context.principal {
			tg::Principal::Root => (),
			tg::Principal::Runner(runner) if runner == &arg.id => (),
			tg::Principal::Runner(runner) => {
				return Err(tg::error!(
					runner = %runner,
					id = %arg.id,
					"invalid runner"
				));
			},
			_ => return Err(tg::error!("unauthorized")),
		}
		let location = self.server.location(arg.location.as_ref())?;
		let stream = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.get_runner_control_stream_local(arg, stream).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.get_runner_control_stream_region(arg, stream, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.get_runner_control_stream_remote(arg, stream, name, region)
					.await?
			},
		};
		Ok(stream)
	}

	async fn get_runner_control_stream_local(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>> {
		let id = arg.id.clone();
		let server_messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(format!("runners.{id}.control.server"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the runner server message stream"
				)
			})?;

		let request =
			crate::scheduler::RequestArg::AddRunner(crate::scheduler::AddRunnerRequestArg {
				capacity: arg.heartbeat.capacity,
				host: arg.host,
				runner: id.clone(),
			});
		self.send_scheduler_request(request).await??;

		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let control = crate::control::Stream::new(stream, sender, crate::control::stream_options());
		let control_sender = control.sender();
		let mut server_messages = server_messages;
		let server_messages_task =
			Task::spawn(move |_| async move {
				while let Some(message) = server_messages.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get a runner server message")
				})? {
					control_sender.send(message.payload.0).await?;
				}
				Ok::<_, tg::Error>(())
			});

		let control_task = Task::spawn({
			let server = self.server.clone();
			let session = self.clone();
			let runner = id.clone();
			move |_| async move {
				let mut control = control;
				while let Some(message) = control.recv().await? {
					// If the message is a heartbeat notification, then notify the scheduler.
					if let tg::runner::control::ClientMessage::Notification(
						tg::runner::control::ClientNotification::Heartbeat(heartbeat),
					) = &message
					{
						let notification = crate::scheduler::Message::Notification(
							crate::scheduler::Notification::Heartbeat(
								crate::scheduler::HeartbeatNotification {
									capacity: heartbeat.capacity,
									runner: runner.clone(),
								},
							),
						);
						server
							.messenger
							.publish("scheduler.server".to_owned(), notification)
							.await
							.inspect_err(|error| {
								tracing::error!(%error, "failed to publish the scheduler heartbeat notification");
							})
							.ok();
					}
					let subject = match message.clone() {
						tg::runner::control::ClientMessage::Response(response) => {
							format!("runners.{runner}.control.client.{}", response.id)
						},
						tg::runner::control::ClientMessage::Notification(_) => {
							format!("runners.{runner}.control.client")
						},
						tg::runner::control::ClientMessage::Ack(_) => unreachable!(),
						tg::runner::control::ClientMessage::Request(request) => {
							match request.arg {}
						},
					};
					server
						.messenger
						.publish(subject, ClientMessage(message))
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to publish the runner client message")
						})?;
				}
				let request = crate::scheduler::RequestArg::RemoveRunner(
					crate::scheduler::RemoveRunnerRequestArg {
						runner: runner.clone(),
					},
				);
				session.send_scheduler_request(request).await??;
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.attach(server_messages_task)
			.attach(control_task)
			.map(Ok)
			.with_stopper(self.context.stopper.clone())
			.boxed();

		Ok(stream)
	}

	async fn get_runner_control_stream_region(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
		region: String,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>> {
		let id = arg.id.clone();
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::runner::control::Arg {
			location: Some(location.into()),
			..arg
		};
		let stream = client
			.get_runner_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?;
		let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
		Ok(stream)
	}

	async fn get_runner_control_stream_remote(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>> {
		let id = arg.id.clone();
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::runner::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let stream = client
			.get_runner_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
		Ok(stream)
	}

	pub(crate) async fn get_runner_control_stream_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		// Parse the arg.
		let arg = request
			.query_params::<tg::runner::control::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("missing the query params"))?;

		// Create the client message stream.
		let stream = request
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read a message"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.with_stopper(self.context.stopper.clone())
			.boxed();

		// Get the server message stream.
		let stream = self
			.get_runner_control_stream_with_context(arg, stream)
			.await?;

		// Create the body.
		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(stream);

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub(crate) async fn send_runner_control_request(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::control::ServerRequestArg,
		options: crate::control::Options,
	) -> tg::Result<tg::Result<tg::runner::control::ClientResponseOutput>> {
		let id = crate::control::id();
		let request =
			tg::runner::control::ServerMessage::Request(tg::runner::control::ServerRequest {
				arg,
				id: id.clone(),
			});
		let request = ServerMessage(request);
		self.send_control_request(crate::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::runner::control::ServerMessage::Ack(
					tg::runner::control::ServerAck { id },
				))
			},
			client_subject: format!("runners.{runner}.control.client.{id}"),
			marker: std::marker::PhantomData,
			request,
			options,
			response: |message: ClientMessage| {
				let tg::runner::control::ClientMessage::Response(message) = message.0 else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing runner control response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: format!("runners.{runner}.control.server"),
		})
		.await
	}
}

impl tangram_messenger::Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}

impl tangram_messenger::Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message =
			serde_json::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}

impl crate::control::Output for tg::runner::control::ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Request(_) => None,
			Self::Notification(notification) => match notification {
				tg::runner::control::ClientNotification::Heartbeat(_) => None,
				tg::runner::control::ClientNotification::SandboxDestroyed(notification) => {
					Some(&notification.id)
				},
			},
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::control::Input<tg::runner::control::ServerMessage>
	for tg::runner::control::ClientMessage
{
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Notification(notification) => crate::control::InputKind::Message {
				id: match notification {
					tg::runner::control::ClientNotification::Heartbeat(_) => None,
					tg::runner::control::ClientNotification::SandboxDestroyed(notification) => {
						Some(&notification.id)
					},
				},
			},
			Self::Response(response) => crate::control::InputKind::Message {
				id: Some(&response.id),
			},
		}
	}

	fn create_ack_message(id: String) -> tg::runner::control::ServerMessage {
		tg::runner::control::ServerMessage::Ack(tg::runner::control::ServerAck { id })
	}
}

impl crate::control::Output for tg::runner::control::ServerMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) | Self::Response(_) => None,
			Self::Request(request) => Some(&request.id),
		}
	}
}

impl crate::control::Input<tg::runner::control::ClientMessage>
	for tg::runner::control::ServerMessage
{
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Notification(_) | Self::Response(_) => {
				crate::control::InputKind::Message { id: None }
			},
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
		}
	}

	fn create_ack_message(id: String) -> tg::runner::control::ClientMessage {
		tg::runner::control::ClientMessage::Ack(tg::runner::control::ClientAck { id })
	}
}
