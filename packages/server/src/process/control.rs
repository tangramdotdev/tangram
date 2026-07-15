use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_index::prelude::*,
	tangram_messenger::Messenger,
};

pub(crate) mod finish;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::process::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::process::control::ServerMessage);

#[derive(Clone, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Connected {
	#[tangram_serialize(id = 0)]
	pub lease: String,
}

pub(crate) fn connected_subject(id: &tg::process::Id) -> String {
	format!("processes.{id}.control.connected")
}

impl Session {
	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>> {
		let location = self.server.location(arg.location.as_ref())?;
		let stream = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_get_process_control_stream_local(id, arg, stream)
					.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_get_process_control_stream_region(id, arg, stream, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.try_get_process_control_stream_remote(id, arg, stream, name, region)
					.await?
			},
		};
		Ok(stream)
	}

	async fn try_get_process_control_stream_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>> {
		let data = arg.data;
		let lease = arg.lease;
		let parent = arg.parent;
		let (sender, receiver) = tokio::sync::mpsc::channel(512);
		let mut control =
			crate::control::Stream::new(stream, sender, crate::control::stream_options());
		let control_sender = control.sender();

		let subject = format!("processes.{id}.control.server");
		let mut requests = self
			.server
			.messenger
			.subscribe::<ServerMessage>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the message stream"))?;
		let request_sender = control_sender.clone();
		let request_task = Task::spawn(move |_| async move {
			while let Some(message) = requests
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the message"))?
			{
				request_sender.send(message.payload.0).await?;
			}
			Ok::<_, tg::Error>(())
		});

		let response_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			move |_| async move {
				while let Some(message) = control.recv().await? {
					match message {
						tg::process::control::ClientMessage::Notification(
							tg::process::control::ClientNotification::ChildSpawned,
						) => {
							session
								.server
								.messenger
								.publish(format!("processes.{id}.children"), ())
								.await
								.map_err(|error| {
									tg::error!(
										!error,
										"failed to publish the child spawned notification"
									)
								})?;
						},
						tg::process::control::ClientMessage::Notification(
							tg::process::control::ClientNotification::BorrowableCapacity(
								notification,
							),
						) => {
							let notification = crate::scheduler::Message::Notification(
								crate::scheduler::Notification::BorrowableCapacity(
									crate::scheduler::BorrowableCapacityNotification {
										capacity: notification.capacity,
										parent: notification.parent,
										runner: notification.runner,
									},
								),
							);
							session
								.server
								.messenger
								.publish("scheduler.server".to_owned(), notification)
								.await
								.map_err(|error| {
									tg::error!(
										!error,
										"failed to publish the borrowable capacity notification"
									)
								})?;
						},
						tg::process::control::ClientMessage::Response(response) => {
							let subject = format!("processes.{id}.control.client.{}", response.id);
							let payload = ClientMessage(
								tg::process::control::ClientMessage::Response(response),
							);
							session
								.server
								.messenger
								.publish(subject, payload)
								.await
								.inspect_err(|error| {
									tracing::error!(%error, "failed to publish the response");
								})
								.ok();
						},
						tg::process::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							let result = match request.arg {
								tg::process::control::ClientRequestArg::Finish(arg) => session
									.finish_process_control_request(&id, arg)
									.await
									.map(tg::process::control::ServerResponseOutput::Finish),
							};
							let response =
								Self::process_control_server_response(request_id, result);
							control_sender.send(response).await?;
						},
						tg::process::control::ClientMessage::Ack(_) => unreachable!(),
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.attach(request_task)
			.attach(response_task)
			.map(Ok)
			.with_stopper(self.context.stopper.clone())
			.boxed();

		if let Some(data) = data {
			let index_arg = tangram_index::batch::Arg {
				put_processes: vec![tangram_index::process::put::Arg {
					children: None,
					command: data.command.clone().into(),
					data: Some(data.clone()),
					error: None,
					id: id.clone(),
					log: None,
					metadata: tg::process::Metadata::default(),
					output: None,
					parent,
					sandbox: Some(data.sandbox.clone()),
					stored: tangram_index::process::Stored::default(),
					time_to_touch: self.server.config.process.time_to_touch,
					touched_at: time::OffsetDateTime::now_utc().unix_timestamp(),
				}],
				..Default::default()
			};
			self.server
				.index_tasks
				.spawn(|_| {
					let server = self.server.clone();
					async move {
						let result = server.index.batch(index_arg).await;
						if let Err(error) = result {
							tracing::error!(error = %error.trace(), "failed to put the process to the index");
						}
					}
				})
				.detach();
		}

		self.server
			.messenger
			.publish(connected_subject(id), Connected { lease })
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the process control connection")
			})?;

		Ok(Some(stream))
	}

	fn process_control_server_response(
		id: String,
		result: tg::Result<tg::process::control::ServerResponseOutput>,
	) -> tg::process::control::ServerMessage {
		let (error, output) = match result {
			Ok(output) => (None, Some(output)),
			Err(error) => (
				Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				None,
			),
		};
		tg::process::control::ServerMessage::Response(tg::process::control::ServerResponse {
			error,
			id,
			output,
		})
	}

	async fn try_get_process_control_stream_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		region: String,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::control::Arg {
			location: Some(location.into()),
			..arg
		};
		let stream = client
			.try_get_process_control_stream(id, arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?;
		let stream = stream.map(|stream| stream.with_stopper(self.context.stopper.clone()).boxed());
		Ok(stream)
	}

	async fn try_get_process_control_stream_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let stream = client
			.try_get_process_control_stream(id, arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let stream = stream.map(|stream| stream.with_stopper(self.context.stopper.clone()).boxed());
		Ok(stream)
	}

	pub(crate) async fn try_get_process_control_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
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

		// Parse the ID.
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params::<tg::process::control::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		// Create the response stream.
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
			.boxed();

		// Get the request stream.
		let Some(stream) = self
			.try_get_process_control_stream_with_context(&id, arg, stream)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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

	pub(crate) async fn send_process_control_request(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::ServerRequestArg,
		options: crate::control::Options,
	) -> tg::Result<tg::Result<tg::process::control::ClientResponseOutput>> {
		let request_id = crate::control::id();
		let payload = ServerMessage(tg::process::control::ServerMessage::Request(
			tg::process::control::ServerRequest {
				arg,
				id: request_id.clone(),
			},
		));
		self.send_control_request(crate::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::process::control::ServerMessage::Ack(
					tg::process::control::ServerAck { id },
				))
			},
			client_subject: format!("processes.{id}.control.client.{request_id}"),
			marker: std::marker::PhantomData,
			request: payload,
			options,
			response: |message: ClientMessage| {
				let ClientMessage(tg::process::control::ClientMessage::Response(message)) = message
				else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing process control response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: format!("processes.{id}.control.server"),
		})
		.await
	}
}

impl tangram_messenger::Payload for ClientMessage {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}
}

impl tangram_messenger::Payload for ServerMessage {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(&self.0).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(tangram_messenger::Error::deserialization)?;
		Ok(Self(message))
	}
}

impl tangram_messenger::Payload for Connected {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes =
			tangram_serialize::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}
}

impl crate::control::Output for tg::process::control::ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::control::Input<tg::process::control::ServerMessage>
	for tg::process::control::ClientMessage
{
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Notification(_) => crate::control::InputKind::Message { id: None },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Response(response) => crate::control::InputKind::Message {
				id: Some(&response.id),
			},
		}
	}

	fn create_ack_message(id: String) -> tg::process::control::ServerMessage {
		tg::process::control::ServerMessage::Ack(tg::process::control::ServerAck { id })
	}
}

impl crate::control::Output for tg::process::control::ServerMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::control::Input<tg::process::control::ClientMessage>
	for tg::process::control::ServerMessage
{
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Notification(_) => crate::control::InputKind::Message { id: None },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
			Self::Response(response) => crate::control::InputKind::Message {
				id: Some(&response.id),
			},
		}
	}

	fn create_ack_message(id: String) -> tg::process::control::ClientMessage {
		tg::process::control::ClientMessage::Ack(tg::process::control::ClientAck { id })
	}
}
