use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
};

mod destroy;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::sandbox::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::sandbox::control::ServerMessage);

pub(crate) fn connected_subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.connected")
}

impl Session {
	pub(crate) async fn get_sandbox_control_stream_with_context(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.get_sandbox_control_stream_local(arg, stream).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.get_sandbox_control_stream_region(arg, stream, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.get_sandbox_control_stream_remote(arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn get_sandbox_control_stream_local(
		&self,
		mut arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		let (id, token) = if let Some(id) = arg.id.take() {
			match &self.context.principal {
				tg::Principal::Sandbox(sandbox) if sandbox == &id => (),
				tg::Principal::Sandbox(sandbox) => {
					return Err(tg::error!(sandbox = %sandbox, %id, "invalid sandbox"));
				},
				_ => return Err(tg::error!("unauthorized")),
			}
			(id, self.context.token.clone())
		} else {
			if !matches!(
				self.context.principal,
				tg::Principal::Root | tg::Principal::Runner(_)
			) {
				return Err(tg::error!("unauthorized"));
			}
			let id = tg::sandbox::Id::new();
			let token = self
				.server
				.create_sandbox_authentication_token(id.clone())?;
			(id, Some(token))
		};
		let context = crate::Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let created_at = arg
			.created_at
			.unwrap_or_else(|| time::OffsetDateTime::now_utc().unix_timestamp());
		let location = self.server.location(arg.location.as_ref())?;
		let data = arg.data.map(|data| tg::sandbox::get::Output {
			cpu: data.arg.cpu,
			creator: data.creator,
			hostname: data.arg.hostname,
			id: id.clone(),
			isolation: data.arg.isolation,
			location: Some(location),
			memory: data.arg.memory,
			mounts: data.arg.mounts,
			network: data.arg.network,
			owner: data.arg.owner,
			status: tg::sandbox::Status::Started,
			ttl: data.arg.ttl,
		});
		let runner = arg.runner;
		let server_messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(format!("sandboxes.{id}.control.server"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the sandbox server message stream"
				)
			})?;

		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let control = crate::control::Stream::new(stream, sender, crate::control::stream_options());
		let control_sender = control.sender();
		let mut server_messages = server_messages;
		let server_message_sender = control_sender.clone();
		let server_messages_task =
			Task::spawn(move |_| async move {
				while let Some(message) = server_messages.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get a sandbox server message")
				})? {
					server_message_sender.send(message.payload.0).await?;
				}
				Ok::<_, tg::Error>(())
			});

		let control_task = Task::spawn({
			let session = session.clone();
			let id = id.clone();
			let runner = runner.clone();
			move |_| async move {
				let mut control = control;
				while let Some(message) = control.recv().await? {
					match message {
						tg::sandbox::control::ClientMessage::Response(response) => {
							let subject = format!("sandboxes.{id}.control.client.{}", response.id);
							session
								.server
								.messenger
								.publish(
									subject,
									ClientMessage(tg::sandbox::control::ClientMessage::Response(
										response,
									)),
								)
								.await
								.map_err(|source| {
									tg::error!(
										!source,
										"failed to publish the sandbox client message"
									)
								})?;
						},
						tg::sandbox::control::ClientMessage::Ack(_) => unreachable!(),
						tg::sandbox::control::ClientMessage::Notification(notification) => {
							match notification {}
						},
						tg::sandbox::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							let result = match request.arg {
								tg::sandbox::control::ClientRequestArg::Destroy(request) => session
									.destroy_sandbox_control_request(
										&id,
										request,
										created_at,
										runner.clone(),
									)
									.await
									.map(tg::sandbox::control::ServerResponseOutput::Destroy),
							};
							let response =
								Self::sandbox_control_server_response(request_id, result);
							control_sender.send(response).await.map_err(|error| {
								tg::error!(!error, "failed to send the destroy sandbox response")
							})?;
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.attach(server_messages_task)
			.attach(control_task)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		if let Some(data) = data {
			let index_arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(
					tangram_index::sandbox::put::Arg {
						created_at,
						data: Some(data),
						id: id.clone(),
						runner,
						touched_at: created_at,
					},
				)],
			};
			self.server
				.index_batch(index_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the sandbox"))?;
		}

		session
			.server
			.messenger
			.publish(connected_subject(&id), ())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the sandbox control connection")
			})?;

		let output = tg::sandbox::control::Output { id, token };

		Ok((output, stream))
	}

	fn sandbox_control_server_response(
		id: String,
		result: tg::Result<tg::sandbox::control::ServerResponseOutput>,
	) -> tg::sandbox::control::ServerMessage {
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
		tg::sandbox::control::ServerMessage::Response(tg::sandbox::control::ServerResponse {
			error,
			id,
			output,
		})
	}

	async fn get_sandbox_control_stream_region(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		region: String,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		let id = arg.id.clone();
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, ?id, "failed to get the region client"),
		)?;
		let arg = tg::sandbox::control::Arg {
			location: Some(
				tg::Location::Local(tg::location::Local {
					region: Some(region.clone()),
				})
				.into(),
			),
			..arg
		};
		let (output, stream) = client
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?;
		let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
		Ok((output, stream))
	}

	async fn get_sandbox_control_stream_remote(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		let id = arg.id.clone();
		let session = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, ?id, "failed to get the remote client"),
		)?;
		let context = session.context().clone();
		context.set_token(self.context.token.clone());
		let session = session.client().session(&context);
		let arg = tg::sandbox::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let (output, stream) = session
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
		Ok((output, stream))
	}

	pub(crate) async fn get_sandbox_control_stream_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
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

		let arg = request
			.query_params::<tg::sandbox::control::Arg>()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

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

		let (output, stream) = self
			.get_sandbox_control_stream_with_context(arg, stream)
			.await?;

		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(stream);
		let body = tangram_http::body::output::set(body, &output)
			.map_err(|error| tg::error!(!error, "failed to serialize the output"))?;
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.header(tangram_http::body::output::HEADER, "true")
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub(crate) async fn send_sandbox_control_request(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: crate::control::Options,
	) -> tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>> {
		let id = crate::control::id();
		let request =
			tg::sandbox::control::ServerMessage::Request(tg::sandbox::control::ServerRequest {
				arg,
				id: id.clone(),
			});
		let request = ServerMessage(request);
		self.send_control_request(crate::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::sandbox::control::ServerMessage::Ack(
					tg::sandbox::control::ServerAck { id },
				))
			},
			client_subject: format!("sandboxes.{sandbox}.control.client.{id}"),
			marker: std::marker::PhantomData,
			request,
			options,
			response: |message: ClientMessage| {
				let tg::sandbox::control::ClientMessage::Response(message) = message.0 else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error)
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing sandbox control response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: format!("sandboxes.{sandbox}.control.server"),
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

impl crate::control::Output for tg::sandbox::control::ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::control::Input<tg::sandbox::control::ServerMessage>
	for tg::sandbox::control::ClientMessage
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

	fn create_ack_message(id: String) -> tg::sandbox::control::ServerMessage {
		tg::sandbox::control::ServerMessage::Ack(tg::sandbox::control::ServerAck { id })
	}
}

impl crate::control::Output for tg::sandbox::control::ServerMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Notification(_) => None,
			Self::Request(request) => Some(&request.id),
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl crate::control::Input<tg::sandbox::control::ClientMessage>
	for tg::sandbox::control::ServerMessage
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

	fn create_ack_message(id: String) -> tg::sandbox::control::ClientMessage {
		tg::sandbox::control::ClientMessage::Ack(tg::sandbox::control::ClientAck { id })
	}
}
