use {
	crate::Session,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
};

mod destroy;

pub(crate) mod local;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::sandbox::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::sandbox::control::ServerMessage);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct Discarded {
	pub error: tg::Either<tg::error::Data, tg::error::Id>,
}

pub(crate) fn connected_subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.connected")
}

pub(crate) fn discarded_subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.discarded")
}

impl Session {
	pub(crate) async fn subscribe_sandbox_connection(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<super::ConnectionFuture> {
		let mut connected_stream = self
			.server
			.messenger
			.subscribe::<()>(connected_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					sandbox = %id,
					"failed to subscribe to the sandbox control connection"
				)
			})?;
		let mut discarded_stream = self
			.server
			.messenger
			.subscribe::<tangram_messenger::payload::Json<Discarded>>(discarded_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					sandbox = %id,
					"failed to subscribe to sandbox discard notifications"
				)
			})?;
		let id = id.clone();
		let future = async move {
			tokio::select! {
				result = connected_stream.try_next() => {
					result
						.map_err(|error| {
							tg::error!(
								!error,
								sandbox = %id,
								"failed to receive the sandbox control connection"
							)
						})?
						.ok_or_else(|| {
							tg::error!(
								sandbox = %id,
								"the sandbox control connection subscription ended"
							)
						})?;

					Ok(())
				},
				result = discarded_stream.try_next() => {
					let discarded = result
						.map_err(|error| {
							tg::error!(
								!error,
								sandbox = %id,
								"failed to receive a sandbox discard notification"
							)
						})?
						.ok_or_else(|| {
							tg::error!(
								sandbox = %id,
								"the sandbox discard notification subscription ended"
							)
						})?
						.payload
						.0;
					let error = tg::Error::try_from(discarded.error).map_err(|source| {
						tg::error!(
							!source,
							sandbox = %id,
							"failed to deserialize the sandbox discard error"
						)
					})?;

					Err(tg::error!(!error, sandbox = %id, "failed to create the sandbox"))
				},
			}
		}
		.boxed();

		Ok(future)
	}

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
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		self.get_sandbox_control_stream_local_inner(arg, stream, None)
			.boxed()
			.await
	}

	async fn get_sandbox_control_stream_local_inner(
		&self,
		mut arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		create_request: Option<String>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		let assign = arg.id.is_none();
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
		if !arg.create {
			if assign && !matches!(self.context.principal, tg::Principal::Runner(_)) {
				return Err(tg::error!(
					"a deferred sandbox control connection requires a runner"
				));
			}
			if arg.created_at.is_some() || arg.data.is_some() {
				return Err(tg::error!(
					"a deferred sandbox control connection must not have data or a creation time"
				));
			}
			let output = tg::sandbox::control::Output {
				id: id.clone(),
				token,
			};
			let stream =
				session.wait_for_sandbox_control_create(id, arg.location, arg.runner, stream);
			crate::checkpoint!(self.server, "sandbox.control.output", sandbox = %output.id).await;

			return Ok((output, stream));
		}
		self.server.spawn_publish_sandbox_status_task(&id);
		let created_at = if let Some(created_at) = arg.created_at {
			created_at
		} else {
			self.server.clock.unix_timestamp()?
		};
		let runner = arg.runner;

		// Prepare and submit initialization before accepting subsequent requests.
		crate::checkpoint!(self.server, "sandbox.control.connect", sandbox = %id).await;
		if let Some(data) = arg.data {
			let sandbox = session
				.prepare_sandbox_control_index_arg(&id, created_at, data, runner.clone())
				.await?;
			let arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(sandbox)],
			};
			self.server
				.index_batch(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the sandbox"))?;
		}
		crate::checkpoint!(self.server, "sandbox.control.index.submitted", sandbox = %id).await;

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

		let forwarded_requests = Arc::new(DashSet::new());
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let mut control =
			crate::control::Stream::new(stream, sender, crate::control::stream_options());
		if let Some(id) = &create_request {
			control.acknowledge_now(id.clone());
		}
		let control_sender = control.sender();
		let mut server_messages = server_messages;
		let server_message_sender = control_sender.clone();
		let server_messages_task = Task::spawn({
			let forwarded_requests = forwarded_requests.clone();
			move |_| async move {
				while let Some(message) = server_messages.try_next().await.map_err(|source| {
					tg::error!(!source, "failed to get a sandbox server message")
				})? {
					let message = message.payload.0;
					let acknowledged_request = match &message {
						tg::sandbox::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
						_ => None,
					};
					if let tg::sandbox::control::ServerMessage::Request(request) = &message {
						forwarded_requests.insert(request.id.clone());
					}
					server_message_sender.send(message).await?;
					if let Some(id) = acknowledged_request {
						forwarded_requests.remove(&id);
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let control_task = Task::spawn({
			let session = session.clone();
			let id = id.clone();
			let forwarded_requests = forwarded_requests.clone();
			let runner = runner.clone();

			move |_| async move {
				let mut control = control;
				while let Some(message) = control.recv_without_ack().await? {
					match message {
						tg::sandbox::control::ClientMessage::Ack(ack) => {
							if forwarded_requests.contains(&ack.id) {
								session.publish_sandbox_control_ack(&id, ack).await?;
							}
						},
						tg::sandbox::control::ClientMessage::Notification(notification) => {
							match notification {}
						},
						tg::sandbox::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							control.acknowledge(request_id.clone()).await?;
							let result = match request.arg {
								tg::sandbox::control::ClientRequestArg::Create(_) => {
									let output =
										tg::sandbox::control::CreateServerResponseOutput {};
									Ok(tg::sandbox::control::ServerResponseOutput::Create(output))
								},
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
						tg::sandbox::control::ClientMessage::Response(response) => {
							session
								.publish_sandbox_control_response(&id, response)
								.await?;
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

	fn wait_for_sandbox_control_create(
		&self,
		id: tg::sandbox::Id,
		location: Option<tg::location::Arg>,
		runner: Option<tg::runner::Id>,
		mut stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>> {
		let session = self.clone();
		futures::stream::once(async move {
			let mut buffered = Vec::new();
			let request = loop {
				let message = stream
					.try_next()
					.await?
					.ok_or_else(|| tg::error!("the sandbox control stream ended before create"))?;
				if let tg::sandbox::control::ClientMessage::Request(request) = &message
					&& matches!(
						&request.arg,
						tg::sandbox::control::ClientRequestArg::Create(_)
					) {
					let tg::sandbox::control::ClientMessage::Request(request) = message else {
						unreachable!();
					};
					break request;
				}
				buffered.push(Ok(message));
			};
			let tg::sandbox::control::ClientRequestArg::Create(create) = request.arg else {
				unreachable!();
			};
			crate::checkpoint!(session.server, "sandbox.control.create.received", sandbox = %id)
				.await;
			let tg::sandbox::control::CreateClientRequestArg { created_at, data } = create;
			let arg = tg::sandbox::control::Arg {
				create: true,
				created_at: Some(created_at),
				data: Some(data),
				id: Some(id),
				location,
				runner,
			};
			let stream = futures::stream::iter(buffered).chain(stream).boxed();
			let output = session
				.get_sandbox_control_stream_local_inner(arg, stream, Some(request.id.clone()))
				.boxed()
				.await;
			let ack = tg::sandbox::control::ServerMessage::Ack(tg::sandbox::control::ServerAck {
				id: request.id.clone(),
			});
			let (response, stream) = match output {
				Ok((_, stream)) => {
					let output = tg::sandbox::control::CreateServerResponseOutput {};
					let output = tg::sandbox::control::ServerResponseOutput::Create(output);
					let response = Self::sandbox_control_server_response(request.id, Ok(output));
					(response, stream)
				},
				Err(error) => {
					let response = Self::sandbox_control_server_response(request.id, Err(error));
					let stream = futures::stream::empty().boxed();
					(response, stream)
				},
			};
			let prefix = futures::stream::iter([Ok(ack), Ok(response)]);
			let stream = prefix.chain(stream).boxed();

			Ok::<_, tg::Error>(stream)
		})
		.try_flatten()
		.boxed()
	}

	pub(crate) async fn prepare_sandbox_control_index_arg(
		&self,
		id: &tg::sandbox::Id,
		created_at: i64,
		data: tg::sandbox::control::Data,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tangram_index::sandbox::put::Arg> {
		let account = match data.arg.owner.as_ref() {
			Some(owner) => self.usage_account(owner).await?,
			None => None,
		};
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				cpu: data.arg.cpu,
				creator: data.creator,
				hostname: data.arg.hostname,
				id: id.clone(),
				isolation: data.arg.isolation,
				memory: data.arg.memory,
				mounts: data.arg.mounts,
				network: data.arg.network,
				owner: data.arg.owner,
				status: tg::sandbox::Status::Started,
				ttl: data.arg.ttl,
				usage: None,
			},
			location: Some(location.clone()),
			tokens: tg::Tokens::default(),
		};
		let arg = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			runner,
			touched_at: created_at,
		};
		Ok(arg)
	}

	async fn publish_sandbox_control_ack(
		&self,
		id: &tg::sandbox::Id,
		ack: tg::sandbox::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Ack(ack));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the sandbox control acknowledgement"
				)
			})?;

		Ok(())
	}

	async fn publish_sandbox_control_response(
		&self,
		id: &tg::sandbox::Id,
		response: tg::sandbox::control::ClientResponse,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Response(response));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the sandbox client message")
			})?;

		Ok(())
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

		let (arg, request) = request
			.arg::<tg::sandbox::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

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
		let kind = match &arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) => "destroy",
			tg::sandbox::control::ServerRequestArg::Get(_) => "get",
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => "spawn_process",
		};
		crate::checkpoint!(self.server, "sandbox.control.request", sandbox = %sandbox, kind).await;
		let id = crate::control::id();
		let request =
			tg::sandbox::control::ServerMessage::Request(tg::sandbox::control::ServerRequest {
				arg,
				id: id.clone(),
			});
		let request = ServerMessage(request);
		self.server
			.send_control_request(crate::control::SendControlRequestArg {
				ack: |id| {
					ServerMessage(tg::sandbox::control::ServerMessage::Ack(
						tg::sandbox::control::ServerAck { id },
					))
				},
				client_subject: format!("sandboxes.{sandbox}.control.client.{id}"),
				is_ack: |message: &ClientMessage| {
					matches!(&message.0, tg::sandbox::control::ClientMessage::Ack(_))
				},
				marker: std::marker::PhantomData,
				options,
				request,
				response: |message: ClientMessage| {
					let tg::sandbox::control::ClientMessage::Response(message) = message.0 else {
						return Ok(None);
					};
					if let Some(error) = message.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the error")
						})?;
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
	fn is_request(&self) -> bool {
		matches!(self, Self::Request(_))
	}

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
			Self::Response(response) => crate::control::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::sandbox::control::ServerMessage {
		tg::sandbox::control::ServerMessage::Ack(tg::sandbox::control::ServerAck { id })
	}
}

impl crate::control::Output for tg::sandbox::control::ServerMessage {
	fn is_request(&self) -> bool {
		matches!(self, Self::Request(_))
	}

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
			Self::Response(response) => crate::control::InputKind::Response { id: &response.id },
		}
	}

	fn create_ack_message(id: String) -> tg::sandbox::control::ClientMessage {
		tg::sandbox::control::ClientMessage::Ack(tg::sandbox::control::ClientAck { id })
	}
}
