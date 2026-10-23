use {
	crate::Component,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
	tangram_server_core::{
		Session,
		sandbox::control::{Discarded, connected_subject, discarded_subject},
	},
};

mod destroy;

pub(crate) mod service;

#[derive(Clone)]
pub(crate) struct ClientMessage(pub(crate) tg::sandbox::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub(crate) tg::sandbox::control::ServerMessage);

impl Component {
	pub async fn subscribe_sandbox_connection(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> tg::Result<super::ConnectionFuture> {
		let mut connected_stream = session
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
		let mut discarded_stream = session
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
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let location = session.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.get_sandbox_control_stream_region(session, arg, stream, region)
					.await?
			},
			tg::Location::Local(_) => {
				let output = self
					.get_sandbox_control_stream_local(session, arg, stream)
					.await?;
				ControlFlow::Break(output)
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.get_sandbox_control_stream_remote(session, arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn get_sandbox_control_stream_local(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		self.get_sandbox_control_stream_local_inner(session, arg, stream)
			.boxed()
			.await
	}

	async fn get_sandbox_control_stream_local_inner(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Header,
		BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	)> {
		// Authenticate the connection as its sandbox, or as a runner that proves that it derived the sandbox's ID, which receives the sandbox's token.
		let id = arg
			.id
			.ok_or_else(|| tg::error!("a sandbox control connection requires a sandbox id"))?;
		let token = match (&session.context.principal, &arg.proof) {
			(tg::Principal::Sandbox(sandbox), _) if sandbox == &id => session.context.token.clone(),
			(tg::Principal::Runner(runner), Some(proof)) => {
				if arg.runner.as_ref() != Some(runner) {
					return Err(tg::error!(
						unauthorized,
						%runner,
						"a sandbox control connection's runner must be its principal"
					));
				}
				let now = session.server.clock.unix_timestamp();
				let now = now.to_u64().unwrap_or_default() * 1000;
				let window = session.server.config.sandbox.id_proof_window;
				proof.verify_recent(&id, runner, now, window)?;
				let token = session
					.server
					.create_sandbox_authentication_token(id.clone())?;
				Some(token)
			},
			(tg::Principal::Sandbox(sandbox), _) => {
				return Err(tg::error!(sandbox = %sandbox, %id, "invalid sandbox"));
			},
			_ => return Err(tg::error!(unauthorized, "unauthorized")),
		};
		let context = tangram_server_core::context::Context {
			principal: tg::Principal::Sandbox(id.clone()),
			token: token.clone(),
			..session.context.clone()
		};
		let session = session.server.session(&context);
		let create = arg
			.create
			.ok_or_else(|| tg::error!("a sandbox control connection requires a create"))?;

		// Answer the requests for the sandbox for as long as the connection lasts, starting before initialization so that the requests that verify the runner do not wait for it.
		let service = self.spawn_sandbox_control_service_task(&session, &id, arg.runner.clone());
		if arg.proof.is_some() {
			tangram_server_core::checkpoint!(session.server, "sandbox.control.shortcut.received", sandbox = %id)
				.await;
		}

		session.server.spawn_publish_sandbox_status_task(&id);
		let created_at = if let Some(created_at) = arg.created_at {
			created_at
		} else {
			session.server.clock.unix_timestamp()
		};
		let runner = arg.runner;

		// Prepare and submit initialization before accepting subsequent requests.
		tangram_server_core::checkpoint!(session.server, "sandbox.control.connect", sandbox = %id)
			.await;
		let sandbox = self
			.prepare_sandbox_control_index_arg(
				&session,
				&id,
				created_at,
				create,
				arg.creator,
				runner.clone(),
			)
			.await?;
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(sandbox)],
		};
		session
			.server
			.index_batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index the sandbox"))?;
		tangram_server_core::checkpoint!(session.server, "sandbox.control.index.submitted", sandbox = %id).await;

		let server_messages = session
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
		let capacity = session.server.config.sandbox.control_channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let control = tangram_server_core::control::Stream::new(
			stream,
			sender,
			tangram_server_core::control::stream_options(&session.server.config.control),
		);
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
			let component = self.clone();
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
								component
									.publish_sandbox_control_ack(&session, &id, ack)
									.await?;
							}
						},
						tg::sandbox::control::ClientMessage::Notification(notification) => {
							match notification {}
						},
						tg::sandbox::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							control.acknowledge(request_id.clone()).await?;
							let result = match request.arg {
								tg::sandbox::control::ClientRequestArg::Destroy(request) => {
									component
										.destroy_sandbox_control_request(
											&session,
											&id,
											request,
											created_at,
											runner.clone(),
										)
										.await
										.map(tg::sandbox::control::ServerResponseOutput::Destroy)
								},
							};
							let response =
								Self::sandbox_control_server_response(request_id, result);
							control_sender.send(response).await.map_err(|error| {
								tg::error!(!error, "failed to send the destroy sandbox response")
							})?;
						},
						tg::sandbox::control::ClientMessage::Response(response) => {
							component
								.publish_sandbox_control_response(&session, &id, response)
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
			.attach(service)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		session
			.server
			.notifications
			.publish_sandbox_control_connected(&id)
			.await?;

		let header = tg::sandbox::control::Header { id, token };

		Ok((header, stream))
	}

	pub async fn prepare_sandbox_control_index_arg(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		created_at: i64,
		create: tg::sandbox::create::Arg,
		creator: Option<tg::Principal>,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tangram_index::sandbox::put::Arg> {
		let account = match create.owner.as_ref() {
			Some(owner) => session.usage_account(owner).await?,
			None => None,
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				cpu: create.cpu,
				creator,
				hostname: create.hostname,
				id: id.clone(),
				memory: create.memory,
				mounts: create.mounts,
				network: create.network,
				owner: create.owner,
				status: tg::sandbox::Status::Started,
				storage: create.storage,
				ttl: create.ttl,
				usage: None,
			},
			location: Some(location.clone()),
			tokens: tg::authorization::Tokens::default(),
		};
		let arg = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes: None,
			runner,
			touched_at: created_at,
		};
		Ok(arg)
	}

	async fn publish_sandbox_control_ack(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		ack: tg::sandbox::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Ack(ack));
		session
			.server
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
		session: &Session,
		id: &tg::sandbox::Id,
		response: tg::sandbox::control::ClientResponse,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Response(response));
		session
			.server
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
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		region: String,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let id = arg.id.clone();
		let client = session.get_region_session(&region).await.map_err(
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
		let output = client
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?
			.map_break(|(header, stream)| {
				let stream = stream.with_stopper(session.context.stopper.clone()).boxed();
				(header, stream)
			})
			.map_continue(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			);
		Ok(output)
	}

	async fn get_sandbox_control_stream_remote(
		&self,
		session: &Session,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
			),
			tg::Error,
		>,
	> {
		let id = arg.id.clone();
		let client = session.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, ?id, "failed to get the remote client"),
		)?;
		let context = client.context().clone();
		context.set_token(session.context.token.clone());
		let client = client.client().session(&context);
		let arg = tg::sandbox::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client
			.get_sandbox_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?
			.map_break(|(header, stream)| {
				let stream = stream.with_stopper(session.context.stopper.clone()).boxed();
				(header, stream)
			})
			.map_continue(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			);
		Ok(output)
	}

	pub async fn request_sandbox_control(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>> {
		self.send_sandbox_control_request(session, id, arg, options)
			.boxed()
			.await?
			.await
	}

	pub(crate) async fn send_sandbox_control_request(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::ServerRequestArg,
		options: tangram_server_core::control::Options,
	) -> tg::Result<
		futures::future::BoxFuture<
			'static,
			tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>,
		>,
	> {
		let kind = match &arg {
			tg::sandbox::control::ServerRequestArg::Destroy(_) => "destroy",
			tg::sandbox::control::ServerRequestArg::Get(_) => "get",
			tg::sandbox::control::ServerRequestArg::GetProcesses(_) => "get_processes",
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_) => "spawn_process",
		};
		tangram_server_core::checkpoint!(session.server, "sandbox.control.request", sandbox = %id, kind).await;
		let request_id = tangram_server_core::control::id();
		let request = ServerMessage(tg::sandbox::control::ServerMessage::Request(
			tg::sandbox::control::ServerRequest {
				arg,
				id: request_id.clone(),
			},
		));
		let arg = tangram_server_core::control::SendControlRequestArg {
			ack: |id| {
				ServerMessage(tg::sandbox::control::ServerMessage::Ack(
					tg::sandbox::control::ServerAck { id },
				))
			},
			client_subject: format!("sandboxes.{id}.control.client.{request_id}"),
			is_ack: |message: &ClientMessage| {
				matches!(&message.0, tg::sandbox::control::ClientMessage::Ack(_))
			},
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: ClientMessage| {
				let ClientMessage(tg::sandbox::control::ClientMessage::Response(message)) = message
				else {
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
			server_subject: format!("sandboxes.{id}.control.server"),
		};
		session.server.send_control_request(arg).await
	}

	pub(crate) async fn get_sandbox_control_stream_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept and content type headers.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| {
				tg::error!(argument, !error, "failed to parse the content type header")
			})?;
		let tangram_content_type = tg::sandbox::control::TANGRAM_CONTENT_TYPE;
		let output_encoding = tangram_server_core::http::frame::Encoding::from_accept(
			accept.as_ref(),
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = tangram_server_core::http::frame::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg::<tg::sandbox::control::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Decode the request stream.
		let max_frame_size = session.server.config.sync.max_frame_size;
		let stream =
			tangram_server_core::http::frame::decode(request, input_encoding, max_frame_size);

		// Get the response stream, responding that the server is unavailable if the stream could not reach a server.
		let output = self
			.get_sandbox_control_stream_with_context(session, arg, stream)
			.boxed()
			.await?;
		let (header, stream) = match output {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => {
				return Err(tg::error!(
					unavailable,
					!error,
					"failed to get the control stream"
				));
			},
		};

		// Create the body.
		let content_type = output_encoding.content_type(tangram_content_type);
		let body =
			tangram_server_core::http::frame::encode(stream, output_encoding, max_frame_size);
		let body = tangram_http::body::header::set(body, &header, output_encoding.serialization())
			.map_err(|error| tg::error!(!error, "failed to serialize the header"))?;

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
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
