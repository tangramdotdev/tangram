use {
	crate::Session,
	dashmap::DashSet,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
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
	pub(crate) async fn subscribe_process_connection(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<super::ConnectionFuture> {
		let mut event_stream = self
			.server
			.messenger
			.subscribe::<Connected>(connected_subject(id))
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					process = %id,
					"failed to subscribe to the process control connection"
				)
			})?;
		let id = id.clone();
		let future = async move {
			let event = event_stream
				.try_next()
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						process = %id,
						"failed to receive the process control connection"
					)
				})?
				.ok_or_else(|| {
					tg::error!(
						process = %id,
						"the process control connection subscription ended"
					)
				})?;

			Ok(event.payload)
		}
		.boxed();

		Ok(future)
	}

	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Output,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_get_process_control_stream_local(arg, stream)
					.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_get_process_control_stream_region(arg, stream, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				self.try_get_process_control_stream_remote(arg, stream, name, region)
					.await?
			},
		};
		Ok(output)
	}

	async fn try_get_process_control_stream_local(
		&self,
		mut arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::control::Output,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let assign = arg.id.is_none();
		let (id, token) = if let Some(id) = arg.id.take() {
			match &self.context.principal {
				tg::Principal::Process(process) if process == &id => (),
				tg::Principal::Process(process) => {
					return Err(tg::error!(process = %process, %id, "invalid process"));
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
			let id = tg::process::Id::new();
			let token = self
				.server
				.create_process_authentication_token(id.clone())?;
			(id, Some(token))
		};
		let context = crate::Context {
			principal: tg::Principal::Process(id.clone()),
			token: token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let data = arg.data;
		let lease = arg.lease;
		let mut options = arg.options;
		options.tokens.clear();
		let parent = arg.parent;
		if assign && data.is_none() {
			return Err(tg::error!("a process on the shortcut path must have data"));
		}
		if assign && parent.is_none() {
			return Err(tg::error!(
				"a process on the shortcut path must have a parent"
			));
		}
		let forwarded_requests = Arc::new(DashSet::new());
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
		let request_task = Task::spawn({
			let forwarded_requests = forwarded_requests.clone();
			move |_| async move {
				while let Some(message) = requests
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the message"))?
				{
					let message = message.payload.0;
					let acknowledged_request = match &message {
						tg::process::control::ServerMessage::Ack(ack) => Some(ack.id.clone()),
						_ => None,
					};
					if let tg::process::control::ServerMessage::Request(request) = &message {
						forwarded_requests.insert(request.id.clone());
					}
					request_sender.send(message).await?;
					if let Some(id) = acknowledged_request {
						forwarded_requests.remove(&id);
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let response_task = Task::spawn({
			let session = session.clone();
			let id = id.clone();
			let forwarded_requests = forwarded_requests.clone();
			move |_| async move {
				while let Some(message) = control.recv_without_ack().await? {
					match message {
						tg::process::control::ClientMessage::Ack(ack) => {
							if forwarded_requests.contains(&ack.id) {
								session.publish_process_control_ack(&id, ack).await?;
							}
						},
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
							let runner = notification.runner;
							let scheduler = notification.scheduler;
							let notification = crate::scheduler::Message::Notification(
								crate::scheduler::Notification::BorrowableCapacity(
									crate::scheduler::BorrowableCapacityNotification {
										capacity: notification.capacity,
										parent: notification.parent,
										runner,
									},
								),
							);
							session
								.server
								.messenger
								.publish(
									crate::scheduler::scheduler_server_subject(Some(&scheduler)),
									notification,
								)
								.await
								.map_err(|error| {
									tg::error!(
										!error,
										"failed to publish the borrowable capacity notification"
									)
								})?;
						},
						tg::process::control::ClientMessage::Request(request) => {
							let request_id = request.id;
							control.acknowledge(request_id.clone()).await?;
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
						tg::process::control::ClientMessage::Response(response) => {
							session
								.publish_process_control_response(&id, response)
								.await?;
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});

		let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
			.attach(request_task)
			.attach(response_task)
			.map(Ok)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		if let Some(data) = data {
			let command = data.command.clone().map(tg::object::Id::from);
			let data = data.without_location_and_tokens();
			let account = session
				.usage_account(&tg::Principal::Sandbox(data.sandbox.clone()))
				.await?;
			let touched_at = self.server.clock.unix_timestamp()?;
			let mut items = vec![tangram_index::batch::Item::PutProcess(
				tangram_index::process::put::Arg {
					cached: false,
					children: None,
					command: data.command.node.clone().into(),
					data: Some(data.clone()),
					error: None,
					id: id.clone(),
					log: None,
					metadata: tg::process::Metadata::default(),
					options,
					output: None,
					parent: parent.clone(),
					sandbox: Some(data.sandbox.clone()),
					storage: tangram_index::process::Storage::default(),
					time_to_touch: session.server.config.process.time_to_touch,
					touched_at,
				},
			)];
			if let Some(account) = account {
				items.push(tangram_index::batch::Item::PutAccountProcess(
					tangram_index::usage::storage::put::ProcessArg {
						account,
						process: id.clone(),
						touched_at,
					},
				));
			}
			if assign {
				let parent = parent.as_ref().ok_or_else(|| {
					tg::error!("a process on the shortcut path must have a parent")
				})?;
				let context = crate::Context {
					principal: tg::Principal::Process(parent.clone()),
					token: None,
					..self.context.clone()
				};
				let parent_session = self.server.session(&context);
				let time_to_live = i64::try_from(
					self.server.config.object.grant_time_to_live.as_secs(),
				)
				.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
				let expires_at = touched_at + time_to_live;
				let grant_arg = parent_session
					.create_process_object_grant_arg(&id, [command], touched_at, Some(expires_at))
					.await?;
				items.push(tangram_index::batch::Item::PutProcessObjectGrants(
					grant_arg,
				));
			}
			let index_arg = tangram_index::batch::Arg { items };
			session
				.server
				.index_batch(index_arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the process"))?;
		}

		session
			.server
			.messenger
			.publish(connected_subject(&id), Connected { lease })
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to publish the process control connection")
			})?;

		let grant = if assign {
			let now = session.server.clock.unix_timestamp()?;
			session.create_process_wait_token(&id, now)?
		} else {
			None
		};
		let output = tg::process::control::Output { grant, id, token };

		Ok(Some((output, stream)))
	}

	async fn publish_process_control_ack(
		&self,
		id: &tg::process::Id,
		ack: tg::process::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Ack(ack));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the process control acknowledgement"
				)
			})?;

		Ok(())
	}

	async fn publish_process_control_response(
		&self,
		id: &tg::process::Id,
		response: tg::process::control::ClientResponse,
	) -> tg::Result<()> {
		let kind = match response.output.as_ref() {
			Some(tg::process::control::ClientResponseOutput::AcquireLease(_)) => "acquire_lease",
			Some(tg::process::control::ClientResponseOutput::Finish(_)) => "finish",
			Some(tg::process::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::process::control::ClientResponseOutput::GetChildren(_)) => "get_children",
			Some(tg::process::control::ClientResponseOutput::Read(_)) => "read",
			Some(tg::process::control::ClientResponseOutput::ReleaseLease(_)) => "release_lease",
			Some(tg::process::control::ClientResponseOutput::Signal(_)) => "signal",
			Some(tg::process::control::ClientResponseOutput::Tty(_)) => "tty",
			Some(tg::process::control::ClientResponseOutput::Write(_)) => "write",
			None => "error",
		};
		let request = response.id.clone();
		crate::checkpoint!(
			self.server,
			"process.control.response.publish",
			kind = %kind,
			process = %id,
			request = request.clone(),
		)
		.await;
		let subject = format!("processes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Response(response));
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the process control response")
			})?;
		crate::checkpoint!(
			self.server,
			"process.control.response.published",
			kind = %kind,
			process = %id,
			request,
		)
		.await;

		Ok(())
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
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		region: String,
	) -> tg::Result<
		Option<(
			tg::process::control::Output,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let id = arg.id.clone();
		let client = self.get_region_session_for_process(&region).await.map_err(
			|error| tg::error!(!error, region = %region, ?id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::control::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client
			.try_get_process_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the control stream"),
			)?;
		let output = output.map(|(output, stream)| {
			let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
			(output, stream)
		});
		Ok(output)
	}

	async fn try_get_process_control_stream_remote(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		Option<(
			tg::process::control::Output,
			BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>,
		)>,
	> {
		let id = arg.id.clone();
		let session = self.get_remote_session_for_process(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, ?id, "failed to get the remote client"),
		)?;
		let context = session.context().clone();
		context.set_token(self.context.token.clone());
		let session = session.client().session(&context);
		let destination = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let mut arg = arg;
		arg.options.tokens = arg.options.tokens.for_location(&destination);
		if let Some(data) = &mut arg.data {
			data.command.options.tokens = data.command.options.tokens.for_location(&destination);
		}
		let arg = tg::process::control::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = session
			.try_get_process_control_stream(arg, stream)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the control stream"),
			)?;
		let output = output.map(|(output, stream)| {
			let stream = stream.with_stopper(self.context.stopper.clone()).boxed();
			(output, stream)
		});
		Ok(output)
	}

	pub(crate) async fn try_get_process_control_stream_request(
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
		let Some((output, stream)) = self
			.try_get_process_control_stream_with_context(arg, stream)
			.boxed()
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
		let body = tangram_http::body::output::set(body, &output)
			.map_err(|error| tg::error!(!error, "failed to serialize the output"))?;

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.header(tangram_http::body::output::HEADER, "true")
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
			is_ack: |message: &ClientMessage| {
				matches!(&message.0, tg::process::control::ClientMessage::Ack(_))
			},
			marker: std::marker::PhantomData,
			options,
			request: payload,
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
