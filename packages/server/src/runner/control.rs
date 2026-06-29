use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
pub(crate) struct RunnerControlClientMessage(pub(crate) tg::runner::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct RunnerControlServerMessage(pub(crate) tg::runner::control::ServerMessage);

impl Session {
	pub(crate) async fn get_runner_control_stream_with_context(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>> {
		let location = self.server.location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local { region: None }) => (),
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				let client = self.get_region_session(&region).await.map_err(
					|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
				)?;
				let location = tg::Location::Local(tg::location::Local {
					region: Some(region.clone()),
				});
				let arg = tg::runner::control::Arg {
					host: arg.host,
					location: Some(location.into()),
				};
				let stream = client
					.get_runner_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, region = %region, "failed to get the control stream"),
					)?;
				return Ok(stream.with_stopper(self.context.stopper.clone()));
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				let client = self.get_remote_session(&name).await.map_err(
					|error| tg::error!(!error, remote = %name, %id, "failed to get the remote client"),
				)?;
				let arg = tg::runner::control::Arg {
					host: arg.host,
					location: Some(tg::Location::Local(tg::location::Local { region }).into()),
				};
				let stream = client
					.get_runner_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %name, "failed to get the control stream"),
					)?;
				return Ok(stream.with_stopper(self.context.stopper.clone()));
			},
		}

		// Subscribe to the server message stream before notifying the scheduler so that no message dispatched
		// to this runner is missed in the window before the subscription is established.
		let server_messages = self
			.server
			.messenger
			.subscribe::<RunnerControlServerMessage>(format!("runners.{id}.server"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the runner server message stream"
				)
			})?;

		// Subscribe to the ack subject before notifying the scheduler so that the ack is not missed.
		let subject = "scheduler.register";
		let ack = self
			.server
			.messenger
			.subscribe::<crate::scheduler::Message>(format!("{subject}.{id}"))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the ack stream"))?;
		let mut ack = pin!(ack);

		// Notify the scheduler that this runner is registered, republishing with backoff until an ack is received.
		let options = tangram_futures::retry::Options::default();
		let mut retries = pin!(tangram_futures::retry::stream(options));
		loop {
			match future::select(ack.next(), retries.next()).await {
				future::Either::Left((message, _)) => {
					let Some(message) = message else {
						return Err(tg::error!("the ack stream ended"));
					};
					let message = message.map_err(|source| {
						tg::error!(!source, "failed to receive the scheduler ack")
					})?;
					let crate::scheduler::Message::Ack(_) = message.payload else {
						return Err(tg::error!("expected an ack"));
					};
					break;
				},
				future::Either::Right((tick, _)) => {
					if tick.is_none() {
						return Err(tg::error!("failed to receive an ack from a scheduler"));
					}
					self.server
						.messenger
						.publish(
							"scheduler".to_owned(),
							crate::scheduler::Message::Notification(
								crate::scheduler::Notification::Register(
									crate::scheduler::RegisterNotification {
										arg: arg.clone(),
										id: id.clone(),
									},
								),
							),
						)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to publish the register message")
						})?;
				},
			}
		}

		// Spawn a task to forward client messages to the scheduler.
		let mut task = Task::spawn({
			let server = self.server.clone();
			let id = id.clone();
			async move |_| {
				{
					let mut stream = pin!(stream);
					while let Some(message) = stream.try_next().await? {
						let subject = match message.clone() {
							tg::runner::control::ClientMessage::Response(response) => {
								format!("runners.{id}.client.{}", response.id())
							},
							tg::runner::control::ClientMessage::Ack(_)
							| tg::runner::control::ClientMessage::Notification(_) => {
								format!("runners.{id}.client")
							},
							tg::runner::control::ClientMessage::Request(request) => {
								match request {}
							},
						};
						server
							.messenger
							.publish(subject, RunnerControlClientMessage(message))
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the runner client message")
							})?;
					}
					Ok::<_, tg::Error>(())
				}
				.inspect_err(|error| tracing::error!(%error, "the runner control task failed"))
			}
		});
		task.detach();

		// End the stream when the server stops, without an end message, so the runner retries while the server restarts.
		let stream = server_messages
			.map_ok(|message| message.payload.0)
			.map_err(|source| tg::error!(!source, "failed to get a runner server message"))
			.with_stopper(self.context.stopper.clone());
		Ok(stream)
	}

	pub(crate) async fn get_runner_control_stream_request(
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
			.parse::<tg::runner::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the runner id"))?;

		match &self.context.principal {
			tg::Principal::Runner(runner) if runner == &id => (),
			tg::Principal::Runner(runner) => {
				return Err(tg::error!(
					runner = %runner,
					id = %id,
					"invalid runner"
				));
			},
			tg::Principal::Root if self.server.config().authentication.is_none() => (),
			_ => return Err(tg::error!("unauthorized")),
		}

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

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
			.boxed();

		// Get the server message stream.
		let stream = self
			.get_runner_control_stream_with_context(&id, arg, stream)
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
}

impl tangram_messenger::Payload for RunnerControlClientMessage {
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

impl tangram_messenger::Payload for RunnerControlServerMessage {
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
