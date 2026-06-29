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
pub(crate) struct SandboxControlClientMessage(pub(crate) tg::sandbox::control::ClientMessage);

#[derive(Clone)]
pub(crate) struct SandboxControlServerMessage(pub(crate) tg::sandbox::control::ServerMessage);

impl Session {
	pub(crate) async fn get_sandbox_control_stream_with_context(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>> {
		let location = self.server.location(arg.location.as_ref())?;
		match location {
			tg::Location::Local(tg::location::Local { region: None }) => (),
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				let client = self.get_region_session(&region).await.map_err(
					|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
				)?;
				let arg = tg::sandbox::control::Arg {
					location: Some(
						tg::Location::Local(tg::location::Local {
							region: Some(region.clone()),
						})
						.into(),
					),
				};
				let stream = client
					.get_sandbox_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, region = %region, "failed to get the control stream"),
					)?;
				return Ok(stream.with_stopper(self.context.stopper.clone()).boxed());
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				let client = self.get_remote_session(&name).await.map_err(
					|error| tg::error!(!error, remote = %name, %id, "failed to get the remote client"),
				)?;
				let arg = tg::sandbox::control::Arg {
					location: Some(tg::Location::Local(tg::location::Local { region }).into()),
				};
				let stream = client
					.get_sandbox_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %name, "failed to get the control stream"),
					)?;
				return Ok(stream.with_stopper(self.context.stopper.clone()).boxed());
			},
		}

		// Subscribe to the server message stream before registering the sandbox with the scheduler so that no process dispatch is missed.
		let server_messages = self
			.server
			.messenger
			.subscribe::<SandboxControlServerMessage>(format!("sandboxes.{id}.server"))
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to subscribe to the sandbox server message stream"
				)
			})?;

		// Spawn a task to forward client messages from the sandbox.
		let mut task = Task::spawn({
			let server = self.server.clone();
			let id = id.clone();
			async move |_| {
				{
					let mut stream = pin!(stream);
					while let Some(message) = stream.try_next().await? {
						let subject = match message.clone() {
							tg::sandbox::control::ClientMessage::Response(response) => {
								format!("sandboxes.{id}.client.{}", response.id())
							},
							tg::sandbox::control::ClientMessage::Ack(_) => {
								format!("sandboxes.{id}.client")
							},
							tg::sandbox::control::ClientMessage::Notification(notification) => {
								match notification {}
							},
							tg::sandbox::control::ClientMessage::Request(request) => {
								match request {}
							},
						};
						server
							.messenger
							.publish(subject, SandboxControlClientMessage(message))
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the sandbox client message")
							})?;
					}
					Ok::<_, tg::Error>(())
				}
				.inspect_err(|error| tracing::error!(%error, "the sandbox control task failed"))
			}
		});
		task.detach();

		let stream = server_messages
			.map_ok(|message| message.payload.0)
			.map_err(|source| tg::error!(!source, "failed to get a sandbox server message"))
			.with_stopper(self.context.stopper.clone())
			.boxed();
		Ok(stream)
	}

	pub(crate) async fn get_sandbox_control_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
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

		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;

		match &self.context.principal {
			tg::Principal::Sandbox(sandbox) if sandbox == &id => (),
			tg::Principal::Sandbox(sandbox) => {
				return Err(tg::error!(
					sandbox = %sandbox,
					id = %id,
					"invalid sandbox"
				));
			},
			tg::Principal::Root if self.server.config().authentication.is_none() => (),
			_ => return Err(tg::error!("unauthorized")),
		}

		let arg = request
			.query_params()
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

		let stream = self
			.get_sandbox_control_stream_with_context(&id, arg, stream)
			.await?;

		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(stream);
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}

impl tangram_messenger::Payload for SandboxControlClientMessage {
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

impl tangram_messenger::Payload for SandboxControlServerMessage {
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
