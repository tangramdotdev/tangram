use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::Messenger,
};

#[derive(
	Debug,
	Clone,
	tangram_serialize::Serialize,
	tangram_serialize::Deserialize,
	derive_more::TryUnwrap,
)]
pub enum Message {
	#[tangram_serialize(id = 1)]
	Request(tg::process::control::ServerMessage),
	#[tangram_serialize(id = 2)]
	Response(tg::process::control::ClientMessage),
}

impl Session {
	pub(crate) async fn try_send_process_control_request(
		&self,
		id: &tg::process::Id,
		mut request: tg::process::control::ServerRequest,
		max_retries: u64,
	) -> tg::Result<Option<tg::process::control::ClientResponse>> {
		// Get the process status stream. Also checks for existence.
		let Some(status) = self
			.try_get_process_status_stream_local(id, self.context.stopper.clone(), None)
			.await?
		else {
			return Ok(None);
		};

		// Wait until the process is started. If the process is finished, then set the deadline after which the request is abandoned, because the runner serves requests for only a bounded time after the process finishes.
		let grace = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(Duration::from_secs(10), |runner| runner.control_timeout);
		let mut deadline = None;
		let mut status = pin!(status);
		loop {
			let Some(event) = status.try_next().await? else {
				deadline.replace(tokio::time::Instant::now() + grace);
				break;
			};
			match event {
				tg::process::status::Event::Status(tg::process::Status::Created) => (),
				tg::process::status::Event::Status(tg::process::Status::Started) => break,
				_ => {
					deadline.replace(tokio::time::Instant::now() + grace);
					break;
				},
			}
		}

		// Create the request.
		request.set_id(tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes()));
		let subject = format!("processes.{id}.control.{}", request.id());
		let responses = self
			.server
			.messenger
			.subscribe::<Message>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the response"))?;
		let mut responses = pin!(responses);

		// Send the request and wait for the response.
		let subject = format!("processes.{id}.control");
		let payload = Message::Request(tg::process::control::ServerMessage::Request(request));
		let options = tangram_futures::retry::Options {
			max_retries,
			..Default::default()
		};
		let mut retries = pin!(tangram_futures::retry::stream(options));
		loop {
			tokio::select! {
				message = responses.next() => match message {
					Some(Ok(message)) => {
						let Message::Response(tg::process::control::ClientMessage::Response(
							response,
						)) = message.payload
						else {
							return Err(tg::error!("expected a response"));
						};

						// Send the ack over the control stream so that the runner can release the cached response.
						let subject = format!("processes.{id}.control");
						let payload = Message::Request(tg::process::control::ServerMessage::Ack(
							tg::process::control::ServerAck {
								id: response.id().to_owned(),
							},
						));
						self.server
							.messenger
							.publish(subject, payload)
							.await
							.inspect_err(
								|error| tracing::error!(%error, "failed to ack the response"),
							)
							.ok();

						// If the response is an error, then return it.
						if let tg::process::control::ClientResponse::Error(response) = &response {
							return Err(response.error.clone().try_into()?);
						}

						return Ok(Some(response));
					},
					Some(Err(source)) => {
						return Err(tg::error!(!source, "failed to receive the response"));
					},
					None => {
						return Err(tg::error!("the response stream ended"));
					},
				},
				tick = retries.next() => {
					if tick.is_none() {
						if deadline.is_some() {
							return Ok(None);
						}
						return Err(tg::error!("timed out waiting for the response"));
					}
					if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
						return Ok(None);
					}
					self.server
						.messenger
						.publish(subject.clone(), payload.clone())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
				},
				event = status.try_next(), if deadline.is_none() => {
					let finished = match event? {
						Some(tg::process::status::Event::Status(status)) => status.is_finished(),
						Some(_) => false,
						None => true,
					};
					if finished {
						deadline.replace(tokio::time::Instant::now() + grace);
					}
				},
			}
		}
	}

	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		mut stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>> {
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
				let arg = tg::process::control::Arg {
					location: Some(location.into()),
				};
				let stream = client
					.try_get_process_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, region = %region, "failed to get the control stream"),
					)?;
				let stream =
					stream.map(|stream| stream.with_stopper(self.context.stopper.clone()).boxed());
				return Ok(stream);
			},
			tg::Location::Remote(tg::location::Remote { name, region }) => {
				let client = self.get_remote_session(&name).await.map_err(
					|error| tg::error!(!error, remote = %name, %id, "failed to get the remote client"),
				)?;
				let arg = tg::process::control::Arg {
					location: Some(tg::Location::Local(tg::location::Local { region }).into()),
				};
				let stream = client
					.try_get_process_control_stream(id, arg, stream)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %name, "failed to get the control stream"),
					)?;
				let stream =
					stream.map(|stream| stream.with_stopper(self.context.stopper.clone()).boxed());
				return Ok(stream);
			},
		}

		// Spawn the response task. It is detached so that it drains and publishes the remaining responses when the request stream is dropped. It completes when the response stream ends. Deduplication and acknowledgement are handled by the runner's control task, so this task only forwards responses to the messenger.
		let mut response_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			move |_| async move {
				while let Some(result) = stream.next().await {
					let message = match result {
						Ok(message) => message,
						Err(error) => {
							tracing::error!(%error, "failed to read the control response");
							continue;
						},
					};
					match message {
						tg::process::control::ClientMessage::Ack(_) => {},
						tg::process::control::ClientMessage::Response(response) => {
							let subject = format!("processes.{id}.control.{}", response.id());
							let payload = Message::Response(
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
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		response_task.detach();

		let subject = format!("processes.{id}.control");
		let requests = self
			.server
			.messenger
			.subscribe::<Message>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the message stream"))?
			.map_err(|source| tg::error!(!source, "failed to get the message"))
			.and_then(|message| {
				let payload = message
					.payload
					.try_unwrap_request()
					.map_err(|source| tg::error!(!source, "expected a request"));
				future::ready(payload)
			})
			.with_stopper(self.context.stopper.clone())
			.boxed();

		Ok(Some(requests))
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
			.query_params()
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
}

impl tangram_messenger::Payload for Message {
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
