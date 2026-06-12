use {
	crate::Session,
	dashmap::DashMap,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{pin::pin, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::Messenger,
};

const ACK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(
	Debug,
	Clone,
	tangram_serialize::Serialize,
	tangram_serialize::Deserialize,
	derive_more::TryUnwrap,
)]
pub enum Message {
	#[tangram_serialize(id = 1)]
	Request(tg::process::control::RequestEvent),
	#[tangram_serialize(id = 2)]
	Response(tg::process::control::ResponseEvent),
}

impl Session {
	pub(crate) async fn try_send_process_control_request(
		&self,
		id: &tg::process::Id,
		request: tg::process::control::RequestKind,
		max_retries: u64,
	) -> tg::Result<Option<tg::process::control::Response>> {
		eprintln!("control request {id} {request:?}");
		// Get the process status stream. Also checks for existence.
		let Some(stream) = self
			.try_get_process_status_stream_local(id, self.context.stopper.clone(), None)
			.await?
		else {
			return Ok(None);
		};

		// Wait until the process is started.
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::process::status::Event::Status(tg::process::Status::Created) => continue,
				_ => break,
			}
		}

		// Create the request.
		let request = tg::process::control::Request {
			id: uuid::Uuid::now_v7(),
			kind: request,
		};
		let subject = format!("processes.{id}.control.{}", request.id);
		let stream = self
			.server
			.messenger
			.subscribe::<Message>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the response"))?;
		let mut stream = pin!(stream);

		// Send the request and wait for the response.
		let subject = format!("processes.{id}.control");
		let payload = Message::Request(tg::process::control::RequestEvent::Request(request));
		let options = tangram_futures::retry::Options {
			max_retries,
			..Default::default()
		};
		let mut retries = pin!(tangram_futures::retry::stream(options));
		loop {
			match future::select(stream.next(), retries.next()).await {
				future::Either::Left((message, _)) => match message {
					Some(Ok(message)) => {
						let Message::Response(tg::process::control::ResponseEvent::Response(
							response,
						)) = message.payload
						else {
							return Err(tg::error!("expected a response"));
						};

						// Send the ack.
						let subject = format!("processes.{id}.control.{}.ack", response.id);
						self.server
							.messenger
							.publish(subject, ())
							.await
							.inspect_err(
								|error| tracing::error!(%error, "failed to ack the response"),
							)
							.ok();

						eprintln!("control response {}", response.id);

						// If the response is an error, then return it.
						if let tg::process::control::ResponseKind::Error(error) = &response.kind {
							return Err(error.clone().try_into()?);
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
				future::Either::Right((tick, _)) => {
					if tick.is_none() {
						return Err(tg::error!("timed out waiting for the response"));
					}
					self.server
						.messenger
						.publish(subject.clone(), payload.clone())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
				},
			}
		}
	}

	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		mut stream: BoxStream<'static, tg::Result<tg::process::control::ResponseEvent>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::RequestEvent>>>> {
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
				return Ok(stream.map(futures::StreamExt::boxed));
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
				return Ok(stream.map(futures::StreamExt::boxed));
			},
		}
		// Create the cache of responses that have been sent but not yet acked.
		let responses = Arc::new(DashMap::new());

		// Spawn the response task. It is detached so that it drains and publishes the remaining responses when the request stream is dropped. It completes when the response stream ends.
		let mut response_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let responses = responses.clone();
			move |_| async move {
				while let Some(result) = stream.next().await {
					let event = match result {
						Ok(event) => event,
						Err(error) => {
							tracing::error!(%error, "failed to read the control response");
							continue;
						},
					};
					match event {
						tg::process::control::ResponseEvent::Response(response) => {
							// Cache the response, publish it, and spawn a task to remove it from the cache when it is acked or the timeout elapses.
							tokio::spawn({
								let session = session.clone();
								let id = id.clone();
								let responses = responses.clone();
								async move {
									responses.insert(response.id, Some(response.clone()));
									let subject =
										format!("processes.{id}.control.{}.ack", response.id);
									let ack = session
										.server
										.messenger
										.subscribe::<()>(subject)
										.await
										.inspect_err(|error| {
											tracing::error!(%error, "failed to subscribe to the ack");
										})
										.ok();
									let subject = format!("processes.{id}.control.{}", response.id);
									let payload = Message::Response(
										tg::process::control::ResponseEvent::Response(
											response.clone(),
										),
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
									if let Some(ack) = ack {
										let mut ack = pin!(ack);
										tokio::time::timeout(ACK_TIMEOUT, ack.next()).await.ok();
									}
									responses.remove(&response.id);
								}
							});
						},
						tg::process::control::ResponseEvent::End => {
							let subject = format!("processes.{id}.control");
							let payload =
								Message::Response(tg::process::control::ResponseEvent::End);
							session
								.server
								.messenger
								.publish(subject, payload)
								.await
								.inspect_err(|error| {
									tracing::error!(%error, "failed to publish the response");
								})
								.ok();
							break;
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
			.map_err(|source| tg::error!(!source, "failed to get the event stream"))?
			.map_err(|source| tg::error!(!source, "failed to get the message"))
			.and_then(|message| {
				let payload = message
					.payload
					.try_unwrap_request()
					.map_err(|source| tg::error!(!source, "expected a request"));
				future::ready(payload)
			})
			.try_filter_map({
				let session = self.clone();
				let id = id.clone();
				move |event| {
					let session = session.clone();
					let id = id.clone();
					let responses = responses.clone();
					async move {
						if let tg::process::control::RequestEvent::Request(request) = &event {
							// Check for an existing entry for the request id before forwarding the request, so that a retried request is not executed more than once.
							let response = match responses.entry(request.id) {
								dashmap::Entry::Vacant(entry) => {
									entry.insert(None);
									return Ok(Some(event));
								},
								dashmap::Entry::Occupied(entry) => entry.get().clone(),
							};

							// If the request was already responded to, then republish the cached response. Otherwise, drop the duplicate, and the response will be published when it is ready.
							if let Some(response) = response {
								let subject = format!("processes.{id}.control.{}", response.id);
								let payload = Message::Response(
									tg::process::control::ResponseEvent::Response(response),
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
							}
							return Ok(None);
						}
						Ok(Some(event))
					}
				}
			})
			.boxed();

		// Mark the process started now that the subscription exists. This is a no-op if
		// the process was already started, which is allowed due to reconnection.
		self.server
			.try_start_process_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the process"))?;

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
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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
