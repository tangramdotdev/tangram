use {
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext, task::Task},
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
		// Get the process status stream. Also checks for existence.
		let Some(stream) = self
			.try_get_process_status_stream_local(id, self.context.stopper.clone(), None)
			.await?
		else {
			return Ok(None);
		};

		// Wait until the process is started, which implies the control stream has been
		// created, or finished.
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			if matches!(
				event,
				tg::process::status::Event::Status(
					tg::process::Status::Started | tg::process::Status::Finished
				) | tg::process::status::Event::End
			) {
				break;
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
		let mut finished = false;
		loop {
			match future::select(stream.next(), retries.next()).await {
				future::Either::Left((message, _)) => match message {
					Some(Ok(message)) => {
						let Message::Response(tg::process::control::ResponseEvent::Response(response)) =
							message.payload
						else {
							return Err(tg::error!("expected a response"));
						};
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

					// If the process was finished on the previous tick and the response
					// has still not arrived, then the runner is gone, so give up.
					if finished {
						return Ok(None);
					}
					finished = self
						.get_process_status_local(id)
						.await
						.map_err(|error| tg::error!(!error, "failed to get the process status"))?
						.is_finished();

					self.server
						.messenger
						.publish(subject.clone(), payload.clone())
						.await
						.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
				},
			}
		}
	}

	pub(crate) async fn try_send_process_control_end(&self, id: &tg::process::Id) -> tg::Result<()> {
		let subject = format!("processes.{id}.control");
		let payload = Message::Request(tg::process::control::RequestEvent::End);
		self.server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the control message"))
	}

	pub(crate) async fn try_get_process_control_stream_with_context(
		&self,
		id: &tg::process::Id,
		mut stream: BoxStream<'static, tg::Result<tg::process::control::ResponseEvent>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::control::RequestEvent>>>> {
		let response_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			move |_| async move {
				while let Some(result) = stream.next().await {
					let event = match result {
						Ok(event) => event,
						Err(error) => {
							tracing::error!(%error, "failed to read the control response");
							continue;
						},
					};
					let end = matches!(event, tg::process::control::ResponseEvent::End);
					let subject = match &event {
						tg::process::control::ResponseEvent::Response(response) => {
							format!("processes.{id}.control.{}", response.id)
						},
						tg::process::control::ResponseEvent::End => {
							format!("processes.{id}.control")
						},
					};
					let payload = Message::Response(event);
					session
						.server
						.messenger
						.publish(subject, payload)
						.await
						.inspect_err(
							|error| tracing::error!(%error, "failed to publish the response"),
						)
						.ok();
					if end {
						break;
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
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
			.attach(response_task)
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
			.try_get_process_control_stream_with_context(&id, stream)
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
