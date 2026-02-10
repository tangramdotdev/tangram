use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	indoc::formatdoc,
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Stop,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::{self as messenger, prelude::*},
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Message {
	#[tangram_serialize(id = 0)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 1)]
	pub parent: Option<tg::process::Id>,
}

impl Server {
	pub(crate) async fn try_dequeue_process_with_context(
		&self,
		context: &Context,
		_arg: tg::process::queue::Arg,
	) -> tg::Result<Option<tg::process::queue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Subscribe to the consumer stream.
		let stream = self
			.messenger
			.get_stream("queue".into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?;
		let consumer = stream
			.get_consumer("queue".into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the consumer"))?;
		let enqueued = consumer.subscribe::<Message>().await.map_err(|source| {
			tg::error!(!source, "failed to subscribe to the process queue stream")
		})?;

		let mut enqueued = pin!(enqueued);

		// Attempt to dequeue a process from the stream.
		while let Some(message) = enqueued
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to dequeue a process"))?
		{
			let (payload, acker) = message.split();

			// Get a database connection.
			let connection = self
				.database
				.write_connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

			// Update the process's status.
			let p = connection.p();
			let statement = formatdoc!(
				"
					update processes
					set
						status = 'dequeued',
						dequeued_at = {p}2
					where
						id = {p}1 and status = 'enqueued';
				"
			);
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let params = db::params![payload.process.to_string(), now];
			let result = connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Ack the message unconditionally to avoid redelivery.
			acker
				.ack()
				.await
				.map_err(|source| tg::error!(!source, "failed to ack the message"))?;

			// Only return the process if we successfully changed the status of the process.
			if result == 0 {
				continue;
			}

			// Publish a message that the status changed.
			let subject = format!("processes.{}.status", payload.process);
			self.messenger.publish(subject, ()).await.ok();

			// Return the dequeued process.
			let output = tg::process::queue::Output {
				process: payload.process,
			};

			return Ok(Some(output));
		}
		Ok(None)
	}

	pub(crate) async fn handle_dequeue_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the arg.
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let handle = self.clone();
		let context = context.clone();
		let future = async move { handle.try_dequeue_process_with_context(&context, arg).await };
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));

		// Stop the stream when the server stops.
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}

impl Message {
	pub fn serialize(&self) -> tg::Result<Bytes> {
		let mut bytes = Vec::new();
		bytes.push(0);
		tangram_serialize::to_writer(&mut bytes, self)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		Ok(bytes.into())
	}

	pub fn deserialize<'a>(bytes: impl Into<tg::bytes::Cow<'a>>) -> tg::Result<Self> {
		let bytes = bytes.into();
		let bytes = bytes.as_ref();
		if bytes.is_empty() {
			return Err(tg::error!("missing format byte"));
		}
		let format = bytes[0];
		match format {
			0 => tangram_serialize::from_slice(&bytes[1..])
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			b'{' => serde_json::from_slice(bytes)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message")),
			_ => Err(tg::error!("invalid format")),
		}
	}
}

impl messenger::Payload for Message {
	fn serialize(&self) -> Result<Bytes, messenger::Error> {
		Message::serialize(self).map_err(messenger::Error::other)
	}

	fn deserialize(bytes: Bytes) -> Result<Self, messenger::Error> {
		Message::deserialize(bytes).map_err(messenger::Error::other)
	}
}
