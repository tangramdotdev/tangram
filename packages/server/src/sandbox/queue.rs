use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
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
	pub id: tg::sandbox::Id,

	#[tangram_serialize(id = 1)]
	pub process: Option<tg::process::Id>,
}

impl Server {
	pub(crate) async fn try_dequeue_sandbox_with_context(
		&self,
		context: &Context,
		_arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let stream = self
			.messenger
			.get_stream("sandboxes.queue".into())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?;
		let consumer_config = messenger::ConsumerConfig {
			deliver_policy: messenger::DeliverPolicy::All,
			ack_policy: messenger::AckPolicy::Explicit,
			durable_name: None,
			filter_subjects: Vec::new(),
		};
		let consumer = stream
			.create_consumer(None, consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the consumer"))?;
		let messages = consumer.subscribe::<Message>().await.map_err(|source| {
			tg::error!(!source, "failed to subscribe to the sandbox queue stream")
		})?;
		let mut messages = pin!(messages);
		while let Some(message) = messages
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to dequeue a sandbox"))?
		{
			let (payload, acker) = message.split();
			let started = self
				.try_start_sandbox_local(&payload.id)
				.await
				.map_err(|source| tg::error!(!source, "failed to start the sandbox"))?;
			let process = if started {
				if let Some(process) = payload.process.clone() {
					let started = self
						.try_start_process_local(&process)
						.await
						.map_err(|source| tg::error!(!source, "failed to start the process"))?;
					if !started {
						return Err(tg::error!("failed to start the process"));
					}
					Some(process)
				} else {
					None
				}
			} else {
				None
			};
			acker
				.ack()
				.await
				.map_err(|source| tg::error!(!source, "failed to ack the message"))?;
			if !started {
				continue;
			}
			return Ok(Some(tg::sandbox::queue::Output {
				sandbox: payload.id,
				process,
			}));
		}
		Ok(None)
	}

	pub(crate) async fn handle_dequeue_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let handle = self.clone();
		let context = context.clone();
		let future = async move { handle.try_dequeue_sandbox_with_context(&context, arg).await };
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));
		let stopper = async move { stopper.wait().await };
		let stream = stream.take_until(stopper);
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
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
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
		match bytes[0] {
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
