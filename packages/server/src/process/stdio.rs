use {
	crate::Server,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, write::Ext as _},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
};

pub mod read;
pub mod write;

#[derive(Clone, Copy)]
pub(super) enum Encoding {
	Sse,
	Tangram,
}

impl Encoding {
	pub(super) fn from_accept(value: Option<&mime::Mime>) -> tg::Result<Self> {
		let Some(value) = value else {
			return Ok(Self::Tangram);
		};
		if value.type_() == mime::STAR && value.subtype() == mime::STAR {
			return Ok(Self::Tangram);
		}
		Self::try_from(value).map_err(|_| tg::error!(accept = %value, "invalid accept type"))
	}

	pub(super) fn content_type(self) -> &'static str {
		match self {
			Self::Sse => tg::process::stdio::SSE_CONTENT_TYPE,
			Self::Tangram => tg::process::stdio::TANGRAM_CONTENT_TYPE,
		}
	}
}

impl TryFrom<&mime::Mime> for Encoding {
	type Error = tg::Error;

	fn try_from(value: &mime::Mime) -> tg::Result<Self> {
		if value.type_() == mime::TEXT && value.subtype() == mime::EVENT_STREAM {
			return Ok(Self::Sse);
		}
		let tangram: mime::Mime = tg::process::stdio::TANGRAM_CONTENT_TYPE.parse().unwrap();
		if value.type_() == tangram.type_() && value.subtype() == tangram.subtype() {
			return Ok(Self::Tangram);
		}

		Err(tg::error!(content_type = %value, "invalid content type"))
	}
}

pub(super) fn decode<T>(
	request: http::Request<BoxBody>,
	encoding: Encoding,
	max_frame_size: u64,
) -> BoxStream<'static, tg::Result<T>>
where
	T: for<'de> tangram_serialize::Deserialize<'de>
		+ Send
		+ TryFrom<tangram_http::sse::Event, Error = tg::Error>
		+ 'static,
{
	match encoding {
		Encoding::Sse => request
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read a stdio message"))
			.and_then(|event| future::ready(event.try_into()))
			.boxed(),
		Encoding::Tangram => decode_tangram(request, max_frame_size),
	}
}

pub(super) fn encode<T>(
	stream: BoxStream<'static, tg::Result<T>>,
	encoding: Encoding,
	max_frame_size: u64,
) -> BoxBody
where
	T: Send + tangram_serialize::Serialize + 'static,
	tangram_http::sse::Event: TryFrom<T, Error = tg::Error>,
{
	match encoding {
		Encoding::Sse => {
			let stream = stream.map(|result| match result {
				Ok(message) => message.try_into(),
				Err(error) => error.try_into(),
			});

			BoxBody::with_sse_stream(stream)
		},
		Encoding::Tangram => encode_tangram(stream, max_frame_size),
	}
}

fn decode_tangram<T>(
	request: http::Request<BoxBody>,
	max_frame_size: u64,
) -> BoxStream<'static, tg::Result<T>>
where
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let reader = request.reader();
	let stream = futures::stream::try_unfold(reader, move |mut reader| async move {
		let Some(length) = reader
			.try_read_uvarint()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the stdio frame length"))?
		else {
			return Ok(None);
		};
		if length > max_frame_size {
			return Err(tg::error!(
				length = %length,
				max = %max_frame_size,
				"stdio frame too large"
			));
		}
		let length = usize::try_from(length).map_err(
			|error| tg::error!(!error, length = %length, "stdio frame length out of range"),
		)?;
		let mut bytes = vec![0; length];
		reader
			.read_exact(&mut bytes)
			.await
			.map_err(|error| tg::error!(!error, "failed to read the stdio message"))?;
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the stdio message"))?;

		Ok(Some((message, reader)))
	});

	stream.boxed()
}

fn encode_tangram<T>(stream: BoxStream<'static, tg::Result<T>>, max_frame_size: u64) -> BoxBody
where
	T: Send + tangram_serialize::Serialize + 'static,
{
	let stream = stream.then(move |result| async move {
		let frame = match result {
			Ok(message) => {
				let message = tangram_serialize::to_vec(&message)
					.map_err(|error| tg::error!(!error, "failed to serialize the stdio message"))?;
				let message_length = message.len();
				let length = u64::try_from(message_length).map_err(
					|error| tg::error!(!error, length = %message_length, "stdio frame length out of range"),
				)?;
				if length > max_frame_size {
					return Err(tg::error!(
						length = %length,
						max = %max_frame_size,
						"stdio frame too large"
					));
				}
				let mut bytes = Vec::with_capacity(9 + message.len());
				bytes.write_uvarint(length).await.unwrap();
				bytes.write_all(&message).await.unwrap();
				hyper::body::Frame::data(bytes.into())
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error.to_data_or_id()).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				hyper::body::Frame::trailers(trailers)
			},
		};

		Ok::<_, tg::Error>(frame)
	});

	BoxBody::with_stream(stream)
}

impl Server {
	pub(crate) fn spawn_publish_process_stdio_close_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) {
		self.spawn_publish_process_stdio_message_task(id, stream, "close");
	}

	fn spawn_publish_process_stdio_message_task(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		action: &str,
	) {
		let id = id.clone();
		let action = action.to_owned();
		let subject = format!("processes.{id}.{stream}.{action}");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(
							%error,
							%id,
							%stream,
							%action,
							"failed to publish the process stdio message"
						);
					})
					.ok();
			}
		});
	}
}
