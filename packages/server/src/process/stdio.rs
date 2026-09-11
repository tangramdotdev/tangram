use {
	crate::Server,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::{
		body::{BodyStream, Boxed as BoxBody},
		request::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn decode_tangram_preserves_error_trailers() {
		let stream =
			futures::stream::iter([Ok(7_u64), Err(tg::error!("the test stream failed"))]).boxed();
		let body = encode_tangram(stream, 1024);
		let request = http::Request::new(body);
		let mut stream = decode_tangram::<u64>(request, 1024);

		assert_eq!(stream.try_next().await.unwrap(), Some(7));
		assert!(stream.try_next().await.is_err());
	}
}

pub mod read;
pub mod write;

#[derive(Clone, Copy)]
pub(super) enum Encoding {
	Sse,
	Tangram,
}

impl Encoding {
	pub(super) fn from_accept(
		value: Option<&mime::Mime>,
		tangram_content_type: &str,
	) -> tg::Result<Self> {
		let Some(value) = value else {
			return Ok(Self::Tangram);
		};
		if value.type_() == mime::STAR && value.subtype() == mime::STAR {
			return Ok(Self::Tangram);
		}
		Self::from_content_type(value, tangram_content_type)
			.map_err(|_| tg::error!(accept = %value, "invalid accept type"))
	}

	pub(super) fn content_type(self, tangram_content_type: &str) -> mime::Mime {
		match self {
			Self::Sse => mime::TEXT_EVENT_STREAM,
			Self::Tangram => tangram_content_type.parse().unwrap(),
		}
	}

	pub(super) fn from_content_type(
		value: &mime::Mime,
		tangram_content_type: &str,
	) -> tg::Result<Self> {
		if value.type_() == mime::TEXT && value.subtype() == mime::EVENT_STREAM {
			return Ok(Self::Sse);
		}
		let tangram: mime::Mime = tangram_content_type.parse().unwrap();
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
	let mut body = BodyStream::new(request.into_body());
	let (data_sender, data_receiver) = tokio::sync::mpsc::channel::<tg::Result<Bytes>>(1);
	let (trailer_sender, trailer_receiver) = tokio::sync::mpsc::channel(1);
	let task = Task::spawn(|_| async move {
		while let Some(result) = body.next().await {
			match result {
				Ok(frame) if frame.is_data() => {
					let data = frame.into_data().unwrap();
					if data_sender.send(Ok(data)).await.is_err() {
						break;
					}
				},
				Ok(frame) if frame.is_trailers() => {
					let trailers = frame.into_trailers().unwrap();
					trailer_sender.send(trailers).await.ok();
				},
				Ok(_) => unreachable!(),
				Err(error) => {
					let error = tg::error!(!error, "failed to read the request body");
					data_sender.send(Err(error)).await.ok();
					break;
				},
			}
		}
	});
	let reader =
		StreamReader::new(ReceiverStream::new(data_receiver).map_err(std::io::Error::other));
	let messages = futures::stream::try_unfold(reader, move |mut reader| async move {
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
	let errors = ReceiverStream::new(trailer_receiver).then(|trailers| async move {
		let event = trailers
			.get("x-tg-event")
			.ok_or_else(|| tg::error!("missing event"))?
			.to_str()
			.map_err(|error| tg::error!(!error, "invalid event"))?;
		if event != "error" {
			return Err(tg::error!("invalid event"));
		}
		let data = trailers
			.get("x-tg-data")
			.ok_or_else(|| tg::error!("missing data"))?
			.to_str()
			.map_err(|error| tg::error!(!error, "invalid data"))?;
		let error = serde_json::from_str(data)
			.map_err(|error| tg::error!(!error, "failed to deserialize the header value"))?;

		Err(error)
	});
	let stream = messages.chain(errors).attach(task);

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
