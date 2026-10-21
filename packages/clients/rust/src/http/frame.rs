use {
	crate::prelude::*,
	bytes::Bytes,
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	tangram_futures::{read::Ext as _, stream::Ext as _, task::Task, write::Ext as _},
	tangram_http::body::{BodyStream, Boxed},
	tokio::io::{AsyncReadExt as _, AsyncWriteExt as _},
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::io::StreamReader,
};

#[cfg(test)]
mod tests;

pub(crate) fn encode<T>(stream: BoxStream<'static, tg::Result<T>>, max_frame_size: u64) -> Boxed
where
	T: Send + tangram_serialize::Serialize + 'static,
{
	let stream = stream.then(move |result| async move {
		let frame = match result {
			Ok(message) => {
				let message = tangram_serialize::to_vec(&message)
					.map_err(|error| tg::error!(!error, "failed to serialize the message"))?;
				let message_length = message.len();
				let length = u64::try_from(message_length).map_err(
					|error| tg::error!(!error, length = %message_length, "the frame length is out of range"),
				)?;
				if length > max_frame_size {
					return Err(tg::error!(
						length = %length,
						max = %max_frame_size,
						"the frame is too large"
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
				let json = error.state().object().map_or_else(
					|| serde_json::to_string(&error.id()).unwrap(),
					|object| serde_json::to_string(&object.to_data()).unwrap(),
				);
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				hyper::body::Frame::trailers(trailers)
			},
		};
		Ok::<_, tg::Error>(frame)
	});

	Boxed::with_stream(stream)
}

pub(crate) fn decode<T>(body: Boxed, max_frame_size: u64) -> BoxStream<'static, tg::Result<T>>
where
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let (reader, trailer_receiver, task) = split_body(body);

	decode_reader_with_trailers(reader, trailer_receiver, task, max_frame_size)
}

/// Decode a body's header and messages, returning `None` if the body ended before its header, which happens when the connection is lost.
pub(crate) async fn decode_with_header<T, H>(
	body: Boxed,
	max_header_length: u64,
	max_frame_size: u64,
) -> tg::Result<Option<(H, BoxStream<'static, tg::Result<T>>)>>
where
	H: serde::de::DeserializeOwned + for<'de> tangram_serialize::Deserialize<'de>,
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let (mut reader, trailer_receiver, task) = split_body(body);
	let result = tangram_http::body::header::get(
		&mut reader,
		max_header_length,
		tangram_http::body::encoding::Encoding::Tangram,
	)
	.await;
	let header = match result {
		Ok(header) => header,
		Err(error)
			if error
				.downcast_ref::<std::io::Error>()
				.is_some_and(|error| error.kind() == std::io::ErrorKind::UnexpectedEof) =>
		{
			return Ok(None);
		},
		Err(error) => return Err(tg::error!(!error, "failed to deserialize the header")),
	};
	let stream = decode_reader_with_trailers(reader, trailer_receiver, task, max_frame_size);

	Ok(Some((header, stream)))
}

pub(crate) fn decode_reader<T, R>(
	reader: R,
	max_frame_size: u64,
) -> BoxStream<'static, tg::Result<T>>
where
	R: tokio::io::AsyncRead + Send + Unpin + 'static,
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let stream = stream::try_unfold(reader, move |mut reader| async move {
		let length = match reader.try_read_uvarint().await {
			Ok(Some(length)) => length,
			Ok(None) => return Ok(None),
			Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
			Err(error) => {
				return Err(tg::error!(!error, "failed to read the stream frame length"));
			},
		};
		if length > max_frame_size {
			return Err(tg::error!(
				length = %length,
				max = %max_frame_size,
				"stream frame too large"
			));
		}
		let length = usize::try_from(length).map_err(
			|error| tg::error!(!error, length = %length, "stream frame length out of range"),
		)?;
		let mut bytes = vec![0; length];
		if let Err(error) = reader.read_exact(&mut bytes).await {
			if error.kind() == std::io::ErrorKind::UnexpectedEof {
				return Ok(None);
			}
			return Err(tg::error!(!error, "failed to read the stream message"));
		}
		let message = tangram_serialize::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the stream message"))?;

		Ok(Some((message, reader)))
	});

	stream.boxed()
}

fn decode_reader_with_trailers<T, R>(
	reader: R,
	trailer_receiver: tokio::sync::mpsc::Receiver<http::HeaderMap>,
	task: Task<()>,
	max_frame_size: u64,
) -> BoxStream<'static, tg::Result<T>>
where
	R: tokio::io::AsyncRead + Send + Unpin + 'static,
	T: for<'de> tangram_serialize::Deserialize<'de> + Send + 'static,
{
	let data_messages = decode_reader(reader, max_frame_size);
	let trailer_messages = ReceiverStream::new(trailer_receiver).then(|trailers| async move {
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
	let stream = data_messages.chain(trailer_messages).attach(task);

	stream.boxed()
}

fn split_body(
	body: Boxed,
) -> (
	impl tokio::io::AsyncRead + Send + Unpin + 'static,
	tokio::sync::mpsc::Receiver<http::HeaderMap>,
	Task<()>,
) {
	let mut stream = BodyStream::new(body);
	let (data_sender, data_receiver) = tokio::sync::mpsc::channel::<Bytes>(1);
	let (trailer_sender, trailer_receiver) = tokio::sync::mpsc::channel(1);
	let task = Task::spawn(|_| async move {
		while let Some(result) = stream.next().await {
			match result {
				Ok(frame) if frame.is_data() => {
					let data = frame.into_data().unwrap();
					if data_sender.send(data).await.is_err() {
						break;
					}
				},
				Ok(frame) if frame.is_trailers() => {
					let trailers = frame.into_trailers().unwrap();
					trailer_sender.send(trailers).await.ok();
				},
				Ok(_) => unreachable!(),
				Err(error) => {
					// Only the protocol end handshake completes the operation, so a lost transport ends this attempt and lets the caller reconnect.
					tracing::debug!(%error, "failed to read the response body");
					break;
				},
			}
		}
	});
	let reader = StreamReader::new(ReceiverStream::new(data_receiver).map(Ok::<_, std::io::Error>));

	(reader, trailer_receiver, task)
}
