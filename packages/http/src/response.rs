use crate::{Body, Error, sse};
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _, future};
use http::HeaderMap;
use http_body_util::{BodyExt as _, BodyStream};
use tokio::io::AsyncBufRead;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::io::StreamReader;

pub mod builder;

pub type Response = http::Response<Body>;

pub trait Ext: Sized {
	fn parse_header<T, E>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: std::str::FromStr<Err = E>,
		E: std::error::Error + Send + Sync + 'static;

	fn header_json<T>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned;

	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn text(self) -> impl Future<Output = Result<String, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn optional_json<T>(self) -> impl Future<Output = Result<Option<T>, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn reader_and_trailers(
		self,
	) -> (
		impl AsyncBufRead + Send + 'static,
		impl Stream<Item = Result<HeaderMap, Error>> + Send + 'static,
	);

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl Ext for http::Response<Body> {
	fn parse_header<T, E>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: std::str::FromStr<Err = E>,
		E: std::error::Error + Send + Sync + 'static,
	{
		self.headers().get(key).map(|value| {
			let value = value.to_str()?;
			let value = value.parse()?;
			Ok(value)
		})
	}

	fn header_json<T>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.headers().get(key).map(|value| {
			let value = value.to_str()?;
			let value = serde_json::from_str(value)?;
			Ok(value)
		})
	}

	async fn bytes(self) -> Result<Bytes, Error> {
		let collected = self.collect().await?;
		Ok(collected.to_bytes())
	}

	async fn text(self) -> Result<String, Error> {
		let bytes = self.bytes().await?;
		let text = String::from_utf8(bytes.to_vec())?;
		Ok(text)
	}

	async fn json<T>(self) -> Result<T, Error>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
		let json = serde_json::from_slice(&bytes)?;
		Ok(json)
	}

	async fn optional_json<T>(self) -> Result<Option<T>, Error>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
		if bytes.is_empty() {
			return Ok(None);
		}
		let json = serde_json::from_slice(&bytes)?;
		Ok(json)
	}

	fn reader(self) -> impl AsyncBufRead + Send + 'static {
		StreamReader::new(
			BodyStream::new(self.into_body())
				.try_filter_map(|frame| future::ok(frame.into_data().ok()))
				.map_err(std::io::Error::other),
		)
	}

	fn reader_and_trailers(
		self,
	) -> (
		impl AsyncBufRead + Send + 'static,
		impl Stream<Item = Result<HeaderMap, Error>> + Send + 'static,
	) {
		let (send, recv) = tokio::sync::mpsc::unbounded_channel();
		let stream = BodyStream::new(self.into_body())
			.try_filter_map(move |item| {
				future::ok({
					match item.into_data() {
						Ok(data) => Some(data),
						Err(frame) => {
							if let Ok(trailers) = frame.into_trailers() {
								send.send(Ok(trailers)).ok();
							}
							None
						},
					}
				})
			})
			.map_err(std::io::Error::other);
		let reader = StreamReader::new(stream);
		let trailers = UnboundedReceiverStream::new(recv);
		(reader, trailers)
	}

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}
}
