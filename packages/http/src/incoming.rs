use crate::{sse, Error};
use bytes::Bytes;
use futures::{future, Future, Stream, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

pub type Incoming = hyper::body::Incoming;

pub trait RequestExt {
	/// Get a bearer token or cookie with the specified name from an HTTP request.
	fn token(&self, name: Option<&str>) -> Option<&str>;

	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl RequestExt for http::Request<Incoming> {
	fn token(&self, name: Option<&str>) -> Option<&str> {
		let bearer = self
			.headers()
			.get(http::header::AUTHORIZATION)
			.and_then(|authorization| authorization.to_str().ok())
			.and_then(|authorization| authorization.split_once(' '))
			.filter(|(name, _)| *name == "Bearer")
			.map(|(_, value)| value);
		let cookie = name.and_then(|name| {
			self.headers()
				.get(http::header::COOKIE)
				.and_then(|cookies| cookies.to_str().ok())
				.and_then(|cookies| {
					cookies
						.split("; ")
						.filter_map(|cookie| {
							let mut components = cookie.split('=');
							let key = components.next()?;
							let value = components.next()?;
							Some((key, value))
						})
						.find(|(key, _)| *key == name)
						.map(|(_, token)| token)
				})
		});
		bearer.or(cookie)
	}

	async fn bytes(self) -> Result<Bytes, Error> {
		Ok(self.into_body().collect().await?.to_bytes())
	}

	async fn json<T>(self) -> Result<T, Error>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
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

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}
}

pub trait ResponseExt: Sized {
	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl ResponseExt for http::Response<Incoming> {
	async fn bytes(self) -> Result<Bytes, Error> {
		let collected = self.collect().await?;
		Ok(collected.to_bytes())
	}

	async fn json<T>(self) -> Result<T, Error>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
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

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}
}
