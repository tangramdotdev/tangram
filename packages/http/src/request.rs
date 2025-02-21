use crate::{Body, Error, sse};
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _, future};
use http_body_util::{BodyExt as _, BodyStream};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

pub mod builder;

pub type Request = http::Request<Body>;

pub trait Ext {
	fn query_params<T>(&self) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned;

	fn parse_header<T, E>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: std::str::FromStr<Err = E>,
		E: std::error::Error + Send + Sync + 'static;

	fn header_json<T>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned;

	/// Get a bearer token or cookie with the specified name from an HTTP request.
	fn token(&self, name: Option<&str>) -> Option<&str>;

	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn text(self) -> impl Future<Output = Result<String, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl Ext for http::Request<Body> {
	fn query_params<T>(&self) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.uri()
			.query()
			.map(|query| serde_urlencoded::from_str(query).map_err(Into::into))
	}

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
