use crate::{sse, Error, Incoming};
use bytes::Bytes;
use futures::{future, Future, Stream, TryStreamExt as _};
use http_body_util::{BodyExt as _, BodyStream};
use std::sync::Arc;
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

pub struct ResponseWrapper {
	arc: Option<Arc<dyn std::any::Any + Send + Sync>>,
	response: http::Response<Incoming>,
}

impl ResponseWrapper {
	#[must_use]
	pub fn new(response: http::Response<Incoming>) -> Self {
		Self {
			arc: None,
			response,
		}
	}

	pub fn add_ref(&mut self, arc: Arc<dyn std::any::Any + Send + Sync>) {
		self.arc.replace(arc);
	}

	#[must_use]
	pub fn status(&self) -> http::StatusCode {
		self.response.status()
	}
}

impl Ext for ResponseWrapper {
	fn parse_header<T, E>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: std::str::FromStr<Err = E>,
		E: std::error::Error + Send + Sync + 'static,
	{
		self.response.parse_header(key)
	}

	fn header_json<T>(&self, key: impl http::header::AsHeaderName) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.response.header_json(key)
	}

	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send {
		self.response.bytes()
	}

	fn text(self) -> impl Future<Output = Result<String, Error>> + Send {
		self.response.text()
	}

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.response.json()
	}

	fn optional_json<T>(self) -> impl Future<Output = Result<Option<T>, Error>> + Send
	where
		T: serde::de::DeserializeOwned,
	{
		self.response.optional_json()
	}

	fn reader(self) -> impl AsyncBufRead + Send + 'static {
		self.response.reader()
	}

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		self.response.sse()
	}
}

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

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;
}

impl Ext for http::Response<Incoming> {
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

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}
}
