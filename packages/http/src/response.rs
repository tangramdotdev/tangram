use {
	crate::body::Ext as _,
	crate::{Error, body, sse},
	bytes::Bytes,
	futures::{Stream, TryStreamExt as _, future},
	tokio::io::AsyncBufRead,
	tokio_util::io::StreamReader,
};

pub mod builder;

pub type Response = http::Response<body::Boxed>;

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

	fn boxed_body(self) -> http::Response<body::Boxed>;
}

impl<B> Ext for http::Response<B>
where
	B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
	B::Error: Into<Error> + Send,
{
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
		Ok(self
			.into_body()
			.collect()
			.await
			.map_err(Into::into)?
			.to_bytes())
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
			body::BodyStream::new(self.into_body())
				.try_filter_map(|frame| future::ok(frame.into_data().ok()))
				.map_err(std::io::Error::other),
		)
	}

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static {
		sse::decode(self.reader()).err_into()
	}

	fn boxed_body(self) -> http::Response<body::Boxed> {
		self.map(body::Boxed::new)
	}
}
