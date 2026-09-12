use {
	crate::body::Ext as _,
	crate::{Error, body, sse},
	bytes::Bytes,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream},
	tokio::io::AsyncBufRead,
	tokio_util::io::StreamReader,
};

pub mod builder;

pub trait Ext: Sized {
	/// Read the arg from the query or a framed body and return the remaining request.
	fn arg<T>(
		self,
	) -> impl Future<Output = Result<(Option<T>, http::Request<body::Boxed>), Error>> + Send
	where
		T: serde::de::DeserializeOwned;

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

	fn token(&self, name: Option<&str>) -> Option<&str>;

	fn bytes(self) -> impl Future<Output = Result<Bytes, Error>> + Send;

	fn text(self) -> impl Future<Output = Result<String, Error>> + Send;

	fn json<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned;

	fn json_or_default<T>(self) -> impl Future<Output = Result<T, Error>> + Send
	where
		T: serde::de::DeserializeOwned + Default;

	fn reader(self) -> impl AsyncBufRead + Send + 'static;

	fn sse(self) -> impl Stream<Item = Result<sse::Event, Error>> + Send + 'static;

	fn boxed_body(self) -> http::Request<body::Boxed>;
}

impl<B> Ext for http::Request<B>
where
	B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
	B::Error: Into<Error> + Send,
{
	async fn arg<T>(self) -> Result<(Option<T>, http::Request<body::Boxed>), Error>
	where
		T: serde::de::DeserializeOwned,
	{
		if !body::arg::get_header(self.headers())? {
			let arg = self.query_params().transpose()?;
			return Ok((arg, self.boxed_body()));
		}

		// Read the arg.
		let (mut parts, body) = self.into_parts();
		let mut frames = body::BodyStream::new(body);
		let (arg, chunk) = {
			let stream = frames.by_ref().map(|result| {
				let frame = result.map_err(|error| std::io::Error::other(error.into()))?;
				frame.into_data().map_err(|_| {
					std::io::Error::other("expected the request arg before the trailers")
				})
			});
			let mut reader = StreamReader::new(stream);
			let arg = body::arg::get(&mut reader, body::arg::MAX_LENGTH).await?;
			let (_, chunk) = reader.into_inner_with_chunk();
			(arg, chunk)
		};

		// Preserve the unused data and trailers after the arg.
		parts.headers.remove(body::arg::HEADER);
		parts.headers.remove(http::header::CONTENT_LENGTH);
		let stream = stream::iter(chunk.map(|chunk| Ok(http_body::Frame::data(chunk))))
			.chain(frames.map(|result| result.map_err(Into::into)));
		let body = body::Boxed::with_stream(stream);
		let request = http::Request::from_parts(parts, body);

		Ok((Some(arg), request))
	}

	fn query_params<T>(&self) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		self.uri().query().map(|query| {
			serde_qs::Config::new()
				.use_form_encoding(true)
				.deserialize_str(query)
				.map_err(Into::into)
		})
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

	async fn json_or_default<T>(self) -> Result<T, Error>
	where
		T: serde::de::DeserializeOwned + Default,
	{
		let bytes = self.bytes().await?;
		if bytes.is_empty() {
			return Ok(T::default());
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

	fn boxed_body(self) -> http::Request<body::Boxed> {
		self.map(body::Boxed::new)
	}
}
