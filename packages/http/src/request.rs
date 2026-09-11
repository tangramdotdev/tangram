use {
	crate::body::Ext as _,
	crate::{Error, body, sse},
	bytes::Bytes,
	futures::{Stream, StreamExt as _, TryStreamExt as _, future, stream},
	tokio::io::AsyncBufRead,
	tokio_util::io::StreamReader,
};

pub mod builder;

#[derive(Clone)]
struct QueryParams(serde_json::Value);

pub fn with_query_params<T, B>(
	mut request: http::Request<B>,
	arg: &T,
) -> Result<http::Request<body::arg::Body<B>>, Error>
where
	T: serde::Serialize,
	B: http_body::Body<Data = Bytes>,
{
	let query = serde_qs::Config::new()
		.use_form_encoding(true)
		.serialize_string(arg)?;
	if query.len() > body::arg::THRESHOLD {
		let (mut parts, body) = request.into_parts();
		parts
			.headers
			.insert(body::arg::HEADER, http::HeaderValue::from_static("true"));
		// A cache must not identify a request with an arg in its body by its URI alone.
		parts.headers.insert(
			http::header::CACHE_CONTROL,
			http::HeaderValue::from_static("no-store"),
		);
		parts.headers.remove(http::header::CONTENT_LENGTH);
		let body = body::arg::Body::with_arg(body, arg)?;
		return Ok(http::Request::from_parts(parts, body));
	}
	if !query.is_empty() {
		let mut uri = request.uri().clone().into_parts();
		uri.path_and_query = Some(format!("{}?{query}", request.uri().path()).parse()?);
		*request.uri_mut() = http::Uri::from_parts(uri)?;
	}
	let request = request.map(body::arg::Body::new);
	Ok(request)
}

pub async fn read_query_params(
	request: http::Request<body::Boxed>,
	max_len: u64,
) -> Result<http::Request<body::Boxed>, Error> {
	if !body::arg::get_header(request.headers())? {
		return Ok(request);
	}
	let (mut parts, body) = request.into_parts();
	let mut frames = body.into_stream();
	let (arg, chunk) = {
		let stream = frames.by_ref().map(|result| {
			let frame = result.map_err(std::io::Error::other)?;
			frame
				.into_data()
				.map_err(|_| std::io::Error::other("expected the request arg before the trailers"))
		});
		let mut reader = StreamReader::new(stream);
		let arg = body::arg::get(&mut reader, max_len).await?;
		let (_, chunk) = reader.into_inner_with_chunk();
		(arg, chunk)
	};
	parts.extensions.insert(QueryParams(arg));
	parts.headers.remove(body::arg::HEADER);
	parts.headers.remove(http::header::CONTENT_LENGTH);
	// Preserve the unused data and trailers after the arg.
	let stream = stream::iter(chunk.map(|chunk| Ok(http_body::Frame::data(chunk)))).chain(frames);
	let body = body::Boxed::with_stream(stream);
	let request = http::Request::from_parts(parts, body);
	Ok(request)
}

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
	fn query_params<T>(&self) -> Option<Result<T, Error>>
	where
		T: serde::de::DeserializeOwned,
	{
		if let Some(arg) = self.extensions().get::<QueryParams>() {
			return Some(serde_json::from_value(arg.0.clone()).map_err(Into::into));
		}
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
