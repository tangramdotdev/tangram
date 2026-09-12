use {
	crate::{Error, body},
	bytes::Bytes,
	futures::Stream,
	tokio::io::AsyncRead,
};

pub trait Ext: Sized {
	/// Put the arg in the query, or prepend it to the body when the query is too large.
	fn arg<T, B>(
		self,
		arg: &T,
		body: B,
	) -> Result<http::Result<http::Request<body::arg::Body<B>>>, Error>
	where
		T: serde::Serialize,
		B: http_body::Body<Data = Bytes>;

	fn empty(self) -> http::Result<http::Request<body::Empty>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<body::Bytes>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> Result<http::Result<http::Request<body::Bytes>>, Error>
	where
		T: serde::Serialize;

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<Bytes>> + 'static,
		E: Into<Error> + 'static;

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Request<body::Boxed>>
	where
		R: AsyncRead + Send + 'static;

	fn sse<S, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static;
}

impl Ext for http::request::Builder {
	fn arg<T, B>(
		mut self,
		arg: &T,
		body: B,
	) -> Result<http::Result<http::Request<body::arg::Body<B>>>, Error>
	where
		T: serde::Serialize,
		B: http_body::Body<Data = Bytes>,
	{
		// Serialize the arg.
		let query = serde_qs::Config::new()
			.use_form_encoding(true)
			.serialize_string(arg)?;
		let arg_in_body = query.len() > body::arg::THRESHOLD;

		// Set the query.
		let uri = self.uri_ref().cloned().unwrap_or_default();
		let path_and_query = if arg_in_body || query.is_empty() {
			uri.path().parse()?
		} else {
			format!("{}?{query}", uri.path()).parse()?
		};
		let mut parts = uri.into_parts();
		parts.path_and_query = Some(path_and_query);
		self = self.uri(http::Uri::from_parts(parts)?);

		// Create the body.
		let body = if arg_in_body {
			if let Some(headers) = self.headers_mut() {
				headers.insert(body::arg::HEADER, http::HeaderValue::from_static("true"));
				// A cache must not identify a request with an arg in its body by its URI alone.
				headers.insert(
					http::header::CACHE_CONTROL,
					http::HeaderValue::from_static("no-store"),
				);
				headers.remove(http::header::CONTENT_LENGTH);
			}
			body::arg::Body::with_arg(body, arg)?
		} else {
			if let Some(headers) = self.headers_mut() {
				headers.remove(body::arg::HEADER);
			}
			body::arg::Body::new(body)
		};
		let request = self.body(body);

		Ok(request)
	}

	fn empty(self) -> http::Result<http::Request<body::Empty>> {
		self.header(
			http::header::CONTENT_LENGTH,
			http::HeaderValue::from_static("0"),
		)
		.body(body::Empty::new())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Request<body::Bytes>>
	where
		T: Into<Bytes>,
	{
		let value = value.into();
		self.header(
			http::header::CONTENT_LENGTH,
			http::HeaderValue::from_str(&value.len().to_string()).unwrap(),
		)
		.body(body::Bytes::new(value))
	}

	fn json<T>(self, value: T) -> Result<http::Result<http::Request<body::Bytes>>, Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_string(&value)?;
		Ok(self.body(body::Bytes::new(value)))
	}

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<Bytes>> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(body::Boxed::with_stream(value))
	}

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(body::Boxed::with_data_stream(value))
	}

	fn reader<R>(self, value: R) -> http::Result<http::Request<body::Boxed>>
	where
		R: AsyncRead + Send + 'static,
	{
		self.body(body::Boxed::with_reader(value))
	}

	fn sse<S, E>(self, value: S) -> http::Result<http::Request<body::Boxed>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		self.body(body::Boxed::with_sse_stream(value))
	}
}
