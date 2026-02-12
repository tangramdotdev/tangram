use {
	crate::{Error, body},
	bytes::Bytes,
	futures::Stream,
	tokio::io::AsyncRead,
};

pub trait Ext: Sized {
	#[must_use]
	fn ok(self) -> Self;

	#[must_use]
	fn bad_request(self) -> Self;

	#[must_use]
	fn not_found(self) -> Self;

	fn header_json<K, V>(self, key: K, value: V) -> Result<Self, Error>
	where
		http::HeaderName: TryFrom<K, Error: Into<http::Error>>,
		V: serde::Serialize;

	fn empty(self) -> http::Result<http::Response<body::Empty>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Response<body::Bytes>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> Result<http::Result<http::Response<body::Bytes>>, Error>
	where
		T: serde::Serialize;

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Response<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Response<body::Boxed>>
	where
		R: AsyncRead + Send + 'static;

	fn sse<S, E>(self, value: S) -> http::Result<http::Response<body::Boxed>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static;
}

impl Ext for http::response::Builder {
	fn ok(self) -> Self {
		self.status(http::StatusCode::OK)
	}

	fn bad_request(self) -> Self {
		self.status(http::StatusCode::BAD_REQUEST)
	}

	fn not_found(self) -> Self {
		self.status(http::StatusCode::NOT_FOUND)
	}

	fn header_json<K, V>(self, key: K, value: V) -> Result<Self, Error>
	where
		http::HeaderName: TryFrom<K, Error: Into<http::Error>>,
		V: serde::Serialize,
	{
		let value = serde_json::to_string(&value)?;
		Ok(self.header(key, value))
	}

	fn empty(self) -> http::Result<http::Response<body::Empty>> {
		self.header(
			http::header::CONTENT_LENGTH,
			http::HeaderValue::from_static("0"),
		)
		.body(body::Empty::new())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Response<body::Bytes>>
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

	fn json<T>(self, value: T) -> Result<http::Result<http::Response<body::Bytes>>, Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_string(&value)?;
		Ok(self.body(body::Bytes::new(value)))
	}

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Response<body::Boxed>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(body::Boxed::with_data_stream(value))
	}

	fn reader<R>(self, value: R) -> http::Result<http::Response<body::Boxed>>
	where
		R: AsyncRead + Send + 'static,
	{
		self.body(body::Boxed::with_reader(value))
	}

	fn sse<S, E>(self, value: S) -> http::Result<http::Response<body::Boxed>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		self.body(body::Boxed::with_sse_stream(value))
	}
}
