use crate::{Body, Error};
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncRead;

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

	fn empty(self) -> http::Result<http::Response<Body>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Response<Body>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Response<Body>>
	where
		T: serde::Serialize + Send + Sync + 'static;

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Response<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Response<Body>>
	where
		R: AsyncRead + Send + 'static;

	fn sse<S, E>(self, value: S) -> http::Result<http::Response<Body>>
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

	fn empty(self) -> http::Result<http::Response<Body>> {
		self.body(Body::empty())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Response<Body>>
	where
		T: Into<Bytes>,
	{
		self.body(Body::with_bytes(value))
	}

	fn json<T>(self, value: T) -> http::Result<http::Response<Body>>
	where
		T: serde::Serialize + Send + Sync + 'static,
	{
		self.body(Body::with_json(value))
	}

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Response<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Body::with_data_stream(value))
	}

	fn reader<R>(self, value: R) -> http::Result<http::Response<Body>>
	where
		R: AsyncRead + Send + 'static,
	{
		self.body(Body::with_reader(value))
	}

	fn sse<S, E>(self, value: S) -> http::Result<http::Response<Body>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Body::with_sse_stream(value))
	}
}
