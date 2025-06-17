use crate::{Body, Error};
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncRead;

pub trait Ext: Sized {
	fn empty(self) -> http::Result<http::Request<Body>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> Result<http::Result<http::Request<Body>>, Error>
	where
		T: serde::Serialize;

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<Bytes>> + 'static,
		E: Into<Error> + 'static;

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Request<Body>>
	where
		R: AsyncRead + Send + 'static;

	fn sse<S, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static;
}

impl Ext for http::request::Builder {
	fn empty(self) -> http::Result<http::Request<Body>> {
		self.header(
			http::header::CONTENT_LENGTH,
			http::HeaderValue::from_static("0"),
		)
		.body(Body::empty())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: Into<Bytes>,
	{
		let value = value.into();
		self.header(
			http::header::CONTENT_LENGTH,
			http::HeaderValue::from_str(&value.len().to_string()).unwrap(),
		)
		.body(Body::with_bytes(value))
	}

	fn json<T>(self, value: T) -> Result<http::Result<http::Request<Body>>, Error>
	where
		T: serde::Serialize,
	{
		let value = serde_json::to_string(&value)?;
		Ok(self.body(Body::with_bytes(value)))
	}

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<hyper::body::Frame<Bytes>> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Body::with_stream(value))
	}

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Body::with_data_stream(value))
	}

	fn reader<R>(self, value: R) -> http::Result<http::Request<Body>>
	where
		R: AsyncRead + Send + 'static,
	{
		self.body(Body::with_reader(value))
	}

	fn sse<S, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Body::with_sse_stream(value))
	}
}
