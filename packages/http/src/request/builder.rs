use crate::{Body, Error};
use bytes::Bytes;
use futures::Stream;
use tokio::io::AsyncRead;

pub trait Ext: Sized {
	fn empty(self) -> http::Result<http::Request<Body>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: serde::Serialize + Send + Sync + 'static;

	fn data_stream<S, T, E>(self, value: S) -> http::Result<http::Request<Body>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Request<Body>>
	where
		R: AsyncRead + Send + 'static;
}

impl Ext for http::request::Builder {
	fn empty(self) -> http::Result<http::Request<Body>> {
		self.body(Body::empty())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: Into<Bytes>,
	{
		self.body(Body::with_bytes(value))
	}

	fn json<T>(self, value: T) -> http::Result<http::Request<Body>>
	where
		T: serde::Serialize + Send + Sync + 'static,
	{
		self.body(Body::with_json(value))
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
}
