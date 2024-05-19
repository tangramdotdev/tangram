use crate::{Error, Outgoing};
use bytes::Bytes;
use futures::{Future, Stream};

pub trait Ext: Sized {
	fn empty(self) -> http::Result<http::Request<Outgoing>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + Sync + 'static;
}

impl Ext for http::request::Builder {
	fn empty(self) -> http::Result<http::Request<Outgoing>> {
		self.body(Outgoing::empty())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: Into<Bytes>,
	{
		self.body(Outgoing::bytes(value))
	}

	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + Sync + 'static,
	{
		self.body(Outgoing::json(value))
	}
}

pub trait ResponseBuilderExt: Sized {
	fn empty(self) -> http::Result<http::Response<Outgoing>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: serde::Serialize + Send + 'static;

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Response<Outgoing>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn future_bytes<F, T, E>(self, value: F) -> http::Result<http::Response<Outgoing>>
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn future_json<F, T, E>(self, value: F) -> http::Result<http::Response<Outgoing>>
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: serde::Serialize,
		E: Into<Error> + 'static;
}
