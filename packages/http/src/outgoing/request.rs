use crate::{Error, Outgoing};
use bytes::Bytes;
use futures::{Stream, TryStreamExt as _};
use tokio::io::AsyncRead;

pub trait Ext: Sized {
	fn empty(self) -> http::Result<http::Request<Outgoing>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + Sync + 'static;

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<Outgoing>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static;

	fn reader<R>(self, value: R) -> http::Result<http::Request<Outgoing>>
	where
		R: AsyncRead + Send + 'static;
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

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Request<Outgoing>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Outgoing::stream(value.err_into()))
	}

	fn reader<R>(self, value: R) -> http::Result<http::Request<Outgoing>>
	where
		R: AsyncRead + Send + 'static,
	{
		self.body(Outgoing::reader(value))
	}
}
