use crate::{sse, Error};
use bytes::Bytes;
use futures::{
	future, stream, Future, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt as _,
};
use http_body::Body;
use std::pin::{pin, Pin};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

pub enum Outgoing {
	Empty,
	Bytes(Option<Bytes>),
	Json(Option<Box<dyn erased_serde::Serialize + Send>>),
	Stream(Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>),
	Body(Pin<Box<dyn Body<Data = Bytes, Error = Error> + Send + 'static>>),
}

impl Outgoing {
	#[must_use]
	pub fn empty() -> Self {
		Self::Empty
	}

	pub fn bytes<T>(bytes: T) -> Self
	where
		T: Into<Bytes>,
	{
		Self::Bytes(Some(bytes.into()))
	}

	pub fn json<T>(json: T) -> Self
	where
		T: erased_serde::Serialize + Send + 'static,
	{
		Self::Json(Some(Box::new(json)))
	}

	pub fn stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self::Stream(Box::pin(stream.map_ok(Into::into).map_err(Into::into)))
	}

	pub fn body<B>(body: B) -> Self
	where
		B: Body<Data = Bytes, Error = Error> + Send + 'static,
	{
		Self::Body(Box::pin(body))
	}

	pub fn reader<R>(reader: R) -> Self
	where
		R: AsyncRead + Send + 'static,
	{
		Self::stream(ReaderStream::new(reader))
	}

	pub fn future_bytes<F, T, E>(value: F) -> Self
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self::stream(stream::once(value.map_into().err_into()))
	}

	pub fn future_json<F, T, E>(value: F) -> Self
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: serde::Serialize,
		E: Into<Error> + 'static,
	{
		Self::future_bytes(value.err_into().and_then(|output| {
			future::ready(
				serde_json::to_vec(&output)
					.map(Bytes::from)
					.map_err(Error::from),
			)
		}))
	}

	pub fn sse<S, T, E>(value: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<sse::Event> + 'static,
		E: Into<Error> + 'static,
	{
		Self::stream(value.map_ok(|event| event.into().to_string()))
	}
}

impl hyper::body::Body for Outgoing {
	type Data = Bytes;

	type Error = Error;

	fn poll_frame(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		match self.get_mut() {
			Outgoing::Empty => std::task::Poll::Ready(None),
			Outgoing::Bytes(option) => {
				std::task::Poll::Ready(option.take().map(hyper::body::Frame::data).map(Ok))
			},
			Outgoing::Json(option) => std::task::Poll::Ready(
				option
					.take()
					.map(|value| serde_json::to_string(&value).map_err(Into::into))
					.map(|result| result.map(Bytes::from).map(hyper::body::Frame::data)),
			),
			Outgoing::Stream(stream) => stream
				.poll_next_unpin(cx)
				.map(|option| option.map(|result| result.map(hyper::body::Frame::data))),
			Outgoing::Body(body) => pin!(body).poll_frame(cx),
		}
	}
}

pub trait ResponseExt: Sized {
	fn ok() -> Self;
	fn bad_request() -> Self;
	fn not_found() -> Self;
}

impl ResponseExt for http::Response<Outgoing> {
	fn ok() -> Self {
		http::Response::builder()
			.status(http::StatusCode::OK)
			.body(Outgoing::empty())
			.unwrap()
	}

	fn bad_request() -> Self {
		http::Response::builder()
			.status(http::StatusCode::BAD_REQUEST)
			.body(Outgoing::empty())
			.unwrap()
	}

	fn not_found() -> Self {
		http::Response::builder()
			.status(http::StatusCode::NOT_FOUND)
			.body(Outgoing::empty())
			.unwrap()
	}
}

pub trait RequestBuilderExt: Sized {
	fn empty(self) -> http::Result<http::Request<Outgoing>>;

	fn bytes<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: Into<Bytes>;

	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + 'static;
}

impl RequestBuilderExt for http::request::Builder {
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
		T: serde::Serialize + Send + 'static,
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

impl ResponseBuilderExt for http::response::Builder {
	fn empty(self) -> http::Result<http::Response<Outgoing>> {
		self.body(Outgoing::empty())
	}

	fn bytes<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: Into<Bytes>,
	{
		self.body(Outgoing::bytes(value))
	}

	fn json<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.body(Outgoing::json(value))
	}

	fn stream<S, T, E>(self, value: S) -> http::Result<http::Response<Outgoing>>
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Outgoing::stream(value.err_into()))
	}

	fn future_bytes<F, T, E>(self, value: F) -> http::Result<http::Response<Outgoing>>
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		self.body(Outgoing::future_bytes(value))
	}

	fn future_json<F, T, E>(self, value: F) -> http::Result<http::Response<Outgoing>>
	where
		F: Future<Output = Result<T, E>> + Send + 'static,
		T: serde::Serialize,
		E: Into<Error> + 'static,
	{
		self.body(Outgoing::future_json(value))
	}
}
