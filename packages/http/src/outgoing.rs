use crate::Error;
use bytes::Bytes;
use futures::{Stream, StreamExt as _};
use http_body::Body;
use std::pin::{pin, Pin};

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

	pub fn bytes(bytes: impl Into<Bytes>) -> Self {
		Self::Bytes(Some(bytes.into()))
	}

	pub fn json(json: impl erased_serde::Serialize + Send + 'static) -> Self {
		Self::Json(Some(Box::new(json)))
	}

	pub fn stream(stream: impl Stream<Item = Result<Bytes, Error>> + Send + 'static) -> Self {
		Self::Stream(Box::pin(stream))
	}

	pub fn body(body: impl Body<Data = Bytes, Error = Error> + Send + 'static) -> Self {
		Self::Body(Box::pin(body))
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

pub trait ResponseBuilderExt: Sized {
	fn empty(self) -> http::Result<http::Response<Outgoing>>;
	fn json<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: serde::Serialize + Send + 'static;
}

impl ResponseBuilderExt for http::response::Builder {
	fn empty(self) -> http::Result<http::Response<Outgoing>> {
		self.body(Outgoing::empty())
	}

	fn json<T>(self, value: T) -> http::Result<http::Response<Outgoing>>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.body(Outgoing::json(value))
	}
}

pub trait RequestBuilderExt: Sized {
	fn empty(self) -> http::Result<http::Request<Outgoing>>;
	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + 'static;
}

impl RequestBuilderExt for http::request::Builder {
	fn empty(self) -> http::Result<http::Request<Outgoing>> {
		self.body(Outgoing::empty())
	}

	fn json<T>(self, value: T) -> http::Result<http::Request<Outgoing>>
	where
		T: serde::Serialize + Send + 'static,
	{
		self.body(Outgoing::json(value))
	}
}
