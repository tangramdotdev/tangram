use crate::Error;
use bytes::Bytes;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use http_body::Body;
use std::{
	pin::{pin, Pin},
	sync::Arc,
};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

pub mod request;
pub mod response;

pub enum Outgoing {
	Empty,
	Bytes(Option<Bytes>),
	Json(Option<Arc<dyn erased_serde::Serialize + Send + Sync + 'static>>),
	Stream(Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>),
	Body(Pin<Box<dyn Body<Data = Bytes, Error = Error> + Send + 'static>>),
}

impl Outgoing {
	pub fn try_clone(&self) -> Option<Self> {
		match self {
			Self::Empty => Some(Self::Empty),
			Self::Bytes(bytes) => Some(Self::Bytes(bytes.clone())),
			Self::Json(json) => Some(Self::Json(json.clone())),
			Self::Stream(_) | Self::Body(_) => None,
		}
	}

	pub fn body<B>(body: B) -> Self
	where
		B: Body<Data = Bytes, Error = Error> + Send + 'static,
	{
		Self::Body(Box::pin(body))
	}

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
		T: erased_serde::Serialize + Send + Sync + 'static,
	{
		Self::Json(Some(Arc::new(json)))
	}

	pub fn stream<S, T, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<T, E>> + Send + 'static,
		T: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self::Stream(Box::pin(stream.map_ok(Into::into).map_err(Into::into)))
	}

	pub fn reader<R>(reader: R) -> Self
	where
		R: AsyncRead + Send + 'static,
	{
		Self::stream(ReaderStream::new(reader))
	}

	pub fn sse<S, E>(value: S) -> Self
	where
		S: Stream<Item = Result<crate::sse::Event, E>> + Send + 'static,
		E: Into<Error> + 'static,
	{
		Self::stream(value.map_ok(|event| event.to_string()))
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
