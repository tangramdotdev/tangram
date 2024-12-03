use crate::Error;
use bytes::Bytes;
use futures::{
	future, stream, Future, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt as _,
};
use http_body::Body;
use std::{
	pin::{pin, Pin},
	sync::Arc,
};
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

pub mod request;
pub mod response;

pub struct Outgoing {
	arc: Option<Arc<dyn std::any::Any + Send + Sync>>,
	inner: Inner,
}

impl Drop for Outgoing {
	fn drop(&mut self) {
		println!("Outgoing::drop: dropping body");
	}
}

pub enum Inner {
	Empty,
	Bytes(Option<Bytes>),
	Json(Option<Arc<dyn erased_serde::Serialize + Send + Sync + 'static>>),
	Stream(Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>),
	Body(Pin<Box<dyn Body<Data = Bytes, Error = Error> + Send + 'static>>),
}

impl Outgoing {
	pub fn add_ref(&mut self, arc: Arc<dyn std::any::Any + Send + Sync>) {
		self.arc.replace(arc);
	}

	pub fn try_clone(&self) -> Option<Self> {
		match &self.inner {
			Inner::Empty => Some(Self {
				arc: self.arc.clone(),
				inner: Inner::Empty,
			}),
			Inner::Bytes(bytes) => Some(Self {
				arc: self.arc.clone(),
				inner: Inner::Bytes(bytes.clone()),
			}),
			Inner::Json(json) => Some(Self {
				arc: self.arc.clone(),
				inner: Inner::Json(json.clone()),
			}),
			Inner::Stream(_) | Inner::Body(_) => None,
		}
	}

	#[must_use]
	pub fn empty() -> Self {
		Self {
			arc: None,
			inner: Inner::Empty,
		}
	}
	//TODO: revert template arg changes

	pub fn bytes<B>(bytes: B) -> Self
	where
		B: Into<Bytes>,
	{
		Self {
			arc: None,
			inner: Inner::Bytes(Some(bytes.into())),
		}
	}

	pub fn json<J>(json: J) -> Self
	where
		J: erased_serde::Serialize + Send + Sync + 'static,
	{
		Self {
			arc: None,
			inner: Inner::Json(Some(Arc::new(json))),
		}
	}

	pub fn stream<S, B, E>(stream: S) -> Self
	where
		S: Stream<Item = Result<B, E>> + Send + 'static,
		B: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self {
			arc: None,
			inner: Inner::Stream(Box::pin(stream.map_ok(Into::into).map_err(Into::into))),
		}
	}

	pub fn body<B>(body: B) -> Self
	where
		B: Body<Data = Bytes, Error = Error> + Send + 'static,
	{
		Self {
			arc: None,
			inner: Inner::Body(Box::pin(body)),
		}
	}

	pub fn reader<R>(reader: R) -> Self
	where
		R: AsyncRead + Send + 'static,
	{
		Self::stream(ReaderStream::new(reader))
	}

	pub fn future_bytes<F, B, E>(value: F) -> Self
	where
		F: Future<Output = Result<B, E>> + Send + 'static,
		B: Into<Bytes> + 'static,
		E: Into<Error> + 'static,
	{
		Self::stream(stream::once(value.map_into().err_into()))
	}

	pub fn future_json<F, S, E>(value: F) -> Self
	where
		F: Future<Output = Result<S, E>> + Send + 'static,
		S: serde::Serialize,
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

	pub fn future_optional_json<F, S, E>(value: F) -> Self
	where
		F: Future<Output = Result<Option<S>, E>> + Send + 'static,
		S: serde::Serialize,
		E: Into<Error> + 'static,
	{
		Self::future_bytes(value.err_into().and_then(|option| {
			future::ready(option.map_or_else(
				|| Ok(Bytes::new()),
				|output| {
					serde_json::to_vec(&output)
						.map(Bytes::from)
						.map_err(Error::from)
				},
			))
		}))
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
		match &mut self.get_mut().inner {
			Inner::Empty => std::task::Poll::Ready(None),
			Inner::Bytes(option) => {
				std::task::Poll::Ready(option.take().map(hyper::body::Frame::data).map(Ok))
			},
			Inner::Json(option) => std::task::Poll::Ready(
				option
					.take()
					.map(|value| serde_json::to_string(&value).map_err(Into::into))
					.map(|result| result.map(Bytes::from).map(hyper::body::Frame::data)),
			),
			Inner::Stream(stream) => stream
				.poll_next_unpin(cx)
				.map(|option| option.map(|result| result.map(hyper::body::Frame::data))),
			Inner::Body(body) => pin!(body).poll_frame(cx),
		}
	}
}
