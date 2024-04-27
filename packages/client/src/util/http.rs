#![allow(dead_code)]

use crate as tg;
use bytes::Bytes;
use futures::{Future, Stream, StreamExt as _};
use http_body::Body;
use http_body_util::BodyExt as _;
use std::pin::{pin, Pin};

pub type Incoming = hyper::body::Incoming;

pub enum Outgoing {
	Empty,
	Bytes(Option<Bytes>),
	Json(Option<Box<dyn erased_serde::Serialize + Send>>),
	Stream(Pin<Box<dyn Stream<Item = tg::Result<Bytes>> + Send + 'static>>),
	Body(Pin<Box<dyn Body<Data = Bytes, Error = tg::Error> + Send + 'static>>),
}

impl Outgoing {
	pub fn empty() -> Self {
		Self::Empty
	}

	pub fn bytes(bytes: impl Into<Bytes>) -> Self {
		Self::Bytes(Some(bytes.into()))
	}

	pub fn json(json: impl erased_serde::Serialize + Send + 'static) -> Self {
		Self::Json(Some(Box::new(json)))
	}

	pub fn stream(stream: impl Stream<Item = tg::Result<Bytes>> + Send + 'static) -> Self {
		Self::Stream(Box::pin(stream))
	}

	pub fn body(body: impl Body<Data = Bytes, Error = tg::Error> + Send + 'static) -> Self {
		Self::Body(Box::pin(body))
	}
}

impl hyper::body::Body for Outgoing {
	type Data = Bytes;

	type Error = tg::Error;

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
					.map(|value| {
						serde_json::to_string(&value)
							.map_err(|source| tg::error!(!source, "failed to serialize the body"))
					})
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
	fn success(self) -> impl Future<Output = tg::Result<Self>> + Send;

	fn bytes(self) -> impl Future<Output = tg::Result<Bytes>> + Send;

	fn json<T>(self) -> impl Future<Output = tg::Result<T>> + Send
	where
		T: serde::de::DeserializeOwned;
}

impl ResponseExt for http::Response<Incoming> {
	async fn success(self) -> tg::Result<Self> {
		if !self.status().is_success() {
			return Err(self.json().await?);
		}
		Ok(self)
	}

	async fn bytes(self) -> tg::Result<Bytes> {
		let collected = self
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?;
		Ok(collected.to_bytes())
	}

	async fn json<T>(self) -> tg::Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		let bytes = self.bytes().await?;
		let json = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		Ok(json)
	}
}
