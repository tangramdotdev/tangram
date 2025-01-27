use crate::Error;
use bytes::Bytes;
use http_body::Body;
use std::pin::{pin, Pin};

pub mod request;
pub mod response;

pub enum Incoming {
	Incoming(hyper::body::Incoming),
	Body(Pin<Box<dyn Body<Data = Bytes, Error = Error> + Send + Sync + 'static>>),
}

impl hyper::body::Body for Incoming {
	type Data = Bytes;

	type Error = Error;

	fn poll_frame(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
		match self.get_mut() {
			Incoming::Incoming(body) => pin!(body).poll_frame(cx).map_err(Into::into),
			Incoming::Body(body) => pin!(body).poll_frame(cx),
		}
	}
}

impl From<hyper::body::Incoming> for Incoming {
	fn from(value: hyper::body::Incoming) -> Self {
		Self::Incoming(value)
	}
}
