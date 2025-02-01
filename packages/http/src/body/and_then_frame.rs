use bytes::Buf;
use http_body::{Body, Frame};
use pin_project::pin_project;
use std::{
	pin::Pin,
	task::{Context, Poll},
};

#[pin_project]
#[derive(Clone, Copy)]
pub struct AndThenFrame<B, F> {
	#[pin]
	inner: B,
	f: F,
}

impl<B, F> AndThenFrame<B, F> {
	pub(crate) fn new(body: B, f: F) -> Self {
		Self { inner: body, f }
	}
}

impl<B, F, B2> Body for AndThenFrame<B, F>
where
	B: Body,
	F: FnMut(Frame<B::Data>) -> Result<Frame<B2>, B::Error>,
	B2: Buf,
{
	type Data = B2;
	type Error = B::Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
		let this = self.project();
		match this.inner.poll_frame(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(None) => Poll::Ready(None),
			Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some((this.f)(frame))),
			Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
		}
	}

	fn is_end_stream(&self) -> bool {
		self.inner.is_end_stream()
	}
}
