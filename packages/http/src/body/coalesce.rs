use {
	bytes::{Bytes, BytesMut},
	http_body::{Body, Frame},
	pin_project::pin_project,
	std::{
		pin::Pin,
		task::{Context, Poll},
	},
};

pub const DEFAULT_COALESCING_TARGET_SIZE: usize = 16 * 1024;

#[pin_project]
pub struct Coalesce<B>
where
	B: Body<Data = Bytes>,
{
	buffer: BytesMut,
	#[pin]
	inner: B,
	pending: Option<Result<Frame<Bytes>, B::Error>>,
	target_size: usize,
}

impl<B> Coalesce<B>
where
	B: Body<Data = Bytes>,
{
	#[must_use]
	pub fn new(inner: B) -> Self {
		Self::with_target_size(inner, DEFAULT_COALESCING_TARGET_SIZE)
	}

	#[must_use]
	pub fn with_target_size(inner: B, target_size: usize) -> Self {
		assert!(target_size > 0, "the target size must be greater than zero");
		Self {
			buffer: BytesMut::with_capacity(target_size),
			inner,
			pending: None,
			target_size,
		}
	}
}

impl<B> Body for Coalesce<B>
where
	B: Body<Data = Bytes>,
{
	type Data = Bytes;
	type Error = B::Error;

	fn poll_frame(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
		let mut this = self.project();
		if let Some(frame) = this.pending.take() {
			return Poll::Ready(Some(frame));
		}
		loop {
			match this.inner.as_mut().poll_frame(cx) {
				Poll::Pending => {
					return if this.buffer.is_empty() {
						Poll::Pending
					} else {
						Poll::Ready(Some(Ok(take_data(this.buffer))))
					};
				},
				Poll::Ready(None) => {
					return if this.buffer.is_empty() {
						Poll::Ready(None)
					} else {
						Poll::Ready(Some(Ok(take_data(this.buffer))))
					};
				},
				Poll::Ready(Some(Err(error))) => {
					if this.buffer.is_empty() {
						return Poll::Ready(Some(Err(error)));
					}
					*this.pending = Some(Err(error));
					return Poll::Ready(Some(Ok(take_data(this.buffer))));
				},
				Poll::Ready(Some(Ok(frame))) => {
					let frame = match frame.into_data() {
						Ok(data) => {
							if data.is_empty() {
								continue;
							}
							this.buffer.extend_from_slice(&data);
							if this.buffer.len() >= *this.target_size {
								return Poll::Ready(Some(Ok(take_data(this.buffer))));
							}
							continue;
						},
						Err(frame) => frame,
					};
					let trailers = frame.into_trailers().unwrap();
					if this.buffer.is_empty() {
						return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
					}
					*this.pending = Some(Ok(Frame::trailers(trailers)));
					return Poll::Ready(Some(Ok(take_data(this.buffer))));
				},
			}
		}
	}

	fn is_end_stream(&self) -> bool {
		self.buffer.is_empty() && self.pending.is_none() && self.inner.is_end_stream()
	}

	fn size_hint(&self) -> http_body::SizeHint {
		http_body::SizeHint::with_exact(self.buffer.len() as u64) + self.inner.size_hint()
	}
}

fn take_data(buffer: &mut BytesMut) -> Frame<Bytes> {
	Frame::data(buffer.split().freeze())
}
