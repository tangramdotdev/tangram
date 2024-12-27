use futures::Stream;
use std::{pin::Pin, task::Poll};

pub struct Attach<S, T> {
	stream: S,
	#[allow(dead_code)]
	value: T,
}

impl<S, T> Attach<S, T> {
	pub fn new(stream: S, value: T) -> Self {
		Self { stream, value }
	}
}

impl<S, T> Stream for Attach<S, T>
where
	S: Stream,
{
	type Item = S::Item;

	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Self::Item>> {
		let stream = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.stream) };
		stream.poll_next(cx)
	}
}
