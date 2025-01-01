use futures::Future;
use std::{pin::Pin, task::Poll};

pub struct Attach<F, T> {
	future: F,
	#[allow(dead_code)]
	value: T,
}

impl<F, T> Attach<F, T> {
	pub fn new(stream: F, value: T) -> Self {
		Self {
			future: stream,
			value,
		}
	}
}

impl<F, T> Future for Attach<F, T>
where
	F: Future,
{
	type Output = F::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
		let future = unsafe { self.as_mut().map_unchecked_mut(|s| &mut s.future) };
		future.poll(cx)
	}
}
