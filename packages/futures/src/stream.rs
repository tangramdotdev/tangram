#![allow(clippy::module_name_repetitions)]

use futures::{Future, Stream, StreamExt as _, TryStream, TryStreamExt as _};
use pin_project::pin_project;
use std::{
	pin::Pin,
	task::{Context, Poll},
};

pub trait StreamExt: Stream {
	fn last(mut self) -> impl Future<Output = Option<Self::Item>>
	where
		Self: Sized + Unpin,
	{
		async move {
			let mut last = None;
			while let Some(item) = self.next().await {
				last = Some(item);
			}
			last
		}
	}

	fn take_while_inclusive<F, Fut>(self, predicate: F) -> TakeWhileInclusive<Self, F, Fut>
	where
		Self: Sized,
		F: FnMut(&Self::Item) -> Fut,
		Fut: Future<Output = bool>,
	{
		TakeWhileInclusive::new(self, predicate)
	}
}

impl<S> StreamExt for S where S: Stream {}

pub trait TryStreamExt: TryStream {
	fn try_last(mut self) -> impl Future<Output = Result<Option<Self::Ok>, Self::Error>>
	where
		Self: Sized + Unpin,
	{
		async move {
			let mut last = None;
			while let Some(item) = self.try_next().await? {
				last = Some(item);
			}
			Ok(last)
		}
	}
}

impl<S> TryStreamExt for S where S: TryStream {}

#[pin_project]
pub struct TakeWhileInclusive<S, F, Fut>
where
	S: Stream,
{
	#[pin]
	stream: S,
	predicate: F,
	#[pin]
	pending_predicate: Option<Fut>,
	pending_item: Option<S::Item>,
	done: bool,
}

impl<S, F, Fut> TakeWhileInclusive<S, F, Fut>
where
	S: Stream,
	F: FnMut(&S::Item) -> Fut,
	Fut: Future<Output = bool>,
{
	pub fn new(stream: S, predicate: F) -> Self {
		Self {
			stream,
			predicate,
			pending_predicate: None,
			pending_item: None,
			done: false,
		}
	}
}

impl<S, F, Fut> Stream for TakeWhileInclusive<S, F, Fut>
where
	S: Stream,
	F: FnMut(&S::Item) -> Fut,
	Fut: Future<Output = bool>,
{
	type Item = S::Item;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();

		if *this.done {
			return Poll::Ready(None);
		}

		loop {
			if let Some(pending) = this.pending_predicate.as_mut().as_pin_mut() {
				match pending.poll(cx) {
					Poll::Ready(true) => {
						let item = this.pending_item.take().unwrap();
						this.pending_predicate.set(None);
						return Poll::Ready(Some(item));
					},
					Poll::Ready(false) => {
						let item = this.pending_item.take().unwrap();
						*this.done = true;
						return Poll::Ready(Some(item));
					},
					Poll::Pending => return Poll::Pending,
				}
			}

			match this.stream.as_mut().poll_next(cx) {
				Poll::Ready(Some(item)) => {
					let fut = (this.predicate)(&item);
					*this.pending_item = Some(item);
					this.pending_predicate.set(Some(fut));
				},
				Poll::Ready(None) => {
					*this.done = true;
					return Poll::Ready(None);
				},
				Poll::Pending => return Poll::Pending,
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::StreamExt as _;
	use futures::{future, stream, StreamExt as _};

	#[tokio::test]
	async fn take_while_inclusive() {
		let values = stream::iter([1, 2, 3, 4, 5])
			.take_while_inclusive(|value| future::ready(*value != 4))
			.collect::<Vec<_>>()
			.await;
		assert_eq!(values, [1, 2, 3, 4]);
	}
}
