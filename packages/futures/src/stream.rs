use {
	self::take_while_inclusive::TakeWhileInclusive,
	crate::{attach::Attach, task::Stopper},
	futures::{
		FutureExt as _, Stream, StreamExt as _, TryStream, TryStreamExt as _, future,
		stream::BoxStream,
	},
};

pub mod take_while_inclusive;

pub trait Ext: Stream {
	fn attach<T>(self, value: T) -> Attach<Self, T>
	where
		Self: Sized,
	{
		Attach::new(self, value)
	}

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

	fn with_stopper(self, stopper: Option<Stopper>) -> BoxStream<'static, Self::Item>
	where
		Self: Sized + Send + 'static,
		Self::Item: Send + 'static,
	{
		let stop = stopper.map_or(future::pending().right_future(), |stopper| {
			stopper.wait().left_future()
		});
		self.take_until(stop).boxed()
	}
}

impl<S> Ext for S where S: Stream {}

pub trait TryExt: TryStream {
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

impl<S> TryExt for S where S: TryStream {}
