use {
	crate::{Error, Message, Messenger, Payload},
	futures::{FutureExt as _, TryFutureExt as _},
	std::future::Future,
	tangram_either::Either,
};

impl<L, R> Messenger for Either<L, R>
where
	L: Messenger,
	R: Messenger,
{
	fn publish<T>(&self, subject: String, payload: T) -> impl Future<Output = Result<(), Error>>
	where
		T: Payload,
	{
		match self {
			Either::Left(messenger) => messenger.publish(subject, payload).left_future(),
			Either::Right(messenger) => messenger.publish(subject, payload).right_future(),
		}
	}

	fn subscribe<T>(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static,
			Error,
		>,
	> + Send
	where
		T: Payload,
	{
		match self {
			Either::Left(messenger) => messenger
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(messenger) => messenger
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}
}
