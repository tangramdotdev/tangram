use crate::{Message, Messenger, PublishFuture};
use futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _};
use tangram_either::Either;

impl<L, R> Messenger for Either<L, R>
where
	L: Messenger,
	R: Messenger,
{
	type Error = Either<L::Error, R::Error>;

	fn publish(
		&self,
		subject: String,
		message: bytes::Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s
				.publish(subject, message)
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.publish(subject, message)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Self::Error>> {
		match self {
			Either::Left(s) => s
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::left_stream)
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::right_stream)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn create_stream(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s.create_stream(subject).map_err(Either::Left).left_future(),
			Either::Right(s) => s
				.create_stream(subject)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn delete_stream(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s.delete_stream(subject).map_err(Either::Left).left_future(),
			Either::Right(s) => s
				.delete_stream(subject)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn stream_publish(
		&self,
		subject: String,
		message: bytes::Bytes,
	) -> impl Future<Output = Result<PublishFuture<Self::Error>, Self::Error>> {
		match self {
			Either::Left(s) => s
				.stream_publish(subject, message)
				.map_err(Either::Left)
				.map(|future| {
					future.map(|inner| {
						let inner = inner.map_err(Either::Left).boxed();
						PublishFuture { inner }
					})
				})
				.left_future(),
			Either::Right(s) => s
				.stream_publish(subject, message)
				.map_err(Either::Right)
				.map(|future| {
					future.map(|inner| {
						let inner = inner.map_err(Either::Right).boxed();
						PublishFuture { inner }
					})
				})
				.right_future(),
		}
	}

	fn stream_subscribe(
		&self,
		subject: String,
		consumer: Option<String>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Self::Error>> + 'static, Self::Error>,
	> {
		match self {
			Either::Left(s) => s
				.stream_subscribe(subject, consumer)
				.map_ok(|s| s.map_err(Either::Left).left_stream())
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.stream_subscribe(subject, consumer)
				.map_ok(|s| s.map_err(Either::Right).right_stream())
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Self::Error>> + Send {
		match self {
			Either::Left(s) => s.stream_info(name).map_err(Either::Left).left_future(),
			Either::Right(s) => s.stream_info(name).map_err(Either::Right).right_future(),
		}
	}
}
