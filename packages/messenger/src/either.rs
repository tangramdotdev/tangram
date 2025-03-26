use crate::{Message, Messenger};
use futures::{FutureExt as _, Stream, StreamExt, TryFutureExt as _, TryStreamExt};
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

	fn destroy_stream(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s.create_stream(subject).map_err(Either::Left).left_future(),
			Either::Right(s) => s
				.create_stream(subject)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn stream_publish(
		&self,
		subject: String,
		message: bytes::Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s
				.stream_publish(subject, message)
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.stream_publish(subject, message)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn stream_subscribe(
		&self,
		subject: String,
		consumer_name: Option<String>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Self::Error>> + 'static, Self::Error>,
	> {
		match self {
			Either::Left(s) => s
				.stream_subscribe(subject, consumer_name)
				.map_ok(|s| s.map_err(Either::Left).left_stream())
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.stream_subscribe(subject, consumer_name)
				.map_ok(|s| s.map_err(Either::Right).right_stream())
				.map_err(Either::Right)
				.right_future(),
		}
	}
}
