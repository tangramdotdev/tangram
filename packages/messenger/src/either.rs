use crate::{Message, Messenger};
use futures::{FutureExt as _, Stream, TryFutureExt as _};
use tangram_either::Either;

impl<L, R> Messenger for Either<L, R>
where
	L: Messenger,
	R: Messenger,
{
	type Error = Either<L::Error, R::Error>;

	fn create_subject(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s
				.create_subject(subject)
				.map_err(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.create_subject(subject)
				.map_err(Either::Right)
				.right_future(),
		}
	}

	fn close_subject(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		match self {
			Either::Left(s) => s.close_subject(subject).map_err(Either::Left).left_future(),
			Either::Right(s) => s
				.close_subject(subject)
				.map_err(Either::Right)
				.right_future(),
		}
	}

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
}
