use crate::{BatchConfig, Error, Message, Messenger, StreamConfig, StreamPublishInfo};
use bytes::Bytes;
use futures::{FutureExt as _, Stream, TryFutureExt as _};
use tangram_either::Either;

impl<L, R> Messenger for Either<L, R>
where
	L: Messenger,
	R: Messenger,
{
	fn publish(&self, subject: String, payload: Bytes) -> impl Future<Output = Result<(), Error>> {
		match self {
			Either::Left(s) => s.publish(subject, payload).left_future(),
			Either::Right(s) => s.publish(subject, payload).right_future(),
		}
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Error>> {
		match self {
			Either::Left(s) => s
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(s) => s
				.subscribe(subject, group)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}

	fn get_or_create_stream(
		&self,
		subject: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<(), Error>> {
		match self {
			Either::Left(s) => s.get_or_create_stream(subject, config).left_future(),
			Either::Right(s) => s.get_or_create_stream(subject, config).right_future(),
		}
	}

	fn delete_stream(&self, subject: String) -> impl Future<Output = Result<(), Error>> {
		match self {
			Either::Left(s) => s.delete_stream(subject).left_future(),
			Either::Right(s) => s.delete_stream(subject).right_future(),
		}
	}

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Error>> + Send {
		match self {
			Either::Left(s) => s.stream_info(name).left_future(),
			Either::Right(s) => s.stream_info(name).right_future(),
		}
	}

	fn stream_publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error>>
	{
		match self {
			Either::Left(s) => s
				.stream_publish(subject, payload)
				.map_ok(futures::FutureExt::left_future)
				.left_future(),
			Either::Right(s) => s
				.stream_publish(subject, payload)
				.map_ok(futures::FutureExt::right_future)
				.right_future(),
		}
	}

	fn stream_subscribe(
		&self,
		subject: String,
		consumer: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Result<Message, Error>> + 'static, Error>>
	{
		match self {
			Either::Left(s) => s
				.stream_subscribe(subject, consumer)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(s) => s
				.stream_subscribe(subject, consumer)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<
		Output = Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error>,
	> + Send {
		match self {
			Either::Left(s) => s
				.stream_batch_publish(name, payloads)
				.map_ok(futures::FutureExt::left_future)
				.left_future(),
			Either::Right(s) => s
				.stream_batch_publish(name, payloads)
				.map_ok(futures::FutureExt::right_future)
				.right_future(),
		}
	}

	fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: BatchConfig,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Error>> + Send + 'static, Error>,
	> + Send {
		match self {
			Either::Left(s) => s
				.stream_batch_subscribe(name, consumer, config)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(s) => s
				.stream_batch_subscribe(name, consumer, config)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}
}
