use {
	crate::{
		BatchConfig, Consumer, ConsumerConfig, ConsumerInfo, Error, Message, Messenger, Stream,
		StreamConfig, StreamInfo,
	},
	bytes::Bytes,
	futures::{FutureExt as _, TryFutureExt as _},
	tangram_either::Either,
};

impl<L, R> Messenger for Either<L, R>
where
	L: Messenger,
	R: Messenger,
{
	type Stream = Either<L::Stream, R::Stream>;

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
	) -> impl Future<Output = Result<impl futures::Stream<Item = Message> + 'static, Error>> {
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

	fn get_stream(&self, subject: String) -> impl Future<Output = Result<Self::Stream, Error>> {
		match self {
			Either::Left(s) => s.get_stream(subject).map_ok(Either::Left).left_future(),
			Either::Right(s) => s.get_stream(subject).map_ok(Either::Right).right_future(),
		}
	}

	fn create_stream(
		&self,
		subject: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<Self::Stream, Error>> {
		match self {
			Either::Left(s) => s
				.create_stream(subject, config)
				.map_ok(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.create_stream(subject, config)
				.map_ok(Either::Right)
				.right_future(),
		}
	}

	fn get_or_create_stream(
		&self,
		subject: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<Self::Stream, Error>> {
		match self {
			Either::Left(s) => s
				.get_or_create_stream(subject, config)
				.map_ok(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.get_or_create_stream(subject, config)
				.map_ok(Either::Right)
				.right_future(),
		}
	}

	fn delete_stream(&self, subject: String) -> impl Future<Output = Result<(), Error>> {
		match self {
			Either::Left(s) => s.delete_stream(subject).left_future(),
			Either::Right(s) => s.delete_stream(subject).right_future(),
		}
	}

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<u64, Error>>, Error>> + Send {
		match self {
			Either::Left(s) => s
				.stream_publish(name, payload)
				.map_ok(futures::FutureExt::left_future)
				.left_future(),
			Either::Right(s) => s
				.stream_publish(name, payload)
				.map_ok(futures::FutureExt::right_future)
				.right_future(),
		}
	}

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<Output = Result<impl Future<Output = Result<Vec<u64>, Error>> + Send, Error>> + Send
	{
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
}

impl<L, R> Stream for Either<L, R>
where
	L: Stream,
	R: Stream,
{
	type Consumer = Either<L::Consumer, R::Consumer>;

	fn info(&self) -> impl Future<Output = Result<StreamInfo, Error>> + Send {
		match self {
			Either::Left(s) => s.info().left_future(),
			Either::Right(s) => s.info().right_future(),
		}
	}

	fn get_consumer(
		&self,
		name: String,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send {
		match self {
			Either::Left(s) => s.get_consumer(name).map_ok(Either::Left).left_future(),
			Either::Right(s) => s.get_consumer(name).map_ok(Either::Right).right_future(),
		}
	}

	fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send {
		match self {
			Either::Left(s) => s
				.create_consumer(name, config)
				.map_ok(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.create_consumer(name, config)
				.map_ok(Either::Right)
				.right_future(),
		}
	}

	fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> impl Future<Output = Result<Self::Consumer, Error>> + Send {
		match self {
			Either::Left(s) => s
				.get_or_create_consumer(name, config)
				.map_ok(Either::Left)
				.left_future(),
			Either::Right(s) => s
				.get_or_create_consumer(name, config)
				.map_ok(Either::Right)
				.right_future(),
		}
	}

	fn delete_consumer(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send {
		match self {
			Either::Left(s) => s.delete_consumer(name).left_future(),
			Either::Right(s) => s.delete_consumer(name).right_future(),
		}
	}
}

impl<L, R> Consumer for Either<L, R>
where
	L: Consumer,
	R: Consumer,
{
	fn info(&self) -> impl Future<Output = Result<ConsumerInfo, Error>> + Send {
		match self {
			Either::Left(s) => s.info().left_future(),
			Either::Right(s) => s.info().right_future(),
		}
	}

	fn subscribe(
		&self,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.subscribe()
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(s) => s
				.subscribe()
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}

	fn batch_subscribe(
		&self,
		config: BatchConfig,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Error>> + Send + 'static,
			Error,
		>,
	> + Send {
		match self {
			Either::Left(s) => s
				.batch_subscribe(config)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			Either::Right(s) => s
				.batch_subscribe(config)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}
}
