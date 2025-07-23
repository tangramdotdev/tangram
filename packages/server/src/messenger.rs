use bytes::Bytes;
use tangram_messenger as messenger;

#[derive(derive_more::IsVariant)]
pub enum Messenger {
	Memory(messenger::memory::Messenger),
	#[cfg(feature = "nats")]
	Nats(messenger::nats::Messenger),
}

#[derive(derive_more::IsVariant)]
pub enum Stream {
	Memory(messenger::memory::Stream),
	#[cfg(feature = "nats")]
	Nats(messenger::nats::Stream),
}

#[derive(derive_more::IsVariant)]
pub enum Consumer {
	Memory(messenger::memory::Consumer),
	#[cfg(feature = "nats")]
	Nats(messenger::nats::Consumer),
}

impl messenger::Messenger for Messenger {
	type Stream = Stream;

	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), messenger::Error> {
		match self {
			Self::Memory(s) => s.publish(subject, payload).await,
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.publish(subject, payload).await,
		}
	}

	async fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = messenger::Message> + 'static, messenger::Error> {
		match self {
			Self::Memory(s) => s
				.subscribe(subject, group)
				.await
				.map(futures::StreamExt::boxed),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.subscribe(subject, group)
				.await
				.map(futures::StreamExt::boxed),
		}
	}

	async fn get_stream(&self, subject: String) -> Result<Self::Stream, messenger::Error> {
		match self {
			Self::Memory(s) => s.get_stream(subject).await.map(Stream::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.get_stream(subject).await.map(Stream::Nats),
		}
	}

	async fn create_stream(
		&self,
		subject: String,
		config: messenger::StreamConfig,
	) -> Result<Self::Stream, messenger::Error> {
		match self {
			Self::Memory(s) => s.create_stream(subject, config).await.map(Stream::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.create_stream(subject, config).await.map(Stream::Nats),
		}
	}

	async fn get_or_create_stream(
		&self,
		subject: String,
		config: messenger::StreamConfig,
	) -> Result<Self::Stream, messenger::Error> {
		match self {
			Self::Memory(s) => s
				.get_or_create_stream(subject, config)
				.await
				.map(Stream::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.get_or_create_stream(subject, config)
				.await
				.map(Stream::Nats),
		}
	}

	async fn delete_stream(&self, subject: String) -> Result<(), messenger::Error> {
		match self {
			Self::Memory(s) => s.delete_stream(subject).await,
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.delete_stream(subject).await,
		}
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<u64, messenger::Error>>, messenger::Error> {
		match self {
			Self::Memory(s) => s
				.stream_publish(name, payload)
				.await
				.map(futures::FutureExt::boxed),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.stream_publish(name, payload)
				.await
				.map(futures::FutureExt::boxed),
		}
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<u64>, messenger::Error>> + Send, messenger::Error>
	{
		match self {
			Self::Memory(s) => s
				.stream_batch_publish(name, payloads)
				.await
				.map(futures::FutureExt::boxed),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.stream_batch_publish(name, payloads)
				.await
				.map(futures::FutureExt::boxed),
		}
	}
}

impl messenger::Stream for Stream {
	type Consumer = Consumer;

	async fn info(&self) -> Result<messenger::StreamInfo, messenger::Error> {
		match self {
			Self::Memory(s) => s.info().await,
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.info().await,
		}
	}

	async fn get_consumer(&self, name: String) -> Result<Self::Consumer, messenger::Error> {
		match self {
			Self::Memory(s) => s.get_consumer(name).await.map(Consumer::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.get_consumer(name).await.map(Consumer::Nats),
		}
	}

	async fn create_consumer(
		&self,
		name: String,
		config: messenger::ConsumerConfig,
	) -> Result<Self::Consumer, messenger::Error> {
		match self {
			Self::Memory(s) => s.create_consumer(name, config).await.map(Consumer::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.create_consumer(name, config).await.map(Consumer::Nats),
		}
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: messenger::ConsumerConfig,
	) -> Result<Self::Consumer, messenger::Error> {
		match self {
			Self::Memory(s) => s
				.get_or_create_consumer(name, config)
				.await
				.map(Consumer::Memory),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.get_or_create_consumer(name, config)
				.await
				.map(Consumer::Nats),
		}
	}

	async fn delete_consumer(&self, name: String) -> Result<(), messenger::Error> {
		match self {
			Self::Memory(s) => s.delete_consumer(name).await,
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.delete_consumer(name).await,
		}
	}
}

impl messenger::Consumer for Consumer {
	async fn info(&self) -> Result<messenger::ConsumerInfo, messenger::Error> {
		match self {
			Self::Memory(s) => s.info().await,
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.info().await,
		}
	}

	async fn subscribe(
		&self,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message, messenger::Error>> + Send + 'static,
		messenger::Error,
	> {
		match self {
			Self::Memory(s) => s.subscribe().await.map(futures::StreamExt::boxed),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s.subscribe().await.map(futures::StreamExt::boxed),
		}
	}

	async fn batch_subscribe(
		&self,
		config: messenger::BatchConfig,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message, messenger::Error>> + Send + 'static,
		messenger::Error,
	> {
		match self {
			Self::Memory(s) => s
				.batch_subscribe(config)
				.await
				.map(futures::StreamExt::boxed),
			#[cfg(feature = "nats")]
			Self::Nats(s) => s
				.batch_subscribe(config)
				.await
				.map(futures::StreamExt::boxed),
		}
	}
}
