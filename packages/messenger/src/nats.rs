use {
	crate::{
		Acker, BatchConfig, ConsumerConfig, ConsumerInfo, DeliverPolicy, DiscardPolicy, Error,
		Message, Payload, RetentionPolicy, StreamConfig, StreamInfo,
	},
	async_nats as nats,
	futures::{FutureExt as _, TryFutureExt as _, prelude::*, stream::FuturesOrdered},
	num::ToPrimitive as _,
	std::future::IntoFuture,
};

#[derive(Clone)]
pub struct Messenger {
	client: nats::Client,
	jetstream: nats::jetstream::Context,
	id: Option<String>,
}

#[derive(Clone)]
pub struct Stream {
	stream: nats::jetstream::stream::Stream,
}

#[derive(Clone)]
pub struct Consumer {
	consumer: nats::jetstream::consumer::Consumer<nats::jetstream::consumer::pull::Config>,
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client, id: Option<String>) -> Self {
		let jetstream = nats::jetstream::new(client.clone());
		Self {
			client,
			jetstream,
			id,
		}
	}

	#[expect(clippy::needless_pass_by_value, reason = "matches trait signature")]
	fn publish<T>(&self, subject: String, payload: T) -> impl Future<Output = Result<(), Error>>
	where
		T: Payload,
	{
		let subject = self.subject_name(subject);
		let bytes = match payload.serialize() {
			Ok(bytes) => bytes,
			Err(error) => return future::err(error).left_future(),
		};
		self.client
			.publish(subject, bytes)
			.map_err(Error::other)
			.right_future()
	}

	fn subscribe<T>(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<
		Output = Result<impl futures::Stream<Item = Result<Message<T>, Error>> + 'static, Error>,
	>
	where
		T: Payload + Clone,
	{
		let subject = self.subject_name(subject);
		match group {
			None => self
				.client
				.subscribe(subject)
				.map_ok(|subscriber| {
					subscriber
						.map(|message| {
							T::deserialize(message.payload)
								.map(|payload| Message {
									subject: message.subject.to_string(),
									payload,
									acker: Acker::default(),
								})
								.map_err(Error::deserialization)
						})
						.left_stream()
				})
				.map_err(Error::other)
				.left_future(),
			Some(group) => self
				.client
				.queue_subscribe(subject, group)
				.map_ok(|subscriber| {
					subscriber
						.map(|message| {
							T::deserialize(message.payload)
								.map(|payload| Message {
									subject: message.subject.to_string(),
									payload,
									acker: Acker::default(),
								})
								.map_err(Error::deserialization)
						})
						.right_stream()
				})
				.map_err(Error::other)
				.right_future(),
		}
	}

	async fn get_stream(&self, name: String) -> Result<Stream, Error> {
		let name = self.stream_name(name);
		let stream = self
			.jetstream
			.get_stream(name)
			.await
			.map_err(Error::other)?;
		let stream = Stream::new(stream);
		Ok(stream)
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<Stream, Error> {
		let name = self.stream_name(name);
		let stream_config = Self::stream_config(name, &config);
		let stream = self
			.jetstream
			.create_stream(stream_config)
			.await
			.map_err(Error::other)?;
		let stream = Stream::new(stream);
		Ok(stream)
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Stream, Error> {
		let name = self.stream_name(name);
		let stream_config = Self::stream_config(name, &config);
		let stream = self
			.jetstream
			.get_or_create_stream(stream_config)
			.await
			.map_err(Error::other)?;
		let stream = Stream::new(stream);
		Ok(stream)
	}

	fn stream_config(name: String, config: &StreamConfig) -> nats::jetstream::stream::Config {
		let discard = match config.discard {
			DiscardPolicy::Old => async_nats::jetstream::stream::DiscardPolicy::Old,
			DiscardPolicy::New => async_nats::jetstream::stream::DiscardPolicy::New,
		};
		let max_bytes = config
			.max_bytes
			.map(|value| value.to_i64().unwrap())
			.unwrap_or_default();
		let max_messages = config
			.max_messages
			.map(|value| value.to_i64().unwrap())
			.unwrap_or_default();
		let retention = match config.retention {
			RetentionPolicy::Interest => nats::jetstream::stream::RetentionPolicy::Interest,
			RetentionPolicy::Limits => nats::jetstream::stream::RetentionPolicy::Limits,
			RetentionPolicy::WorkQueue => nats::jetstream::stream::RetentionPolicy::WorkQueue,
		};
		nats::jetstream::stream::Config {
			discard,
			name,
			max_bytes,
			max_messages,
			retention,
			..Default::default()
		}
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		let name = self.stream_name(name);
		self.jetstream
			.delete_stream(name)
			.await
			.map_err(Error::other)?;
		Ok(())
	}

	async fn stream_publish<T>(
		&self,
		name: String,
		payload: T,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error>
	where
		T: Payload,
	{
		let name = self.subject_name(name);
		let bytes = payload.serialize()?;
		let future = self
			.jetstream
			.publish(name, bytes)
			.await
			.map_err(Error::other)?
			.into_future()
			.map_ok(|ack| ack.sequence)
			.map_err(Error::other);
		Ok(future)
	}

	async fn stream_batch_publish<T>(
		&self,
		name: String,
		payloads: Vec<T>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error>
	where
		T: Payload,
	{
		let name = self.subject_name(name);
		let mut futures = Vec::new();
		for payload in payloads {
			let bytes = payload.serialize()?;
			let future = self
				.jetstream
				.publish(name.clone(), bytes)
				.await
				.map_err(Error::other)?;
			futures.push(future);
		}
		Ok(futures
			.into_iter()
			.map(IntoFuture::into_future)
			.collect::<FuturesOrdered<_>>()
			.map_ok(|ack| ack.sequence)
			.map_err(Error::other)
			.try_collect::<Vec<_>>())
	}

	fn subject_name(&self, name: String) -> String {
		match &self.id {
			Some(id) => format!("{id}.{name}"),
			None => name,
		}
	}

	fn stream_name(&self, name: String) -> String {
		match &self.id {
			Some(id) => format!("{name}_{id}"),
			None => name,
		}
	}
}

impl Stream {
	fn new(stream: nats::jetstream::stream::Stream) -> Self {
		Self { stream }
	}

	async fn info(&self) -> Result<StreamInfo, Error> {
		let info = self.stream.get_info().await.map_err(Error::other)?;
		let info = StreamInfo {
			first_sequence: info.state.first_sequence,
			last_sequence: info.state.last_sequence,
		};
		Ok(info)
	}

	async fn get_consumer(&self, name: String) -> Result<Consumer, Error> {
		let consumer = self
			.stream
			.get_consumer(&name)
			.await
			.map_err(Error::other)?;
		let consumer = Consumer::new(consumer);
		Ok(consumer)
	}

	async fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Consumer, Error> {
		let config = Self::consumer_config(name, &config);
		let consumer = self
			.stream
			.create_consumer(config)
			.await
			.map_err(Error::other)?;
		let consumer = Consumer::new(consumer);
		Ok(consumer)
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Consumer, Error> {
		let config = Self::consumer_config(name.clone(), &config);
		let consumer = self
			.stream
			.get_or_create_consumer(&name, config)
			.await
			.map_err(Error::other)?;
		let consumer = Consumer::new(consumer);
		Ok(consumer)
	}

	fn consumer_config(
		name: String,
		config: &ConsumerConfig,
	) -> async_nats::jetstream::consumer::pull::Config {
		let deliver_policy = match config.deliver {
			DeliverPolicy::All => nats::jetstream::consumer::DeliverPolicy::All,
			DeliverPolicy::New => nats::jetstream::consumer::DeliverPolicy::New,
		};
		nats::jetstream::consumer::pull::Config {
			deliver_policy,
			durable_name: Some(name),
			..Default::default()
		}
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		self.stream
			.delete_consumer(&name)
			.await
			.map_err(Error::other)?;
		Ok(())
	}
}

impl Consumer {
	fn new(
		consumer: nats::jetstream::consumer::Consumer<nats::jetstream::consumer::pull::Config>,
	) -> Self {
		Self { consumer }
	}

	async fn info(&self) -> Result<ConsumerInfo, Error> {
		let info = self.consumer.get_info().await.map_err(Error::other)?;
		let info = ConsumerInfo {
			sequence: info.ack_floor.consumer_sequence,
		};
		Ok(info)
	}

	async fn subscribe<T>(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		let stream = self
			.consumer
			.stream()
			.max_messages_per_batch(1)
			.messages()
			.await
			.map_err(Error::other)?
			.filter_map(|result| async {
				match result {
					Ok(message) => {
						let (message, acker) = message.split();
						match T::deserialize(message.payload.clone()) {
							Ok(payload) => Some(Ok(Message {
								subject: message.subject.to_string(),
								payload,
								acker: acker.into(),
							})),
							Err(error) => Some(Err(error)),
						}
					},
					Err(error) => Some(Err(Error::other(error))),
				}
			});
		Ok(stream)
	}

	async fn batch_subscribe<T>(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		let stream = stream::try_unfold(
			(self.consumer.clone(), config),
			move |(consumer, config)| async move {
				let mut batch = consumer.batch();
				if let Some(max_bytes) = config.max_bytes {
					batch = batch.max_bytes(max_bytes.to_usize().unwrap());
				}
				if let Some(max_messages) = config.max_messages {
					batch = batch.max_messages(max_messages.to_usize().unwrap());
				}
				if let Some(timeout) = config.timeout {
					batch = batch.expires(timeout);
				}
				let batch = batch
					.messages()
					.await
					.map_err(|error| Error::Other(error.into()))?
					.filter_map(|result| async {
						match result {
							Ok(message) => {
								let (message, acker) = message.split();
								match T::deserialize(message.payload) {
									Ok(payload) => Some(Ok(Message {
										subject: message.subject.to_string(),
										payload,
										acker: acker.into(),
									})),
									Err(error) => Some(Err(error)),
								}
							},
							Err(error) => Some(Err(Error::Other(error))),
						}
					});
				Ok::<_, Error>(Some((batch, (consumer, config))))
			},
		)
		.try_flatten();
		Ok(stream)
	}
}

impl From<nats::jetstream::message::Acker> for Acker {
	fn from(value: nats::jetstream::message::Acker) -> Self {
		let ack = async move { value.ack().await.map_err(Error::other) };
		Acker::new(ack)
	}
}

impl crate::Messenger for Messenger {
	type Stream = Stream;

	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		self.publish(subject, payload).await
	}

	async fn subscribe<T>(
		&self,
		subject: String,
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + 'static, Error>
	where
		T: Payload + Clone,
	{
		self.subscribe(subject, group).await
	}

	async fn get_stream(&self, name: String) -> Result<Self::Stream, Error> {
		self.get_stream(name).await
	}

	async fn create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		self.create_stream(name, config).await
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		self.get_or_create_stream(name, config).await
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> {
		self.delete_stream(name)
	}

	async fn stream_publish<T>(
		&self,
		name: String,
		payload: T,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error>
	where
		T: Payload,
	{
		self.stream_publish(name, payload).await
	}

	async fn stream_batch_publish<T>(
		&self,
		name: String,
		payloads: Vec<T>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error>
	where
		T: Payload,
	{
		self.stream_batch_publish(name, payloads).await
	}
}

impl crate::Stream for Stream {
	type Consumer = Consumer;

	async fn info(&self) -> Result<StreamInfo, Error> {
		self.info().await
	}

	async fn get_consumer(&self, name: String) -> Result<Self::Consumer, Error> {
		self.get_consumer(name).await
	}

	async fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		self.create_consumer(name, config).await
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		self.get_or_create_consumer(name, config).await
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		self.delete_consumer(name).await
	}
}

impl crate::Consumer for Consumer {
	async fn info(&self) -> Result<ConsumerInfo, Error> {
		self.info().await
	}

	async fn subscribe<T>(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		self.subscribe().await
	}

	async fn batch_subscribe<T>(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		self.batch_subscribe(config).await
	}
}
