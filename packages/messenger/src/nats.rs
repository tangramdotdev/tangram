use crate::{Acker, BatchConfig, Error, Message, StreamConfig, StreamInfo, StreamPublishInfo};
use async_nats as nats;
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, prelude::*};
use num::ToPrimitive;

pub struct Messenger {
	pub client: nats::Client,
	pub jetstream: nats::jetstream::Context,
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client) -> Self {
		let jetstream = nats::jetstream::new(client.clone());
		Self { client, jetstream }
	}

	fn publish(&self, subject: String, message: Bytes) -> impl Future<Output = Result<(), Error>> {
		self.client.publish(subject, message).map_err(Error::other)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Error>> {
		match group {
			None => self
				.client
				.subscribe(subject)
				.map_ok(|subscriber| {
					subscriber
						.map(|message| Message {
							subject: message.subject.to_string(),
							payload: message.payload,
							acker: Acker::default(),
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
						.map(|message| Message {
							subject: message.subject.to_string(),
							payload: message.payload,
							acker: Acker::default(),
						})
						.right_stream()
				})
				.map_err(Error::other)
				.right_future(),
		}
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<(), Error> {
		let max_bytes = config
			.max_bytes
			.map(|value| value.to_i64().unwrap())
			.unwrap_or_default();
		let max_messages = config
			.max_messages
			.map(|value| value.to_i64().unwrap())
			.unwrap_or_default();
		let stream_config = nats::jetstream::stream::Config {
			name,
			max_bytes,
			max_messages,
			..Default::default()
		};
		self.jetstream
			.create_stream(stream_config)
			.await
			.map_err(Error::other)?;
		Ok(())
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		self.jetstream
			.delete_stream(name)
			.await
			.map_err(Error::other)?;
		Ok(())
	}

	async fn stream_info(&self, name: String) -> Result<StreamInfo, Error> {
		let mut stream = self
			.jetstream
			.get_stream(name)
			.await
			.map_err(Error::other)?;
		let info = stream.info().await.map_err(Error::other)?;
		let info = StreamInfo {
			first_sequence: Some(info.state.first_sequence),
			last_sequence: info.state.last_sequence,
		};
		Ok(info)
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error> {
		let future = self
			.jetstream
			.publish(name, payload)
			.await
			.map_err(Error::other)?;
		let future = async move {
			future
				.await
				.map(|ack| StreamPublishInfo {
					sequence: ack.sequence,
				})
				.map_err(Error::other)
		}
		.boxed();
		Ok(future)
	}

	async fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> Result<impl Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		// Get the stream.
		let stream = self
			.jetstream
			.get_stream(&name)
			.await
			.map_err(Error::other)?;

		// Get or create the consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: consumer,
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer(&name, consumer_config)
			.await
			.map_err(Error::other)?;

		// Create the stream.
		let stream = consumer
			.stream()
			.messages()
			.await
			.map_err(Error::other)?
			.map_ok(|message| {
				let (message, acker) = message.split();
				Message {
					subject: message.subject.to_string(),
					payload: message.payload.clone(),
					acker: acker.into(),
				}
			})
			.map_err(Error::other);

		Ok(stream)
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error> {
		Ok(future::ok(todo!()))
	}

	async fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: BatchConfig,
	) -> Result<impl Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		// Get the stream.
		let stream = self
			.jetstream
			.get_stream(&name)
			.await
			.map_err(Error::other)?;

		// Get or create the consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: consumer,
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer(&name, consumer_config)
			.await
			.map_err(Error::other)?;

		// Create the stream.
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
		let stream = batch
			.messages()
			.await
			.map_err(Error::other)?
			.map_ok(|message| {
				let (message, acker) = message.split();
				Message {
					subject: message.subject.to_string(),
					payload: message.payload.clone(),
					acker: acker.into(),
				}
			})
			.map_err(Error::other);

		Ok(stream)
	}
}

impl crate::Messenger for Messenger {
	fn publish(&self, subject: String, payload: Bytes) -> impl Future<Output = Result<(), Error>> {
		self.publish(subject, payload)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Error>> {
		self.subscribe(subject, group)
	}

	fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> impl Future<Output = Result<(), Error>> + Send {
		self.create_stream(name, config)
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> + Send {
		self.delete_stream(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error>> + Send
	{
		self.stream_publish(name, payload)
	}

	fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Error>> + Send + 'static, Error>,
	> + Send {
		self.stream_subscribe(name, consumer)
	}

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Error>> + Send {
		self.stream_info(name)
	}

	fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> impl Future<
		Output = Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error>,
	> + Send {
		self.stream_batch_publish(name, payloads)
	}

	fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: crate::BatchConfig,
	) -> impl Future<
		Output = Result<impl Stream<Item = Result<Message, Error>> + Send + 'static, Error>,
	> + Send {
		self.stream_batch_subscribe(name, consumer, config)
	}
}

impl From<nats::jetstream::message::Acker> for Acker {
	fn from(value: nats::jetstream::message::Acker) -> Self {
		let ack = async move { value.ack().await.map_err(Error::other) };
		let retry = future::ready(());
		Acker::new(ack, retry)
	}
}
