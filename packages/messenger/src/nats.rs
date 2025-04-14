use crate::{Acker, Message, PublishFuture, Published, StreamInfo};
use async_nats as nats;
use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, prelude::*};

pub struct Messenger {
	pub client: nats::Client,
	pub jetstream: nats::jetstream::Context,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	Publish(nats::PublishError),
	Subscribe(nats::SubscribeError),
	GetStream(nats::jetstream::context::GetStreamError),
	Info(nats::jetstream::context::RequestError),
	CreateStream(nats::jetstream::context::CreateStreamError),
	Consumer(nats::jetstream::stream::ConsumerError),
	Stream(nats::jetstream::consumer::StreamError),
	Messages(nats::jetstream::consumer::pull::MessagesError),
	PublishStream(nats::jetstream::context::PublishError),
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client) -> Self {
		let jetstream = nats::jetstream::new(client.clone());
		Self { client, jetstream }
	}

	async fn create_stream(&self, name: String) -> Result<(), Error> {
		let stream_config = nats::jetstream::stream::Config {
			name,
			max_messages: 256,
			..Default::default()
		};
		self.jetstream
			.create_stream(stream_config)
			.await
			.map_err(Error::CreateStream)?;
		Ok(())
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		self.jetstream
			.delete_stream(name)
			.await
			.map_err(Error::GetStream)?;
		Ok(())
	}

	async fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> Result<impl Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		let stream = self
			.jetstream
			.get_stream(&name)
			.await
			.map_err(Error::GetStream)?;

		// Create a consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: consumer,
			..Default::default()
		};

		let consumer = stream
			.get_or_create_consumer(&name, consumer_config)
			.await
			.map_err(Error::Consumer)?;

		// Create the stream.
		let stream = consumer
			.stream()
			.messages()
			.await
			.map_err(Error::Stream)?
			.map_ok(|message| {
				let (message, acker) = message.split();
				Message {
					subject: message.subject.to_string(),
					payload: message.payload.clone(),
					acker: acker.into(),
				}
			})
			.map_err(Error::Messages);

		Ok(stream)
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<PublishFuture<Error>, Error> {
		let future = self
			.jetstream
			.publish(name, payload)
			.await
			.map_err(Error::PublishStream)?;
		let inner = async move {
			future
				.await
				.map(|ack| Published {
					sequence: ack.sequence,
				})
				.map_err(Error::PublishStream)
		}
		.boxed();
		Ok(PublishFuture { inner })
	}

	async fn stream_info(&self, name: String) -> Result<StreamInfo, Error> {
		let mut stream = self
			.jetstream
			.get_stream(name)
			.await
			.map_err(Error::GetStream)?;
		let info = stream.info().await.map_err(Error::Info)?;
		Ok(StreamInfo {
			first_sequence: Some(info.state.first_sequence),
			last_sequence: info.state.last_sequence,
		})
	}
}

impl crate::Messenger for Messenger {
	type Error = Error;

	fn publish(
		&self,
		subject: String,
		message: bytes::Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		self.client
			.publish(subject, message)
			.map_err(Error::Publish)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Self::Error>> {
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
				.map_err(Error::Subscribe)
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
				.map_err(Error::Subscribe)
				.right_future(),
		}
	}

	fn create_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.create_stream(name)
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.delete_stream(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: bytes::Bytes,
	) -> impl Future<Output = Result<PublishFuture<Self::Error>, Self::Error>> + Send {
		self.stream_publish(name, payload)
	}

	fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> impl Future<
		Output = Result<
			impl Stream<Item = Result<Message, Self::Error>> + Send + 'static,
			Self::Error,
		>,
	> + Send {
		self.stream_subscribe(name, consumer)
	}

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Self::Error>> + Send {
		self.stream_info(name)
	}
}

impl From<nats::jetstream::message::Acker> for Acker {
	fn from(value: nats::jetstream::message::Acker) -> Self {
		let ack = async move { value.ack().await };
		let retry = future::ready(());
		Acker::new(ack, retry)
	}
}
