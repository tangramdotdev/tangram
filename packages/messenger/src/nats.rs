use std::fmt;

use crate::Message;
use async_nats as nats;
use bytes::Bytes;
use futures::prelude::*;

pub struct Messenger {
	pub client: nats::Client,
	pub jetstream: nats::jetstream::Context,
}

#[derive(Debug)]
pub enum Error {
	Publish(nats::PublishError),
	Subscribe(nats::SubscribeError),
	GetStream(nats::jetstream::context::GetStreamError),
	CreateStream(nats::jetstream::context::CreateStreamError),
	Consumer(nats::jetstream::stream::ConsumerError),
	Stream(nats::jetstream::consumer::StreamError),
	Messages(nats::jetstream::consumer::pull::MessagesError),
	PublishStream(nats::jetstream::context::PublishError),
}

impl std::error::Error for Error {}
impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Publish(e) => write!(f, "{e}"),
			Self::Subscribe(e) => write!(f, "{e}"),
			Self::GetStream(e) => write!(f, "{e}"),
			Self::CreateStream(e) => write!(f, "{e}"),
			Self::Consumer(e) => write!(f, "{e}"),
			Self::Stream(e) => write!(f, "{e}"),
			Self::Messages(e) => write!(f, "{e}"),
			Self::PublishStream(e) => write!(f, "{e}"),
		}
	}
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

	async fn destroy_stream(&self, name: String) -> Result<(), Error> {
		self.jetstream
			.delete_stream(name)
			.await
			.map_err(Error::GetStream)?;
		Ok(())
	}

	async fn stream_subscribe(
		&self,
		name: String,
		consumer_name: Option<String>,
	) -> Result<impl Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		let stream = self
			.jetstream
			.get_stream(&name)
			.await
			.map_err(Error::GetStream)?;

		// Create a consumer.
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: consumer_name,
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
			.map_ok(|message| Message {
				subject: message.subject.to_string(),
				payload: message.payload.clone(),
			})
			.map_err(Error::Messages);

		Ok(stream)
	}

	async fn stream_publish(&self, name: String, payload: Bytes) -> Result<(), Error> {
		self.jetstream
			.publish(name, payload)
			.await
			.map_err(Error::PublishStream)?
			.await
			.map_err(Error::PublishStream)?;
		Ok(())
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

	fn destroy_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.destroy_stream(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: bytes::Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.stream_publish(name, payload)
	}

	fn stream_subscribe(
		&self,
		name: String,
		consumer_name: Option<String>,
	) -> impl Future<
		Output = Result<
			impl Stream<Item = Result<Message, Self::Error>> + Send + 'static,
			Self::Error,
		>,
	> + Send {
		self.stream_subscribe(name, consumer_name)
	}
}
