use crate::Message;
use async_nats as nats;
use futures::prelude::*;
use tangram_either::Either;

pub struct Messenger {
	pub client: nats::Client,
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client) -> Self {
		Self { client }
	}
}

impl crate::Messenger for Messenger {
	type Error = Either<nats::PublishError, nats::SubscribeError>;

	fn publish(
		&self,
		subject: String,
		message: bytes::Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		self.client.publish(subject, message).map_err(Either::Left)
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
				.map_err(Either::Right)
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
				.map_err(Either::Right)
				.right_future(),
		}
	}
}
