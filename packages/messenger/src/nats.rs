use {
	crate::{Error, Message, Payload},
	async_nats as nats,
	futures::StreamExt as _,
};

#[derive(Clone)]
pub struct Messenger {
	client: nats::Client,
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client) -> Self {
		Self { client }
	}
}

impl crate::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		let payload = payload.serialize()?;
		self.client
			.publish(subject, payload)
			.await
			.map_err(Error::other)?;
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		let subscriber = self.client.subscribe(subject).await.map_err(Error::other)?;
		let stream = subscriber.map(|message| {
			T::deserialize(message.payload)
				.map(|payload| Message {
					subject: message.subject.to_string(),
					payload,
				})
				.map_err(Error::deserialization)
		});
		Ok(stream)
	}

	async fn queue_subscribe<T>(
		&self,
		subject: String,
		queue_group: String,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		let subscriber = self
			.client
			.queue_subscribe(subject, queue_group)
			.await
			.map_err(Error::other)?;
		let stream = subscriber.map(|message| {
			T::deserialize(message.payload)
				.map(|payload| Message {
					subject: message.subject.to_string(),
					payload,
				})
				.map_err(Error::deserialization)
		});
		Ok(stream)
	}
}
