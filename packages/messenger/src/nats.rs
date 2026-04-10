use {
	crate::{Error, Message, Payload},
	async_nats as nats,
	futures::StreamExt as _,
};

#[derive(Clone)]
pub struct Messenger {
	client: nats::Client,
	id: Option<String>,
}

impl Messenger {
	#[must_use]
	pub fn new(client: nats::Client, id: Option<String>) -> Self {
		Self { client, id }
	}

	fn subject_name(&self, name: String) -> String {
		match &self.id {
			Some(id) => format!("{id}.{name}"),
			None => name,
		}
	}
}

impl crate::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		let subject = self.subject_name(subject);
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
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		let subject = self.subject_name(subject);
		let subscriber = match group {
			None => self
				.client
				.subscribe(subject)
				.await
				.map_err(Error::other)?
				.left_stream(),
			Some(group) => self
				.client
				.queue_subscribe(subject, group)
				.await
				.map_err(Error::other)?
				.right_stream(),
		};
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
