use {
	crate::{Delivery, Error, Message, Payload},
	async_broadcast as broadcast,
	bytes::Bytes,
	futures::{StreamExt as _, future},
	std::sync::Arc,
};

#[derive(Clone)]
pub struct Messenger {
	inner: Arc<MessengerInner>,
}

struct MessengerInner {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = broadcast::broadcast(1_000_000);
		sender.set_overflow(true);
		sender.set_await_active(false);
		let inner = Arc::new(MessengerInner {
			receiver: receiver.deactivate(),
			sender,
		});
		Self { inner }
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		let payload = payload.serialize()?;
		self.inner.sender.try_broadcast((subject, payload)).ok();
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		self.subscribe_with_delivery(subject, Delivery::All).await
	}

	async fn subscribe_with_delivery<T>(
		&self,
		subject: String,
		_delivery: Delivery,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload,
	{
		let stream =
			self.inner
				.receiver
				.activate_cloned()
				.filter_map(move |(subject_, payload)| {
					if subject_ != subject {
						return future::ready(None);
					}
					let message = T::deserialize(payload)
						.map(|payload| Message {
							subject: subject_,
							payload,
						})
						.map_err(Error::deserialization);
					future::ready(Some(message))
				});
		Ok(stream)
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::Messenger as _,
		futures::TryStreamExt as _,
		serde::{Deserialize, Serialize},
	};

	#[tokio::test]
	async fn subscribes_only_to_matching_subject() {
		let messenger = Messenger::new();
		let mut stream = messenger
			.subscribe::<Bytes>("subject1".into())
			.await
			.unwrap();

		messenger
			.publish("subject2".into(), Bytes::from_static(b"skip"))
			.await
			.unwrap();
		messenger
			.publish("subject1".into(), Bytes::from_static(b"hello"))
			.await
			.unwrap();

		let message = stream.try_next().await.unwrap().unwrap();
		assert_eq!(message.payload, Bytes::from_static(b"hello"));
	}

	#[derive(Debug, Deserialize, PartialEq, Serialize)]
	struct Event {
		value: String,
	}

	#[tokio::test]
	async fn subscribes_with_typed_payloads() {
		let messenger = Messenger::new();
		let mut stream = messenger
			.subscribe::<crate::payload::Json<Event>>("subject".into())
			.await
			.unwrap();

		messenger
			.publish(
				"subject".into(),
				crate::payload::Json(Event {
					value: "hello".into(),
				}),
			)
			.await
			.unwrap();

		let message = stream.try_next().await.unwrap().unwrap();
		assert_eq!(
			message.payload.0,
			Event {
				value: "hello".into(),
			}
		);
	}
}
