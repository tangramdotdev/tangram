use tangram_messenger as messenger;

#[derive(derive_more::IsVariant)]
pub enum Messenger {
	#[cfg(feature = "nats")]
	Nats(messenger::nats::Messenger),
	Unix(messenger::unix::Messenger),
}

impl messenger::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), messenger::Error>
	where
		T: messenger::Payload,
	{
		match self {
			#[cfg(feature = "nats")]
			Self::Nats(messenger) => messenger.publish(subject, payload).await,
			Self::Unix(messenger) => messenger.publish(subject, payload).await,
		}
	}

	async fn subscribe<T>(
		&self,
		subject: String,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message<T>, messenger::Error>> + Send + 'static,
		messenger::Error,
	>
	where
		T: messenger::Payload,
	{
		match self {
			#[cfg(feature = "nats")]
			Self::Nats(messenger) => messenger
				.subscribe(subject)
				.await
				.map(futures::StreamExt::boxed),
			Self::Unix(messenger) => messenger
				.subscribe(subject)
				.await
				.map(futures::StreamExt::boxed),
		}
	}

	async fn subscribe_with_delivery<T>(
		&self,
		subject: String,
		delivery: messenger::Delivery,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message<T>, messenger::Error>> + Send + 'static,
		messenger::Error,
	>
	where
		T: messenger::Payload,
	{
		match self {
			#[cfg(feature = "nats")]
			Self::Nats(messenger) => messenger
				.subscribe_with_delivery(subject, delivery)
				.await
				.map(futures::StreamExt::boxed),
			Self::Unix(messenger) => messenger
				.subscribe_with_delivery(subject, delivery)
				.await
				.map(futures::StreamExt::boxed),
		}
	}
}
