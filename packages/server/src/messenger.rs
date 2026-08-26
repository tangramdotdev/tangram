use tangram_messenger::{self as messenger, Messenger as _};

#[derive(Clone)]
enum Inner {
	Memory(messenger::memory::Messenger),
	#[cfg(feature = "nats")]
	Nats(messenger::nats::Messenger),
}

#[derive(Clone)]
pub struct Messenger {
	inner: Inner,
	instance: Option<String>,
	region: Option<String>,
}

impl Messenger {
	#[must_use]
	pub fn memory(instance: Option<String>, region: Option<String>) -> Self {
		let inner = Inner::Memory(messenger::memory::Messenger::new());
		Self {
			inner,
			instance,
			region,
		}
	}

	#[cfg(feature = "nats")]
	#[must_use]
	pub fn nats(
		client: async_nats::Client,
		instance: Option<String>,
		region: Option<String>,
	) -> Self {
		let inner = Inner::Nats(messenger::nats::Messenger::new(client));
		Self {
			inner,
			instance,
			region,
		}
	}

	pub async fn publish_to_region<T>(
		&self,
		region: Option<&str>,
		subject: String,
		payload: T,
	) -> Result<(), messenger::Error>
	where
		T: messenger::Payload,
	{
		let subject = self.subject_name(region, subject);
		match &self.inner {
			Inner::Memory(messenger) => messenger.publish(subject, payload).await,
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger.publish(subject, payload).await,
		}
	}

	fn subject_name(&self, region: Option<&str>, subject: String) -> String {
		let prefix = [self.instance.as_deref(), region]
			.into_iter()
			.flatten()
			.filter(|component| !component.is_empty())
			.collect::<Vec<_>>()
			.join(".");
		if prefix.is_empty() {
			subject
		} else {
			format!("{prefix}.{subject}")
		}
	}
}

impl messenger::Messenger for Messenger {
	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), messenger::Error>
	where
		T: messenger::Payload,
	{
		self.publish_to_region(self.region.as_deref(), subject, payload)
			.await
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
		let subject = self.subject_name(self.region.as_deref(), subject);
		match &self.inner {
			Inner::Memory(messenger) => messenger
				.subscribe(subject)
				.await
				.map(futures::StreamExt::boxed),
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger
				.subscribe(subject)
				.await
				.map(futures::StreamExt::boxed),
		}
	}

	async fn queue_subscribe<T>(
		&self,
		subject: String,
		queue_group: String,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message<T>, messenger::Error>> + Send + 'static,
		messenger::Error,
	>
	where
		T: messenger::Payload,
	{
		let subject = self.subject_name(self.region.as_deref(), subject);
		match &self.inner {
			Inner::Memory(messenger) => messenger
				.queue_subscribe(subject, queue_group)
				.await
				.map(futures::StreamExt::boxed),
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger
				.queue_subscribe(subject, queue_group)
				.await
				.map(futures::StreamExt::boxed),
		}
	}
}
