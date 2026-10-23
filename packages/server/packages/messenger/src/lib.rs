use {
	futures::{StreamExt as _, TryStreamExt as _},
	std::{
		pin::Pin,
		sync::{Arc, Mutex},
	},
	tangram_messenger::{self as messenger, Messenger as _},
};

#[cfg(test)]
mod tests;

/// A listener for the messenger's connection events, which receives the name of each event.
pub type Listener = Arc<dyn Fn(&'static str) -> ListenerFuture + Send + Sync>;

/// The future returned by a listener, which must be `Sync` because the NATS event callback's future must be `Sync`.
pub type ListenerFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

#[derive(Clone)]
enum Inner {
	Memory(tangram_messenger_memory::Messenger),
	#[cfg(feature = "nats")]
	Nats(tangram_messenger_nats::Messenger),
}

#[derive(Clone)]
pub struct Messenger {
	inner: Inner,
	instance: Option<String>,
	listeners: Listeners,
	region: Option<String>,
}

/// The listeners for the messenger's connection events.
#[derive(Clone, Default)]
pub struct Listeners(Arc<Mutex<Vec<Listener>>>);

impl Messenger {
	#[must_use]
	pub fn memory(
		config: &tangram_messenger_memory::Config,
		instance: Option<String>,
		region: Option<String>,
	) -> Self {
		let inner = Inner::Memory(tangram_messenger_memory::Messenger::new(config));
		let listeners = Listeners::default();
		Self {
			inner,
			instance,
			listeners,
			region,
		}
	}

	#[cfg(feature = "nats")]
	#[must_use]
	pub fn nats(
		client: async_nats::Client,
		listeners: Listeners,
		instance: Option<String>,
		region: Option<String>,
	) -> Self {
		let inner = Inner::Nats(tangram_messenger_nats::Messenger::new(client));
		Self {
			inner,
			instance,
			listeners,
			region,
		}
	}

	pub fn add_listener<F>(&self, listener: F)
	where
		F: Fn(&'static str) -> ListenerFuture + Send + Sync + 'static,
	{
		self.listeners.add(listener);
	}

	#[must_use]
	pub fn connected(&self) -> bool {
		match &self.inner {
			Inner::Memory(_) => true,
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger.connected(),
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

	pub async fn subscribe_to_region<T>(
		&self,
		region: Option<&str>,
		subject: String,
	) -> Result<
		impl futures::Stream<Item = Result<messenger::Message<T>, messenger::Error>> + Send + 'static,
		messenger::Error,
	>
	where
		T: messenger::Payload,
	{
		let prefix = self.prefix(region);
		let subject = Self::prefixed(&prefix, subject);
		let messages = match &self.inner {
			Inner::Memory(messenger) => messenger.subscribe(subject).await?.boxed(),
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger.subscribe(subject).await?.boxed(),
		};
		let messages = Self::unprefixed(&prefix, messages);
		Ok(messages)
	}

	fn subject_name(&self, region: Option<&str>, subject: String) -> String {
		let prefix = self.prefix(region);
		Self::prefixed(&prefix, subject)
	}

	/// Get the prefix of the subjects in a region, which is the instance and the region, each of which is optional.
	fn prefix(&self, region: Option<&str>) -> String {
		[self.instance.as_deref(), region]
			.into_iter()
			.flatten()
			.filter(|component| !component.is_empty())
			.collect::<Vec<_>>()
			.join(".")
	}

	fn prefixed(prefix: &str, subject: String) -> String {
		if prefix.is_empty() {
			subject
		} else {
			format!("{prefix}.{subject}")
		}
	}

	/// Remove the prefix from the subjects of the received messages, so that each message has the subject that it was published to.
	fn unprefixed<T>(
		prefix: &str,
		messages: futures::stream::BoxStream<
			'static,
			Result<messenger::Message<T>, messenger::Error>,
		>,
	) -> futures::stream::BoxStream<'static, Result<messenger::Message<T>, messenger::Error>>
	where
		T: Send + 'static,
	{
		if prefix.is_empty() {
			return messages;
		}
		let prefix = format!("{prefix}.");
		messages
			.map_ok(move |mut message| {
				if let Some(subject) = message.subject.strip_prefix(&prefix) {
					message.subject = subject.to_owned();
				}
				message
			})
			.boxed()
	}
}

impl Listeners {
	pub fn add<F>(&self, listener: F)
	where
		F: Fn(&'static str) -> ListenerFuture + Send + Sync + 'static,
	{
		self.0.lock().unwrap().push(Arc::new(listener));
	}

	pub async fn notify(&self, event: &'static str) {
		let listeners = self.0.lock().unwrap().clone();
		for listener in listeners {
			listener(event).await;
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
		self.subscribe_to_region(self.region.as_deref(), subject)
			.await
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
		let prefix = self.prefix(self.region.as_deref());
		let subject = Self::prefixed(&prefix, subject);
		let messages = match &self.inner {
			Inner::Memory(messenger) => messenger
				.queue_subscribe(subject, queue_group)
				.await?
				.boxed(),
			#[cfg(feature = "nats")]
			Inner::Nats(messenger) => messenger
				.queue_subscribe(subject, queue_group)
				.await?
				.boxed(),
		};
		let messages = Self::unprefixed(&prefix, messages);
		Ok(messages)
	}
}
