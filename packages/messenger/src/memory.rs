use crate::Message;
use bytes::Bytes;
use futures::{FutureExt as _, Stream, StreamExt as _, future};
use std::{convert::Infallible, ops::Deref, sync::Arc};

pub struct Messenger(Arc<Inner>);

pub struct Inner {
	sender: async_broadcast::Sender<Message>,
	receiver: async_broadcast::InactiveReceiver<Message>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);
		Self(Arc::new(Inner { sender, receiver }))
	}

	async fn publish(&self, subject: String, payload: Bytes) {
		let message = Message { subject, payload };
		self.sender.try_broadcast(message.clone()).ok();
	}

	async fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> impl Stream<Item = Message> + Send + 'static {
		self.receiver
			.activate_cloned()
			.filter(move |message| future::ready(message.subject == subject))
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Messenger for Messenger {
	type Error = Infallible;

	fn publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		self.publish(subject, payload).map(Ok)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Self::Error>> {
		self.subscribe(subject, group).map(Ok)
	}
}

impl Deref for Messenger {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
