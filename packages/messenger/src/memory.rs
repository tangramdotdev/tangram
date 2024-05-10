use crate::Message;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{
	future, stream::FuturesUnordered, Future, FutureExt as _, Stream, StreamExt as _,
	TryStreamExt as _,
};
use std::{convert::Infallible, sync::Arc};

pub struct Messenger(Arc<Inner>);

pub struct Inner {
	sender: async_broadcast::Sender<Message>,
	receiver: async_broadcast::Receiver<Message>,
	groups: DashMap<String, Group, fnv::FnvBuildHasher>,
}

struct Group {
	sender: async_channel::Sender<Message>,
	receiver: async_channel::Receiver<Message>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (sender, receiver) = async_broadcast::broadcast(1_000);
		let groups = DashMap::default();
		Self(Arc::new(Inner {
			sender,
			receiver,
			groups,
		}))
	}

	async fn publish(&self, subject: String, payload: Bytes) {
		let message = Message { subject, payload };
		self.sender.broadcast(message.clone()).await.unwrap();
		self.groups
			.iter()
			.map(|group| {
				let message = message.clone();
				async move { group.sender.send(message.clone()).await }
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
	}

	async fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Stream<Item = Message> + Send + 'static {
		match group {
			None => self
				.receiver
				.clone()
				.filter(move |message| future::ready(message.subject == subject))
				.left_stream(),
			Some(group) => {
				let group = self.groups.entry(group).or_insert_with(|| {
					let (sender, receiver) = async_channel::unbounded();
					Group { sender, receiver }
				});
				group
					.receiver
					.clone()
					.filter(move |message| future::ready(message.subject == subject))
					.right_stream()
			},
		}
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

impl std::ops::Deref for Messenger {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
