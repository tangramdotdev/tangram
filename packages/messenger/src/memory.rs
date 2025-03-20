use crate::Message;
use bytes::Bytes;
use core::fmt;
use dashmap::DashMap;
use futures::{StreamExt, future};
use std::{ops::Deref, sync::Arc};

pub struct Messenger(Arc<Inner>);

#[derive(Debug)]

pub enum Error {
	NotFound,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::NotFound => write!(f, "subject not found"),
		}
	}
}

pub struct Inner {
	global: Stream,
	streams: Streams,
}

pub struct Streams(DashMap<String, Stream, fnv::FnvBuildHasher>);

struct Stream {
	sender: async_broadcast::Sender<Message>,
	receiver: async_broadcast::InactiveReceiver<Message>,
}

impl Streams {
	pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		self.0
			.get(&subject)
			.ok_or(Error::NotFound)?
			.sender
			.broadcast_direct(Message { subject: subject.clone(), payload })
			.await
			.ok();
		Ok(())
	}

	pub async fn create_stream(&self, subject: String) -> Result<(), Error> {
		self.0.entry(subject).or_insert_with(|| {
			let (mut sender, receiver) = async_broadcast::broadcast(128);
			sender.set_await_active(false);
			sender.set_overflow(false);
			let receiver = receiver.deactivate();
			Stream { sender, receiver }
		});
		Ok(())
	}

	pub async fn close_stream(&self, subject: String) -> Result<(), Error> {
		let (_, stream) = self.0.remove(&subject).ok_or(Error::NotFound)?;
		stream.sender.close();
		Ok(())
	}

	pub async fn subscribe(
		&self,
		subject: String,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		self.0
			.get(&subject)
			.ok_or(Error::NotFound)
			.map(|s| s.receiver.activate_cloned())
	}
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);

		let global = Stream { sender, receiver };
		let streams = Streams(DashMap::default());
		Self(Arc::new(Inner { global, streams }))
	}

	#[must_use]
	pub fn streams(&self) -> &Streams {
		&self.streams
	}

	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		self.global
			.sender
			.try_broadcast(Message { subject, payload })
			.ok();
		Ok(())
	}

	async fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		Ok(self
			.global
			.receiver
			.activate_cloned()
			.filter(move |message| future::ready(message.subject == subject)))
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Messenger for Messenger {
	type Error = Error;

	fn publish(
		&self,
		subject: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> {
		self.publish(subject, payload)
	}

	fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> impl Future<Output = Result<impl futures::Stream<Item = Message> + 'static, Self::Error>>
	{
		self.subscribe(subject, group)
	}
}

impl Deref for Messenger {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
