use crate::Message;
use bytes::Bytes;
use core::fmt;
use dashmap::DashMap;
use futures::{Stream, StreamExt as _, future};
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
	subjects: DashMap<String, Subject, fnv::FnvBuildHasher>,
}

struct Subject {
	sender: async_broadcast::Sender<Message>,
	receiver: async_broadcast::InactiveReceiver<Message>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let subjects = DashMap::default();
		Self(Arc::new(Inner { subjects }))
	}

	async fn create_subject(&self, subject: String) -> Result<(), Error> {
		self.subjects.entry(subject).or_insert_with(|| {
			let (sender, receiver) = async_broadcast::broadcast(128);
			let receiver = receiver.deactivate();
			Subject { sender, receiver }
		});
		Ok(())
	}

	async fn close_subject(&self, subject: String) -> Result<(), Error> {
		self.subjects.remove(&subject);
		Ok(())
	}

	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		let sender = self.subjects.get(&subject).ok_or(Error::NotFound)?;
		let message = Message { subject, payload };
		sender.sender.broadcast_direct(message).await.ok().flatten();
		Ok(())
	}

	async fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> Result<impl Stream<Item = Message> + Send + 'static, Error> {
		Ok(self
			.subjects
			.get(&subject)
			.ok_or(Error::NotFound)?
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

	fn create_subject(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		self.create_subject(subject)
	}

	fn close_subject(&self, subject: String) -> impl Future<Output = Result<(), Self::Error>> {
		self.close_subject(subject)
	}

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
	) -> impl Future<Output = Result<impl Stream<Item = Message> + 'static, Self::Error>> {
		self.subscribe(subject, group)
	}
}

impl Deref for Messenger {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
