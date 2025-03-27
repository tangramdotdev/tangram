use crate::Message;
use async_broadcast as broadcast;
use async_channel as channel;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{StreamExt as _, TryFutureExt, future};
use std::{fmt, ops::Deref, sync::Arc};
use tokio::sync::RwLock;

pub struct Messenger(Arc<Inner>);
#[derive(Debug)]
pub enum Error {
	NotFound,
	StreamClosed,
	SendError(broadcast::SendError<Message>),
	StreamSendError(channel::SendError<Message>),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::NotFound => write!(f, "stream not found"),
			Error::StreamClosed => write!(f, "stream closed"),
			Error::SendError(e) => write!(f, "{e}"),
			Error::StreamSendError(e) => write!(f, "{e}"),
		}
	}
}

pub struct Inner {
	receiver: broadcast::InactiveReceiver<Message>,
	sender: broadcast::Sender<Message>,
	streams: DashMap<String, Stream>,
}

struct Stream {
	sender: channel::Sender<Message>,
	state: Arc<State>,
	task: Option<tokio::task::JoinHandle<()>>,
}

struct State {
	notify: tokio::sync::watch::Sender<()>,
	receivers: RwLock<Vec<channel::Sender<Message>>>,
}

impl Stream {
	pub fn new() -> Self {
		let (sender, receiver) = channel::bounded::<Message>(256);
		let (notify, mut changed) = tokio::sync::watch::channel(());
		let receivers = RwLock::new(Vec::new());
		let state = Arc::new(State { notify, receivers });

		// Spawn a task to drain the input buffer.
		let task = tokio::task::spawn({
			let state = state.clone();
			async move {
				while let Ok(next_message) = receiver.recv().await {
					// If there are no receivers, wait until one is added.
					let is_empty = state.receivers.read().await.is_empty();
					if is_empty {
						// If the sender drops, this task is canceled and we can exit early.
						if changed.changed().await.is_err() {
							break;
						}
					}

					// Send the message to all current receivers.
					let receivers = state.receivers.read().await;
					for sender in receivers.iter() {
						sender.send(next_message.clone()).await.ok();
					}
				}
			}
		});

		Self {
			sender,
			state,
			task: Some(task),
		}
	}

	async fn subscribe(&self) -> channel::Receiver<Message> {
		let mut receivers = self.state.receivers.write().await;
		let (sender, receiver) = channel::bounded(256);
		receivers.push(sender);
		self.state.notify.send_replace(());
		receiver
	}

	async fn close(&mut self) {
		self.sender.close();
		let mut receivers = self.state.receivers.write().await;
		while let Some(receiver) = receivers.pop() {
			receiver.close();
		}
		if let Some(task) = &self.task.take() {
			task.abort();
		}
	}
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);
		sender.set_await_active(false);
		let streams = DashMap::new();
		Self(Arc::new(Inner {
			receiver,
			sender,
			streams,
		}))
	}

	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		self.sender.try_broadcast(Message { subject, payload }).ok();
		Ok(())
	}

	async fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		// This guarantees that the memory variant has the same semantics as NATs.
		if let Ok(stream) = self.stream_subscribe(subject.clone()).await {
			return Ok(stream.left_stream());
		}
		let receiver = self
			.receiver
			.activate_cloned()
			.filter(move |msg| future::ready(msg.subject == subject))
			.right_stream();
		Ok(receiver)
	}

	async fn create_stream(&self, name: String) -> Result<(), Error> {
		self.streams.entry(name).or_insert_with(Stream::new);
		Ok(())
	}

	async fn destroy_stream(&self, name: String) -> Result<(), Error> {
		let (_, mut stream) = self.streams.remove(&name).ok_or_else(|| Error::NotFound)?;
		stream.close().await;
		Ok(())
	}

	async fn stream_publish(&self, name: String, payload: Bytes) -> Result<(), Error> {
		let sender = self
			.streams
			.get(&name)
			.ok_or_else(|| Error::NotFound)?
			.sender
			.clone();
		let msg = Message {
			subject: name,
			payload,
		};
		sender.send(msg).await.map_err(Error::StreamSendError)?;
		Ok(())
	}

	async fn stream_subscribe(
		&self,
		name: String,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		let receiver = self
			.streams
			.get(&name)
			.ok_or_else(|| Error::NotFound)?
			.subscribe()
			.await;
		Ok(receiver)
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

	fn create_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.create_stream(name)
	}

	fn destroy_stream(&self, name: String) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.destroy_stream(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<(), Self::Error>> + Send {
		self.stream_publish(name, payload)
	}

	fn stream_subscribe(
		&self,
		name: String,
		_consumer_name: Option<String>,
	) -> impl Future<
		Output = Result<
			impl futures::Stream<Item = Result<Message, Self::Error>> + Send + 'static,
			Self::Error,
		>,
	> + Send {
		self.stream_subscribe(name)
			.map_ok(|stream| stream.map(Ok::<_, Self::Error>))
	}
}

impl Deref for Messenger {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	use super::Messenger;
	use futures::stream::TryStreamExt as _;

	#[tokio::test]
	async fn stream() {
		let messenger: Messenger = Messenger::new();

		// Create a stream, publish a message before anyone has subscribed.
		messenger.create_stream("stream".into()).await.unwrap();
		messenger
			.stream_publish("stream".into(), b"hello!".to_vec().into())
			.await
			.unwrap();

		// Create some subscribers.
		let sub1 = crate::Messenger::stream_subscribe(&messenger, "stream".into(), None)
			.await
			.unwrap();
		let sub2 = crate::Messenger::stream_subscribe(&messenger, "stream".into(), None)
			.await
			.unwrap();

		// Ensure both subscribers got the message.
		let m = std::pin::pin!(sub1).try_next().await.unwrap().unwrap();
		assert_eq!(m.payload.as_ref(), b"hello!");
		let m = std::pin::pin!(sub2).try_next().await.unwrap().unwrap();
		assert_eq!(m.payload.as_ref(), b"hello!");
	}
}
