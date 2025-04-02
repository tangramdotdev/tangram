use crate::{Acker, Message, StdError, StreamInfo};
use async_broadcast as broadcast;
use async_channel as channel;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{StreamExt as _, TryFutureExt, future, stream::FuturesUnordered};
use std::{
	collections::BTreeSet,
	ops::Deref,
	sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
};
use tokio::sync::RwLock;

pub struct Messenger(Arc<Inner>);

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	#[display("stream not found")]
	NotFound,
	#[display("stream closed")]
	StreamClosed,
	#[display("failed to publish to subject")]
	SendError,
	#[display("failed to publish to stream")]
	StreamSendError,
}

pub struct Inner {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
	streams: DashMap<String, Stream>,
}

struct Stream {
	sender: channel::Sender<(u64, Bytes)>,
	state: Arc<State>,
}

struct State {
	last_sequence: RwLock<u64>,
	notify: tokio::sync::watch::Sender<()>,
	pending: RwLock<BTreeSet<u64>>,
	receivers: RwLock<Vec<channel::Sender<(u64, Message)>>>,
	task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Stream {
	pub fn new(subject: &str) -> Self {
		let subject = subject.to_owned();
		let (sender, receiver) = channel::bounded::<(u64, Bytes)>(256);
		let (notify, mut changed) = tokio::sync::watch::channel(());
		let receivers = RwLock::new(Vec::new());
		let last_sequence = RwLock::new(0);
		let state = Arc::new(State {
			notify,
			receivers,
			last_sequence,
			pending: RwLock::new(BTreeSet::new()),
			task: Mutex::new(None),
		});

		// Spawn a task to drain the input buffer.
		let task = tokio::task::spawn({
			let state = Arc::downgrade(&state);
			let subject = subject.clone();
			async move {
				// Receive the next message.
				while let Ok((sequence_number, payload)) = receiver.recv().await {
					// Exit early if the state has been dropped.
					let Some(state) = state.upgrade() else {
						break;
					};

					// If there are no receivers, wait until one is added.
					let is_empty = state.receivers.read().await.is_empty();
					if is_empty {
						// If the sender drops, this task is canceled and we can exit early.
						if changed.changed().await.is_err() {
							break;
						}
					}

					// Acquire a lock on all current receivers. This implies that a message cannot be delivered to a receiver created after the message was sent, unless that is the first receiver.
					let receivers = state.receivers.read().await;

					// Create a counter to track how many receivers have ack'd the message.
					let counter = Arc::new(AtomicU64::new(receivers.len().try_into().unwrap()));

					// Deliver the payload to each consumer.
					receivers
						.iter()
						.map(|consumer| {
							deliver_stream_message(
								consumer,
								&counter,
								&payload,
								sequence_number,
								&state,
								&subject,
							)
						})
						.collect::<FuturesUnordered<_>>()
						.collect::<()>()
						.await;
				}
			}
		});
		state.task.lock().unwrap().replace(task);
		Self { sender, state }
	}

	async fn publish(&self, payload: Bytes) -> Result<(), Error> {
		// Acquire a new sequence number.
		let mut last_sequence = self.state.last_sequence.write().await;
		let sequence_number = *last_sequence;
		*last_sequence += 1;

		// Update the pending state.
		self.state.pending.write().await.insert(sequence_number);

		// Send the message.
		self.sender
			.send((sequence_number, payload))
			.await
			.map_err(|_| Error::StreamSendError)?;

		Ok(())
	}

	async fn subscribe(&self) -> channel::Receiver<(u64, Message)> {
		let mut receivers = self.state.receivers.write().await;
		let (sender, receiver) = channel::bounded(256);
		receivers.push(sender);
		self.state.notify.send_replace(());
		receiver
	}

	async fn close(&self) {
		self.sender.close();
		let mut receivers = self.state.receivers.write().await;
		while let Some(receiver) = receivers.pop() {
			receiver.close();
		}
		if let Some(task) = &self.state.task.lock().unwrap().take() {
			task.abort();
		}
	}
}

impl Drop for State {
	fn drop(&mut self) {
		let Some(task) = self.task.lock().unwrap().take() else {
			return;
		};
		task.abort();
	}
}

fn deliver_stream_message(
	consumer: &channel::Sender<(u64, Message)>,
	counter: &Arc<AtomicU64>,
	payload: &Bytes,
	sequence_number: u64,
	state: &Arc<State>,
	subject: &str,
) -> impl Future<Output = ()> + Send {
	let state = state.clone();
	let consumer = consumer.clone();
	let counter = counter.clone();
	let subject = subject.to_string();
	let payload = payload.clone();
	async move {
		let ack = {
			let state = state.clone();
			let counter = counter.clone();
			async move {
				// Decrement the counter.
				let counter = counter.fetch_sub(1, Ordering::AcqRel);
				if counter > 1 {
					return Ok(());
				}

				// Mark the message as received.
				state.pending.write().await.remove(&sequence_number);

				Ok::<_, StdError>(())
			}
		};

		let retry = {
			let consumer = consumer.clone();
			let state = state.clone();
			let counter = counter.clone();
			let subject = subject.clone();
			let payload = payload.clone();
			async move {
				if consumer.is_closed() {
					return;
				}
				deliver_stream_message(
					&consumer,
					&counter,
					&payload,
					sequence_number,
					&state,
					&subject,
				)
				.await;
			}
		};

		let acker = Acker::new(ack, retry);
		let message = Message {
			subject: subject.clone(),
			payload: payload.clone(),
			acker,
		};

		consumer.send((sequence_number, message)).await.ok();
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
		self.sender.try_broadcast((subject, payload)).ok();
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
			.filter_map(move |(subject_, payload)| {
				future::ready({
					(subject_ == subject).then(|| Message {
						subject: subject_,
						payload,
						acker: Acker::default(),
					})
				})
			})
			.right_stream();
		Ok(receiver)
	}

	async fn create_stream(&self, name: String) -> Result<(), Error> {
		self.streams
			.entry(name.clone())
			.or_insert_with(|| Stream::new(&name));
		Ok(())
	}

	async fn destroy_stream(&self, name: String) -> Result<(), Error> {
		// Close the stream.
		{
			let stream = self.streams.get(&name).ok_or(Error::NotFound)?;
			stream.close().await;
		}

		// Remove the stream.
		self.streams.remove(&name);

		Ok(())
	}

	async fn stream_publish(&self, name: String, payload: Bytes) -> Result<(), Error> {
		self.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.publish(payload)
			.await
	}

	async fn stream_subscribe(
		&self,
		name: String,
	) -> Result<impl futures::Stream<Item = Message> + Send + 'static, Error> {
		let receiver = self
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.subscribe()
			.await
			.map(move |(_, msg)| msg);
		Ok(receiver)
	}

	async fn stream_info(&self, name: String) -> Result<StreamInfo, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?;
		let last_sequence = *stream.state.last_sequence.read().await;
		let first_sequence = stream.state.pending.read().await.iter().min().copied();
		Ok(StreamInfo {
			first_sequence,
			last_sequence,
		})
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

	fn stream_info(
		&self,
		name: String,
	) -> impl Future<Output = Result<crate::StreamInfo, Self::Error>> + Send {
		self.stream_info(name)
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
	use futures::{StreamExt, stream::TryStreamExt as _};

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
		m.split().1.ack().await.ok();

		let m = std::pin::pin!(sub2).try_next().await.unwrap().unwrap();
		assert_eq!(m.payload.as_ref(), b"hello!");
		m.split().1.ack().await.ok();
	}

	#[tokio::test]
	async fn messages_in_order() {
		let messenger: Messenger = Messenger::new();

		messenger.create_stream("stream".into()).await.unwrap();

		// Create a subscriber
		let stream = messenger.stream_subscribe("stream".into()).await.unwrap();

		// Spawn a task to dump messages to the stream.
		tokio::spawn(async move {
			for i in 1..1_000_000u32 {
				let bytes = i.to_be_bytes().to_vec().into();
				messenger
					.stream_publish("stream".into(), bytes)
					.await
					.unwrap();
			}
			messenger.destroy_stream("stream".into()).await.unwrap();
		});

		// Drain the subscriber.
		let mut last = 0;
		let mut stream = std::pin::pin!(stream);
		while let Some(message) = stream.next().await {
			let (payload, acker) = message.split();
			let i = u32::from_be_bytes(payload.as_ref().try_into().unwrap());
			assert_eq!(last + 1, i);
			last = i;
			acker.ack().await.unwrap();
		}
	}

	#[tokio::test]
	async fn stream_info() {
		let messenger: Messenger = Messenger::new();

		messenger.create_stream("stream".into()).await.unwrap();

		let subscriber = messenger.stream_subscribe("stream".into()).await.unwrap();

		let info = messenger.stream_info("stream".into()).await.unwrap();
		assert_eq!(info.first_sequence, None);
		assert_eq!(info.last_sequence, 0);
		messenger
			.stream_publish("stream".into(), vec![].into())
			.await
			.unwrap();

		let info = messenger.stream_info("stream".into()).await.unwrap();
		assert_eq!(info.first_sequence, Some(0));
		assert_eq!(info.last_sequence, 1);

		std::pin::pin!(subscriber)
			.next()
			.await
			.unwrap()
			.acker
			.ack()
			.await
			.unwrap();

		let info = messenger.stream_info("stream".into()).await.unwrap();
		assert_eq!(info.first_sequence, None);
		assert_eq!(info.last_sequence, 1);
	}
}
