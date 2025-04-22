use crate::{Acker, BatchConfig, Error, Message, StreamConfig, StreamInfo, StreamPublishInfo};
use async_broadcast as broadcast;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{FutureExt, StreamExt as _, TryFutureExt, TryStreamExt, future, stream};
use num::ToPrimitive;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Notify, RwLock};

pub struct Messenger {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
	streams: DashMap<String, Arc<Stream>>,
}

struct Stream {
	state: Arc<tokio::sync::RwLock<StreamState>>,
}

struct StreamState {
	bytes: u64,
	closed: bool,
	config: StreamConfig,
	consumers: HashMap<String, Consumer>,
	messages: Vec<(u64, Bytes)>,
	notify: Arc<tokio::sync::Notify>,
	sequence: u64,
	subject: String,
}

struct Consumer {
	sequence: u64,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);
		sender.set_await_active(false);
		let streams = DashMap::new();
		Self {
			receiver,
			sender,
			streams,
		}
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
		let stream = self
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
			});
		Ok(stream)
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<(), Error> {
		self.streams
			.entry(name.clone())
			.or_insert_with(|| Arc::new(Stream::new(name, config)));
		Ok(())
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		if let Some((_, stream)) = self.streams.remove(&name) {
			stream.close().await;
		}
		Ok(())
	}

	async fn stream_info(&self, name: String) -> Result<StreamInfo, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?.clone();
		let state = stream.state.read().await;
		let first_sequence = state.messages.first().map_or(state.sequence, |msg| msg.0);
		let last_sequence = state.sequence;
		Ok(StreamInfo {
			first_sequence,
			last_sequence,
		})
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?.clone();
		let future = async move { stream.publish(payload).await };
		Ok(future)
	}

	async fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?.clone();
		let stream = stream.subscribe(consumer).await?.boxed();
		Ok(stream)
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error> {
		let future = self
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.batch_publish(payloads)
			.boxed();
		Ok(future)
	}

	async fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + 'static + Send, Error> {
		let stream = self
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.batch_subscribe(consumer, config)
			.await?
			.boxed();
		Ok(stream)
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Messenger for Messenger {
	async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
		self.publish(subject, payload).await
	}

	async fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = Message> + 'static, Error> {
		self.subscribe(subject, group).await
	}

	async fn get_or_create_stream(&self, name: String, config: StreamConfig) -> Result<(), Error> {
		self.create_stream(name, config).await
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> {
		self.delete_stream(name)
	}

	fn stream_info(&self, name: String) -> impl Future<Output = Result<crate::StreamInfo, Error>> {
		self.stream_info(name)
	}

	fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> impl Future<Output = Result<impl Future<Output = Result<StreamPublishInfo, Error>>, Error>>
	{
		self.stream_publish(name, payload)
	}

	async fn stream_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		self.stream_subscribe(name, consumer).await
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<StreamPublishInfo>, Error>>, Error> {
		self.stream_batch_publish(name, payloads).await
	}

	async fn stream_batch_subscribe(
		&self,
		name: String,
		consumer: Option<String>,
		config: crate::BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		self.stream_batch_subscribe(name, consumer, config).await
	}
}

impl Stream {
	fn new(subject: String, config: StreamConfig) -> Self {
		let state = StreamState {
			bytes: 0,
			consumers: HashMap::default(),
			config,
			closed: false,
			messages: Vec::new(),
			notify: Arc::new(Notify::new()),
			sequence: 0,
			subject,
		};
		Self {
			state: Arc::new(RwLock::new(state)),
		}
	}

	async fn close(&self) {
		self.state.write().await.closed = true;
	}

	fn batch_publish(
		&self,
		payloads: Vec<Bytes>,
	) -> impl Future<Output = Result<Vec<StreamPublishInfo>, Error>> + Send + 'static {
		// Create a channel to be notified when a message is published.
		let (tx, rx) = tokio::sync::oneshot::channel();

		// Spawn a task to send the message.
		let state = self.state.clone();
		tokio::spawn(async move {
			// Acquire an exclusive lock on the state.
			let mut state = state.write().await;

			// Check if the stream is full of messages/bytes and return an error.
			if let Some(max_messages) = state.config.max_messages {
				if state.messages.len().to_u64().unwrap() >= max_messages {
					tx.send(Err(Error::MaxMessages)).ok();
					return;
				}
			}
			if let Some(max_messages) = state.config.max_messages {
				if state.messages.len().to_u64().unwrap() >= max_messages {
					tx.send(Err(Error::MaxMessages)).ok();
					return;
				}
			}

			// Publish as many messages as possible.
			let infos = payloads
				.into_iter()
				.filter_map(|payload| {
					if state.config.max_messages.map_or_else(
						|| false,
						|max_messages| state.messages.len().to_u64().unwrap() >= max_messages,
					) && state.config.max_bytes.map_or_else(
						|| true,
						|max| state.bytes + payload.len().to_u64().unwrap() > max,
					) {
						return None;
					}

					let sequence = state.sequence;
					state.sequence += 1;
					state.bytes += payload.len().to_u64().unwrap();
					state.messages.push((sequence, payload));
					Some(StreamPublishInfo { sequence })
				})
				.collect::<Vec<_>>();

			// Notify consumers.
			state.notify.notify_waiters();

			// Send the infos.
			tx.send(Ok(infos)).ok();
		});

		rx.map_err(|_| Error::PublishFailed).and_then(future::ready)
	}

	async fn batch_subscribe(
		&self,
		consumer: Option<String>,
		config: crate::BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		let consumer = consumer.unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
		let stream = self.state.clone();

		// Acquire a lock on the stream.
		let mut stream_ = stream.write().await;

		// Create a new consumer if it does not exist.
		if !stream_.consumers.contains_key(&consumer) {
			let sequence = stream_
				.messages
				.first()
				.map_or(stream_.sequence, |(sequence, _)| *sequence);
			stream_
				.consumers
				.insert(consumer.clone(), Consumer { sequence });
		}

		// Get the notify.
		let notify = stream_.notify.clone();
		drop(stream_);

		// Construct the state.
		struct State {
			config: BatchConfig,
			consumer: String,
			notify: Arc<Notify>,
			stream: Arc<RwLock<StreamState>>,
		}
		let state = State {
			config,
			consumer,
			notify,
			stream,
		};
		let stream = stream::try_unfold(state, move |state| async move {
			// Wait for a notification if the stream is empty
			{
				let stream = state.stream.read().await;
				if stream.messages.is_empty() {
					// Check if the stream is closed.
					if stream.closed {
						return Ok(None);
					}

					// Otherwise wait for a notification.
					drop(stream);
					state.notify.notified().await;
				}
			}

			// Acquire a lock on the stream.
			let mut stream = state.stream.write().await;

			// Get the consumer.
			let first_sequence = stream.consumers.get_mut(&state.consumer).unwrap().sequence;

			// Get the undelivered messages.
			let undelivered = stream
				.messages
				.iter()
				.skip_while(|(sequence, _)| *sequence < first_sequence)
				.take(
					state
						.config
						.max_messages
						.map_or(stream.messages.len(), |max| max.to_usize().unwrap()),
				)
				.cloned()
				.collect::<Vec<_>>();

			// Update the last sequence of the consumer if the messages are not empty.
			if let Some((sequence, _)) = undelivered.last() {
				stream.consumers.get_mut(&state.consumer).unwrap().sequence = *sequence;
			}

			// Get the first sequence of the stream.
			let first_sequence = stream
				.consumers
				.values()
				.map(|consumer| consumer.sequence)
				.min()
				.unwrap();

			// Remove any messages that are younger than the new first sequence.
			for (sequence, _) in &undelivered {
				if *sequence <= first_sequence {
					let index = stream
						.messages
						.iter()
						.position(|(s, _)| s == sequence)
						.unwrap();
					stream.messages.remove(index);
				}
			}

			// Collect the results.
			let messages = undelivered
				.into_iter()
				.map(|(_, payload)| {
					let acker = Acker::new(future::ok(()));
					Ok(Message {
						acker,
						payload,
						subject: stream.subject.clone(),
					})
				})
				.collect::<Vec<_>>();

			// Release the lock.
			drop(stream);

			// Return the result.
			Ok::<_, Error>(Some((messages, state)))
		})
		.map_ok(stream::iter)
		.try_flatten();

		Ok(stream)
	}

	fn publish(
		&self,
		payload: Bytes,
	) -> impl Future<Output = Result<StreamPublishInfo, Error>> + Send + 'static {
		self.batch_publish(vec![payload])
			.and_then(|mut infos| async move {
				if infos.len() != 1 {
					return Err(Error::MaxMessages);
				}
				let info = infos.pop().unwrap();
				Ok(info)
			})
	}

	async fn subscribe(
		&self,
		consumer: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		let config = BatchConfig {
			max_bytes: None,
			max_messages: None,
			timeout: None,
		};
		self.batch_subscribe(consumer, config).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn stream() {
		let stream = Stream::new(
			"subject".into(),
			StreamConfig {
				max_bytes: Some(1024),
				max_messages: Some(256),
				retention: None,
			},
		);
		let infos = stream
			.batch_publish(vec![
				vec![b'1'].into(),
				vec![b'2'].into(),
				vec![b'3'].into(),
				vec![b'4'].into(),
			])
			.await
			.unwrap();
		assert_eq!(infos.len(), 4);
		assert_eq!(infos[0].sequence, 0);
		assert_eq!(infos[1].sequence, 1);
		assert_eq!(infos[2].sequence, 2);
		assert_eq!(infos[3].sequence, 3);
		let messages = stream
			.batch_subscribe(
				None,
				BatchConfig {
					max_bytes: None,
					max_messages: None,
					timeout: None,
				},
			)
			.await
			.unwrap()
			.take(4)
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		assert_eq!(&messages[0].payload, b"1".as_slice());
		assert_eq!(&messages[1].payload, b"2".as_slice());
		assert_eq!(&messages[2].payload, b"3".as_slice());
		assert_eq!(&messages[3].payload, b"4".as_slice());
	}
}
