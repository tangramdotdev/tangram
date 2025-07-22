use crate::{
	Acker, BatchConfig, ConsumerConfig, ConsumerInfo, Error, Message, StreamConfig, StreamInfo,
};
use async_broadcast as broadcast;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use num::ToPrimitive as _;
use std::{
	collections::HashMap,
	ops::Deref,
	pin::pin,
	sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Notify, RwLock};

pub struct Messenger {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
	streams: DashMap<String, Stream>,
}

#[derive(Clone)]
pub struct Stream {
	inner: Arc<StreamInner>,
}

pub struct StreamInner {
	config: StreamConfig,
	name: String,
	notify: tokio::sync::Notify,
	state: tokio::sync::RwLock<StreamState>,
}

struct StreamState {
	bytes: u64,
	closed: bool,
	consumers: HashMap<String, Consumer>,
	messages: Vec<(u64, Bytes)>,
	id: u64,
}

#[derive(Clone)]
pub struct Consumer {
	inner: Arc<ConsumerInner>,
}

pub struct ConsumerInner {
	#[allow(dead_code)]
	config: ConsumerConfig,
	#[allow(dead_code)]
	name: String,
	state: Mutex<ConsumerState>,
	stream: Weak<StreamInner>,
}

struct ConsumerState {
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

	async fn get_stream(&self, name: String) -> Result<Stream, Error> {
		Ok(self.streams.get(&name).ok_or(Error::NotFound)?.clone())
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<Stream, Error> {
		self.delete_stream(name.clone()).await?;
		let stream = Stream::new(name.clone(), config);
		self.streams.insert(name, stream.clone());
		Ok(stream)
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Stream, Error> {
		let stream = self
			.streams
			.entry(name.clone())
			.or_insert_with(|| Stream::new(name, config))
			.value()
			.clone();
		Ok(stream)
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		if let Some((_, stream)) = self.streams.remove(&name) {
			stream.close().await;
		}
		Ok(())
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?.clone();
		let future = tokio::spawn(async move { stream.publish(payload).await })
			.map(|result| result.unwrap());
		Ok(future)
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error> {
		let stream = self.streams.get(&name).ok_or(Error::NotFound)?.clone();
		let future = tokio::spawn(async move { stream.batch_publish(payloads).await })
			.map(|result| result.unwrap());
		Ok(future)
	}
}

impl Stream {
	fn new(name: String, config: StreamConfig) -> Self {
		let state = StreamState {
			bytes: 0,
			consumers: HashMap::default(),
			closed: false,
			messages: Vec::new(),
			id: 0,
		};
		let inner = Arc::new(StreamInner {
			config,
			name,
			notify: Notify::new(),
			state: RwLock::new(state),
		});
		Self { inner }
	}

	async fn close(&self) {
		self.state.write().await.closed = true;
	}

	async fn info(&self) -> Result<StreamInfo, Error> {
		let state = self.state.read().await;
		let first_sequence = state.messages.first().map_or_else(
			|| {
				if state.id == 0 { 0 } else { state.id + 1 }
			},
			|(sequence, _)| *sequence,
		);
		let last_sequence = state.id;
		let info = StreamInfo {
			first_sequence,
			last_sequence,
		};
		Ok(info)
	}

	async fn get_consumer(&self, name: String) -> Result<Consumer, Error> {
		Ok(self
			.state
			.read()
			.await
			.consumers
			.get(&name)
			.ok_or(Error::NotFound)?
			.clone())
	}

	async fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Consumer, Error> {
		let mut state = self.state.write().await;
		state.consumers.remove(&name);
		let sequence = state
			.messages
			.first()
			.map_or(state.id, |(sequence, _)| *sequence);
		let consumer = Consumer::new(name.clone(), config, sequence, Arc::downgrade(&self.inner));
		state.consumers.insert(name, consumer.clone());
		Ok(consumer)
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Consumer, Error> {
		let mut state = self.state.write().await;
		if let Some(consumer) = state.consumers.get(&name) {
			return Ok(consumer.clone());
		}
		let sequence = state
			.messages
			.first()
			.map_or(state.id, |(sequence, _)| *sequence);
		let consumer = Consumer::new(name.clone(), config, sequence, Arc::downgrade(&self.inner));
		state.consumers.insert(name, consumer.clone());
		Ok(consumer)
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		let mut state = self.state.write().await;
		state.consumers.remove(&name);
		Ok(())
	}

	async fn publish(&self, payload: Bytes) -> Result<u64, Error> {
		let mut ids = self.batch_publish(vec![payload]).await?;
		let id = ids.pop().ok_or_else(|| Error::MaxMessages)?;
		Ok(id)
	}

	async fn batch_publish(&self, payloads: Vec<Bytes>) -> Result<Vec<u64>, Error> {
		// Lock the state.
		let mut state = self.state.write().await;

		// Publish as many messages as possible.
		let mut ids = Vec::new();
		for payload in payloads {
			if let Some(max_messages) = self.config.max_messages {
				if 1 + state.messages.len().to_u64().unwrap() > max_messages {
					break;
				}
			}
			if let Some(max_bytes) = self.config.max_bytes {
				if state.bytes + payload.len().to_u64().unwrap() > max_bytes {
					break;
				}
			}
			state.id += 1;
			let id = state.id;
			state.bytes += payload.len().to_u64().unwrap();
			state.messages.push((id, payload));
			ids.push(id);
		}

		// Notify consumers.
		self.notify.notify_waiters();

		Ok(ids)
	}
}

impl Consumer {
	fn new(name: String, config: ConsumerConfig, sequence: u64, stream: Weak<StreamInner>) -> Self {
		let state = Mutex::new(ConsumerState { sequence });
		let inner = Arc::new(ConsumerInner {
			config,
			name,
			state,
			stream,
		});
		Self { inner }
	}

	async fn info(&self) -> Result<ConsumerInfo, Error> {
		let state = self.state.lock().unwrap();
		let info = ConsumerInfo {
			sequence: state.sequence,
		};
		Ok(info)
	}

	async fn subscribe(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		let config = BatchConfig {
			max_bytes: None,
			max_messages: None,
			timeout: None,
		};
		self.batch_subscribe(config).await
	}

	async fn batch_subscribe(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		// Get the stream.
		let stream = Stream {
			inner: self.stream.upgrade().ok_or_else(|| Error::NotFound)?,
		};

		// Create the state.
		struct State {
			config: BatchConfig,
			consumer: Consumer,
			stream: Stream,
		}
		let state = State {
			config,
			consumer: self.clone(),
			stream,
		};

		// Create the stream.
		let stream = stream::try_unfold(state, move |state| async move {
			// If the stream is empty, then wait for a notification.
			{
				let state_ = state.stream.state.read().await;
				if state_.messages.is_empty() {
					if state_.closed {
						return Ok(None);
					}
					let notified = state.stream.notify.notified();
					let mut notified = pin!(notified);
					notified.as_mut().enable();
					drop(state_);
					notified.await;
				}
			}

			// Acquire a lock on the stream.
			let mut state_ = state.stream.state.write().await;

			// Get the messages.
			let consumer_sequence = state.consumer.state.lock().unwrap().sequence;
			let max_messages = state
				.config
				.max_messages
				.map_or(state_.messages.len(), |max_messages| {
					max_messages.to_usize().unwrap()
				});
			let messages = state_
				.messages
				.iter()
				.skip_while(|(sequence, _)| *sequence < consumer_sequence)
				.take(max_messages)
				.cloned()
				.collect::<Vec<_>>();

			// Set the consumer's sequence number.
			if let Some((sequence, _)) = messages.last() {
				state.consumer.state.lock().unwrap().sequence = *sequence;
			}

			// Get the new first sequence number of the stream.
			let first_sequence = state_
				.consumers
				.values()
				.map(|consumer| consumer.state.lock().unwrap().sequence)
				.min()
				.unwrap();

			// Remove messages whose sequence number is less than the new first sequence number.
			for (sequence, _) in &messages {
				if *sequence <= first_sequence {
					let index = state_
						.messages
						.iter()
						.position(|(s, _)| s == sequence)
						.unwrap();
					state_.messages.remove(index);
				}
			}

			// Create the messages.
			let messages = messages
				.into_iter()
				.map(|(_, payload)| {
					let acker = Acker::new(future::ok(()));
					Ok(Message {
						acker,
						payload,
						subject: state.stream.name.clone(),
					})
				})
				.collect::<Vec<_>>();

			// Drop the state.
			drop(state_);

			Ok::<_, Error>(Some((messages, state)))
		})
		.map_ok(stream::iter)
		.try_flatten();

		Ok(stream)
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl Deref for Stream {
	type Target = StreamInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl Deref for Consumer {
	type Target = ConsumerInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl crate::Messenger for Messenger {
	type Stream = Stream;

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

	async fn get_stream(&self, name: String) -> Result<Self::Stream, Error> {
		self.get_stream(name).await
	}

	async fn create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		self.create_stream(name, config).await
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		self.get_or_create_stream(name, config).await
	}

	fn delete_stream(&self, name: String) -> impl Future<Output = Result<(), Error>> {
		self.delete_stream(name)
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error> {
		self.stream_publish(name, payload).await
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error> {
		self.stream_batch_publish(name, payloads).await
	}
}

impl crate::Stream for Stream {
	type Consumer = Consumer;

	async fn info(&self) -> Result<StreamInfo, Error> {
		self.info().await
	}

	async fn get_consumer(&self, name: String) -> Result<Self::Consumer, Error> {
		self.get_consumer(name).await
	}

	async fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		self.create_consumer(name, config).await
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		self.get_or_create_consumer(name, config).await
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		self.delete_consumer(name).await
	}
}

impl crate::Consumer for Consumer {
	async fn info(&self) -> Result<ConsumerInfo, Error> {
		self.info().await
	}

	async fn subscribe(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		self.subscribe().await
	}

	async fn batch_subscribe(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message, Error>> + Send + 'static, Error> {
		self.batch_subscribe(config).await
	}
}
