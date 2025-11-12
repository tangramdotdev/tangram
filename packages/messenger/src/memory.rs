use {
	crate::{
		Acker, BatchConfig, ConsumerConfig, ConsumerInfo, Error, Message, StreamConfig, StreamInfo,
	},
	async_broadcast as broadcast,
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, HashMap},
		ops::Deref,
		sync::{Arc, RwLock, Weak},
	},
	tokio::sync::Notify,
};

#[derive(Clone)]
pub struct Messenger {
	inner: Arc<MessengerInner>,
}

pub struct MessengerInner {
	receiver: broadcast::InactiveReceiver<(String, Bytes)>,
	sender: broadcast::Sender<(String, Bytes)>,
	state: RwLock<MessengerState>,
}

struct MessengerState {
	streams: HashMap<String, Stream>,
}

#[derive(Clone)]
pub struct Stream {
	inner: Arc<StreamInner>,
}

pub struct StreamInner {
	name: String,
	state: RwLock<StreamState>,
	notify: Arc<tokio::sync::Notify>,
}

struct StreamState {
	bytes: u64,
	closed: bool,
	config: StreamConfig,
	consumers: HashMap<String, (Consumer, ConsumerState)>,
	messages: BTreeMap<u64, Bytes>,
	sequence: u64,
}

#[derive(Debug)]
struct ConsumerState {
	#[expect(dead_code)]
	config: ConsumerConfig,
	sequence: u64,
}

#[derive(Clone)]
pub struct Consumer {
	inner: Arc<ConsumerInner>,
}

pub struct ConsumerInner {
	name: String,
	stream: Weak<StreamInner>,
}

impl Messenger {
	#[must_use]
	pub fn new() -> Self {
		let (mut sender, receiver) = async_broadcast::broadcast(1_000_000);
		let receiver = receiver.deactivate();
		sender.set_overflow(true);
		sender.set_await_active(false);
		let streams = HashMap::new();
		let state = RwLock::new(MessengerState { streams });
		let inner = Arc::new(MessengerInner {
			receiver,
			sender,
			state,
		});
		Self { inner }
	}

	fn publish(&self, subject: String, payload: Bytes) {
		self.sender.try_broadcast((subject, payload)).ok();
	}

	fn subscribe(
		&self,
		subject: String,
		_group: Option<String>,
	) -> impl futures::Stream<Item = Message> + Send + 'static {
		self.receiver
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
	}

	fn get_stream(&self, name: &str) -> Result<Stream, Error> {
		self.state
			.read()
			.unwrap()
			.streams
			.get(name)
			.ok_or(Error::NotFound)
			.cloned()
	}

	fn create_stream(&self, name: String, config: StreamConfig) -> Stream {
		let mut state = self.state.write().unwrap();
		let stream = state.streams.remove(&name);
		if let Some(stream) = stream {
			stream.close();
		}
		let stream = Stream::new(name.clone(), config);
		state.streams.insert(name, stream.clone());
		stream
	}

	fn get_or_create_stream(&self, name: String, config: StreamConfig) -> Stream {
		self.state
			.write()
			.unwrap()
			.streams
			.entry(name.clone())
			.or_insert_with(|| Stream::new(name, config))
			.clone()
	}

	fn delete_stream(&self, name: &str) {
		let stream = self.state.write().unwrap().streams.remove(name);
		if let Some(stream) = stream {
			stream.close();
		}
	}

	async fn stream_publish(
		&self,
		name: String,
		payload: Bytes,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error> {
		let stream = self
			.state
			.read()
			.unwrap()
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.clone();
		let future = tokio::spawn(async move { stream.publish(payload).await })
			.map(|result| result.unwrap());
		Ok(future)
	}

	async fn stream_batch_publish(
		&self,
		name: String,
		payloads: Vec<Bytes>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error> {
		let stream = self
			.state
			.read()
			.unwrap()
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.clone();
		let future = tokio::spawn(async move { stream.batch_publish(payloads).await })
			.map(|result| result.unwrap());
		Ok(future)
	}
}

impl Stream {
	fn new(name: String, config: StreamConfig) -> Self {
		let state = StreamState {
			bytes: 0,
			closed: false,
			config,
			consumers: HashMap::default(),
			messages: BTreeMap::new(),
			sequence: 0,
		};
		let inner = Arc::new(StreamInner {
			name,
			state: RwLock::new(state),
			notify: Arc::new(tokio::sync::Notify::new()),
		});
		Self { inner }
	}

	fn close(&self) {
		// Mark this stream as closed.
		self.state.write().unwrap().closed = true;

		// Notify any waiting consumers that the stream was closed.
		self.notify.notify_waiters();
	}

	fn info(&self) -> StreamInfo {
		let state = self.state.read().unwrap();
		let first_sequence = state.messages.iter().next().map_or_else(
			|| {
				if state.sequence == 0 {
					0
				} else {
					state.sequence + 1
				}
			},
			|(sequence, _)| *sequence,
		);
		let last_sequence = state.sequence;
		StreamInfo {
			first_sequence,
			last_sequence,
		}
	}

	fn get_consumer(&self, name: &str) -> Result<Consumer, Error> {
		Ok(self
			.state
			.read()
			.unwrap()
			.consumers
			.get(name)
			.ok_or(Error::NotFound)?
			.0
			.clone())
	}

	fn create_consumer(&self, name: String, config: ConsumerConfig) -> Consumer {
		let mut state = self.state.write().unwrap();
		state.consumers.remove(&name);
		let sequence = state
			.messages
			.iter()
			.next()
			.map_or(state.sequence, |(sequence, _)| *sequence);
		let consumer = Consumer::new(name.clone(), Arc::downgrade(&self.inner));
		let consumer_state = ConsumerState { config, sequence };
		state
			.consumers
			.insert(name, (consumer.clone(), consumer_state));
		consumer
	}

	fn get_or_create_consumer(&self, name: String, config: ConsumerConfig) -> Consumer {
		let mut state = self.state.write().unwrap();
		if let Some((consumer, _)) = state.consumers.get(&name) {
			return consumer.clone();
		}
		let sequence = state
			.messages
			.iter()
			.next()
			.map_or(state.sequence, |(sequence, _)| *sequence);
		let consumer = Consumer::new(name.clone(), Arc::downgrade(&self.inner));
		let consumer_state = ConsumerState { config, sequence };
		state
			.consumers
			.insert(name, (consumer.clone(), consumer_state));
		consumer
	}

	fn delete_consumer(&self, name: &str) {
		let mut state = self.state.write().unwrap();
		state.consumers.remove(name);
	}

	async fn publish(&self, payload: Bytes) -> Result<u64, Error> {
		let mut sequences = self.batch_publish(vec![payload]).await?;
		let sequence = sequences.pop().ok_or_else(|| Error::MaxMessages)?;
		Ok(sequence)
	}

	async fn batch_publish(&self, payloads: Vec<Bytes>) -> Result<Vec<u64>, Error> {
		// Lock the state.
		let mut state = self.state.write().unwrap();

		// Publish as many messages as possible.
		let mut sequences = Vec::new();
		for payload in payloads {
			if let Some(max_messages) = state.config.max_messages
				&& state.messages.len().to_u64().unwrap() + 1 > max_messages
			{
				tracing::warn!("max messages reached");
				break;
			}
			if let Some(max_bytes) = state.config.max_bytes
				&& state.bytes + payload.len().to_u64().unwrap() > max_bytes
			{
				tracing::warn!("max bytes reached");
				break;
			}
			state.sequence += 1;
			let sequence = state.sequence;
			state.bytes += payload.len().to_u64().unwrap();
			state.messages.insert(sequence, payload);
			sequences.push(sequence);
		}

		// Release the lock.
		drop(state);

		// Notify consumers.
		self.notify.notify_waiters();

		Ok(sequences)
	}
}

impl Consumer {
	fn new(name: String, stream: Weak<StreamInner>) -> Self {
		let inner = Arc::new(ConsumerInner { name, stream });
		Self { inner }
	}

	async fn info(&self) -> Result<ConsumerInfo, Error> {
		let inner = self.stream.upgrade().ok_or_else(|| Error::NotFound)?;
		let stream = Stream { inner };
		let state = stream.state.read().unwrap();
		let consumer_state = &state
			.consumers
			.get(&self.name)
			.ok_or_else(|| Error::NotFound)?
			.1;
		let info = ConsumerInfo {
			sequence: consumer_state.sequence,
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
		// Create the state.
		struct State {
			consumer: Consumer,
			stream: Weak<StreamInner>,
			name: String,
			notify: Arc<Notify>,
		}

		// Get the notify
		let stream = self
			.stream
			.upgrade()
			.ok_or_else(|| Error::Other("stream closed".into()))?;

		let name = stream.name.clone();
		let notify = stream.notify.clone();

		// Compute the max number of messages.
		let max_messages = config.max_messages.map(|u| u.to_usize().unwrap());

		// Create the state.
		let state = State {
			consumer: self.clone(),
			stream: Arc::downgrade(&stream),
			name,
			notify,
		};

		// Create the stream.
		let stream = stream::try_unfold(state, move |state| async move {
			// Wait for a notification if the queue is empty.
			{
				let is_empty = state
					.stream
					.upgrade()
					.and_then(|stream| {
						let stream = stream.state.read().unwrap();
						let (_, consumer) = stream.consumers.get(&state.consumer.name)?;
						Some(consumer.sequence == stream.sequence)
					})
					.unwrap_or_default();
				if is_empty {
					state.notify.notified().await;
				}
			}

			// Get the stream or return None if the stream was dropped.
			let Some(stream) = state.stream.upgrade() else {
				return Ok(None);
			};

			// Get the stream state.
			let mut stream_state = stream.state.write().unwrap();

			// Check if the stream was closed and all messages have been consumed.
			if stream_state.closed && stream_state.messages.is_empty() {
				return Ok(None);
			}

			// Get the messages.
			let max_messages = max_messages.unwrap_or(stream_state.messages.len());
			let consumer_sequence = stream_state
				.consumers
				.get(&state.consumer.name)
				.ok_or_else(|| Error::NotFound)?
				.1
				.sequence;
			let messages = stream_state
				.messages
				.iter()
				.skip_while(|(sequence, _)| **sequence <= consumer_sequence)
				.take(max_messages)
				.map(|(sequence, bytes)| (*sequence, bytes.clone()))
				.collect::<Vec<_>>();

			// Set the consumer's sequence number.
			if let Some((sequence, _)) = messages.last() {
				stream_state
					.consumers
					.get_mut(&state.consumer.name)
					.ok_or_else(|| Error::NotFound)?
					.1
					.sequence = *sequence;
			}

			// Drop the state.
			drop(stream_state);

			// Create the messages.
			let messages = messages
				.into_iter()
				.map(|(sequence, payload)| {
					let inner = Arc::downgrade(&state.consumer.inner);
					let acker = Acker::new(async move {
						if let Some(inner) = inner.upgrade() {
							let consumer = Consumer { inner };
							consumer.ack(sequence).await?;
						}
						Ok::<_, Error>(())
					});
					let message = Message {
						acker,
						payload,
						subject: state.name.clone(),
					};
					Ok(message)
				})
				.collect::<Vec<_>>();

			Ok::<_, Error>(Some((messages, state)))
		})
		.map_ok(stream::iter)
		.try_flatten();

		Ok(stream)
	}

	async fn ack(&self, sequence: u64) -> Result<(), Error> {
		let stream = self
			.stream
			.upgrade()
			.ok_or_else(|| Error::other("the stream was destroyed"))?;
		let mut state = stream.state.write().unwrap();
		let message = state.messages.remove(&sequence).unwrap();
		state.bytes -= message.len().to_u64().unwrap();
		Ok(())
	}
}

impl Default for Messenger {
	fn default() -> Self {
		Self::new()
	}
}

impl Deref for Messenger {
	type Target = MessengerInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
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
		self.publish(subject, payload);
		Ok(())
	}

	async fn subscribe(
		&self,
		subject: String,
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = Message> + 'static, Error> {
		Ok(self.subscribe(subject, group))
	}

	async fn get_stream(&self, name: String) -> Result<Self::Stream, Error> {
		self.get_stream(&name)
	}

	async fn create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		Ok(self.create_stream(name, config))
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Self::Stream, Error> {
		Ok(self.get_or_create_stream(name, config))
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		self.delete_stream(&name);
		Ok(())
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
		Ok(self.info())
	}

	async fn get_consumer(&self, name: String) -> Result<Self::Consumer, Error> {
		self.get_consumer(&name)
	}

	async fn create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		Ok(self.create_consumer(name, config))
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		Ok(self.get_or_create_consumer(name, config))
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		self.delete_consumer(&name);
		Ok(())
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

impl Drop for StreamInner {
	fn drop(&mut self) {
		// Notify consumers if the stream was dropped.
		self.notify.notify_waiters();
	}
}
