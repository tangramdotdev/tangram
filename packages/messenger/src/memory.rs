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
		pin::pin,
		sync::{Arc, RwLock, Weak},
	},
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
	state: tokio::sync::RwLock<StreamState>,
}

struct StreamState {
	bytes: u64,
	closed: bool,
	config: StreamConfig,
	consumers: HashMap<String, (Consumer, ConsumerState)>,
	messages: BTreeMap<u64, Bytes>,
	notify: Arc<tokio::sync::Notify>,
	sequence: u64,
}

struct ConsumerState {
	#[allow(dead_code)]
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
		Ok(self
			.state
			.read()
			.unwrap()
			.streams
			.get(&name)
			.ok_or(Error::NotFound)?
			.clone())
	}

	async fn create_stream(&self, name: String, config: StreamConfig) -> Result<Stream, Error> {
		self.delete_stream(name.clone()).await?;
		let stream = Stream::new(name.clone(), config);
		self.state
			.write()
			.unwrap()
			.streams
			.insert(name, stream.clone());
		Ok(stream)
	}

	async fn get_or_create_stream(
		&self,
		name: String,
		config: StreamConfig,
	) -> Result<Stream, Error> {
		let stream = self
			.state
			.write()
			.unwrap()
			.streams
			.entry(name.clone())
			.or_insert_with(|| Stream::new(name, config))
			.clone();
		Ok(stream)
	}

	async fn delete_stream(&self, name: String) -> Result<(), Error> {
		let stream = self.state.write().unwrap().streams.remove(&name);
		if let Some(stream) = stream {
			stream.close().await;
		}
		Ok(())
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
			notify: Arc::new(tokio::sync::Notify::new()),
			sequence: 0,
		};
		let inner = Arc::new(StreamInner {
			name,
			state: tokio::sync::RwLock::new(state),
		});
		Self { inner }
	}

	async fn close(&self) {
		self.state.write().await.closed = true;
	}

	async fn info(&self) -> Result<StreamInfo, Error> {
		let state = self.state.read().await;
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
			.0
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
			.iter()
			.next()
			.map_or(state.sequence, |(sequence, _)| *sequence);
		let consumer = Consumer::new(name.clone(), Arc::downgrade(&self.inner));
		let consumer_state = ConsumerState { config, sequence };
		state
			.consumers
			.insert(name, (consumer.clone(), consumer_state));
		Ok(consumer)
	}

	async fn get_or_create_consumer(
		&self,
		name: String,
		config: ConsumerConfig,
	) -> Result<Consumer, Error> {
		let mut state = self.state.write().await;
		if let Some((consumer, _)) = state.consumers.get(&name) {
			return Ok(consumer.clone());
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
		Ok(consumer)
	}

	async fn delete_consumer(&self, name: String) -> Result<(), Error> {
		let mut state = self.state.write().await;
		state.consumers.remove(&name);
		Ok(())
	}

	async fn publish(&self, payload: Bytes) -> Result<u64, Error> {
		let mut sequences = self.batch_publish(vec![payload]).await?;
		let sequence = sequences.pop().ok_or_else(|| Error::MaxMessages)?;
		Ok(sequence)
	}

	async fn batch_publish(&self, payloads: Vec<Bytes>) -> Result<Vec<u64>, Error> {
		// Lock the state.
		let mut state = self.state.write().await;

		// Publish as many messages as possible.
		let mut sequences = Vec::new();
		for payload in payloads {
			if let Some(max_messages) = state.config.max_messages {
				if state.messages.len().to_u64().unwrap() + 1 > max_messages {
					break;
				}
			}
			if let Some(max_bytes) = state.config.max_bytes {
				if state.bytes + payload.len().to_u64().unwrap() > max_bytes {
					break;
				}
			}
			state.sequence += 1;
			let sequence = state.sequence;
			state.bytes += payload.len().to_u64().unwrap();
			state.messages.insert(sequence, payload);
			sequences.push(sequence);
		}

		// Notify consumers.
		state.notify.notify_waiters();

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
		let state = stream.state.read().await;
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
		// Get the stream.
		let inner = self.stream.upgrade().ok_or_else(|| Error::NotFound)?;
		let stream = Stream { inner };

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
				let stream_state = state.stream.state.read().await;
				let consumer_state = &stream_state
					.consumers
					.get(&state.consumer.name)
					.ok_or_else(|| Error::NotFound)?
					.1;
				if consumer_state.sequence == stream_state.sequence {
					if stream_state.closed {
						return Ok(None);
					}
					let notify = stream_state.notify.clone();
					let notified = notify.notified();
					let mut notified = pin!(notified);
					notified.as_mut().enable();
					drop(stream_state);
					notified.await;
				}
			}

			// Get the stream state.
			let mut stream_state = state.stream.state.write().await;

			// Get the messages.
			let consumer_sequence = stream_state
				.consumers
				.get_mut(&state.consumer.name)
				.ok_or_else(|| Error::NotFound)?
				.1
				.sequence;
			let max_messages = state
				.config
				.max_messages
				.map_or(stream_state.messages.len(), |max_messages| {
					max_messages.to_usize().unwrap()
				});
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
						subject: state.stream.name.clone(),
					};
					Ok(message)
				})
				.collect::<Vec<_>>();

			// Drop the state.
			drop(stream_state);

			Ok::<_, Error>(Some((messages, state)))
		})
		.map_ok(stream::iter)
		.try_flatten();

		Ok(stream)
	}

	async fn ack(&self, sequence: u64) -> Result<(), Error> {
		let inner = self
			.stream
			.upgrade()
			.ok_or_else(|| Error::other("the stream was destroyed"))?;
		let stream = Stream { inner };
		let mut state = stream.state.write().await;
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
