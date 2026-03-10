use {
	crate::{
		AckPolicy, Acker, BatchConfig, ConsumerConfig, ConsumerInfo, Error, Message, Payload,
		RetentionPolicy, StreamConfig, StreamInfo,
	},
	async_broadcast as broadcast,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream},
	num::ToPrimitive as _,
	std::{
		any::Any,
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
	receiver: broadcast::InactiveReceiver<(String, Arc<dyn Payload>)>,
	sender: broadcast::Sender<(String, Arc<dyn Payload>)>,
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
	state: RwLock<StreamState>,
	notify: Arc<tokio::sync::Notify>,
}

struct StreamState {
	closed: bool,
	config: StreamConfig,
	consumers: HashMap<String, (Consumer, ConsumerState)>,
	messages: BTreeMap<u64, StreamMessage>,
	bytes: u64,
	sequence: u64,
}

struct StreamMessage {
	bytes: u64,
	payload: Arc<dyn Payload>,
	retention: MessageRetention,
	subject: String,
}

enum MessageRetention {
	Interest(usize),
	Limits,
	WorkQueue(bool),
}

#[derive(Debug)]
struct ConsumerState {
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

	fn publish<T>(&self, subject: String, payload: T)
	where
		T: Payload,
	{
		let payload: Arc<dyn Payload> = Arc::new(payload);
		self.sender.try_broadcast((subject, payload)).ok();
	}

	fn subscribe<T>(
		&self,
		subject: String,
		_group: Option<String>,
	) -> impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static
	where
		T: Payload + Clone,
	{
		self.receiver
			.activate_cloned()
			.filter_map(move |(subject_, payload)| {
				if subject_ != subject {
					return future::ready(None);
				}
				let payload = payload as Arc<dyn Any + Send + Sync>;
				let Ok(payload) = payload.downcast::<T>() else {
					return future::ready(Some(Err(Error::deserialization("downcast failed"))));
				};
				let message = Message {
					subject: subject_,
					payload: Arc::unwrap_or_clone(payload),
					acker: Acker::default(),
				};
				future::ready(Some(Ok(message)))
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
		let stream = Stream::new(config);
		state.streams.insert(name, stream.clone());
		stream
	}

	#[expect(clippy::needless_pass_by_value, reason = "matches trait signature")]
	fn get_or_create_stream(&self, name: String, config: StreamConfig) -> Stream {
		self.state
			.write()
			.unwrap()
			.streams
			.entry(name.clone())
			.or_insert_with(|| Stream::new(config))
			.clone()
	}

	fn delete_stream(&self, name: &str) {
		let stream = self.state.write().unwrap().streams.remove(name);
		if let Some(stream) = stream {
			stream.close();
		}
	}

	async fn stream_publish<T>(
		&self,
		stream: String,
		subject: String,
		payload: T,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error>
	where
		T: Payload,
	{
		let stream = self
			.state
			.read()
			.unwrap()
			.streams
			.get(&stream)
			.ok_or(Error::NotFound)?
			.clone();
		let future = tokio::spawn(async move { stream.publish(subject, payload).await })
			.map_err(Error::other)
			.and_then(future::ready);
		Ok(future)
	}

	async fn stream_batch_publish<T>(
		&self,
		stream: String,
		subject: String,
		payloads: Vec<T>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error>
	where
		T: Payload,
	{
		let stream = self
			.state
			.read()
			.unwrap()
			.streams
			.get(&stream)
			.ok_or(Error::NotFound)?
			.clone();
		let future = tokio::spawn(async move { stream.batch_publish(subject, payloads).await })
			.map_err(Error::other)
			.and_then(future::ready);
		Ok(future)
	}
}

impl Stream {
	fn new(config: StreamConfig) -> Self {
		let state = StreamState {
			closed: false,
			config,
			consumers: HashMap::default(),
			messages: BTreeMap::new(),
			bytes: 0,
			sequence: 0,
		};
		let inner = Arc::new(StreamInner {
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

	fn create_consumer(&self, name: Option<String>, config: ConsumerConfig) -> Consumer {
		let mut state = self.state.write().unwrap();
		let (name, config) = Self::normalize_consumer(&state, name, config);
		state.consumers.remove(&name);
		let sequence = state
			.messages
			.iter()
			.next()
			.map_or(state.sequence, |(sequence, _)| *sequence)
			.saturating_sub(1);
		let consumer = Consumer::new(name.clone(), Arc::downgrade(&self.inner));
		let consumer_state = ConsumerState { config, sequence };
		state
			.consumers
			.insert(name, (consumer.clone(), consumer_state));
		consumer
	}

	fn get_or_create_consumer(&self, name: Option<String>, config: ConsumerConfig) -> Consumer {
		let mut state = self.state.write().unwrap();
		let (name, config) = Self::normalize_consumer(&state, name, config);
		if let Some((consumer, _)) = state.consumers.get(&name) {
			return consumer.clone();
		}
		let sequence = state
			.messages
			.iter()
			.next()
			.map_or(state.sequence, |(sequence, _)| *sequence)
			.saturating_sub(1);
		let consumer = Consumer::new(name.clone(), Arc::downgrade(&self.inner));
		let consumer_state = ConsumerState { config, sequence };
		state
			.consumers
			.insert(name, (consumer.clone(), consumer_state));
		consumer
	}

	fn normalize_consumer(
		state: &StreamState,
		name: Option<String>,
		mut config: ConsumerConfig,
	) -> (String, ConsumerConfig) {
		let explicit_name = name.or_else(|| config.durable_name.clone());
		let name = explicit_name
			.clone()
			.unwrap_or_else(|| Self::ephemeral_consumer_name(state));
		if explicit_name.is_some() {
			config.durable_name = Some(name.clone());
		} else {
			config.durable_name = None;
		}
		(name, config)
	}

	fn ephemeral_consumer_name(state: &StreamState) -> String {
		let mut n = state.sequence.saturating_add(1);
		loop {
			let candidate = format!("ephemeral-{n}");
			if !state.consumers.contains_key(&candidate) {
				return candidate;
			}
			n = n.saturating_add(1);
		}
	}

	fn delete_consumer(&self, name: &str) {
		let mut state = self.state.write().unwrap();
		let Some((_, consumer_state)) = state.consumers.remove(name) else {
			return;
		};
		// Ack all unacknowledged messages that this consumer was responsible for.
		let sequences = state
			.messages
			.range(..=consumer_state.sequence)
			.filter(|(_, message)| {
				consumer_state
					.config
					.filter_subject
					.as_ref()
					.is_none_or(|filter| filter == &message.subject)
			})
			.map(|(sequence, _)| *sequence)
			.collect::<Vec<_>>();
		for sequence in sequences {
			Self::ack_inner(&mut state, sequence);
		}
	}

	async fn publish<T>(&self, subject: String, payload: T) -> Result<u64, Error>
	where
		T: Payload,
	{
		let mut state = self.state.write().unwrap();
		let sequence = Self::try_publish_inner(&mut state, subject, payload)?;
		drop(state);
		self.notify.notify_waiters();
		Ok(sequence)
	}

	async fn batch_publish<T>(&self, subject: String, payloads: Vec<T>) -> Result<Vec<u64>, Error>
	where
		T: Payload,
	{
		// Lock the state.
		let mut state = self.state.write().unwrap();

		// Publish as many messages as possible.
		let mut sequences = Vec::new();
		for payload in payloads {
			let sequence = Self::try_publish_inner(&mut state, subject.clone(), payload)?;
			sequences.push(sequence);
		}

		// Release the lock.
		drop(state);

		// Notify consumers.
		self.notify.notify_waiters();

		Ok(sequences)
	}

	fn try_publish_inner(
		state: &mut StreamState,
		subject: String,
		payload: impl Payload,
	) -> Result<u64, Error> {
		if state
			.config
			.max_messages
			.is_some_and(|max| state.messages.len().to_u64().unwrap() == max)
		{
			if matches!(state.config.retention, RetentionPolicy::Limits)
				&& let Some((_, message)) = state.messages.pop_first()
			{
				state.bytes -= message.bytes;
			} else {
				return Err(Error::MaxMessages);
			}
		}
		let bytes = payload
			.serialize()
			.map_err(Error::other)?
			.len()
			.to_u64()
			.unwrap();

		if state
			.config
			.max_bytes
			.is_some_and(|max| state.bytes + bytes > max)
		{
			if matches!(state.config.retention, RetentionPolicy::Limits) {
				while state
					.config
					.max_bytes
					.is_some_and(|max| state.bytes + bytes > max)
				{
					let Some((_, message)) = state.messages.pop_first() else {
						return Err(Error::MaxBytes);
					};
					state.bytes -= message.bytes;
				}
			} else {
				return Err(Error::MaxBytes);
			}
		}

		// Update the sequence number.
		state.sequence += 1;

		// Create the message state.
		let retention = match state.config.retention {
			RetentionPolicy::Interest => {
				let count = state
					.consumers
					.values()
					.filter(|(_, state)| {
						state
							.config
							.filter_subject
							.as_ref()
							.is_none_or(|filter| filter == &subject)
					})
					.count();
				MessageRetention::Interest(count)
			},
			RetentionPolicy::Limits => MessageRetention::Limits,
			RetentionPolicy::WorkQueue => MessageRetention::WorkQueue(false),
		};
		let payload: Arc<dyn Payload> = Arc::new(payload);
		let sequence = state.sequence;
		let message = StreamMessage {
			bytes,
			payload,
			retention,
			subject,
		};
		state.messages.insert(sequence, message);
		state.bytes += bytes;

		Ok(sequence)
	}

	fn ack_inner(state: &mut StreamState, sequence: u64) {
		let Some(message) = state.messages.get_mut(&sequence) else {
			return;
		};
		match &mut message.retention {
			MessageRetention::Limits => (),
			MessageRetention::Interest(count) => {
				*count = count.saturating_sub(1);
				if *count == 0 {
					state.messages.remove(&sequence);
				}
			},
			MessageRetention::WorkQueue(_) => {
				state.messages.remove(&sequence);
			},
		}
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

	async fn subscribe<T>(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		let config = BatchConfig {
			max_bytes: None,
			max_messages: Some(1),
			timeout: None,
		};
		self.batch_subscribe(config).await
	}

	async fn batch_subscribe<T>(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		// Create the state.
		struct State {
			consumer: Consumer,
			stream: Weak<StreamInner>,
			notify: Arc<Notify>,
		}

		// Get the notify
		let stream = self
			.stream
			.upgrade()
			.ok_or_else(|| Error::Other("stream closed".into()))?;

		let notify = stream.notify.clone();

		// Compute the max number of messages.
		let max_messages = config.max_messages.map(|u| u.to_usize().unwrap());

		// Create the state.
		let state = State {
			consumer: self.clone(),
			stream: Arc::downgrade(&stream),
			notify,
		};

		// Create the stream.
		let stream = stream::try_unfold(state, move |state| async move {
			// Wait for a notification if the queue is empty.
			{
				let notified = state.notify.notified();
				let is_empty = state
					.stream
					.upgrade()
					.and_then(|stream| {
						let stream = stream.state.read().unwrap();
						let (_, consumer_state) = stream.consumers.get(&state.consumer.name)?;
						let has_pending = stream.messages.iter().any(|(sequence, message)| {
							*sequence > consumer_state.sequence
								&& !matches!(message.retention, MessageRetention::WorkQueue(true))
								&& consumer_state
									.config
									.filter_subject
									.as_ref()
									.is_none_or(|filter| filter == &message.subject)
						});
						Some(!has_pending)
					})
					.unwrap_or_default();
				if is_empty {
					notified.await;
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
			let consumer_state = &stream_state
				.consumers
				.get(&state.consumer.name)
				.ok_or_else(|| Error::NotFound)?
				.1;
			let consumer_sequence = consumer_state.sequence;
			let ack_policy = consumer_state.config.ack_policy.clone();
			let filter_subject = consumer_state.config.filter_subject.clone();
			let messages = stream_state
				.messages
				.iter_mut()
				.skip_while(|(sequence, _)| **sequence <= consumer_sequence)
				.filter(|(_, message)| {
					!matches!(message.retention, MessageRetention::WorkQueue(true))
						&& filter_subject
							.as_ref()
							.is_none_or(|filter| filter == &message.subject)
				})
				.take(max_messages)
				.map(|(sequence, message)| {
					if let MessageRetention::WorkQueue(delivered) = &mut message.retention {
						*delivered = true;
					}
					(
						*sequence,
						message.subject.clone(),
						message.payload.clone(),
						message.bytes,
					)
				})
				.collect::<Vec<_>>();

			// Set the consumer's sequence number.
			if let Some((sequence, _, _, _)) = messages.last() {
				stream_state
					.consumers
					.get_mut(&state.consumer.name)
					.ok_or_else(|| Error::NotFound)?
					.1
					.sequence = *sequence;
			}

			// Auto-ack messages on delivery when requested.
			if matches!(ack_policy, AckPolicy::None) {
				for (sequence, _, _, _) in &messages {
					Stream::ack_inner(&mut stream_state, *sequence);
				}
			}

			// Drop the state.
			drop(stream_state);

			// Create the messages.
			let messages = messages
				.into_iter()
				.filter_map(|(sequence, subject, payload, _)| {
					let payload = payload as Arc<dyn Any + Send + Sync>;
					let payload = payload.downcast::<T>().ok()?;
					let payload = Arc::unwrap_or_clone(payload);
					let acker = if matches!(ack_policy, AckPolicy::None) {
						Acker::default()
					} else {
						let inner = Arc::downgrade(&state.consumer.inner);
						Acker::new(async move {
							let Some(inner) = inner.upgrade() else {
								return Ok(());
							};
							let Some(stream) = inner.stream.upgrade() else {
								return Ok(());
							};
							let mut state = stream.state.write().unwrap();
							Stream::ack_inner(&mut state, sequence);
							Ok::<_, Error>(())
						})
					};
					let message = Message {
						subject,
						payload,
						acker,
					};
					Some(Ok(message))
				})
				.collect::<Vec<_>>();

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

	async fn publish<T>(&self, subject: String, payload: T) -> Result<(), Error>
	where
		T: Payload,
	{
		self.publish(subject, payload);
		Ok(())
	}

	async fn subscribe<T>(
		&self,
		subject: String,
		group: Option<String>,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + 'static, Error>
	where
		T: Payload + Clone,
	{
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

	async fn stream_publish<T>(
		&self,
		stream: String,
		subject: String,
		payload: T,
	) -> Result<impl Future<Output = Result<u64, Error>>, Error>
	where
		T: Payload,
	{
		self.stream_publish(stream, subject, payload).await
	}

	async fn stream_batch_publish<T>(
		&self,
		stream: String,
		subject: String,
		payloads: Vec<T>,
	) -> Result<impl Future<Output = Result<Vec<u64>, Error>>, Error>
	where
		T: Payload,
	{
		self.stream_batch_publish(stream, subject, payloads).await
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
		name: Option<String>,
		config: ConsumerConfig,
	) -> Result<Self::Consumer, Error> {
		Ok(self.create_consumer(name, config))
	}

	async fn get_or_create_consumer(
		&self,
		name: Option<String>,
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

	async fn subscribe<T>(
		&self,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		self.subscribe().await
	}

	async fn batch_subscribe<T>(
		&self,
		config: BatchConfig,
	) -> Result<impl futures::Stream<Item = Result<Message<T>, Error>> + Send + 'static, Error>
	where
		T: Payload + Clone,
	{
		self.batch_subscribe(config).await
	}
}

impl Drop for StreamInner {
	fn drop(&mut self) {
		// Notify consumers if the stream was dropped.
		self.notify.notify_waiters();
	}
}

impl Drop for ConsumerInner {
	fn drop(&mut self) {
		let Some(stream) = self.stream.upgrade() else {
			return;
		};
		let mut state = stream.state.write().unwrap();
		let remove = state
			.consumers
			.get(&self.name)
			.is_some_and(|(_, state_)| state_.config.durable_name.is_none());
		if !remove {
			return;
		}
		let Some((_, consumer_state)) = state.consumers.remove(&self.name) else {
			return;
		};
		// Ack all unacknowledged messages that this consumer was responsible for.
		let sequences = state
			.messages
			.range(..=consumer_state.sequence)
			.filter(|(_, message)| {
				consumer_state
					.config
					.filter_subject
					.as_ref()
					.is_none_or(|filter| filter == &message.subject)
			})
			.map(|(sequence, _)| *sequence)
			.collect::<Vec<_>>();
		for sequence in sequences {
			Stream::ack_inner(&mut state, sequence);
		}
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	#[tokio::test]
	async fn workqueue_retention() {
		let messenger = Messenger::new();
		let stream = messenger.create_stream(
			"stream".into(),
			StreamConfig {
				discard: crate::DiscardPolicy::New,
				max_bytes: None,
				max_messages: None,
				retention: crate::RetentionPolicy::WorkQueue,
			},
		);

		// Publish some messages.
		stream
			.publish("subject1".into(), Bytes::from(b"hello, subject1".to_vec()))
			.await
			.unwrap();
		stream
			.publish("subject2".into(), Bytes::from(b"hello, subject2".to_vec()))
			.await
			.unwrap();
		let config = ConsumerConfig {
			deliver_policy: crate::DeliverPolicy::All,
			ack_policy: crate::AckPolicy::None,
			durable_name: None,
			filter_subject: Some("subject1".into()),
		};
		let consumer1 = stream.create_consumer(None, config);
		let messages = consumer1
			.subscribe::<Bytes>()
			.await
			.unwrap()
			.take(1)
			.map_ok(|result| result.payload)
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		assert_eq!(messages.len(), 1);

		let config = ConsumerConfig {
			deliver_policy: crate::DeliverPolicy::All,
			ack_policy: crate::AckPolicy::None,
			durable_name: None,
			filter_subject: Some("subject2".into()),
		};
		let consumer2 = stream.create_consumer(None, config);
		let messages = consumer2
			.subscribe::<Bytes>()
			.await
			.unwrap()
			.take(1)
			.map_ok(|result| result.payload)
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		assert_eq!(messages.len(), 1);
	}
}
