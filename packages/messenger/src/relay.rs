use {
	crate::{Messenger, Payload},
	bytes::{Buf as _, BufMut as _, Bytes, BytesMut},
	futures::{Stream, StreamExt as _, stream::BoxStream},
	std::{
		collections::VecDeque,
		marker::PhantomData,
		pin::{Pin, pin},
		sync::Arc,
		task::{Context, Poll},
		time::Duration,
	},
	tokio::{
		sync::{mpsc, oneshot},
		time::{Instant, sleep_until},
	},
};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug)]
pub struct Options {
	/// How long a receiver waits after its consumer drains before it acknowledges fewer than `ack_threshold` bytes.
	pub ack_delay: Duration,

	/// The number of consumed bytes at which a receiver acknowledges without waiting for `ack_delay`.
	pub ack_threshold: u64,

	/// The number of consecutive retransmissions without a response after which an end times out.
	pub max_retransmits: u32,

	/// The number of bytes that each message counts against the window in addition to its payload.
	pub message_overhead: u64,

	/// How long a sender waits for an acknowledgment before it retransmits.
	pub timeout: Duration,

	/// The number of unacknowledged bytes that a receiver allows its peer to send.
	pub window: u64,
}

#[derive(Clone, Debug)]
pub enum Error {
	Aborted(Bytes),
	Closed,
	Messenger(Arc<crate::Error>),
	Options(&'static str),
	Reset,
	Timeout,
}

/// The sending half of a connection. Dropping it without `finish` or `abort` resets the connection.
pub struct Sender<T> {
	_alive: oneshot::Sender<()>,
	events: mpsc::Sender<SendEvent>,
	marker: PhantomData<fn(T)>,
	result: Option<oneshot::Receiver<Result<(), Error>>>,
}

/// The receiving half of a connection, which is a stream of the peer's items. Each item is acknowledged when the next one is polled, so a consumer that stops polling holds the peer back once the window fills. Dropping it before the peer's end arrives resets the connection, and dropping it afterward acknowledges the remaining items.
pub struct Receiver<T> {
	acks: mpsc::UnboundedSender<u64>,
	events: mpsc::UnboundedReceiver<Result<(u64, T), Error>>,
	last: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Message {
	Abort {
		payload: Bytes,
		sequence: u64,
	},
	Ack {
		received: u64,
		sequence: u64,
		window: u64,
	},
	Data {
		payload: Bytes,
		sequence: u64,
	},
	End {
		sequence: u64,
	},
	Probe,
	Reset,
}

enum SendEvent {
	Abort(Bytes),
	Finish,
	Item(Bytes),
}

struct Connection<M, T> {
	heard: Instant,
	messenger: M,
	options: Options,
	receive: Incoming<T>,
	reset: bool,
	send: Outgoing,
	subject: String,
}

struct Outgoing {
	abort: Option<Bytes>,
	acknowledged: u64,
	alive: Option<oneshot::Receiver<()>>,
	attempts: u32,
	deadline: Instant,
	end: Option<u64>,
	events: mpsc::Receiver<SendEvent>,
	finished: bool,
	last_resend: Instant,
	next: u64,
	pending: Option<(Bytes, u64)>,
	received: u64,
	result: Option<oneshot::Sender<Result<(), Error>>>,
	unacknowledged: VecDeque<Entry>,
	unacknowledged_size: u64,
	window: Option<u64>,
}

struct Entry {
	kind: EntryKind,
	sequence: u64,
	size: u64,
}

enum EntryKind {
	Abort(Bytes),
	Data(Bytes),
	End,
}

struct Incoming<T> {
	ack_deadline: Option<Instant>,
	acked: u64,
	acks: Option<mpsc::UnboundedReceiver<u64>>,
	consumed: u64,
	end: Option<u64>,
	events: Option<mpsc::UnboundedSender<Result<(u64, T), Error>>>,
	expected: u64,
	sizes: VecDeque<(u64, u64)>,
	unacknowledged: u64,
}

/// Open one end of a relay, a duplex stream between two equal ends, sending on one subject and receiving on the other. The peer opens the other end with the same two subjects swapped. There is no handshake: the peer may open before or after this end, and a peer that never opens is reported as `Error::Timeout`. The subjects identify the connection, so each connection must use new subjects.
pub async fn open<S, R, M>(
	messenger: &M,
	send_subject: impl Into<String>,
	receive_subject: impl Into<String>,
	options: Options,
) -> Result<(Sender<S>, Receiver<R>), Error>
where
	S: Payload,
	R: Payload,
	M: Messenger + Clone + Send + Sync + 'static,
{
	// Validate the options.
	options.validate()?;

	// Subscribe to the peer's subject before publishing, so that no message from the peer is missed.
	let send_subject = send_subject.into();
	let receive_subject = receive_subject.into();
	let incoming = messenger
		.subscribe::<Message>(receive_subject)
		.await
		.map_err(Error::messenger)?
		.boxed();

	// Create the halves.
	let (alive_sender, alive) = oneshot::channel();
	let (event_sender, events) = mpsc::channel(1);
	let (result_sender, result) = oneshot::channel();
	let (ack_sender, acks) = mpsc::unbounded_channel();
	let (receive_event_sender, receive_events) = mpsc::unbounded_channel();
	let sender = Sender {
		_alive: alive_sender,
		events: event_sender,
		marker: PhantomData,
		result: Some(result),
	};
	let receiver = Receiver {
		acks: ack_sender,
		events: receive_events,
		last: None,
	};

	// Run the connection.
	let now = Instant::now();
	let send = Outgoing {
		abort: None,
		acknowledged: 0,
		alive: Some(alive),
		attempts: 0,
		deadline: now + options.timeout,
		end: None,
		events,
		finished: false,
		last_resend: now,
		next: 1,
		pending: None,
		received: 0,
		result: Some(result_sender),
		unacknowledged: VecDeque::new(),
		unacknowledged_size: 0,
		window: None,
	};
	let receive = Incoming {
		ack_deadline: None,
		acked: 0,
		acks: Some(acks),
		consumed: 0,
		end: None,
		events: Some(receive_event_sender),
		expected: 1,
		sizes: VecDeque::new(),
		unacknowledged: 0,
	};
	let connection = Connection {
		heard: now,
		messenger: messenger.clone(),
		options,
		receive,
		reset: false,
		send,
		subject: send_subject,
	};
	tokio::spawn(connection.run(incoming));

	Ok((sender, receiver))
}

impl Options {
	/// Get the duration after which an end that has not heard from its peer stops.
	#[must_use]
	pub fn peer_timeout(&self) -> Duration {
		self.timeout * self.max_retransmits
	}

	pub fn validate(&self) -> Result<(), Error> {
		if self.timeout.is_zero()
			|| self.window == 0
			|| self.ack_threshold == 0
			|| self.max_retransmits == 0
		{
			return Err(Error::Options(
				"the timeout, window, ack threshold, and max retransmits must be greater than zero",
			));
		}
		if self.ack_threshold > self.window {
			return Err(Error::Options(
				"the ack threshold must not exceed the window",
			));
		}
		if self.ack_delay >= self.timeout {
			return Err(Error::Options(
				"the ack delay must be less than the timeout",
			));
		}
		Ok(())
	}
}

impl<T> Sender<T>
where
	T: Payload,
{
	/// Send an item, waiting while the peer's window is full.
	pub async fn send(&mut self, item: T) -> Result<(), Error> {
		let payload = item.serialize().map_err(Error::messenger)?;
		self.event(SendEvent::Item(payload)).await
	}

	/// Send every item of the stream, then finish, or abort with the payload of an error item.
	pub async fn send_all<S>(mut self, items: S) -> Result<(), Error>
	where
		S: Stream<Item = Result<T, Bytes>> + Send,
	{
		let mut items = pin!(items);
		while let Some(item) = items.next().await {
			match item {
				Ok(item) => self.send(item).await?,
				Err(payload) => return self.abort(payload).await,
			}
		}
		self.finish().await
	}

	/// Close this direction, and wait for the peer to acknowledge its end.
	pub async fn finish(mut self) -> Result<(), Error> {
		self.event(SendEvent::Finish).await?;
		self.result().await
	}

	/// Abort this direction with a payload that the peer receives as `Error::Aborted`.
	pub async fn abort(mut self, payload: Bytes) -> Result<(), Error> {
		self.event(SendEvent::Abort(payload)).await?;
		self.result().await
	}

	async fn event(&mut self, event: SendEvent) -> Result<(), Error> {
		if self.events.send(event).await.is_err() {
			let error = self.result().await.err().unwrap_or(Error::Closed);
			return Err(error);
		}
		Ok(())
	}

	async fn result(&mut self) -> Result<(), Error> {
		let Some(result) = self.result.take() else {
			return Err(Error::Closed);
		};
		result.await.unwrap_or(Err(Error::Closed))
	}
}

impl Error {
	fn messenger(error: crate::Error) -> Self {
		Self::Messenger(Arc::new(error))
	}
}

impl<M, T> Connection<M, T>
where
	M: Messenger + Send + Sync + 'static,
	T: Payload,
{
	async fn run(
		mut self,
		mut incoming: BoxStream<'static, Result<crate::Message<Message>, crate::Error>>,
	) {
		let result = self.run_inner(&mut incoming).await;
		let Err(error) = result else {
			return;
		};

		// Tell the peer to stop unless the peer reset the connection, and fail both halves.
		if !self.reset {
			self.publish(Message::Reset).await.ok();
		}
		if let Some(result) = self.send.result.take() {
			result.send(Err(error.clone())).ok();
		}
		if let Some(events) = self.receive.events.take() {
			events.send(Err(error)).ok();
		}
	}

	async fn run_inner(
		&mut self,
		incoming: &mut BoxStream<'static, Result<crate::Message<Message>, crate::Error>>,
	) -> Result<(), Error> {
		// Advertise the window and probe the peer, so that each side learns the other's window without waiting.
		self.publish_ack(0).await?;
		self.publish(Message::Probe).await?;

		loop {
			self.send_pending().await?;

			// Complete the sending half once the peer acknowledges its end.
			if self
				.send
				.end
				.is_some_and(|end| self.send.acknowledged >= end)
				&& let Some(result) = self.send.result.take()
			{
				result.send(Ok(())).ok();
			}

			let sending = self.send.result.is_some();
			let received = self
				.receive
				.end
				.is_some_and(|end| self.receive.acks.is_none() || self.receive.consumed + 1 >= end);
			let pull = sending
				&& self.send.window.is_some()
				&& self.send.pending.is_none()
				&& !self.send.finished;
			let peer_deadline = self.heard + self.options.peer_timeout();
			tokio::select! {
				biased;
				sequence = recv_ack(&mut self.receive.acks), if self.receive.acks.is_some() => {
					let Some(sequence) = sequence else {
						// The receiving half was dropped, so reset the connection if the peer has not ended, and otherwise acknowledge every item through the end.
						self.receive.acks = None;
						let Some(end) = self.receive.end else {
							return Err(Error::Reset);
						};
						self.receive.ack_deadline = None;
						self.receive.consumed = self.receive.consumed.max(end - 1);
						self.receive.sizes.clear();
						self.flush().await?;
						continue;
					};
					self.consume(sequence).await?;
				},
				message = incoming.next() => {
					let message = message
						.ok_or(Error::Closed)?
						.map_err(Error::messenger)?
						.payload;
					self.handle(message).await?;
				},
				event = self.send.events.recv(), if pull => match event {
					Some(SendEvent::Abort(payload)) => {
						self.send.abort = Some(payload);
						self.send.finished = true;
					},
					Some(SendEvent::Finish) => self.send.finished = true,
					Some(SendEvent::Item(payload)) => {
						let size = payload.len() as u64 + self.options.message_overhead;
						self.send.pending = Some((payload, size));
					},
					None => return Err(Error::Reset),
				},
				() = dropped(&mut self.send.alive), if self.send.alive.is_some() && !self.send.finished => {
					// The sending half was dropped without finishing, so reset the connection.
					return Err(Error::Reset);
				},
				() = sleep_until(self.send.deadline), if sending => self.retransmit().await?,
				() = sleep_until_some(self.receive.ack_deadline), if self.receive.ack_deadline.is_some() => {
					self.receive.ack_deadline = None;
					self.flush().await?;
				},
				() = sleep_until(peer_deadline), if self.receive.end.is_none() => return Err(Error::Timeout),
				() = sleep_until(peer_deadline), if !sending && received => {
					// Linger until the peer is silent once both halves are done, so that a peer whose acknowledgment of its end was lost receives it again.
					return Ok(());
				},
			}
		}
	}

	async fn handle(&mut self, message: Message) -> Result<(), Error> {
		self.heard = Instant::now();
		match message {
			Message::Abort { payload, sequence } => self.receive_abort(payload, sequence).await?,
			Message::Ack {
				received,
				sequence,
				window,
			} => self.acknowledge(received, sequence, window).await?,
			Message::Data { payload, sequence } => self.receive_data(payload, sequence).await?,
			Message::End { sequence } => self.receive_end(sequence).await?,
			Message::Probe => self.flush().await?,
			Message::Reset => {
				self.reset = true;
				return Err(Error::Reset);
			},
		}
		Ok(())
	}

	async fn send_pending(&mut self) -> Result<(), Error> {
		// Send the abort after every message that precedes it, without waiting for the window.
		if let Some(payload) = self.send.abort.take() {
			let sequence = self.send.next;
			self.send.end = Some(sequence);
			let message = Message::Abort {
				payload: payload.clone(),
				sequence,
			};
			let size = self.options.message_overhead;
			self.send_entry(EntryKind::Abort(payload), message, size)
				.await?;
		}

		// Send the pending message while the window allows, and then the end once the sending half finishes.
		let Some(window) = self.send.window else {
			return Ok(());
		};
		if let Some((payload, size)) = self.send.pending.take() {
			if self.send.unacknowledged.is_empty() || self.send.unacknowledged_size + size <= window
			{
				let message = Message::Data {
					payload: payload.clone(),
					sequence: self.send.next,
				};
				self.send_entry(EntryKind::Data(payload), message, size)
					.await?;
			} else {
				self.send.pending = Some((payload, size));
			}
		}
		if self.send.finished && self.send.pending.is_none() && self.send.end.is_none() {
			let sequence = self.send.next;
			self.send.end = Some(sequence);
			let message = Message::End { sequence };
			let size = self.options.message_overhead;
			self.send_entry(EntryKind::End, message, size).await?;
		}

		Ok(())
	}

	async fn send_entry(
		&mut self,
		kind: EntryKind,
		message: Message,
		size: u64,
	) -> Result<(), Error> {
		self.publish(message).await?;
		if self.send.unacknowledged.is_empty() {
			self.send.deadline = Instant::now() + self.options.timeout;
		}
		let entry = Entry {
			kind,
			sequence: self.send.next,
			size,
		};
		self.send.unacknowledged.push_back(entry);
		self.send.unacknowledged_size += size;
		self.send.next += 1;
		Ok(())
	}

	async fn acknowledge(
		&mut self,
		received: u64,
		sequence: u64,
		window: u64,
	) -> Result<(), Error> {
		if self.send.result.is_none() {
			return Ok(());
		}
		self.send.window = Some(window);

		// Restart the retransmission count when the peer receives a new message or holds every message, so that a peer whose consumer holds back the window stays alive, but a peer that keeps missing a message times out.
		let progressed = received > self.send.received;
		let complete = self
			.send
			.unacknowledged
			.back()
			.is_none_or(|entry| entry.sequence <= received);
		if progressed || complete {
			self.send.attempts = 0;
			self.send.deadline = Instant::now() + self.options.timeout;
		}
		self.send.received = self.send.received.max(received);
		if sequence > self.send.acknowledged {
			while self
				.send
				.unacknowledged
				.front()
				.is_some_and(|entry| entry.sequence <= sequence)
			{
				let entry = self.send.unacknowledged.pop_front().unwrap();
				self.send.unacknowledged_size -= entry.size;
			}
			self.send.acknowledged = sequence;
		} else if self
			.send
			.unacknowledged
			.back()
			.is_some_and(|entry| entry.sequence > self.send.received)
			&& self.send.last_resend.elapsed() >= self.options.timeout
		{
			// A repeated acknowledgment that has not received every message means the peer is missing a message.
			self.resend().await?;
			self.send.last_resend = Instant::now();
		}

		Ok(())
	}

	async fn retransmit(&mut self) -> Result<(), Error> {
		let now = Instant::now();
		self.send.deadline = now + self.options.timeout;

		// Skip the probe of an idle sender that knows the peer's window and heard from the peer recently, so that an idle connection is kept alive by one side.
		if self.send.unacknowledged.is_empty()
			&& self.send.window.is_some()
			&& now.duration_since(self.heard) < self.options.timeout
		{
			self.send.attempts = 0;
			return Ok(());
		}

		// Resend what the peer has not received, or probe it, so that both sides know the other is alive.
		self.send.attempts += 1;
		if self.send.attempts > self.options.max_retransmits {
			return Err(Error::Timeout);
		}
		let resent = self.resend().await?;
		if resent == 0 {
			self.publish(Message::Probe).await?;
		}
		self.send.last_resend = now;

		Ok(())
	}

	async fn resend(&self) -> Result<usize, Error> {
		let mut count = 0;
		for entry in self
			.send
			.unacknowledged
			.iter()
			.filter(|entry| entry.sequence > self.send.received)
		{
			let sequence = entry.sequence;
			let message = match &entry.kind {
				EntryKind::Abort(payload) => Message::Abort {
					payload: payload.clone(),
					sequence,
				},
				EntryKind::Data(payload) => Message::Data {
					payload: payload.clone(),
					sequence,
				},
				EntryKind::End => Message::End { sequence },
			};
			self.publish(message).await?;
			count += 1;
		}
		Ok(count)
	}

	async fn receive_abort(&mut self, payload: Bytes, sequence: u64) -> Result<(), Error> {
		if sequence == self.receive.expected {
			// The peer stops at an abort, so acknowledge it without waiting for the consumer.
			self.receive.expected += 1;
			self.receive.end = Some(sequence);
			if let Some(events) = self.receive.events.take() {
				events.send(Err(Error::Aborted(payload))).ok();
			}
			self.publish_ack(sequence).await?;
		} else if self.receive.end == Some(sequence) {
			self.publish_ack(sequence).await?;
		} else if sequence > self.receive.expected {
			self.publish_ack(self.receive.acked).await?;
		}
		Ok(())
	}

	async fn receive_data(&mut self, payload: Bytes, sequence: u64) -> Result<(), Error> {
		if sequence == self.receive.expected {
			self.receive.expected += 1;
			let size = payload.len() as u64 + self.options.message_overhead;
			self.receive.sizes.push_back((sequence, size));
			let item = T::deserialize(payload);
			if let Some(events) = self.receive.events.as_ref() {
				let event = item.map(|item| (sequence, item)).map_err(Error::messenger);
				events.send(event).ok();
			}
		} else if sequence > self.receive.expected {
			// A gap means a message was lost, so ask the peer to resend.
			self.publish_ack(self.receive.acked).await?;
		} else {
			// A duplicate means an acknowledgment may have been lost, so acknowledge again.
			self.flush().await?;
		}
		Ok(())
	}

	async fn receive_end(&mut self, sequence: u64) -> Result<(), Error> {
		if sequence == self.receive.expected {
			self.receive.expected += 1;
			self.receive.end = Some(sequence);
			self.receive.events.take();
			self.flush().await?;
		} else if self.receive.end == Some(sequence) {
			// The peer did not receive the acknowledgment of the end.
			self.flush().await?;
		} else if sequence > self.receive.expected {
			self.publish_ack(self.receive.acked).await?;
		}
		Ok(())
	}

	async fn consume(&mut self, sequence: u64) -> Result<(), Error> {
		if sequence <= self.receive.consumed {
			return Ok(());
		}
		self.receive.consumed = sequence;
		while self
			.receive
			.sizes
			.front()
			.is_some_and(|(entry, _)| *entry <= sequence)
		{
			let (_, size) = self.receive.sizes.pop_front().unwrap();
			self.receive.unacknowledged += size;
		}
		if self.receive.unacknowledged >= self.options.ack_threshold
			|| self
				.receive
				.end
				.is_some_and(|end| self.receive.consumed + 1 >= end)
		{
			self.receive.ack_deadline = None;
			self.flush().await?;
		} else if self.receive.consumed + 1 == self.receive.expected {
			// Acknowledge shortly after the consumer drains, so that small writes do not wait for the threshold.
			self.receive
				.ack_deadline
				.get_or_insert_with(|| Instant::now() + self.options.ack_delay);
		}
		Ok(())
	}

	async fn flush(&mut self) -> Result<(), Error> {
		let sequence = match self.receive.end {
			Some(end) if self.receive.consumed + 1 >= end => end,
			_ => self.receive.consumed,
		};
		self.receive.unacknowledged = 0;
		self.publish_ack(sequence).await
	}

	async fn publish_ack(&mut self, sequence: u64) -> Result<(), Error> {
		self.receive.acked = self.receive.acked.max(sequence);
		let message = Message::Ack {
			received: self.receive.expected - 1,
			sequence,
			window: self.options.window,
		};
		self.publish(message).await
	}

	async fn publish(&self, message: Message) -> Result<(), Error> {
		self.messenger
			.publish(self.subject.clone(), message)
			.await
			.map_err(Error::messenger)
	}
}

impl Payload for Message {
	fn serialize(&self) -> Result<Bytes, crate::Error> {
		let mut bytes = BytesMut::new();
		match self {
			Self::Abort { payload, sequence } => {
				bytes.put_u8(0);
				bytes.put_u64(*sequence);
				bytes.put_slice(payload);
			},
			Self::Ack {
				received,
				sequence,
				window,
			} => {
				bytes.put_u8(1);
				bytes.put_u64(*received);
				bytes.put_u64(*sequence);
				bytes.put_u64(*window);
			},
			Self::Data { payload, sequence } => {
				bytes.put_u8(2);
				bytes.put_u64(*sequence);
				bytes.put_slice(payload);
			},
			Self::End { sequence } => {
				bytes.put_u8(3);
				bytes.put_u64(*sequence);
			},
			Self::Probe => {
				bytes.put_u8(4);
			},
			Self::Reset => {
				bytes.put_u8(5);
			},
		}
		Ok(bytes.freeze())
	}

	fn deserialize(mut bytes: Bytes) -> Result<Self, crate::Error> {
		let invalid = || crate::Error::deserialization("invalid relay message");
		if bytes.is_empty() {
			return Err(invalid());
		}
		let kind = bytes.get_u8();
		let message = match kind {
			0 => {
				if bytes.len() < 8 {
					return Err(invalid());
				}
				let sequence = bytes.get_u64();
				Self::Abort {
					payload: bytes,
					sequence,
				}
			},
			1 => {
				if bytes.len() != 24 {
					return Err(invalid());
				}
				let received = bytes.get_u64();
				let sequence = bytes.get_u64();
				let window = bytes.get_u64();
				Self::Ack {
					received,
					sequence,
					window,
				}
			},
			2 => {
				if bytes.len() < 8 {
					return Err(invalid());
				}
				let sequence = bytes.get_u64();
				Self::Data {
					payload: bytes,
					sequence,
				}
			},
			3 => {
				if bytes.len() != 8 {
					return Err(invalid());
				}
				let sequence = bytes.get_u64();
				Self::End { sequence }
			},
			4 | 5 => {
				if !bytes.is_empty() {
					return Err(invalid());
				}
				if kind == 4 { Self::Probe } else { Self::Reset }
			},
			_ => return Err(invalid()),
		};
		Ok(message)
	}
}

impl<T> Stream for Receiver<T> {
	type Item = Result<T, Error>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();

		// Acknowledge the previous item, which the consumer has finished with now that it polls for the next one.
		if let Some(sequence) = this.last.take() {
			this.acks.send(sequence).ok();
		}

		this.events.poll_recv(cx).map(|event| {
			event.map(|event| {
				event.map(|(sequence, item)| {
					this.last = Some(sequence);
					item
				})
			})
		})
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Aborted(_) => write!(f, "the relay was aborted"),
			Self::Closed => write!(f, "the relay subscription closed"),
			Self::Messenger(_) => write!(f, "the relay messenger failed"),
			Self::Options(message) => write!(f, "invalid relay options: {message}"),
			Self::Reset => write!(f, "the relay was reset"),
			Self::Timeout => write!(f, "the relay timed out"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Messenger(error) => Some(error.as_ref()),
			Self::Aborted(_) | Self::Closed | Self::Options(_) | Self::Reset | Self::Timeout => {
				None
			},
		}
	}
}

async fn recv_ack(acks: &mut Option<mpsc::UnboundedReceiver<u64>>) -> Option<u64> {
	acks.as_mut().unwrap().recv().await
}

async fn dropped(alive: &mut Option<oneshot::Receiver<()>>) {
	alive.as_mut().unwrap().await.ok();
	alive.take();
}

async fn sleep_until_some(deadline: Option<Instant>) {
	sleep_until(deadline.unwrap()).await;
}
