use {
	super::{Error, Event, Message, Observer, Options, Protocol, announce_subject, server_subject},
	crate::{Messenger, Payload},
	bytes::Bytes,
	futures::{Stream, StreamExt as _, stream::BoxStream},
	std::{
		collections::{BTreeMap, BTreeSet},
		marker::PhantomData,
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
	},
	tokio::{
		sync::{mpsc, watch},
		time::{Instant, MissedTickBehavior, interval},
	},
};

/// The server end of a service, which is a stream of incoming requests. Dropping it stops answering, and clients recover their requests with the next server that starts on the same subject.
pub struct Server<P: Protocol> {
	incoming: mpsc::UnboundedReceiver<Incoming<P>>,
	status: watch::Receiver<Status>,
	task: tokio::task::JoinHandle<()>,
}

/// A request received by the server.
pub struct Incoming<P: Protocol> {
	pub reply: Reply<P>,
	pub request: P::Request,
}

/// The handle with which the server answers a request.
pub struct Reply<P: Protocol> {
	attempt: String,
	cancelled: watch::Receiver<bool>,
	commands: mpsc::UnboundedSender<Command>,
	id: String,
	marker: PhantomData<fn(P::Response)>,
}

/// The server's attempts and responses, which a server that is stopping waits on.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Status {
	/// The number of live attempts.
	pub attempts: usize,

	/// The number of responses that have not been acknowledged.
	pub unacknowledged: usize,
}

enum Command {
	Acknowledge {
		attempt: String,
		id: String,
	},
	Respond {
		attempt: String,
		id: String,
		response: Bytes,
	},
}

struct Task<P: Protocol, M> {
	commands: mpsc::UnboundedSender<Command>,
	incoming: mpsc::UnboundedSender<Incoming<P>>,
	messenger: M,
	observer: Arc<dyn Observer<P>>,
	options: Options,
	state: State,
	status: watch::Sender<Status>,
}

#[derive(Default)]
struct State {
	attempts: BTreeMap<String, Attempt>,
	clients: BTreeMap<String, String>,
	response_cursor: Option<(String, String)>,
}

struct Attempt {
	cancelled: BTreeSet<String>,
	client: String,
	entries: BTreeMap<String, Entry>,
	expires_at: Instant,
}

struct Entry {
	/// When the client acknowledged the response.
	acknowledged_at: Option<Instant>,

	cancelled: Option<watch::Sender<bool>>,

	/// Whether the server acknowledged the request.
	received: bool,

	response: Option<Message>,
}

/// Start answering requests on a subject. The server announces itself, so that clients that arrived before it recover their requests immediately.
pub async fn serve<P, M>(
	messenger: &M,
	subject: impl Into<String>,
	options: Options,
	observer: Arc<dyn Observer<P>>,
) -> Result<Server<P>, Error>
where
	P: Protocol,
	M: Messenger + Clone + Send + Sync + 'static,
{
	// Validate the options.
	options.validate()?;

	// Subscribe to the clients before announcing, so that no request resent in response to the announcement is missed.
	let subject = subject.into();
	let messages = messenger
		.subscribe::<Message>(server_subject(&subject))
		.await
		.map_err(Error::messenger)?
		.boxed();
	messenger
		.publish(announce_subject(&subject), Message::Announce)
		.await
		.map_err(Error::messenger)?;

	// Spawn the task that answers heartbeats, deduplicates requests, resends responses, and expires attempts.
	let (incoming_sender, incoming) = mpsc::unbounded_channel();
	let (command_sender, commands) = mpsc::unbounded_channel();
	let (status_sender, status) = watch::channel(Status::default());
	let task = Task {
		commands: command_sender,
		incoming: incoming_sender,
		messenger: messenger.clone(),
		observer,
		options,
		state: State::default(),
		status: status_sender,
	};
	let task = tokio::spawn(async move {
		task.run(messages, commands).await;
	});
	let server = Server {
		incoming,
		status,
		task,
	};

	Ok(server)
}

impl<P: Protocol> Server<P> {
	/// Get the server's attempts and responses.
	#[must_use]
	pub fn status(&self) -> Status {
		*self.status.borrow()
	}

	/// Wait until the server's attempts or responses change.
	pub async fn changed(&mut self) -> Result<(), Error> {
		self.status.changed().await.map_err(|_| Error::Closed)
	}
}

impl<P: Protocol> Reply<P> {
	/// Get the ID of the request.
	#[must_use]
	pub fn id(&self) -> &str {
		&self.id
	}

	/// Get the ID of the attempt under which the request was received.
	#[must_use]
	pub fn attempt(&self) -> &str {
		&self.attempt
	}

	/// Acknowledge the request without responding, so that the client stops resending it and waits for the response.
	pub fn acknowledge(&self) -> Result<(), Error> {
		let command = Command::Acknowledge {
			attempt: self.attempt.clone(),
			id: self.id.clone(),
		};
		self.commands.send(command).map_err(|_| Error::Closed)?;
		Ok(())
	}

	/// Send the response. The server resends it until the client acknowledges it.
	pub fn send(&self, response: &P::Response) -> Result<(), Error> {
		let response = response.serialize().map_err(Error::messenger)?;
		let command = Command::Respond {
			attempt: self.attempt.clone(),
			id: self.id.clone(),
			response,
		};
		self.commands.send(command).map_err(|_| Error::Closed)?;
		Ok(())
	}

	/// Wait until the client cancels the request or its attempt expires.
	pub async fn cancelled(&self) {
		let mut cancelled = self.cancelled.clone();
		cancelled.wait_for(|cancelled| *cancelled).await.ok();
	}

	/// Check whether the client has cancelled the request or its attempt has expired.
	#[must_use]
	pub fn is_cancelled(&self) -> bool {
		*self.cancelled.borrow() || self.cancelled.has_changed().is_err()
	}
}

impl<P, M> Task<P, M>
where
	P: Protocol,
	M: Messenger + Clone + Send + Sync + 'static,
{
	async fn run(
		mut self,
		mut messages: BoxStream<'static, Result<crate::Message<Message>, crate::Error>>,
		mut commands: mpsc::UnboundedReceiver<Command>,
	) {
		let mut retry = interval(self.options.retry_interval);
		retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				message = messages.next() => {
					let Some(Ok(message)) = message else {
						break;
					};
					self.message(message.payload).await;
				},
				Some(command) = commands.recv() => {
					match command {
						Command::Acknowledge { attempt, id } => self.acknowledge(attempt, id).await,
						Command::Respond { attempt, id, response } => self.respond(attempt, id, response).await,
					}
				},
				_ = retry.tick() => {
					self.expire();
					self.send_responses().await;
				},
			}
			self.update_status();
		}

		// Cancel the pending requests when the server stops.
		for attempt in std::mem::take(&mut self.state.attempts).into_values() {
			for entry in attempt.entries.into_values() {
				if let Some(cancelled) = entry.cancelled {
					cancelled.send_replace(true);
				}
			}
		}
	}

	async fn message(&mut self, message: Message) {
		match message {
			Message::Ack { attempt, id } => {
				if let Some(entry) = self
					.state
					.attempts
					.get_mut(&attempt)
					.and_then(|attempt| attempt.entries.get_mut(&id))
					&& entry.response.is_some()
				{
					entry.acknowledged_at.get_or_insert_with(Instant::now);
					let event = Event::ResponseAcknowledged {
						attempt: &attempt,
						id: &id,
					};
					self.observer.observe(event).await;
				}
			},
			Message::Cancel {
				attempt,
				client,
				id,
			} => {
				// Cancel a request that was sent without an attempt under the client's current attempt.
				let Some(attempt) = attempt.or_else(|| self.state.clients.get(&client).cloned())
				else {
					return;
				};
				let Some(entry) = self.state.attempts.get_mut(&attempt) else {
					return;
				};
				entry.cancelled.insert(id.clone());
				if let Some(entry) = entry.entries.remove(&id)
					&& let Some(cancelled) = entry.cancelled
				{
					cancelled.send_replace(true);
				}
				let event = Event::Cancelled {
					attempt: &attempt,
					id: &id,
				};
				self.observer.observe(event).await;
			},
			Message::Heartbeat { client, id } => {
				self.heartbeat(client, id).await;
			},
			Message::Request {
				attempt,
				client,
				id,
				request,
			} => {
				self.request(attempt, client, id, request).await;
			},
			Message::Announce
			| Message::HeartbeatResponse { .. }
			| Message::RequestAck { .. }
			| Message::Response { .. } => (),
		}
	}

	async fn heartbeat(&mut self, client: String, id: String) {
		let ttl = self.options.attempt_ttl;
		let attempt = self.attempt(&client);

		// Answer a retransmitted heartbeat with its response, without extending the attempt again.
		let entry = self.state.attempts.get_mut(&attempt).unwrap();
		if let Some(previous) = entry.entries.get(&id) {
			if let Some(response) = previous.response.clone() {
				self.publish(&client, response).await;
			}
			return;
		}

		// Extend the attempt and respond.
		entry.expires_at = Instant::now() + ttl;
		let response = Message::HeartbeatResponse {
			attempt: attempt.clone(),
			id: id.clone(),
			ttl,
		};
		let heartbeat = Entry {
			acknowledged_at: None,
			cancelled: None,
			received: true,
			response: Some(response.clone()),
		};
		entry.entries.insert(id.clone(), heartbeat);
		let event = Event::HeartbeatResponded {
			attempt: &attempt,
			client: &client,
			id: &id,
		};
		self.observer.observe(event).await;
		self.publish(&client, response).await;
	}

	async fn request(
		&mut self,
		attempt: Option<String>,
		client: String,
		id: String,
		request: Bytes,
	) {
		// Establish the client's attempt with a request sent without one, extending it because the request shows that the client is alive.
		let attempt = attempt.unwrap_or_else(|| {
			let attempt = self.attempt(&client);
			let entry = self.state.attempts.get_mut(&attempt).unwrap();
			entry.expires_at = Instant::now() + self.options.attempt_ttl;
			attempt
		});

		// Accept requests only under a live attempt of the client. A request under an unknown or expired attempt is recovered when the client resends it under a new one.
		let Some(entry) = self.state.attempts.get(&attempt) else {
			return;
		};
		if entry.client != client || Instant::now() >= entry.expires_at {
			return;
		}
		if entry.cancelled.contains(&id) {
			return;
		}

		// Answer a retransmitted request with its response, or acknowledge it again if the server acknowledged it. A request that the server has not acknowledged yet is still being handled.
		if let Some(previous) = entry.entries.get(&id) {
			let message = match &previous.response {
				Some(response) => Some(response.clone()),
				None if previous.received => Some(Message::RequestAck {
					attempt: attempt.clone(),
					id: id.clone(),
					ttl: self.options.attempt_ttl,
				}),
				None => None,
			};
			if let Some(message) = message {
				self.publish(&client, message).await;
			}
			return;
		}

		// Deliver a new request to the server, which acknowledges it or responds.
		let Ok(request) = P::Request::deserialize(request) else {
			return;
		};
		let (cancelled_sender, cancelled) = watch::channel(false);
		let entry = Entry {
			acknowledged_at: None,
			cancelled: Some(cancelled_sender),
			received: false,
			response: None,
		};
		self.state
			.attempts
			.get_mut(&attempt)
			.unwrap()
			.entries
			.insert(id.clone(), entry);
		let reply = Reply {
			attempt: attempt.clone(),
			cancelled,
			commands: self.commands.clone(),
			id: id.clone(),
			marker: PhantomData,
		};
		let incoming = Incoming { reply, request };
		self.incoming.send(incoming).ok();
	}

	async fn acknowledge(&mut self, attempt: String, id: String) {
		let Some(entry) = self.state.attempts.get_mut(&attempt) else {
			return;
		};
		let client = entry.client.clone();
		let Some(entry) = entry.entries.get_mut(&id) else {
			return;
		};
		if entry.received || entry.response.is_some() {
			return;
		}
		entry.received = true;
		let message = Message::RequestAck {
			attempt,
			id,
			ttl: self.options.attempt_ttl,
		};
		self.publish(&client, message).await;
	}

	async fn respond(&mut self, attempt: String, id: String, response: Bytes) {
		let Some(entry) = self.state.attempts.get_mut(&attempt) else {
			return;
		};
		let client = entry.client.clone();
		let Some(entry) = entry.entries.get_mut(&id) else {
			return;
		};
		if entry.response.is_some() {
			return;
		}
		let message = Message::Response {
			attempt,
			id,
			response,
		};
		entry.response = Some(message.clone());
		self.publish(&client, message).await;
	}

	/// Find the client's attempt, replacing it if it expired, or create one.
	fn attempt(&mut self, client: &str) -> String {
		// Replace the client's attempt if it expired.
		if self
			.state
			.clients
			.get(client)
			.is_some_and(|attempt| Instant::now() >= self.state.attempts[attempt].expires_at)
		{
			self.expire();
		}

		// Find or create the client's attempt.
		if let Some(attempt) = self.state.clients.get(client) {
			return attempt.clone();
		}
		let attempt = super::id();
		let entry = Attempt {
			cancelled: BTreeSet::new(),
			client: client.to_owned(),
			entries: BTreeMap::new(),
			expires_at: Instant::now() + self.options.attempt_ttl,
		};
		self.state.attempts.insert(attempt.clone(), entry);
		self.state
			.clients
			.insert(client.to_owned(), attempt.clone());

		attempt
	}

	/// Expire the attempts whose clients stopped sending heartbeats, and the acknowledged responses.
	fn expire(&mut self) {
		let now = Instant::now();
		let ttl = self.options.attempt_ttl;
		self.state.attempts.retain(|_, attempt| {
			attempt
				.entries
				.retain(|_, entry| entry.acknowledged_at.is_none_or(|time| now < time + ttl));
			let alive = now < attempt.expires_at;
			if !alive {
				for entry in attempt.entries.values() {
					if let Some(cancelled) = &entry.cancelled {
						cancelled.send_replace(true);
					}
				}
			}
			alive
		});
		let attempts = &self.state.attempts;
		self.state
			.clients
			.retain(|_, attempt| attempts.contains_key(attempt));
	}

	async fn send_responses(&mut self) {
		let mut responses = self
			.state
			.attempts
			.iter()
			.flat_map(|(attempt, entry)| {
				entry
					.entries
					.iter()
					.filter(|(_, entry)| {
						entry.acknowledged_at.is_none() && entry.response.is_some()
					})
					.map(|(id, _)| (attempt.clone(), id.clone()))
			})
			.collect::<Vec<_>>();
		responses.sort_unstable();
		if let Some(cursor) = &self.state.response_cursor {
			let offset = responses.partition_point(|key| key <= cursor);
			responses.rotate_left(offset);
		}

		// Resume at the next response if a slow messenger exhausts this batch's time.
		let timeout = self.options.retry_interval;
		let future = async {
			for (attempt, id) in responses {
				self.state.response_cursor = Some((attempt.clone(), id.clone()));
				let entry = &self.state.attempts[&attempt];
				let client = entry.client.clone();
				let message = entry.entries[&id].response.clone().unwrap();
				self.publish(&client, message).await;
			}
		};
		tokio::time::timeout(timeout, future).await.ok();
	}

	fn update_status(&self) {
		let unacknowledged = self
			.state
			.attempts
			.values()
			.flat_map(|attempt| attempt.entries.values())
			.filter(|entry| entry.response.is_some() && entry.acknowledged_at.is_none())
			.count();
		let status = Status {
			attempts: self.state.attempts.len(),
			unacknowledged,
		};
		self.status.send_if_modified(|current| {
			let modified = *current != status;
			*current = status;
			modified
		});
	}

	/// Publish a message to a client, whose identity is the subject that its responses are published to.
	async fn publish(&self, client: &str, message: Message) {
		tokio::time::timeout(
			self.options.retry_interval,
			self.messenger.publish(client.to_owned(), message),
		)
		.await
		.ok();
	}
}

impl<P: Protocol> Stream for Server<P> {
	type Item = Incoming<P>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.get_mut().incoming.poll_recv(cx)
	}
}

impl<P: Protocol> Drop for Server<P> {
	fn drop(&mut self) {
		self.task.abort();
	}
}
