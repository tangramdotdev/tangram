use {
	super::{
		Error, Event, Message, Observer, Options, Protocol, announce_subject, client_subject, id,
		server_subject,
	},
	crate::{Messenger, Payload},
	bytes::Bytes,
	futures::{StreamExt as _, stream::BoxStream},
	std::{
		collections::{BTreeMap, BTreeSet},
		marker::PhantomData,
		sync::{Arc, Mutex},
	},
	tokio::{
		sync::{mpsc, watch},
		time::{Instant, MissedTickBehavior, interval},
	},
};

/// The clients of a messenger, with one shared client per subject that stops after it has been idle.
pub struct Clients<P: Protocol, M> {
	messenger: M,
	observer: Arc<dyn Observer<P>>,
	options: Options,
	stopped: watch::Sender<bool>,
	subjects: Arc<Mutex<BTreeMap<String, Arc<Client<P>>>>>,
}

/// A pending request. Dropping it cancels the request at the server.
pub struct Request<P: Protocol> {
	client: Arc<Client<P>>,
	id: String,
	marker: PhantomData<fn() -> P::Response>,
	receiver: watch::Receiver<Output>,
}

/// The client for one subject, which a task runs.
struct Client<P: Protocol> {
	id: String,
	sender: mpsc::UnboundedSender<Command<P>>,
}

#[derive(Clone)]
enum Output {
	Pending,
	Ready(Result<Bytes, Error>),
}

enum Command<P: Protocol> {
	Cancel(String),
	Request {
		id: String,
		request: P::Request,
		sender: watch::Sender<Output>,
	},
}

struct Task<P: Protocol, M> {
	messenger: M,
	observer: Arc<dyn Observer<P>>,
	options: Options,
	state: State<P>,
	subjects: Arc<Mutex<BTreeMap<String, Arc<Client<P>>>>>,
}

struct State<P: Protocol> {
	attempt: Option<Attempt>,
	client: String,
	heartbeat_at: Instant,
	heartbeats: BTreeMap<String, Heartbeat>,
	idle_at: Option<Instant>,
	last_heartbeat: Option<Instant>,
	requests: BTreeMap<String, Pending<P>>,
	subject: String,
}

struct Attempt {
	deadline: Instant,
	id: String,
}

struct Heartbeat {
	acknowledged: bool,
	retry_at: Instant,
	sent_at: Instant,
}

struct Pending<P: Protocol> {
	acknowledged: bool,
	attempts: BTreeSet<String>,
	bytes: Bytes,
	deadline: Option<Instant>,
	request: P::Request,
	retry_at: Instant,
	sender: watch::Sender<Output>,
}

impl<P, M> Clients<P, M>
where
	P: Protocol,
	M: Messenger + Clone + Send + Sync + 'static,
{
	pub fn new(
		messenger: M,
		options: Options,
		observer: Arc<dyn Observer<P>>,
	) -> Result<Self, Error> {
		options.validate()?;
		let (stopped, _) = watch::channel(false);
		let clients = Self {
			messenger,
			observer,
			options,
			stopped,
			subjects: Arc::new(Mutex::new(BTreeMap::new())),
		};
		Ok(clients)
	}

	/// Send a request to the server of a subject. The server may start before or after the request is sent.
	pub fn request(&self, subject: &str, request: P::Request) -> Result<Request<P>, Error> {
		let id = id();
		let (sender, receiver) = watch::channel(Output::Pending);
		let command = Command::Request {
			id: id.clone(),
			request,
			sender,
		};

		// Send the request while holding the lock, so that an idle client cannot remove itself between being found and receiving the request.
		let mut subjects = self.subjects.lock().unwrap();
		let client = if let Some(client) = subjects
			.get(subject)
			.filter(|client| !client.sender.is_closed())
			.cloned()
		{
			client
		} else {
			let client = self.spawn(subject);
			subjects.insert(subject.to_owned(), client.clone());
			client
		};
		client.sender.send(command).map_err(|_| Error::Closed)?;
		drop(subjects);

		let request = Request {
			client,
			id,
			marker: PhantomData,
			receiver,
		};
		Ok(request)
	}

	/// Stop every client, failing their pending requests with `Error::Stopped`.
	pub fn stop(&self) {
		self.stopped.send_replace(true);
	}

	fn spawn(&self, subject: &str) -> Arc<Client<P>> {
		let (sender, commands) = mpsc::unbounded_channel();
		let client = Arc::new(Client { id: id(), sender });
		let state = State {
			attempt: None,
			client: client.id.clone(),
			heartbeat_at: Instant::now(),
			heartbeats: BTreeMap::new(),
			idle_at: None,
			last_heartbeat: None,
			requests: BTreeMap::new(),
			subject: subject.to_owned(),
		};
		let task = Task {
			messenger: self.messenger.clone(),
			observer: self.observer.clone(),
			options: self.options,
			state,
			subjects: self.subjects.clone(),
		};
		let stopped = self.stopped.subscribe();
		tokio::spawn(async move {
			task.run(commands, stopped).await;
		});
		client
	}
}

impl<P: Protocol> Request<P> {
	/// Wait for the response.
	pub async fn wait(&mut self) -> Result<P::Response, Error> {
		loop {
			let output = self.receiver.borrow_and_update().clone();
			match output {
				Output::Pending => {
					self.receiver.changed().await.map_err(|_| Error::Closed)?;
				},
				Output::Ready(result) => {
					let response = P::Response::deserialize(result?).map_err(Error::messenger)?;
					return Ok(response);
				},
			}
		}
	}
}

impl<P, M> Task<P, M>
where
	P: Protocol,
	M: Messenger + Clone + Send + Sync + 'static,
{
	async fn run(
		mut self,
		mut commands: mpsc::UnboundedReceiver<Command<P>>,
		mut stopped: watch::Receiver<bool>,
	) {
		// Subscribe to the responses and the announcements.
		let messages = self
			.messenger
			.subscribe::<Message>(client_subject(&self.state.subject, &self.state.client))
			.await;
		let announcements = self
			.messenger
			.subscribe::<Message>(announce_subject(&self.state.subject))
			.await;
		let (mut messages, mut announcements): (BoxStream<'static, _>, BoxStream<'static, _>) =
			match (messages, announcements) {
				(Ok(messages), Ok(announcements)) => (messages.boxed(), announcements.boxed()),
				(Err(error), _) | (_, Err(error)) => {
					let error = Error::messenger(error);
					self.fail(&error);
					commands.close();
					while let Some(command) = commands.recv().await {
						if let Command::Request { sender, .. } = command {
							sender.send_replace(Output::Ready(Err(error.clone())));
						}
					}
					return;
				},
			};
		let event = Event::Started {
			client: &self.state.client,
			subject: &self.state.subject,
		};
		self.observer.observe(event).await;

		let mut tick = interval(self.options.tick());
		tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
		let mut closed_at = None;
		let mut stopping = false;
		loop {
			tokio::select! {
				stopped = async { stopped.wait_for(|stopped| *stopped).await.is_ok() }, if !stopping => {
					stopping = true;
					if stopped {
						closed_at = Some(Instant::now());
						self.fail(&Error::Stopped);
					}
				},
				command = commands.recv(), if closed_at.is_none() => {
					match command {
						Some(Command::Cancel(id)) => {
							if let Some(request) = self.state.requests.remove(&id) {
								self.cancel(&id, request.attempts).await;
							}
						},
						Some(Command::Request { id, request, sender }) => {
							self.state.idle_at = None;
							let bytes = match request.serialize() {
								Ok(bytes) => bytes,
								Err(error) => {
									sender.send_replace(Output::Ready(Err(Error::messenger(error))));
									continue;
								},
							};
							let pending = Pending {
								acknowledged: false,
								attempts: BTreeSet::new(),
								bytes,
								deadline: Some(Instant::now() + self.options.request_timeout),
								request,
								retry_at: Instant::now(),
								sender,
							};
							self.state.requests.insert(id.clone(), pending);
							if self.state.attempt.is_none() {
								tick.reset_immediately();
							}
							self.send_request(&id).await;
						},
						None => {
							closed_at = Some(Instant::now());
						},
					}
				},
				message = messages.next() => {
					// Fail the pending requests with the response stream's error, so that they report its cause.
					let error = match message {
						Some(Ok(message)) => {
							self.message(message.payload).await;
							continue;
						},
						Some(Err(error)) => Error::messenger(error),
						None => Error::Closed,
					};
					self.fail(&error);
					return;
				},
				message = announcements.next() => {
					if message.is_none() {
						self.fail(&Error::Closed);
						return;
					}

					// Resend the pending heartbeat to a server that has just started, or start one if none is pending.
					if closed_at.is_none() && !self.state.requests.is_empty() {
						let now = Instant::now();
						let mut pending = false;
						for heartbeat in self.state.heartbeats.values_mut().filter(|heartbeat| !heartbeat.acknowledged) {
							heartbeat.retry_at = now;
							pending = true;
						}
						if !pending {
							self.state.heartbeat_at = now;
						}
						tick.reset_immediately();
					}
				},
				_ = tick.tick() => {
					if self.tick(closed_at.is_none(), &commands).await {
						break;
					}
				},
			}
			if closed_at.is_some_and(|time| Instant::now() >= time + self.options.attempt_ttl) {
				break;
			}
		}
	}

	/// Run the periodic work, and return whether the client stopped after being idle.
	async fn tick(&mut self, open: bool, commands: &mpsc::UnboundedReceiver<Command<P>>) -> bool {
		let now = Instant::now();
		let options = self.options;
		self.state
			.heartbeats
			.retain(|_, heartbeat| now < heartbeat.sent_at + options.attempt_ttl);
		if !open {
			return false;
		}

		// Every request to the subject shares this client, so stop it only after it has been idle for a while.
		if self.state.requests.is_empty() {
			let idle_at = *self.state.idle_at.get_or_insert(now);
			return now >= idle_at + options.idle_timeout && self.remove_idle(commands);
		}

		// Recover the requests if the attempt expired.
		if self
			.state
			.attempt
			.as_ref()
			.is_some_and(|attempt| now >= attempt.deadline)
		{
			self.state.attempt = None;
			self.recover();
		}

		// Fail the requests that were not acknowledged in time.
		let expired = self
			.state
			.requests
			.iter()
			.filter_map(|(id, request)| {
				request
					.deadline
					.filter(|deadline| now >= *deadline)
					.map(|_| id.clone())
			})
			.collect::<Vec<_>>();
		for id in expired {
			let request = self.state.requests.remove(&id).unwrap();
			request
				.sender
				.send_replace(Output::Ready(Err(Error::Timeout)));
		}
		if self.state.requests.is_empty() {
			return false;
		}

		// Start a heartbeat if one is due.
		if now >= self.state.heartbeat_at {
			let heartbeat = Heartbeat {
				acknowledged: false,
				retry_at: now,
				sent_at: now,
			};
			self.state.heartbeats.insert(id(), heartbeat);
			self.state.heartbeat_at = now + options.heartbeat_interval;
		}

		// Send the heartbeats that are due, bounding the batch so that a slow messenger cannot delay the deadlines.
		let due = self
			.state
			.heartbeats
			.iter_mut()
			.filter(|(_, heartbeat)| !heartbeat.acknowledged && now >= heartbeat.retry_at)
			.map(|(id, heartbeat)| {
				heartbeat.retry_at = now + options.retry_interval;
				id.clone()
			})
			.collect::<Vec<_>>();
		let future = async {
			for id in due {
				let message = Message::Heartbeat {
					client: self.state.client.clone(),
					id: id.clone(),
				};
				self.publish(server_subject(&self.state.subject), message)
					.await;
				let event = Event::HeartbeatSent {
					client: &self.state.client,
					id: &id,
				};
				self.observer.observe(event).await;
			}
		};
		tokio::time::timeout(options.tick(), future).await.ok();
		self.send_requests().await;

		false
	}

	async fn message(&mut self, message: Message) {
		match message {
			Message::HeartbeatResponse { attempt, id, ttl } => {
				if let Some(heartbeat) = self.state.heartbeats.remove(&id) {
					let now = Instant::now();
					if self
						.state
						.last_heartbeat
						.is_none_or(|time| heartbeat.sent_at >= time)
						&& let Some(deadline) = heartbeat.sent_at.checked_add(ttl)
						&& now < deadline && now < heartbeat.sent_at + self.options.attempt_ttl
					{
						let changed = self
							.state
							.attempt
							.as_ref()
							.is_none_or(|current| current.id != attempt);
						self.state.last_heartbeat = Some(heartbeat.sent_at);
						self.state.attempt = Some(Attempt {
							deadline,
							id: attempt.clone(),
						});
						if changed {
							self.recover();
							let event = Event::Attempt {
								attempt: &attempt,
								client: &self.state.client,
							};
							self.observer.observe(event).await;
							self.send_requests().await;
						}
					}
				}

				// Acknowledge the response so that the server stops resending it.
				let message = Message::Ack { attempt, id };
				self.publish(server_subject(&self.state.subject), message)
					.await;
			},
			Message::RequestAck { attempt, id } => {
				if self
					.state
					.attempt
					.as_ref()
					.is_some_and(|current| current.id == attempt)
					&& let Some(request) = self.state.requests.get_mut(&id)
				{
					request.acknowledged = true;
					request.deadline = None;
					request.sender.send_replace(Output::Pending);
					let event = Event::Acknowledged {
						attempt: &attempt,
						id: &id,
						request: &request.request,
					};
					self.observer.observe(event).await;
				}
			},
			Message::Response {
				attempt,
				id,
				response,
			} => {
				// A response from any attempt satisfies the request if the protocol accepts it. Leave an unaccepted response unacknowledged.
				if let Some(request) = self.state.requests.get(&id) {
					let accepted = P::Response::deserialize(response.clone())
						.map_or(true, |deserialized| {
							P::accepts(&request.request, &deserialized)
						});
					if !accepted {
						return;
					}
					let mut request = self.state.requests.remove(&id).unwrap();
					request.attempts.remove(&attempt);
					request.sender.send_replace(Output::Ready(Ok(response)));
					self.cancel(&id, request.attempts).await;
				}
				let event = Event::Responded {
					attempt: &attempt,
					id: &id,
				};
				self.observer.observe(event).await;

				// Acknowledge duplicates and cancelled requests even after their local state is gone.
				let message = Message::Ack { attempt, id };
				self.publish(server_subject(&self.state.subject), message)
					.await;
			},
			Message::Ack { .. }
			| Message::Announce
			| Message::Cancel { .. }
			| Message::Heartbeat { .. }
			| Message::Request { .. } => (),
		}
	}

	/// Mark the requests unacknowledged, so that they are sent under the current attempt, and fail them if they are not recovered in time.
	fn recover(&mut self) {
		let now = Instant::now();
		for request in self.state.requests.values_mut() {
			request.acknowledged = false;
			request.retry_at = now;
			request
				.deadline
				.get_or_insert(now + self.options.recovery_timeout);
			request.sender.send_replace(Output::Pending);
		}
	}

	async fn send_requests(&mut self) {
		let now = Instant::now();
		let mut ids = self
			.state
			.requests
			.iter()
			.filter(|(_, request)| !request.acknowledged && now >= request.retry_at)
			.map(|(id, request)| (request.retry_at, id.clone()))
			.collect::<Vec<_>>();
		ids.sort_unstable();

		// Bound the batch so that a slow messenger cannot prevent heartbeats and deadlines from running.
		let timeout = self.options.tick();
		let future = async {
			for (_, id) in ids {
				self.send_request(&id).await;
			}
		};
		tokio::time::timeout(timeout, future).await.ok();
	}

	async fn send_request(&mut self, id: &str) {
		let Some(attempt) = self
			.state
			.attempt
			.as_ref()
			.map(|attempt| attempt.id.clone())
		else {
			return;
		};
		let request = self.state.requests.get_mut(id).unwrap();
		request.attempts.insert(attempt.clone());
		request.retry_at = Instant::now() + self.options.retry_interval;
		let message = Message::Request {
			attempt: attempt.clone(),
			client: self.state.client.clone(),
			id: id.to_owned(),
			request: request.bytes.clone(),
		};
		self.publish(server_subject(&self.state.subject), message)
			.await;
		let request = &self.state.requests[id];
		let event = Event::RequestSent {
			attempt: &attempt,
			id,
			request: &request.request,
		};
		self.observer.observe(event).await;
	}

	/// Cancel a request under each attempt it was sent under.
	async fn cancel(&self, id: &str, attempts: BTreeSet<String>) {
		for attempt in attempts {
			let message = Message::Cancel {
				attempt,
				id: id.to_owned(),
			};
			self.publish(server_subject(&self.state.subject), message)
				.await;
		}
	}

	fn fail(&mut self, error: &Error) {
		for (_, request) in std::mem::take(&mut self.state.requests) {
			request
				.sender
				.send_replace(Output::Ready(Err(error.clone())));
		}
	}

	/// Remove this client if it is idle, unless a command arrived for it.
	fn remove_idle(&self, commands: &mpsc::UnboundedReceiver<Command<P>>) -> bool {
		let mut subjects = self.subjects.lock().unwrap();
		if !commands.is_empty() {
			return false;
		}
		if subjects
			.get(&self.state.subject)
			.is_some_and(|client| client.id == self.state.client)
		{
			subjects.remove(&self.state.subject);
		}
		true
	}

	async fn publish(&self, subject: String, message: Message) {
		tokio::time::timeout(
			self.options.retry_interval,
			self.messenger.publish(subject, message),
		)
		.await
		.ok();
	}
}

impl<P: Protocol> Drop for Request<P> {
	fn drop(&mut self) {
		self.client
			.sender
			.send(Command::Cancel(self.id.clone()))
			.ok();
	}
}
