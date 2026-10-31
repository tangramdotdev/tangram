use {
	super::{
		Error, Event, Message, Observer, Options, Protocol, announce_subject, client_subject, id,
		pool_subject, server_subject,
	},
	crate::{Messenger, Payload},
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream},
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

type Messages = BoxStream<'static, Result<crate::Message<Message>, crate::Error>>;

/// The client of a messenger's services, with one shared connection per subject that stops after it has been idle, and one subscription for the responses to all of them.
pub struct Client<P: Protocol, M> {
	messenger: M,
	observer: Arc<dyn Observer<P>>,
	options: Options,
	pool: Arc<Pool>,
	stopped: watch::Sender<bool>,
	subjects: Arc<Mutex<BTreeMap<String, Arc<Connection<P>>>>>,
}

/// A pending request. Dropping it cancels the request at the server.
struct Request<P: Protocol> {
	connection: Arc<Connection<P>>,
	id: String,
	marker: PhantomData<fn() -> P::Response>,
	receiver: watch::Receiver<Output>,
}

/// The connection for one subject, which a task runs. Its ID is the subject that its responses are published to.
struct Connection<P: Protocol> {
	id: String,
	sender: mpsc::UnboundedSender<Command<P>>,
}

/// The subscription of a pool of clients, which routes each response to its client by the subject that it was published to.
struct Pool {
	id: String,
	routes: Mutex<Routes>,
	subject: String,
	subscribing: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Routes {
	senders: BTreeMap<String, mpsc::UnboundedSender<Result<Message, Error>>>,
	subscribed: bool,
}

/// A client's route in its pool, which is removed when the client stops.
struct Route {
	client: String,
	pool: Arc<Pool>,
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
	pool: Arc<Pool>,
	state: State<P>,
	subjects: Arc<Mutex<BTreeMap<String, Arc<Connection<P>>>>>,
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

	/// When the request was last sent, from which an attempt that it establishes is timed.
	sent_at: Instant,

	/// Whether the request was sent without an attempt, so that the server holds it under the attempt that it established.
	sent_without_attempt: bool,
}

impl<P, M> Client<P, M>
where
	P: Protocol,
	M: Messenger + Clone + Send + Sync + 'static,
{
	/// Create the client of a messenger's services, whose responses are published to subjects under the given subject.
	pub fn new(
		messenger: M,
		subject: impl Into<String>,
		options: Options,
		observer: Arc<dyn Observer<P>>,
	) -> Result<Self, Error> {
		options.validate()?;
		let (stopped, _) = watch::channel(false);
		let pool = Pool {
			id: id(),
			routes: Mutex::default(),
			subject: subject.into(),
			subscribing: tokio::sync::Mutex::new(()),
		};
		let client = Self {
			messenger,
			observer,
			options,
			pool: Arc::new(pool),
			stopped,
			subjects: Arc::new(Mutex::new(BTreeMap::new())),
		};
		Ok(client)
	}

	/// Send a request to the server of a subject and wait for its response. The server may start before or after the request is sent. Dropping the future cancels the request at the server.
	pub async fn request(&self, subject: &str, request: P::Request) -> Result<P::Response, Error> {
		let mut request = self.send(subject, request)?;
		request.wait().await
	}

	/// Stop every connection, failing their pending requests with `Error::Stopped`.
	pub fn stop(&self) {
		self.stopped.send_replace(true);
	}

	fn send(&self, subject: &str, request: P::Request) -> Result<Request<P>, Error> {
		let id = id();
		let (sender, receiver) = watch::channel(Output::Pending);
		let command = Command::Request {
			id: id.clone(),
			request,
			sender,
		};

		// Send the request while holding the lock, so that an idle connection cannot remove itself between being found and receiving the request.
		let mut subjects = self.subjects.lock().unwrap();
		let connection = if let Some(connection) = subjects
			.get(subject)
			.filter(|connection| !connection.sender.is_closed())
			.cloned()
		{
			connection
		} else {
			let connection = self.spawn(subject);
			subjects.insert(subject.to_owned(), connection.clone());
			connection
		};
		connection.sender.send(command).map_err(|_| Error::Closed)?;
		drop(subjects);

		let request = Request {
			connection,
			id,
			marker: PhantomData,
			receiver,
		};
		Ok(request)
	}

	fn spawn(&self, subject: &str) -> Arc<Connection<P>> {
		let (sender, commands) = mpsc::unbounded_channel();
		let connection = Arc::new(Connection {
			id: client_subject(&self.pool.subject, &self.pool.id, &id()),
			sender,
		});
		let state = State {
			attempt: None,
			client: connection.id.clone(),
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
			pool: self.pool.clone(),
			state,
			subjects: self.subjects.clone(),
		};
		let stopped = self.stopped.subscribe();
		tokio::spawn(async move {
			task.run(commands, stopped).await;
		});
		connection
	}
}

impl Pool {
	async fn register<M>(
		self: &Arc<Self>,
		messenger: &M,
		client: &str,
		stopped: watch::Receiver<bool>,
	) -> Result<(Route, mpsc::UnboundedReceiver<Result<Message, Error>>), Error>
	where
		M: Messenger + Clone + Send + Sync + 'static,
	{
		// Subscribe once for the responses to every client of the pool.
		let _subscribing = self.subscribing.lock().await;
		if !self.routes.lock().unwrap().subscribed {
			let messages = messenger
				.subscribe::<Message>(pool_subject(&self.subject, &self.id))
				.await
				.map_err(Error::messenger)?
				.boxed();
			self.routes.lock().unwrap().subscribed = true;
			let pool = self.clone();
			tokio::spawn(async move {
				pool.dispatch(messages, stopped).await;
			});
		}

		// Add the client's route.
		let (sender, receiver) = mpsc::unbounded_channel();
		self.routes
			.lock()
			.unwrap()
			.senders
			.insert(client.to_owned(), sender);
		let route = Route {
			client: client.to_owned(),
			pool: self.clone(),
		};

		Ok((route, receiver))
	}

	async fn dispatch(&self, mut messages: Messages, mut stopped: watch::Receiver<bool>) {
		// Route each response to its client until the subscription ends or the clients stop.
		let error = loop {
			tokio::select! {
				message = messages.next() => {
					match message {
						Some(Ok(message)) => {
							let routes = self.routes.lock().unwrap();
							if let Some(sender) = routes.senders.get(&message.subject) {
								sender.send(Ok(message.payload)).ok();
							}
						},
						Some(Err(crate::Error::Deserialization(_))) => (),
						Some(Err(error)) => break Error::messenger(error),
						None => break Error::Closed,
					}
				},
				_ = stopped.wait_for(|stopped| *stopped) => {
					break Error::Stopped;
				},
			}
		};

		// Fail every client, so that the next client subscribes again.
		let mut routes = self.routes.lock().unwrap();
		routes.subscribed = false;
		for sender in std::mem::take(&mut routes.senders).into_values() {
			sender.send(Err(error.clone())).ok();
		}
	}
}

impl<P: Protocol> Request<P> {
	/// Wait for the response.
	async fn wait(&mut self) -> Result<P::Response, Error> {
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
		// Register with the pool before sending anything, so that every response to this client is routed to it.
		let registered = self
			.pool
			.register(&self.messenger, &self.state.client, stopped.clone())
			.await;
		let (_route, mut messages) = match registered {
			Ok(registered) => registered,
			Err(error) => {
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

		// Subscribe to the announcements without delaying the first request.
		let mut subscribing: Option<BoxFuture<'static, Result<Messages, crate::Error>>> = {
			let messenger = self.messenger.clone();
			let subject = announce_subject(&self.state.subject);
			let future = async move {
				let announcements = messenger.subscribe::<Message>(subject).await?;
				Ok(announcements.boxed())
			};
			Some(future.boxed())
		};
		let mut announcements = None::<Messages>;

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
								self.cancel(&id, request.attempts, request.sent_without_attempt).await;
							}
						},
						Some(Command::Request { id, request, sender }) => {
							self.state.idle_at = None;

							// Start the heartbeats one interval after a request arrives at a client without pending requests, because the request establishes the attempt itself.
							if self.state.requests.is_empty() {
								self.state.heartbeat_at = Instant::now() + self.options.heartbeat_interval;
							}
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
								sent_at: Instant::now(),
								sent_without_attempt: false,
							};
							self.state.requests.insert(id.clone(), pending);
							self.send_request(&id).await;
						},
						None => {
							closed_at = Some(Instant::now());
						},
					}
				},
				message = messages.recv() => {
					// Fail the pending requests with the pool subscription's error, so that they report its cause.
					let error = match message {
						Some(Ok(message)) => {
							self.message(message).await;
							continue;
						},
						Some(Err(error)) => error,
						None => Error::Closed,
					};
					self.fail(&error);
					return;
				},
				result = async { subscribing.as_mut().unwrap().await }, if subscribing.is_some() => {
					subscribing = None;
					match result {
						Ok(stream) => {
							announcements = Some(stream);
						},
						Err(error) => {
							self.fail(&Error::messenger(error));
							return;
						},
					}
				},
				message = async { announcements.as_mut().unwrap().next().await }, if announcements.is_some() => {
					if message.is_none() {
						self.fail(&Error::Closed);
						return;
					}

					// Resend the pending requests without an attempt to a server that has just started, so that they establish a new one.
					if closed_at.is_none() && !self.state.requests.is_empty() {
						self.state.attempt = None;
						self.recover();
						self.send_requests().await;
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
			Message::RequestAck { attempt, id, ttl } => {
				let Some(request) = self.state.requests.get(&id) else {
					return;
				};
				let (sent_at, sent_without_attempt) =
					(request.sent_at, request.sent_without_attempt);

				// Accept an acknowledgment under the current attempt, or adopt the attempt that a request sent without one established.
				match self.state.attempt.as_ref() {
					Some(current) if current.id == attempt => (),
					None if sent_without_attempt => {
						let now = Instant::now();
						let Some(deadline) =
							sent_at.checked_add(ttl).filter(|deadline| now < *deadline)
						else {
							return;
						};
						self.state.attempt = Some(Attempt {
							deadline,
							id: attempt.clone(),
						});
						let event = Event::Attempt {
							attempt: &attempt,
							client: &self.state.client,
						};
						self.observer.observe(event).await;
					},
					_ => {
						return;
					},
				}

				// Mark the request acknowledged, so that the client stops resending it and waits for the response.
				if let Some(request) = self.state.requests.get_mut(&id) {
					request.attempts.insert(attempt.clone());
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
					self.cancel(&id, request.attempts, false).await;
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

	/// Send a request under the current attempt, or without one, so that the server establishes the attempt.
	async fn send_request(&mut self, id: &str) {
		let attempt = self
			.state
			.attempt
			.as_ref()
			.map(|attempt| attempt.id.clone());
		let now = Instant::now();
		let request = self.state.requests.get_mut(id).unwrap();
		match &attempt {
			Some(attempt) => {
				request.attempts.insert(attempt.clone());
			},
			None => {
				request.sent_without_attempt = true;
			},
		}
		request.retry_at = now + self.options.retry_interval;
		request.sent_at = now;
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
			attempt: attempt.as_deref(),
			id,
			request: &request.request,
		};
		self.observer.observe(event).await;
	}

	/// Cancel a request under each attempt it was sent under, and under the client's attempt at the server if it was sent without one.
	async fn cancel(&self, id: &str, attempts: BTreeSet<String>, sent_without_attempt: bool) {
		let attempts = attempts
			.into_iter()
			.map(Some)
			.chain(sent_without_attempt.then_some(None));
		for attempt in attempts {
			let message = Message::Cancel {
				attempt,
				client: self.state.client.clone(),
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
			.is_some_and(|connection| connection.id == self.state.client)
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

impl Drop for Route {
	fn drop(&mut self) {
		self.pool
			.routes
			.lock()
			.unwrap()
			.senders
			.remove(&self.client);
	}
}

impl<P: Protocol> Drop for Request<P> {
	fn drop(&mut self) {
		self.connection
			.sender
			.send(Command::Cancel(self.id.clone()))
			.ok();
	}
}
