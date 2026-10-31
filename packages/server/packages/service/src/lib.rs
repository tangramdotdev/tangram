use {
	futures::{FutureExt as _, future::BoxFuture},
	std::{fmt::Display, marker::PhantomData, sync::Arc},
	tangram_client::prelude::*,
	tangram_messenger::{Messenger, Payload, service},
	tokio::sync::{Notify, mpsc, oneshot, watch},
};

#[cfg(test)]
mod tests;

pub use tangram_messenger::service::{Error, Event, Observer, Options, Status};

/// The attempt that the events of a request to a local server report.
const LOCAL: &str = "local";

/// The client of a service, which asks a server on this server over a local channel and the other servers over the messenger.
pub struct Client<K, Req, Res, M> {
	marker: PhantomData<fn(&K)>,
	messenger: service::Client<Req, Res, M>,
	observer: Arc<dyn Observer<Req>>,
	subject: String,
}

/// The server of a service for one key, which answers the requests from this server over a local channel and the requests from other servers over the messenger.
pub struct Server<Req, Res> {
	shared: Arc<Shared<Req, Res>>,
}

/// The handle with which a server answers a request.
pub struct Reply<Res> {
	inner: Inner<Res>,
}

/// Observes a service's events as checkpoints named after the service.
pub struct Checkpoints<Req> {
	name: &'static str,
	params: fn(&Req, &mut tg::checkpoint::Params),
	state: Option<Arc<tangram_server_checkpoint_core::State>>,
}

/// The state that the clones of a server share. Dropping it aborts the start of the server on the messenger.
struct Shared<Req, Res> {
	receiver: tokio::sync::Mutex<mpsc::UnboundedReceiver<Local<Req, Res>>>,
	sender: mpsc::UnboundedSender<Local<Req, Res>>,
	startup: watch::Receiver<Startup<Req, Res>>,
	stopped: watch::Sender<bool>,
	task: tokio::task::AbortHandle,
}

/// The start of a server on the messenger.
enum Startup<Req, Res> {
	/// The server stopped or failed to start before it started on the messenger.
	Closed,

	Pending,

	Started(service::Server<Req, Res>),
}

/// A request from this server to a local server.
struct Local<Req, Res> {
	acknowledged: Arc<Notify>,
	request: Req,
	sender: oneshot::Sender<Res>,
}

enum Inner<Res> {
	Local {
		acknowledged: Arc<Notify>,
		sender: oneshot::Sender<Res>,
	},
	Messenger(service::Reply<Res>),
}

impl<K, Req, Res, M> Client<K, Req, Res, M>
where
	K: Display,
	Req: Payload + Clone,
	Res: Payload,
	M: Messenger + Clone + Send + Sync + 'static,
{
	/// Create the client of a service whose servers serve subjects under the given subject.
	pub fn new(
		messenger: M,
		subject: impl Into<String>,
		options: Options,
		observer: Arc<dyn Observer<Req>>,
	) -> Result<Self, Error> {
		let subject = subject.into();
		let messenger = service::Client::new(
			messenger,
			format!("{subject}.clients"),
			options,
			observer.clone(),
		)?;
		let client = Self {
			marker: PhantomData,
			messenger,
			observer,
			subject,
		};
		Ok(client)
	}

	/// Send a request to the server of a key and wait for its response. Ask the given local server first, and ask over the messenger if it stops before answering.
	pub async fn request(
		&self,
		key: &K,
		server: Option<&Server<Req, Res>>,
		request: Req,
	) -> Result<Res, Error> {
		// Ask the local server.
		if let Some(server) = server
			&& let Some(response) = self.request_local(server, &request).await
		{
			return Ok(response);
		}

		// Ask over the messenger.
		let subject = key_subject(&self.subject, key);
		let response = self.messenger.request(&subject, request).await?;

		Ok(response)
	}

	/// Stop the client, failing its pending requests over the messenger with `Error::Stopped`.
	pub fn stop(&self) {
		self.messenger.stop();
	}

	/// Ask a local server, returning `None` if it stops before answering.
	async fn request_local(&self, server: &Server<Req, Res>, request: &Req) -> Option<Res> {
		// Send the request.
		if server.is_stopped() {
			return None;
		}
		let id = uuid::Uuid::now_v7().to_string();
		let event = Event::RequestSent {
			attempt: None,
			id: &id,
			request,
		};
		self.observer.observe(event).await;
		let acknowledged = Arc::new(Notify::new());
		let (sender, mut receiver) = oneshot::channel();
		let local = Local {
			acknowledged: acknowledged.clone(),
			request: request.clone(),
			sender,
		};
		server.shared.sender.send(local).ok()?;

		// Wait for the response, observing the acknowledgment, until the server stops.
		let mut stopped = server.shared.stopped.subscribe();
		let mut is_acknowledged = false;
		let response = loop {
			tokio::select! {
				biased;
				response = &mut receiver => break response.ok(),
				() = acknowledged.notified(), if !is_acknowledged => {
					is_acknowledged = true;
					let event = Event::Acknowledged {
						attempt: LOCAL,
						id: &id,
						request,
					};
					self.observer.observe(event).await;
				},
				() = async { stopped.wait_for(|stopped| *stopped).await.ok(); } => break None,
			}
		}?;
		let event = Event::Responded {
			attempt: LOCAL,
			id: &id,
		};
		self.observer.observe(event).await;

		Some(response)
	}
}

impl<Req, Res> Server<Req, Res>
where
	Req: Payload,
	Res: Payload,
{
	/// Start answering the requests for a key. The server answers the requests from this server immediately, and starts on the messenger in the background.
	pub fn start<K, M>(
		messenger: &M,
		subject: &str,
		key: &K,
		options: Options,
		observer: Arc<dyn Observer<Req>>,
	) -> Self
	where
		K: Display,
		M: Messenger + Clone + Send + Sync + 'static,
	{
		// Start the server on the messenger in the background, unless it stops first.
		let subject = key_subject(subject, key);
		let (stopped, mut stop) = watch::channel(false);
		let (startup_sender, startup) = watch::channel(Startup::Pending);
		let task = tokio::spawn({
			let messenger = messenger.clone();
			async move {
				let start = service::Server::start(&messenger, subject, options, observer);
				let result = tokio::select! {
					result = start => result,
					() = async { stop.wait_for(|stop| *stop).await.ok(); } => {
						startup_sender.send_replace(Startup::Closed);
						return;
					},
				};
				match result {
					Ok(server) => {
						startup_sender.send_replace(Startup::Started(server.clone()));
						if *stop.borrow() {
							server.stop();
						}
					},
					Err(error) => {
						tracing::error!(%error, "failed to start the service on the messenger");
						startup_sender.send_replace(Startup::Closed);
					},
				}
			}
		});

		// Create the server.
		let (sender, receiver) = mpsc::unbounded_channel();
		let shared = Shared {
			receiver: tokio::sync::Mutex::new(receiver),
			sender,
			startup,
			stopped,
			task: task.abort_handle(),
		};
		Self {
			shared: Arc::new(shared),
		}
	}

	/// Get the next request from this server or the messenger, or `None` once the server stops.
	pub async fn next(&self) -> Option<(Req, Reply<Res>)> {
		let mut receiver = self.shared.receiver.lock().await;
		let mut startup = self.shared.startup.clone();
		loop {
			// Return `None` once the server stops.
			if self.is_stopped() {
				return None;
			}

			// Receive the next request from this server, and from the messenger once the server starts on it.
			let server = match &*startup.borrow_and_update() {
				Startup::Closed => return None,
				Startup::Pending => None,
				Startup::Started(server) => Some(server.clone()),
			};
			let Some(server) = server else {
				tokio::select! {
					local = receiver.recv() => return local.map(Self::local),
					result = startup.changed() => {
						if result.is_err() {
							return None;
						}
						continue;
					},
				}
			};
			tokio::select! {
				local = receiver.recv() => return local.map(Self::local),
				incoming = server.next() => {
					let (request, reply) = incoming?;
					let inner = Inner::Messenger(reply);
					return Some((request, Reply { inner }));
				},
			}
		}
	}

	/// Get the server's attempts and responses over the messenger.
	#[must_use]
	pub fn status(&self) -> Status {
		match &*self.shared.startup.borrow() {
			Startup::Closed | Startup::Pending => Status::default(),
			Startup::Started(server) => server.status(),
		}
	}

	/// Check whether the server was stopped or failed to start.
	#[must_use]
	pub fn is_stopped(&self) -> bool {
		*self.shared.stopped.borrow() || matches!(&*self.shared.startup.borrow(), Startup::Closed)
	}

	/// Stop answering new requests, and finish once the responses over the messenger are acknowledged or the attempt time to live elapses.
	pub fn stop(&self) {
		self.shared.stopped.send_replace(true);
		if let Startup::Started(server) = &*self.shared.startup.borrow() {
			server.stop();
		}
	}

	/// Wait for the server to finish.
	pub async fn wait(&self) {
		let mut startup = self.shared.startup.clone();
		let server = match startup
			.wait_for(|startup| !matches!(startup, Startup::Pending))
			.await
			.as_deref()
		{
			Ok(Startup::Started(server)) => server.clone(),
			Ok(Startup::Closed | Startup::Pending) | Err(_) => return,
		};
		server.wait().await;
	}

	fn local(local: Local<Req, Res>) -> (Req, Reply<Res>) {
		let Local {
			acknowledged,
			request,
			sender,
		} = local;
		let inner = Inner::Local {
			acknowledged,
			sender,
		};
		(request, Reply { inner })
	}
}

impl<Res> Reply<Res>
where
	Res: Payload + Clone,
{
	/// Acknowledge the request without responding, so that the client stops resending it and waits for the response.
	pub fn acknowledge(&self) {
		match &self.inner {
			Inner::Local { acknowledged, .. } => {
				acknowledged.notify_one();
			},
			Inner::Messenger(reply) => {
				reply.acknowledge();
			},
		}
	}

	/// Check whether the client has cancelled the request.
	#[must_use]
	pub fn is_cancelled(&self) -> bool {
		match &self.inner {
			Inner::Local { sender, .. } => sender.is_closed(),
			Inner::Messenger(reply) => reply.is_cancelled(),
		}
	}

	/// Send the response, logging a response that cannot be serialized.
	pub fn send(self, response: &Res) {
		match self.inner {
			Inner::Local { sender, .. } => {
				sender.send(response.clone()).ok();
			},
			Inner::Messenger(reply) => {
				if let Err(error) = reply.send(response) {
					tracing::error!(%error, "failed to send the service response");
				}
			},
		}
	}
}

impl<Req> Checkpoints<Req> {
	/// Create an observer whose checkpoints are named after the service, such as `{name}.ack`.
	#[must_use]
	pub fn new(
		checkpoints: Option<Arc<tangram_server_checkpoint_core::State>>,
		name: &'static str,
	) -> Self {
		Self::with_params(checkpoints, name, |_, _| ())
	}

	/// Create an observer that also adds the params of a request to the checkpoints of its events.
	#[must_use]
	pub fn with_params(
		checkpoints: Option<Arc<tangram_server_checkpoint_core::State>>,
		name: &'static str,
		params: fn(&Req, &mut tg::checkpoint::Params),
	) -> Self {
		Self {
			name,
			params,
			state: checkpoints,
		}
	}
}

impl<Req, Res> Clone for Server<Req, Res> {
	fn clone(&self) -> Self {
		Self {
			shared: self.shared.clone(),
		}
	}
}

impl<Req, Res> Drop for Shared<Req, Res> {
	fn drop(&mut self) {
		self.task.abort();
	}
}

impl<Req: Send + Sync + 'static> Observer<Req> for Checkpoints<Req> {
	fn observe<'a>(&'a self, event: Event<'a, Req>) -> BoxFuture<'a, ()> {
		async move {
			let Some(state) = &self.state else {
				return;
			};
			let mut params = tg::checkpoint::Params::new();
			let mut insert = |key: &str, value: &str| {
				params.insert(key.to_owned(), value.to_owned().into());
			};
			let (name, request) = match event {
				Event::Acknowledged {
					attempt,
					id,
					request,
				} => {
					insert("attempt", attempt);
					insert("id", id);
					("ack", Some(request))
				},
				Event::Attempt { attempt, client } => {
					insert("attempt", attempt);
					insert("client", client);
					("attempt", None)
				},
				Event::Cancelled { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					("cancel", None)
				},
				Event::HeartbeatResponded {
					attempt,
					client,
					id,
				} => {
					insert("attempt", attempt);
					insert("client", client);
					insert("id", id);
					("heartbeat.response", None)
				},
				Event::HeartbeatSent { client, id } => {
					insert("client", client);
					insert("id", id);
					("heartbeat.request", None)
				},
				Event::RequestSent {
					attempt,
					id,
					request,
				} => {
					if let Some(attempt) = attempt {
						insert("attempt", attempt);
					}
					insert("id", id);
					("request", Some(request))
				},
				Event::Responded { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					("response", None)
				},
				Event::ResponseAcknowledged { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					("response_ack", None)
				},
				Event::Started { client, subject } => {
					insert("client", client);
					insert("subject", subject);
					("heartbeat.start", None)
				},
			};
			if let Some(request) = request {
				(self.params)(request, &mut params);
			}
			let name = format!("{}.{name}", self.name);
			state.hit(&name, params).await;
		}
		.boxed()
	}
}

/// Get the subject of the server of a key.
fn key_subject(subject: &str, key: &impl Display) -> String {
	format!("{subject}.{key}")
}
