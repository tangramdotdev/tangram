use {
	crate::{Server, Session},
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::service,
	tokio::time::Instant,
};

/// The sender with which a connect tells its connect service the location of the server that it went to, its command sync, its opens, and its process. Dropping every sender finishes the service.
#[derive(Clone)]
pub(crate) struct Sender {
	sender: tokio::sync::mpsc::UnboundedSender<Event>,
}

/// The stdio streams that a connect declared, which the runner of a scheduled process opens when the process starts, and which the connect service hands to the first readers and writers of those streams.
#[derive(Clone)]
pub(crate) struct Open {
	nonce: tg::process::Nonce,
	sender: Sender,
	streams: Vec<tg::process::stdio::Stream>,
}

enum Event {
	Location(tg::Location),
	Open(Vec<Offer>),
	Process(tg::Referent<tg::process::Id>),
	Sync(tg::Referent<tg::sync::Id>),
}

/// An open that a connect service offers to the first request for its streams.
struct Offer {
	claimed: bool,
	id: String,
	streams: Vec<tg::process::stdio::Stream>,
}

/// A request from this server to one of its own connect services, which the service answers over a local channel without the messenger.
pub(crate) struct LocalRequest {
	reply: tokio::sync::oneshot::Sender<Response>,
	request: Request,
}

/// The output of resolving a process.
#[derive(Default)]
pub(crate) struct Output {
	/// The ID of the connect's open that the request claimed.
	pub open: Option<String>,
}

/// The protocol of a connect service, which answers with the process or the command sync of a connect.
pub(crate) struct Protocol;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum Request {
	#[tangram_serialize(id = 0)]
	Process(ProcessRequest),

	#[tangram_serialize(id = 1)]
	Sync(SyncRequest),
}

/// A request for the connect's process.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct ProcessRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,

	/// The stdio streams whose open on the connect the request claims.
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<tg::process::stdio::Stream>,
}

/// A request for the connect's command sync.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct SyncRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Response {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	/// The location of the server that the connect went to, whose connect service answers for the nonce in its place.
	#[tangram_serialize(id = 4, default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::Location>,

	/// The ID of the open that the request claimed.
	#[tangram_serialize(id = 3, default, skip_serializing_if = "Option::is_none")]
	pub open: Option<String>,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub process: Option<tg::Referent<tg::process::Id>>,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

/// Observes the process connect service's events as checkpoints.
pub(crate) struct Observer {
	checkpoints: Option<Arc<crate::checkpoint::State>>,
}

/// The reply to a request that a connect service received over the local channel or the messenger.
enum Reply {
	Local(tokio::sync::oneshot::Sender<Response>),
	Messenger(service::Reply<Protocol>),
}

/// The state of a connect service.
#[derive(Default)]
struct State {
	finished: bool,
	location: Option<tg::Location>,
	open: Vec<Offer>,
	process: Option<tg::Referent<tg::process::Id>>,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

impl Sender {
	pub fn set_location(&self, location: tg::Location) {
		self.sender.send(Event::Location(location)).ok();
	}

	/// Create the open of a connect that declared the given streams.
	#[must_use]
	pub fn open(&self, nonce: &tg::process::Nonce, streams: &[tg::process::stdio::Stream]) -> Open {
		let mut streams = streams.to_vec();
		streams.sort();
		streams.dedup();
		Open {
			nonce: *nonce,
			sender: self.clone(),
			streams,
		}
	}

	fn offer(&self, offers: Vec<Offer>) {
		self.sender.send(Event::Open(offers)).ok();
	}

	pub fn set_process(&self, process: tg::Referent<tg::process::Id>) {
		self.sender.send(Event::Process(process)).ok();
	}

	pub fn set_sync(&self, sync: tg::Referent<tg::sync::Id>) {
		self.sender.send(Event::Sync(sync)).ok();
	}
}

impl Open {
	/// Get the opens that the runner services when the process starts, one that reads the output streams and one that writes stdin.
	#[must_use]
	pub fn requests(&self) -> Vec<tg::process::control::ServerRequest> {
		self.offers()
			.into_iter()
			.map(|offer| {
				let arg = tg::process::stdio::read::Arg {
					id: None,
					length: None,
					location: None,
					position: None,
					size: None,
					streams: offer.streams,
					timeout: None,
					tokens: tg::authorization::Tokens::default(),
				};
				let open = tg::process::control::OpenServerRequestArg { arg };
				tg::process::control::ServerRequest {
					arg: tg::process::control::ServerRequestArg::Open(open),
					id: offer.id,
				}
			})
			.collect()
	}

	/// Offer the opens to the readers and writers that locate the process by the connect's nonce.
	pub fn offer(&self) {
		self.sender.offer(self.offers());
	}

	fn offers(&self) -> Vec<Offer> {
		let output = self
			.streams
			.iter()
			.copied()
			.filter(|stream| *stream != tg::process::stdio::Stream::Stdin)
			.collect::<Vec<_>>();
		let stdin = self.streams.contains(&tg::process::stdio::Stream::Stdin);
		let mut offers = Vec::new();
		if !output.is_empty() {
			let offer = Offer {
				claimed: false,
				id: format!("{}-read", self.nonce),
				streams: output,
			};
			offers.push(offer);
		}
		if stdin {
			let offer = Offer {
				claimed: false,
				id: format!("{}-write", self.nonce),
				streams: vec![tg::process::stdio::Stream::Stdin],
			};
			offers.push(offer);
		}
		offers
	}
}

impl Observer {
	pub fn new(checkpoints: Option<Arc<crate::checkpoint::State>>) -> Self {
		Self { checkpoints }
	}
}

impl Request {
	fn principal(&self) -> &tg::Principal {
		match self {
			Self::Process(request) => &request.principal,
			Self::Sync(request) => &request.principal,
		}
	}
}

impl State {
	/// Get the response to a request, claiming the open for its streams, or `None` if the connect cannot answer it yet.
	fn response(&mut self, request: &Request) -> Option<Response> {
		let response = match request {
			Request::Process(request) => {
				if self.location.is_none() && self.process.is_none() && !self.finished {
					return None;
				}
				let mut streams = request.streams.clone();
				streams.sort();
				streams.dedup();
				let open = if self.process.is_some() && !streams.is_empty() {
					self.open
						.iter_mut()
						.find(|offer| !offer.claimed && offer.streams == streams)
						.map(|offer| {
							offer.claimed = true;
							offer.id.clone()
						})
				} else {
					None
				};
				Response {
					error: None,
					location: self.location.clone(),
					open,
					process: self.process.clone(),
					sync: None,
				}
			},
			Request::Sync(_) => {
				if self.sync.is_none() && !self.finished {
					return None;
				}
				Response {
					error: None,
					location: self.location.clone(),
					open: None,
					process: None,
					sync: self.sync.clone(),
				}
			},
		};
		Some(response)
	}
}

impl Session {
	/// Resolve a process named by the nonce of a connect in this instance, replacing the nonce with the process's ID and the location with the process's location, and inheriting the process's tokens. The nonce stays in place when the request's locations exclude this server and no connect on this server has the nonce, when no connect in this instance has the nonce, or when the connect went to another server, whose location replaces the request's. A request that claims the connect's open for the given streams receives the open's ID. Returns `None` if the connect finished without a process.
	pub(crate) async fn try_resolve_process(
		&self,
		process: &mut tg::Either<tg::process::Nonce, tg::process::Id>,
		location: &mut Option<tg::location::Arg>,
		tokens: Option<&mut tg::authorization::Tokens>,
		streams: &[tg::process::stdio::Stream],
	) -> tg::Result<Option<Output>> {
		// Leave an ID in place.
		let tg::Either::Left(nonce) = *process else {
			return Ok(Some(Output::default()));
		};

		// Leave the nonce in place if the request's locations exclude this server, unless a connect on this server has the nonce.
		if location.is_some() && !self.server.process_connect_services.contains_key(&nonce) {
			let locations = self
				.locations(location.as_ref())
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
			if !locations.local.is_some_and(|local| local.current) {
				return Ok(Some(Output::default()));
			}
		}

		// Ask this instance's connect service, leaving the nonce in place if no connect in this instance has it.
		let request = ProcessRequest {
			principal: self.context.principal.clone(),
			streams: streams.to_vec(),
		};
		let request = Request::Process(request);
		let Some(response) = self
			.server
			.request_process_connect_service(&nonce, request)
			.await?
		else {
			return Ok(Some(Output::default()));
		};

		// Leave the nonce in place and route the request to the location of the server that the connect went to.
		if let Some(connect_location) = response.location {
			*location = Some(connect_location.into());
			return Ok(Some(Output::default()));
		}
		let Response {
			open,
			process: Some(referent),
			..
		} = response
		else {
			return Ok(None);
		};

		// Replace the nonce with the process's ID, route by the process's location, and inherit its tokens.
		*process = tg::Either::Right(referent.node);
		*location = referent.options.location.map(Into::into);
		if let Some(tokens) = tokens {
			tokens.inherit(&referent.options.tokens);
		}
		let output = Output { open };

		Ok(Some(output))
	}

	/// Get the command sync of the connect that a nonce names, waiting until the connect creates it.
	pub(crate) async fn try_resolve_process_command_sync(
		&self,
		nonce: &tg::process::Nonce,
	) -> tg::Result<Option<tg::Referent<tg::sync::Id>>> {
		let request = SyncRequest {
			principal: self.context.principal.clone(),
		};
		let request = Request::Sync(request);
		let response = self
			.server
			.request_process_connect_service(nonce, request)
			.await?;
		let sync = response.and_then(|response| response.sync);
		Ok(sync)
	}

	/// Start answering the requests that locate this session's connect by its nonce, over the messenger and, for requests from this server, over a local channel.
	pub(crate) fn spawn_process_connect_service_task(
		&self,
		nonce: &tg::process::Nonce,
	) -> tg::Result<Sender> {
		// Register the local channel before the task starts, so that later requests on this server find it.
		let (local_sender, local_requests) = tokio::sync::mpsc::unbounded_channel();
		match self.server.process_connect_services.entry(*nonce) {
			dashmap::Entry::Occupied(_) => {
				return Err(tg::error!(argument, "the nonce is in use"));
			},
			dashmap::Entry::Vacant(entry) => {
				entry.insert(local_sender.clone());
			},
		}
		self.server
			.process_connect_services_changed
			.notify_waiters();

		// Spawn the task, and remove its local channel when it exits.
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let sender = Sender { sender };
		let principal = self.context.principal.clone();
		let subject = subject(nonce);
		self.server
			.process_connect_service_tasks
			.spawn({
				let server = self.server.clone();
				let nonce = *nonce;
				move |stopper| async move {
					let result = server
						.process_connect_service_task(
							subject,
							principal,
							receiver,
							local_requests,
							stopper,
						)
						.await;
					server
						.process_connect_services
						.remove_if(&nonce, |_, sender| sender.same_channel(&local_sender));
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the process connect service task failed");
					}
					result
				}
			})
			.detach();

		Ok(sender)
	}
}

impl Server {
	/// Ask a connect service for its connect's process or command sync, waiting until the connect can answer. A nonce that no connect owns resolves to `None` once the request times out.
	async fn request_process_connect_service(
		&self,
		nonce: &tg::process::Nonce,
		request: Request,
	) -> tg::Result<Option<Response>> {
		// Ask this server's own connect service over the local channel. Otherwise, ask over the messenger while watching for the connect service to start on this server, so that a request that arrives before its connect is answered over the local channel once the connect starts.
		let mut messenger = None;
		let response = loop {
			let started = self.process_connect_services_changed.notified();
			let mut started = std::pin::pin!(started);
			started.as_mut().enable();
			if let Some(response) = self
				.request_process_connect_service_local(nonce, &request)
				.await
			{
				break response;
			}
			if messenger.is_none() {
				let pending = self
					.process_connect_clients
					.request(&subject(nonce), request.clone())
					.map_err(|source| {
						tg::error!(!source, "failed to send the connect service request")
					})?;
				messenger = Some(pending);
			}
			let pending = messenger.as_mut().unwrap();
			tokio::select! {
				result = pending.wait() => {
					let response = match result {
						Ok(response) => response,
						Err(service::Error::Timeout) => return Ok(None),
						Err(service::Error::Stopped) => {
							return Err(tg::error!(
								code = tg::error::Code::Cancellation,
								"the server is stopping"
							));
						},
						Err(error) => return Err(tg::error!(!error, "the connect service request failed")),
					};
					crate::checkpoint!(self, "process.connect.service.messenger", nonce = %nonce).await;
					break response;
				},
				() = &mut started => {},
			}
		};

		// Return the response or its error.
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error).unwrap_or_else(|source| {
				tg::error!(
					!source,
					"failed to deserialize the process connect service error"
				)
			});
			return Err(error);
		}
		Ok(Some(response))
	}

	/// Ask this server's own connect service for a nonce over the local channel, returning `None` if this server has no connect with the nonce or its service stopped before answering.
	async fn request_process_connect_service_local(
		&self,
		nonce: &tg::process::Nonce,
		request: &Request,
	) -> Option<Response> {
		let sender = self.process_connect_services.get(nonce)?.clone();
		let (reply, receiver) = tokio::sync::oneshot::channel();
		let local_request = LocalRequest {
			reply,
			request: request.clone(),
		};
		sender.send(local_request).ok()?;
		receiver.await.ok()
	}

	async fn process_connect_service_task(
		&self,
		subject: String,
		principal: tg::Principal,
		mut events: tokio::sync::mpsc::UnboundedReceiver<Event>,
		mut local_requests: tokio::sync::mpsc::UnboundedReceiver<LocalRequest>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Start answering the requests on the messenger concurrently with the local channel, so that a local request does not wait for the messenger.
		let options = self.config.process.connect.service;
		let observer = Arc::new(Observer::new(self.checkpoints.clone()));
		let serve =
			service::serve::<Protocol, _>(&self.messenger, subject.clone(), options, observer);
		let mut serve = std::pin::pin!(serve);
		let mut server = None::<service::Server<Protocol>>;

		// Answer each request once the connect can, retaining it until then.
		let mut state = State::default();
		let mut finished_at = None::<Instant>;
		let mut retained = Vec::<(Request, Reply)>::new();
		let mut prune = tokio::time::interval(options.retry_interval);
		prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut stopping_at = None;
		loop {
			tokio::select! {
				() = stopper.wait(), if stopping_at.is_none() => {
					stopping_at = Some(Instant::now() + options.attempt_ttl);
					finished_at.get_or_insert_with(Instant::now);
					state.finished = true;
					Self::process_connect_service_answer(&mut state, &mut retained);
				},
				event = events.recv(), if !state.finished => {
					match event {
						Some(Event::Location(location)) => state.location = Some(location),
						Some(Event::Open(offers)) => state.open = offers,
						Some(Event::Process(process)) => state.process = Some(process),
						Some(Event::Sync(sync)) => state.sync = Some(sync),
						None => {
							finished_at = Some(Instant::now());
							state.finished = true;
						},
					}
					Self::process_connect_service_answer(&mut state, &mut retained);
				},
				result = &mut serve, if server.is_none() => {
					let started = result.map_err(|source| {
						tg::error!(!source, "failed to start the process connect service")
					})?;
					server = Some(started);
					crate::checkpoint!(self, "process.connect.service.started", subject = %subject).await;
				},
				incoming = async { server.as_mut().unwrap().next().await }, if server.is_some() => {
					let Some(incoming) = incoming else {
						return Err(tg::error!("the process connect service stopped"));
					};
					let service::Incoming { reply, request } = incoming;
					let reply = Reply::Messenger(reply);
					self.process_connect_service_request(&principal, &mut state, request, reply, &mut retained).await;
				},
				Some(local_request) = local_requests.recv() => {
					let LocalRequest { reply, request } = local_request;
					let reply = Reply::Local(reply);
					self.process_connect_service_request(&principal, &mut state, request, reply, &mut retained).await;
				},
				_ = prune.tick() => {
					retained.retain(|(_, reply)| !reply.is_cancelled());
				},
			}

			// Stop once a stopping server's responses are acknowledged, or once a finished connect has no attempts left.
			let now = Instant::now();
			let status = server
				.as_ref()
				.map(service::Server::status)
				.unwrap_or_default();
			let drained = stopping_at.is_some() && status.unacknowledged == 0;
			if stopping_at.is_some_and(|deadline| drained || now >= deadline)
				|| finished_at.is_some_and(|finished_at| {
					now >= finished_at + options.attempt_ttl && status.attempts == 0
				}) {
				break;
			}
		}

		crate::checkpoint!(self, "process.connect.service.stopped", subject = %subject).await;

		Ok(())
	}

	/// Answer a request from the local channel or the messenger if the connect can answer it, or retain it until the connect can.
	async fn process_connect_service_request(
		&self,
		principal: &tg::Principal,
		state: &mut State,
		request: Request,
		reply: Reply,
		retained: &mut Vec<(Request, Reply)>,
	) {
		// Reject a request from another principal.
		if request.principal() != principal {
			let error = tg::error!(unauthorized, "the nonce belongs to another principal");
			let tg::Either::Left(error) = error.to_data_or_id() else {
				unreachable!()
			};
			let response = Response {
				error: Some(error),
				location: None,
				open: None,
				process: None,
				sync: None,
			};
			reply.send(&response);
			return;
		}

		// Answer the request if the connect can.
		if let Some(response) = state.response(&request) {
			reply.send(&response);
			return;
		}

		// Retain the request until the connect can answer it.
		let sync = matches!(request, Request::Sync(_));
		crate::checkpoint!(self, "process.connect.service.retain", id = %reply.id(), sync = sync)
			.await;
		reply.acknowledge();
		retained.push((request, reply));
	}

	/// Answer the retained requests that the connect can now answer.
	fn process_connect_service_answer(state: &mut State, retained: &mut Vec<(Request, Reply)>) {
		for (request, reply) in std::mem::take(retained) {
			if reply.is_cancelled() {
				continue;
			}
			match state.response(&request) {
				Some(response) => reply.send(&response),
				None => retained.push((request, reply)),
			}
		}
	}
}

impl Reply {
	/// Get the ID of the request, which is `local` for a request over the local channel.
	fn id(&self) -> &str {
		match self {
			Self::Local(_) => "local",
			Self::Messenger(reply) => reply.id(),
		}
	}

	/// Acknowledge a request over the messenger, so that the client stops resending it. A request over the local channel needs no acknowledgment.
	fn acknowledge(&self) {
		match self {
			Self::Local(_) => (),
			Self::Messenger(reply) => {
				reply.acknowledge().ok();
			},
		}
	}

	/// Check whether the requester stopped waiting for the response.
	fn is_cancelled(&self) -> bool {
		match self {
			Self::Local(reply) => reply.is_closed(),
			Self::Messenger(reply) => reply.is_cancelled(),
		}
	}

	fn send(self, response: &Response) {
		match self {
			Self::Local(reply) => {
				reply.send(response.clone()).ok();
			},
			Self::Messenger(reply) => {
				reply.send(response).ok();
			},
		}
	}
}

impl service::Protocol for Protocol {
	type Request = Request;
	type Response = Response;
}

impl service::Observer<Protocol> for Observer {
	fn observe<'a>(&'a self, event: service::Event<'a, Protocol>) -> BoxFuture<'a, ()> {
		async move {
			let Some(checkpoints) = &self.checkpoints else {
				return;
			};
			let mut params = tg::checkpoint::Params::new();
			let mut insert = |key: &str, value: &str| {
				params.insert(key.to_owned(), value.to_owned().into());
			};
			let name = match event {
				service::Event::Acknowledged { attempt, id, .. } => {
					insert("attempt", attempt);
					insert("id", id);
					"process.connect.service.ack"
				},
				service::Event::Attempt { attempt, client } => {
					insert("attempt", attempt);
					insert("client", client);
					"process.connect.service.attempt"
				},
				service::Event::Cancelled { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"process.connect.service.cancel"
				},
				service::Event::HeartbeatResponded {
					attempt,
					client,
					id,
				} => {
					insert("attempt", attempt);
					insert("client", client);
					insert("id", id);
					"process.connect.service.heartbeat.response"
				},
				service::Event::HeartbeatSent { client, id } => {
					insert("client", client);
					insert("id", id);
					"process.connect.service.heartbeat.request"
				},
				service::Event::RequestSent { attempt, id, .. } => {
					if let Some(attempt) = attempt {
						insert("attempt", attempt);
					}
					insert("id", id);
					"process.connect.service.request"
				},
				service::Event::Responded { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"process.connect.service.response"
				},
				service::Event::ResponseAcknowledged { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"process.connect.service.response_ack"
				},
				service::Event::Started { client, subject } => {
					insert("client", client);
					insert("subject", subject);
					"process.connect.service.heartbeat.start"
				},
			};
			checkpoints.hit(name, params).await;
		}
		.boxed()
	}
}

impl tangram_messenger::Payload for Request {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}
}

impl tangram_messenger::Payload for Response {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}
}

fn subject(nonce: &tg::process::Nonce) -> String {
	format!("processes.{nonce}.service")
}
