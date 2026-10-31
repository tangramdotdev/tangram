use {
	crate::Component,
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_server_core::{Server, Session},
	tangram_server_service as service,
	tokio::time::Instant,
};

/// The name of the attach service's checkpoints.
pub(crate) const NAME: &str = "process.attach.service";

/// The subject under which the attach services serve the subjects of their nonces.
pub(crate) const SUBJECT: &str = "processes.attach.service";

/// The sender with which an attach tells its attach service the location of the server that it went to, its command sync, its opens, and its process. Dropping every sender finishes the service.
#[derive(Clone)]
pub(crate) struct Sender {
	sender: tokio::sync::mpsc::UnboundedSender<Event>,
}

/// The stdio streams that an attach declared, which the runner of a scheduled process opens when the process starts, and which the attach service hands to the first readers and writers of those streams.
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

/// An open that an attach service offers to the first request for its streams.
struct Offer {
	claimed: bool,
	id: String,
	streams: Vec<tg::process::stdio::Stream>,
}

/// The output of resolving a process.
#[derive(Default)]
pub(crate) struct Output {
	/// The ID of the attach's open that the request claimed.
	pub open: Option<String>,
}

/// The state of an attach service.
#[derive(Default)]
struct State {
	finished: bool,
	location: Option<tg::Location>,
	open: Vec<Offer>,
	process: Option<tg::Referent<tg::process::Id>>,
	sync: Option<tg::Referent<tg::sync::Id>>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum Request {
	#[tangram_serialize(id = 0)]
	Process(ProcessRequest),

	#[tangram_serialize(id = 1)]
	Sync(SyncRequest),
}

/// A request for the attach's process.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct ProcessRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,

	/// The stdio streams whose open on the attach the request claims.
	#[tangram_serialize(id = 1, default, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<tg::process::stdio::Stream>,
}

/// A request for the attach's command sync.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct SyncRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Response {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	/// The location of the server that the attach went to, whose attach service answers for the nonce in its place.
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

impl Sender {
	pub fn set_location(&self, location: tg::Location) {
		self.sender.send(Event::Location(location)).ok();
	}

	/// Create the open of an attach that declared the given streams.
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

	/// Offer the opens to the readers and writers that locate the process by the attach's nonce.
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

impl State {
	/// Get the response to a request, claiming the open for its streams, or `None` if the attach cannot answer it yet.
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

impl Request {
	#[must_use]
	pub fn principal(&self) -> &tg::Principal {
		match self {
			Self::Process(request) => &request.principal,
			Self::Sync(request) => &request.principal,
		}
	}
}

impl Component {
	/// Resolve a process named by the nonce of an attach in this instance, replacing the nonce with the process's ID and the location with the process's location, and inheriting the process's tokens. The nonce stays in place when the request's locations exclude this server and no attach on this server has the nonce, when no attach in this instance has the nonce, or when the attach went to another server, whose location replaces the request's. A request that claims the attach's open for the given streams receives the open's ID. Returns `None` if the attach finished without a process.
	pub(crate) async fn try_resolve_process(
		&self,
		session: &Session,
		process: &mut tg::Either<tg::process::Nonce, tg::process::Id>,
		location: &mut Option<tg::location::Arg>,
		tokens: Option<&mut tg::authorization::Tokens>,
		streams: &[tg::process::stdio::Stream],
	) -> tg::Result<Option<Output>> {
		// Leave an ID in place.
		let tg::Either::Left(nonce) = *process else {
			return Ok(Some(Output::default()));
		};

		// Leave the nonce in place if the request's locations exclude this server, unless an attach on this server has the nonce.
		if location.is_some() && !self.attach.servers.contains_key(&nonce) {
			let locations = session
				.locations(location.as_ref())
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
			if !locations.local.is_some_and(|local| local.current) {
				return Ok(Some(Output::default()));
			}
		}

		// Ask this instance's attach service, leaving the nonce in place if no attach in this instance has it.
		let request = ProcessRequest {
			principal: session.context.principal.clone(),
			streams: streams.to_vec(),
		};
		let request = Request::Process(request);
		let Some(response) = self.request_process_attach_service(&nonce, request).await? else {
			return Ok(Some(Output::default()));
		};

		// Leave the nonce in place and route the request to the location of the server that the attach went to.
		if let Some(attach_location) = response.location {
			*location = Some(attach_location.into());
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

	/// Get the command sync of the attach that a nonce names, waiting until the attach creates it.
	pub async fn try_resolve_process_command_sync(
		&self,
		session: &Session,
		nonce: &tg::process::Nonce,
	) -> tg::Result<Option<tg::Referent<tg::sync::Id>>> {
		let request = SyncRequest {
			principal: session.context.principal.clone(),
		};
		let request = Request::Sync(request);
		let response = self.request_process_attach_service(nonce, request).await?;
		let sync = response.and_then(|response| response.sync);
		Ok(sync)
	}

	/// Start answering the requests that locate this session's attach by its nonce, over the messenger and, for requests from this server, over a local channel.
	pub(crate) async fn spawn_process_attach_service_task(
		&self,
		session: &Session,
		nonce: &tg::process::Nonce,
	) -> tg::Result<Sender> {
		// Start the attach service, and register it so that the requests on this server skip the messenger.
		let options = session.server.config.process.attach.service;
		let observer = Arc::new(service::Checkpoints::new(
			session.server.checkpoints.clone(),
			NAME,
		));
		let service_server = match self.attach.servers.entry(*nonce) {
			dashmap::Entry::Occupied(_) => {
				return Err(tg::error!(argument, "the nonce is in use"));
			},
			dashmap::Entry::Vacant(entry) => {
				let service_server = service::Server::start(
					&session.server.messenger,
					SUBJECT,
					nonce,
					options,
					observer,
				);
				entry.insert(service_server.clone());
				service_server
			},
		};
		tangram_server_core::checkpoint!(session.server, "process.attach.service.started", nonce = %nonce).await;

		// Stop and unregister the service when its task ends, even if the task panics or is aborted.
		let guard = scopeguard::guard((), {
			let component = self.clone();
			let nonce = *nonce;
			let service_server = service_server.clone();
			move |()| {
				service_server.stop();
				component
					.attach
					.servers
					.remove_if(&nonce, |_, server| server.is_stopped());
			}
		});

		// Spawn the task that answers the requests, and wait for the service's responses to be acknowledged once it ends.
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let sender = Sender { sender };
		let principal = session.context.principal.clone();
		self.attach
			.service_tasks
			.spawn({
				let component = self.clone();
				let server = session.server.clone();
				let nonce = *nonce;
				move |stopper| async move {
					let result = component
						.process_attach_service_task(
							&server,
							principal,
							&service_server,
							receiver,
							stopper,
						)
						.await;
					drop(guard);
					service_server.wait().await;
					tangram_server_core::checkpoint!(server, "process.attach.service.stopped", nonce = %nonce).await;
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the process attach service task failed");
					}
					result
				}
			})
			.detach();

		Ok(sender)
	}

	/// Ask an attach service for its attach's process or command sync, over the local channel if it runs on this server. A nonce that no attach owns resolves to `None` once the request times out.
	async fn request_process_attach_service(
		&self,
		nonce: &tg::process::Nonce,
		request: Request,
	) -> tg::Result<Option<Response>> {
		// Ask the attach service.
		let server = self
			.attach
			.servers
			.get(nonce)
			.map(|server| server.value().clone());
		let result = self
			.attach
			.client
			.request(nonce, server.as_ref(), request)
			.await;
		let response = match result {
			Ok(response) => response,
			Err(service::Error::Timeout) => return Ok(None),
			Err(service::Error::Stopped) => {
				return Err(tg::error!(
					code = tg::error::Code::Cancellation,
					"the server is stopping"
				));
			},
			Err(error) => return Err(tg::error!(!error, "the attach service request failed")),
		};

		// Return the response or its error.
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error).unwrap_or_else(|source| {
				tg::error!(
					!source,
					"failed to deserialize the process attach service error"
				)
			});
			return Err(error);
		}

		Ok(Some(response))
	}

	async fn process_attach_service_task(
		&self,
		server: &Server,
		principal: tg::Principal,
		service_server: &service::Server<Request, Response>,
		mut events: tokio::sync::mpsc::UnboundedReceiver<Event>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Answer each request once the attach can, retaining it until then.
		let options = server.config.process.attach.service;
		let mut state = State::default();
		let mut finished_at = None::<Instant>;
		let mut retained = Vec::<(Request, service::Reply<Response>)>::new();
		let mut prune = tokio::time::interval(options.retry_interval);
		prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => {
					state.finished = true;
					Self::process_attach_service_answer(&mut state, &mut retained);
					break;
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
					Self::process_attach_service_answer(&mut state, &mut retained);
				},
				incoming = service_server.next() => {
					let Some((request, reply)) = incoming else {
						return Err(tg::error!("the process attach service stopped"));
					};
					self.process_attach_service_request(server, &principal, &mut state, request, reply, &mut retained).await;
				},
				_ = prune.tick() => {
					retained.retain(|(_, reply)| !reply.is_cancelled());
				},
			}

			// Stop once a finished attach has no attempts left.
			let now = Instant::now();
			let status = service_server.status();
			if finished_at.is_some_and(|finished_at| {
				now >= finished_at + options.attempt_ttl && status.attempts == 0
			}) {
				break;
			}
		}

		Ok(())
	}

	/// Answer a request from the local channel or the messenger if the attach can answer it, or retain it until the attach can.
	async fn process_attach_service_request(
		&self,
		server: &Server,
		principal: &tg::Principal,
		state: &mut State,
		request: Request,
		reply: service::Reply<Response>,
		retained: &mut Vec<(Request, service::Reply<Response>)>,
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

		// Answer the request if the attach can.
		if let Some(response) = state.response(&request) {
			reply.send(&response);
			return;
		}

		// Retain the request until the attach can answer it.
		let sync = matches!(request, Request::Sync(_));
		tangram_server_core::checkpoint!(server, "process.attach.service.retain", sync = sync)
			.await;
		reply.acknowledge();
		retained.push((request, reply));
	}

	/// Answer the retained requests that the attach can now answer.
	fn process_attach_service_answer(
		state: &mut State,
		retained: &mut Vec<(Request, service::Reply<Response>)>,
	) {
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
