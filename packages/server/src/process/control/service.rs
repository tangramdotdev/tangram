use {
	crate::{Server, Session},
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::service,
};

/// The handle with which a process control connection keeps its control service running. Dropping every sender finishes the service.
#[derive(Clone)]
pub(crate) struct Sender {
	_sender: tokio::sync::mpsc::UnboundedSender<()>,
}

/// The local channel of a control service on this server.
pub(crate) struct Local {
	sender: tokio::sync::mpsc::UnboundedSender<LocalRequest>,
}

/// A request from this server to one of its own control services, which the service answers over a local channel without the messenger.
pub(crate) struct LocalRequest {
	reply: tokio::sync::oneshot::Sender<Response>,
	request: Request,
}

/// The arg of a control service.
pub(crate) struct Arg {
	pub indexed: bool,
	pub parent: Option<tg::process::Id>,
	pub runner: Option<tg::runner::Id>,
	pub syncs: bool,
}

/// The protocol of a control service, which answers for a process whose start its server accepted.
pub(crate) struct Protocol;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum Request {
	#[tangram_serialize(id = 0)]
	Authorize(AuthorizeRequest),

	#[tangram_serialize(id = 1)]
	Parent(ParentRequest),

	#[tangram_serialize(id = 2)]
	Sync(SyncRequest),
}

/// A request that verifies that a principal may act for the process.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct AuthorizeRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,
}

/// A request for the parent of the process.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct ParentRequest {}

/// A request for one of the process's syncs.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct SyncRequest {
	#[tangram_serialize(id = 0)]
	pub kind: tg::sync::join::Kind,

	#[tangram_serialize(id = 1)]
	pub principal: tg::Principal,
}

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Response {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1, default, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::process::Id>,

	#[tangram_serialize(id = 2, default, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::Referent<tg::sync::Id>>,
}

/// The reply to a request that a control service received over the local channel or the messenger.
enum Reply {
	Local(tokio::sync::oneshot::Sender<Response>),
	Messenger(service::Reply<Protocol>),
}

/// The state of a control service.
struct State {
	id: tg::process::Id,
	indexed: bool,
	parent: Option<tg::process::Id>,
	rejection: Option<tg::error::Data>,
	runner: Option<tg::runner::Id>,
	syncs: bool,
}

/// Observes nothing, because the control service records its own checkpoints.
pub(crate) struct Observer;

impl Reply {
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

impl State {
	/// Get the response to a request.
	async fn response(&self, session: &Session, request: &Request) -> Response {
		match self.response_inner(session, request).await {
			Ok(response) => response,
			Err(error) => {
				let tg::Either::Left(error) = error.to_data_or_id() else {
					unreachable!()
				};
				Response {
					error: Some(error),
					..Default::default()
				}
			},
		}
	}

	async fn response_inner(&self, session: &Session, request: &Request) -> tg::Result<Response> {
		// Reject every request for a process whose start was rejected.
		if let Some(rejection) = &self.rejection {
			let error = tg::Error::try_from(rejection.clone())
				.unwrap_or_else(|_| tg::error!("the process start was rejected"));
			return Err(tg::error!(!error, process = %self.id, "the process start was rejected"));
		}

		let response = match request {
			Request::Authorize(request) => {
				self.authorize(session, &request.principal, false).await?;
				Response::default()
			},
			Request::Parent(_) => Response {
				parent: self.parent.clone(),
				..Default::default()
			},
			Request::Sync(request) => {
				let parent = request.kind == tg::sync::join::Kind::Command;
				self.authorize(session, &request.principal, parent).await?;
				if !self.syncs {
					return Err(tg::error!(process = %self.id, "the process has no syncs"));
				}
				let id = tg::sync::Id::with_process(&self.id, request.kind);
				let sync = session.refresh_sync(id)?;
				Response {
					sync: Some(sync),
					..Default::default()
				}
			},
		};

		Ok(response)
	}

	/// Verify that a principal is the process, the runner that runs it, the root, or, if the parent is allowed, the process's parent or, for an indexed process, an ancestor that the index records.
	async fn authorize(
		&self,
		session: &Session,
		principal: &tg::Principal,
		parent: bool,
	) -> tg::Result<()> {
		let authorized = match principal {
			tg::Principal::Process(process) if process == &self.id => true,
			tg::Principal::Root => true,
			tg::Principal::Process(process) if parent => {
				self.parent.as_ref() == Some(process)
					|| (self.indexed
						&& session
							.server
							.index
							.process_has_ancestor(&self.id, process)
							.await?)
			},
			tg::Principal::Runner(runner) => self.runner.as_ref() == Some(runner),
			_ => false,
		};
		if !authorized {
			return Err(tg::error!(
				unauthorized,
				?principal,
				process = %self.id,
				"the principal may not act for the process"
			));
		}
		Ok(())
	}
}

impl Session {
	/// Start answering the requests for a process whose start this server accepted, over the messenger and, for requests from this server, over a local channel.
	pub(crate) fn spawn_process_control_service_task(
		&self,
		id: &tg::process::Id,
		arg: Arg,
	) -> Sender {
		let state = State {
			id: id.clone(),
			indexed: arg.indexed,
			parent: arg.parent,
			rejection: None,
			runner: arg.runner,
			syncs: arg.syncs,
		};
		self.spawn_process_control_service_task_with_state(state)
	}

	/// Answer every request for a process whose start this server rejected with the rejection, so that the requests waiting for the start fail.
	pub(crate) fn spawn_process_control_rejection_task(
		&self,
		id: &tg::process::Id,
		error: &tg::Error,
	) {
		let tg::Either::Left(rejection) = error.to_data_or_id() else {
			return;
		};
		let state = State {
			id: id.clone(),
			indexed: false,
			parent: None,
			rejection: Some(rejection),
			runner: None,
			syncs: false,
		};
		let sender = self.spawn_process_control_service_task_with_state(state);

		// Keep answering for the service's attempt time to live.
		let ttl = self.server.config.process.control_service.attempt_ttl;
		tokio::spawn(async move {
			tokio::time::sleep(ttl).await;
			drop(sender);
		});
	}

	fn spawn_process_control_service_task_with_state(&self, state: State) -> Sender {
		// Register the local channel before the task starts, so that later requests on this server find it. A reconnect replaces the service of the connection that it reconnects.
		let id = state.id.clone();
		let (local_sender, local_requests) = tokio::sync::mpsc::unbounded_channel();
		let local = Local {
			sender: local_sender.clone(),
		};
		self.server
			.process_control_services
			.insert(id.clone(), local);
		self.server
			.process_control_services_changed
			.notify_waiters();

		// Spawn the task, and remove its local channel when it exits.
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let sender = Sender { _sender: sender };
		let mut session = self.clone();
		session.context.stopper = None;
		self.server
			.process_control_service_tasks
			.spawn({
				let server = self.server.clone();
				move |stopper| async move {
					let result = session
						.process_control_service_task(state, receiver, local_requests, stopper)
						.await;
					server
						.process_control_services
						.remove_if(&id, |_, local| local.sender.same_channel(&local_sender));
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the process control service task failed");
					}
					result
				}
			})
			.detach();

		sender
	}

	async fn process_control_service_task(
		&self,
		state: State,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
		mut local_requests: tokio::sync::mpsc::UnboundedReceiver<LocalRequest>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Start answering the requests on the messenger concurrently with the local channel, so that a local request does not wait for the messenger.
		let subject = subject(&state.id);
		let options = self.server.config.process.control_service;
		let observer = Arc::new(Observer);
		let serve = service::serve::<Protocol, _>(
			&self.server.messenger,
			subject.clone(),
			options,
			observer,
		);
		let mut serve = std::pin::pin!(serve);
		let mut server = None::<service::Server<Protocol>>;

		// Answer each request until every sender is dropped or the server stops.
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				event = receiver.recv() => {
					if event.is_none() {
						break;
					}
				},
				result = &mut serve, if server.is_none() => {
					let started = result.map_err(|source| {
						tg::error!(!source, "failed to start the process control service")
					})?;
					server = Some(started);
					crate::checkpoint!(self.server, "process.control.service.started", process = %state.id).await;
				},
				incoming = async { server.as_mut().unwrap().next().await }, if server.is_some() => {
					let Some(incoming) = incoming else {
						return Err(tg::error!("the process control service stopped"));
					};
					let service::Incoming { reply, request } = incoming;
					let response = state.response(self, &request).await;
					Reply::Messenger(reply).send(&response);
				},
				Some(local_request) = local_requests.recv() => {
					let LocalRequest { reply, request } = local_request;
					let response = state.response(self, &request).await;
					Reply::Local(reply).send(&response);
				},
			}
		}

		crate::checkpoint!(self.server, "process.control.service.stopped", process = %state.id)
			.await;

		Ok(())
	}

	/// Get one of a process's syncs from its control service, waiting until a control connection for the process can answer.
	pub(crate) async fn try_resolve_process_control_sync(
		&self,
		id: &tg::process::Id,
		kind: tg::sync::join::Kind,
	) -> tg::Result<Option<tg::Referent<tg::sync::Id>>> {
		let request = Request::Sync(SyncRequest {
			kind,
			principal: self.context.principal.clone(),
		});
		let response = self
			.server
			.request_process_control_service(id, request)
			.await?;
		let sync = response.and_then(|response| response.sync);
		Ok(sync)
	}

	/// Verify with a process's control service that a principal may act for the process, waiting until a control connection for the process can answer.
	pub(crate) async fn authorize_with_process_control_service(
		&self,
		id: &tg::process::Id,
		principal: &tg::Principal,
	) -> tg::Result<()> {
		let request = Request::Authorize(AuthorizeRequest {
			principal: principal.clone(),
		});
		self.server
			.request_process_control_service(id, request)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process control service"))?;
		Ok(())
	}

	/// Get the parent of a process from its control service, waiting until a control connection for the process can answer. Returns `None` if no control connection answers before the request times out.
	pub(crate) async fn try_get_process_parent_from_control_service(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Id>> {
		let request = Request::Parent(ParentRequest {});
		let response = self
			.server
			.request_process_control_service(id, request)
			.await?;
		let parent = response.and_then(|response| response.parent);
		Ok(parent)
	}
}

impl Server {
	/// Ask a control service, waiting until it can answer. A process with no control service resolves to `None` once the request times out.
	async fn request_process_control_service(
		&self,
		id: &tg::process::Id,
		request: Request,
	) -> tg::Result<Option<Response>> {
		// Ask this server's own control service over the local channel. Otherwise, ask over the messenger while watching for the control service to start on this server, so that a request that arrives before its control connection is answered over the local channel once the control connection starts.
		let mut messenger = None;
		let response = loop {
			let started = self.process_control_services_changed.notified();
			let mut started = std::pin::pin!(started);
			started.as_mut().enable();
			if let Some(response) = self
				.request_process_control_service_local(id, &request)
				.await
			{
				break response;
			}
			if messenger.is_none() {
				let pending = self
					.process_control_clients
					.request(&subject(id), request.clone())
					.map_err(|source| {
						tg::error!(
							!source,
							"failed to send the process control service request"
						)
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
						Err(error) => return Err(tg::error!(!error, "the process control service request failed")),
					};
					crate::checkpoint!(self, "process.control.service.messenger", process = %id).await;
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
					"failed to deserialize the process control service error"
				)
			});
			return Err(error);
		}
		Ok(Some(response))
	}

	/// Ask this server's own control service over the local channel, returning `None` if this server has no control service for the process or its service stopped before answering.
	async fn request_process_control_service_local(
		&self,
		id: &tg::process::Id,
		request: &Request,
	) -> Option<Response> {
		let sender = self.process_control_services.get(id)?.sender.clone();
		let (reply, receiver) = tokio::sync::oneshot::channel();
		let local_request = LocalRequest {
			reply,
			request: request.clone(),
		};
		sender.send(local_request).ok()?;
		receiver.await.ok()
	}
}

impl service::Protocol for Protocol {
	type Request = Request;
	type Response = Response;
}

impl service::Observer<Protocol> for Observer {
	fn observe<'a>(&'a self, _event: service::Event<'a, Protocol>) -> BoxFuture<'a, ()> {
		async {}.boxed()
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

fn subject(id: &tg::process::Id) -> String {
	format!("processes.{id}.control.service")
}
