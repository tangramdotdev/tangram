use {
	crate::{Server, Session},
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture},
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::service,
};

/// The handle with which a sandbox control connection keeps its control service running. Dropping every sender finishes the service.
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

/// The protocol of a control service, which answers for a sandbox whose control connection its server accepted.
pub(crate) struct Protocol;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum Request {
	#[tangram_serialize(id = 0)]
	Authorize(AuthorizeRequest),
}

/// A request that verifies that a principal may act for the sandbox.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct AuthorizeRequest {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,
}

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct Response {
	#[tangram_serialize(id = 0, default, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::error::Data>,
}

/// The reply to a request that a control service received over the local channel or the messenger.
enum Reply {
	Local(tokio::sync::oneshot::Sender<Response>),
	Messenger(service::Reply<Protocol>),
}

/// The state of a control service.
struct State {
	id: tg::sandbox::Id,
	runner: Option<tg::runner::Id>,
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
	fn response(&self, request: &Request) -> Response {
		match self.response_inner(request) {
			Ok(response) => response,
			Err(error) => {
				let tg::Either::Left(error) = error.to_data_or_id() else {
					unreachable!()
				};
				Response { error: Some(error) }
			},
		}
	}

	fn response_inner(&self, request: &Request) -> tg::Result<Response> {
		match request {
			Request::Authorize(request) => self.authorize(&request.principal)?,
		}
		Ok(Response::default())
	}

	/// Verify that a principal is the sandbox, the runner that hosts it, or the root.
	fn authorize(&self, principal: &tg::Principal) -> tg::Result<()> {
		let authorized = match principal {
			tg::Principal::Root => true,
			tg::Principal::Runner(runner) => self.runner.as_ref() == Some(runner),
			tg::Principal::Sandbox(sandbox) => sandbox == &self.id,
			_ => false,
		};
		if !authorized {
			return Err(tg::error!(
				unauthorized,
				?principal,
				sandbox = %self.id,
				"the principal may not act for the sandbox"
			));
		}
		Ok(())
	}
}

impl Session {
	/// Start answering the requests for a sandbox whose control connection this server accepted, over the messenger and, for requests from this server, over a local channel.
	pub(crate) fn spawn_sandbox_control_service_task(
		&self,
		id: &tg::sandbox::Id,
		runner: Option<tg::runner::Id>,
	) -> Sender {
		// Register the local channel before the task starts, so that later requests on this server find it. A reconnect replaces the service of the connection that it reconnects.
		let state = State {
			id: id.clone(),
			runner,
		};
		let (local_sender, local_requests) = tokio::sync::mpsc::unbounded_channel();
		let local = Local {
			sender: local_sender.clone(),
		};
		self.server
			.sandbox_control_services
			.insert(id.clone(), local);

		// Spawn the task, and remove its local channel when it exits.
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let sender = Sender { _sender: sender };
		let mut session = self.clone();
		session.context.stopper = None;
		let id = id.clone();
		self.server
			.sandbox_control_service_tasks
			.spawn({
				let server = self.server.clone();
				move |stopper| async move {
					let result = session
						.sandbox_control_service_task(state, receiver, local_requests, stopper)
						.await;
					server
						.sandbox_control_services
						.remove_if(&id, |_, local| local.sender.same_channel(&local_sender));
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the sandbox control service task failed");
					}
					result
				}
			})
			.detach();

		sender
	}

	async fn sandbox_control_service_task(
		&self,
		state: State,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
		mut local_requests: tokio::sync::mpsc::UnboundedReceiver<LocalRequest>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Start answering the requests on the messenger concurrently with the local channel, so that a local request does not wait for the messenger.
		let subject = subject(&state.id);
		let options = self.server.config.sandbox.control_service;
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
						tg::error!(!source, "failed to start the sandbox control service")
					})?;
					server = Some(started);
					crate::checkpoint!(self.server, "sandbox.control.service.started", sandbox = %state.id).await;
				},
				incoming = async { server.as_mut().unwrap().next().await }, if server.is_some() => {
					let Some(incoming) = incoming else {
						return Err(tg::error!("the sandbox control service stopped"));
					};
					let service::Incoming { reply, request } = incoming;
					let response = state.response(&request);
					Reply::Messenger(reply).send(&response);
				},
				Some(local_request) = local_requests.recv() => {
					let LocalRequest { reply, request } = local_request;
					let response = state.response(&request);
					Reply::Local(reply).send(&response);
				},
			}
		}

		crate::checkpoint!(self.server, "sandbox.control.service.stopped", sandbox = %state.id)
			.await;

		Ok(())
	}

	/// Verify with a sandbox's control service that a principal may act for the sandbox, waiting until a control connection for the sandbox can answer.
	pub(crate) async fn authorize_with_sandbox_control_service(
		&self,
		id: &tg::sandbox::Id,
		principal: &tg::Principal,
	) -> tg::Result<()> {
		let request = Request::Authorize(AuthorizeRequest {
			principal: principal.clone(),
		});
		self.server
			.request_sandbox_control_service(id, request)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox control service"))?;
		Ok(())
	}
}

impl Server {
	/// Ask a control service, waiting until it can answer. A sandbox with no control service resolves to `None` once the request times out.
	async fn request_sandbox_control_service(
		&self,
		id: &tg::sandbox::Id,
		request: Request,
	) -> tg::Result<Option<Response>> {
		// Ask this server's own control service over the local channel, or ask over the messenger.
		let response = if let Some(response) = self
			.request_sandbox_control_service_local(id, &request)
			.await
		{
			response
		} else {
			let mut request = self
				.sandbox_control_clients
				.request(&subject(id), request)
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to send the sandbox control service request"
					)
				})?;
			match request.wait().await {
				Ok(response) => response,
				Err(service::Error::Timeout) => return Ok(None),
				Err(service::Error::Stopped) => {
					return Err(tg::error!(
						code = tg::error::Code::Cancellation,
						"the server is stopping"
					));
				},
				Err(error) => {
					return Err(tg::error!(
						!error,
						"the sandbox control service request failed"
					));
				},
			}
		};

		// Return the response or its error.
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error).unwrap_or_else(|source| {
				tg::error!(
					!source,
					"failed to deserialize the sandbox control service error"
				)
			});
			return Err(error);
		}
		Ok(Some(response))
	}

	/// Ask this server's own control service over the local channel, returning `None` if this server has no control service for the sandbox or its service stopped before answering.
	async fn request_sandbox_control_service_local(
		&self,
		id: &tg::sandbox::Id,
		request: &Request,
	) -> Option<Response> {
		let sender = self.sandbox_control_services.get(id)?.sender.clone();
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

fn subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.service")
}
