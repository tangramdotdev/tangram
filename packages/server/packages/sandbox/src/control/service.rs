use {
	crate::Component, std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Stopper,
	tangram_server_core::Session, tangram_server_core_service as service,
};

/// The name of the control service's checkpoints.
pub(crate) const NAME: &str = "sandbox.control.service";

/// The subject under which the control services serve the subjects of their sandboxes.
pub(crate) const SUBJECT: &str = "sandboxes.control.service";

/// The handle with which a sandbox control connection keeps its control service running. Dropping every sender finishes the service.
#[derive(Clone)]
pub(crate) struct Sender {
	_sender: tokio::sync::mpsc::UnboundedSender<()>,
}

/// The state of a control service.
struct State {
	id: tg::sandbox::Id,
	runner: Option<tg::runner::Id>,
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

impl Component {
	/// Start answering the requests for a sandbox whose control connection this server accepted, over the messenger and, for requests from this server, over a local channel.
	pub(crate) async fn spawn_sandbox_control_service_task(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		runner: Option<tg::runner::Id>,
	) -> Sender {
		// Start the control service, and register it so that the requests on this server skip the messenger. A reconnect replaces the service of the connection that it reconnects.
		let options = session.server.config.sandbox.control_service;
		let observer = Arc::new(service::Checkpoints::new(
			session.server.checkpoints.clone(),
			NAME,
		));
		let server =
			service::Server::start(&session.server.messenger, SUBJECT, id, options, observer);
		self.control.servers.insert(id.clone(), server.clone());
		tangram_server_core::checkpoint!(session.server, "sandbox.control.service.started", sandbox = %id).await;

		// Stop and unregister the service when its task ends, even if the task panics or is aborted, unless a reconnect replaced it.
		let guard = scopeguard::guard((), {
			let component = self.clone();
			let id = id.clone();
			let server = server.clone();
			move |()| {
				server.stop();
				component
					.control
					.servers
					.remove_if(&id, |_, server| server.is_stopped());
			}
		});

		// Spawn the task that answers the requests, and wait for the service's responses to be acknowledged once it ends.
		let state = State {
			id: id.clone(),
			runner,
		};
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let sender = Sender { _sender: sender };
		self.control
			.service_tasks
			.spawn({
				let mut session = session.clone();
				session.context.stopper = None;
				let id = id.clone();
				move |stopper| async move {
					let result =
						Self::sandbox_control_service_task(state, &server, receiver, stopper).await;
					drop(guard);
					server.wait().await;
					tangram_server_core::checkpoint!(session.server, "sandbox.control.service.stopped", sandbox = %id).await;
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
		state: State,
		server: &service::Server<Protocol>,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Answer each request until every sender is dropped or the server stops.
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				event = receiver.recv() => {
					if event.is_none() {
						break;
					}
				},
				incoming = server.next() => {
					let Some((request, reply)) = incoming else {
						return Err(tg::error!("the sandbox control service stopped"));
					};
					let response = state.response(&request);
					reply.send(&response);
				},
			}
		}

		Ok(())
	}

	/// Verify with a sandbox's control service that a principal may act for the sandbox, waiting until a control connection for the sandbox can answer.
	pub async fn authorize_with_sandbox_control_service(
		&self,
		id: &tg::sandbox::Id,
		principal: &tg::Principal,
	) -> tg::Result<()> {
		let request = Request::Authorize(AuthorizeRequest {
			principal: principal.clone(),
		});
		self.request_sandbox_control_service(id, request)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox control service"))?;
		Ok(())
	}

	/// Ask a control service, over the local channel if it runs on this server. A sandbox with no control service resolves to `None` once the request times out.
	async fn request_sandbox_control_service(
		&self,
		id: &tg::sandbox::Id,
		request: Request,
	) -> tg::Result<Option<Response>> {
		// Ask the control service.
		let server = self
			.control
			.servers
			.get(id)
			.map(|server| server.value().clone());
		let result = self
			.control
			.client
			.request(id, server.as_ref(), request)
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
			Err(error) => {
				return Err(tg::error!(
					!error,
					"the sandbox control service request failed"
				));
			},
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
}

impl service::Protocol for Protocol {
	type Request = Request;
	type Response = Response;
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
