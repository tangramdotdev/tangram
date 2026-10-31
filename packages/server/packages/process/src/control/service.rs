use {
	crate::Component, std::sync::Arc, tangram_client::prelude::*, tangram_futures::task::Stopper,
	tangram_server_core::Session, tangram_server_service as service,
};

/// The name of the control service's checkpoints.
pub(crate) const NAME: &str = "process.control.service";

/// The subject under which the control services serve the subjects of their processes.
pub(crate) const SUBJECT: &str = "processes.control.service";

/// The handle with which a process control connection keeps its control service running. Dropping it stops the service.
pub(crate) struct Handle {
	server: service::Server<Request, Response>,
}

/// The arg of a control service.
pub(crate) struct Arg {
	pub indexed: bool,
	pub parent: Option<tg::process::Id>,
	pub runner: Option<tg::runner::Id>,
	pub syncs: bool,
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
				let sync = session.server.components.sync.refresh_sync(session, id)?;
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

impl Component {
	/// Start answering the requests for a process whose start this server accepted, over the messenger and, for requests from this server, over a local channel.
	pub(crate) fn spawn_process_control_service_task(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: Arg,
	) -> Handle {
		let state = State {
			id: id.clone(),
			indexed: arg.indexed,
			parent: arg.parent,
			rejection: None,
			runner: arg.runner,
			syncs: arg.syncs,
		};
		self.spawn_process_control_service_task_with_state(session, state)
	}

	/// Answer every request for a process whose start this server rejected with the rejection, so that the requests waiting for the start fail.
	pub(crate) fn spawn_process_control_rejection_task(
		&self,
		session: &Session,
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
		let handle = self.spawn_process_control_service_task_with_state(session, state);

		// Keep answering for the service's attempt time to live.
		let ttl = session.server.config.process.control_service.attempt_ttl;
		let sleep = session.server.clock.sleep(ttl);
		tokio::spawn(async move {
			sleep.await;
			drop(handle);
		});
	}

	fn spawn_process_control_service_task_with_state(
		&self,
		session: &Session,
		state: State,
	) -> Handle {
		// Start the control service, and register it so that the requests on this server skip the messenger. A reconnect replaces the service of the connection that it reconnects.
		let options = session.server.config.process.control_service;
		let observer = Arc::new(service::Checkpoints::new(
			session.server.checkpoints.clone(),
			NAME,
		));
		let server = service::Server::start(
			&session.server.messenger,
			SUBJECT,
			&state.id,
			options,
			observer,
		);
		self.control
			.servers
			.insert(state.id.clone(), server.clone());

		// Stop and unregister the service when its task ends, even if the task panics or is aborted, unless a reconnect replaced it.
		let guard = scopeguard::guard((), {
			let component = self.clone();
			let id = state.id.clone();
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
		let handle = Handle {
			server: server.clone(),
		};
		let mut task_session = session.clone();
		task_session.context.stopper = None;
		self.control
			.service_tasks
			.spawn({
				move |stopper| async move {
					Self::process_control_service_task(&task_session, &state, &server, stopper)
						.await;
					drop(guard);
					server.wait().await;
					Ok(())
				}
			})
			.detach();

		handle
	}

	async fn process_control_service_task(
		session: &Session,
		state: &State,
		server: &service::Server<Request, Response>,
		stopper: Stopper,
	) {
		// Answer each request until the handle is dropped or the server stops.
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				incoming = server.next() => {
					let Some((request, reply)) = incoming else {
						break;
					};
					let response = state.response(session, &request).await;
					reply.send(&response);
				},
			}
		}
	}

	/// Get one of a process's syncs from its control service, waiting until a control connection for the process can answer.
	pub async fn try_resolve_process_control_sync(
		&self,
		session: &Session,
		id: &tg::process::Id,
		kind: tg::sync::join::Kind,
	) -> tg::Result<Option<tg::Referent<tg::sync::Id>>> {
		let request = Request::Sync(SyncRequest {
			kind,
			principal: session.context.principal.clone(),
		});
		let response = self.request_process_control_service(id, request).await?;
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
		self.request_process_control_service(id, request)
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
		let response = self.request_process_control_service(id, request).await?;
		let parent = response.and_then(|response| response.parent);
		Ok(parent)
	}

	/// Ask a control service, over the local channel if it runs on this server. A process with no control service resolves to `None` once the request times out.
	async fn request_process_control_service(
		&self,
		id: &tg::process::Id,
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
					"the process control service request failed"
				));
			},
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
}

impl Drop for Handle {
	fn drop(&mut self) {
		self.server.stop();
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
