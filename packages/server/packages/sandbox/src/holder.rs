use {
	crate::{
		Component,
		control::{ClientMessage, ServerMessage},
	},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_futures::{stream::TryExt as _, task::Task},
	tangram_messenger::Messenger as _,
	tangram_server_core::{Server, Session},
};

/// The record of a sandbox in the index.
pub(crate) type Indexed = tangram_index::sandbox::Sandbox;

pub(crate) type Arg<'a> = tangram_server_core::sandbox::HolderArg<'a>;

pub(crate) type Output = tangram_server_core::sandbox::HolderOutput;

/// The sandbox whose holder a request is sent to.
struct Kind<'a> {
	component: &'a Component,
	destroy: bool,
	get: bool,
	id: &'a tg::sandbox::Id,
	processes: bool,
	session: &'a Session,
	spawn: bool,
}

impl Component {
	/// Send a request to the holder of a sandbox while reading the sandbox from the index, and return the holder's reply or the index's answer, whichever is decisive first. Return `None` if the wakeups end.
	pub(crate) async fn request_sandbox_holder(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: Arg<'_>,
		index_required: impl Fn(&tg::sandbox::control::ClientResponseOutput) -> bool + Send,
	) -> tg::Result<Option<Output>> {
		let destroy = matches!(
			arg.request,
			tg::sandbox::control::ServerRequestArg::Destroy(_)
		);
		let get = matches!(arg.request, tg::sandbox::control::ServerRequestArg::Get(_));
		let processes = matches!(
			arg.request,
			tg::sandbox::control::ServerRequestArg::GetProcesses(_)
		);
		let spawn = matches!(
			arg.request,
			tg::sandbox::control::ServerRequestArg::SpawnProcess(_)
		);
		let kind = Kind {
			component: self,
			destroy,
			get,
			id,
			processes,
			session,
			spawn,
		};
		tangram_server_core::holder::request(&kind, arg, index_required).await
	}

	/// Create a future that waits for a sandbox to leave the starting status.
	fn create_wait_sandbox_start_future(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
	) -> BoxFuture<'static, ()> {
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		async move {
			let Ok(mut wakeups) = component
				.create_sandbox_status_wakeup_stream(
					&session,
					&id,
					session.context.stopper.clone(),
					None,
				)
				.await
			else {
				return;
			};
			loop {
				let starting = component
					.try_get_sandbox_from_index(&session, &id)
					.await
					.ok()
					.flatten()
					.and_then(|sandbox| sandbox.data)
					.is_some_and(|data| data.data.status.is_starting());
				if !starting || wakeups.next().await.is_none() {
					return;
				}
			}
		}
		.boxed()
	}

	/// Serve the requests to the holder of a sandbox by passing each message to the holder, which answers a request or replies that it does not hold the sandbox.
	pub(crate) async fn serve_sandbox_holder<F>(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		mut handle: F,
	) -> tg::Result<Task<tg::Result<()>>>
	where
		F: FnMut(tg::sandbox::control::ServerMessage) -> BoxFuture<'static, tg::Result<()>>
			+ Send
			+ 'static,
	{
		let subject = format!("sandboxes.{id}.control.server");
		let mut messages = server
			.messenger
			.subscribe::<ServerMessage>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the message stream"))?;
		let task = Task::spawn(move |_| async move {
			while let Some(message) = messages
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the message"))?
			{
				handle(message.payload.0).await?;
			}
			Ok::<_, tg::Error>(())
		});

		Ok(task)
	}

	/// Publish a holder's acknowledgement of a request to the server that sent the request.
	pub(crate) async fn publish_sandbox_holder_ack(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		ack: tg::sandbox::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("sandboxes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Ack(ack));
		server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the sandbox control acknowledgement"
				)
			})?;

		Ok(())
	}

	/// Publish a holder's response to a request to the server that sent the request.
	pub(crate) async fn publish_sandbox_holder_response(
		&self,
		server: &Server,
		id: &tg::sandbox::Id,
		response: tg::sandbox::control::ClientResponse,
	) -> tg::Result<()> {
		let kind = match response.output.as_ref() {
			Some(tg::sandbox::control::ClientResponseOutput::Destroy(_)) => "destroy",
			Some(tg::sandbox::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::sandbox::control::ClientResponseOutput::GetProcesses(_)) => "get_processes",
			Some(tg::sandbox::control::ClientResponseOutput::NotHolder(_)) => "not_holder",
			Some(tg::sandbox::control::ClientResponseOutput::SpawnProcess(_)) => "spawn_process",
			Some(tg::sandbox::control::ClientResponseOutput::Start(_)) => "start",
			Some(tg::sandbox::control::ClientResponseOutput::Stop(_)) => "stop",
			None => "error",
		};
		tangram_server_core::checkpoint!(
			server,
			"sandbox.control.response.publish",
			kind = %kind,
			sandbox = %id,
		)
		.await;
		let subject = format!("sandboxes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::sandbox::control::ClientMessage::Response(response));
		server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the sandbox control response")
			})?;

		Ok(())
	}
}

impl tangram_server_core::holder::Kind for Kind<'_> {
	type Indexed = Indexed;

	type Reply = tg::sandbox::control::ClientResponseOutput;

	type Request = tg::sandbox::control::ServerRequestArg;

	fn index(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		async move {
			let mut indexed = self.reindex(exhaustive).await?;

			// Confirm a stopped sandbox after pending indexing for a write, because a start may not be indexed yet.
			let stopped = indexed
				.as_ref()
				.and_then(|sandbox| sandbox.data.as_ref())
				.is_some_and(|data| data.data.status.is_stopped());
			if stopped && !self.get {
				self.session
					.index(tg::index::Arg::default())
					.await?
					.try_last()
					.await?;
				indexed = self.reindex(exhaustive).await?;
			}
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "sandbox.get.index", sandbox = %self.id)
					.await;
			}
			if self.destroy {
				tangram_server_core::checkpoint!(self.session.server, "sandbox.destroy.index", sandbox = %self.id)
					.await;
			}

			Ok(indexed)
		}
		.boxed()
	}

	fn reindex(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		async move {
			// A request for a sandbox's processes requires a record that has them.
			let indexed = self
				.component
				.try_get_sandbox_from_index_with_exhaustive(self.session, self.id, exhaustive)
				.await?;
			Ok(indexed.filter(|sandbox| !self.processes || sandbox.set.processes))
		}
		.boxed()
	}

	fn send(
		&self,
		request: Self::Request,
		options: tangram_server_core::control::Options,
		hold: Option<BoxFuture<'static, ()>>,
	) -> BoxFuture<'_, tg::Result<Self::Reply>> {
		async move {
			if self.destroy {
				tangram_server_core::checkpoint!(self.session.server, "sandbox.destroy.control", sandbox = %self.id)
					.await;
			}
			let output = async {
				self.component
					.send_sandbox_control_request(self.session, self.id, request, options, hold)
					.boxed()
					.await?
					.await?
			}
			.await;
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "sandbox.get.control", sandbox = %self.id)
					.await;
			}
			output
		}
		.boxed()
	}

	fn hold(&self, indexed: Option<&Self::Indexed>) -> Option<BoxFuture<'static, ()>> {
		let starting = indexed
			.and_then(|sandbox| sandbox.data.as_ref())
			.is_some_and(|data| data.data.status.is_starting());
		starting.then(|| {
			self.component
				.create_wait_sandbox_start_future(self.session, self.id)
		})
	}

	/// A spawn reads the index before it sends its request, so that the request is held while the sandbox is starting.
	fn holds(&self) -> bool {
		self.spawn
	}

	fn index_required(&self) -> BoxFuture<'_, ()> {
		async {}.boxed()
	}

	/// The index answers for a sandbox that is stopped or destroyed, or that lives in a remote, and for the processes that it has. The scheduler answers for a starting sandbox.
	fn is_decisive(&self, indexed: Option<&Self::Indexed>, _fallback: bool) -> bool {
		indexed.is_some_and(|sandbox| {
			self.processes
				|| sandbox
					.data
					.as_ref()
					.is_some_and(|data| data.data.status.is_stopped_or_destroyed())
				|| sandbox
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
		})
	}

	fn is_complete(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|sandbox| {
			self.processes
				|| sandbox.data.as_ref().is_some_and(|data| {
					data.data.status.is_starting() || data.data.status.is_stopped_or_destroyed()
				})
		})
	}

	fn has_status(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|sandbox| sandbox.data.is_some())
	}
}
