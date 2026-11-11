use {
	crate::{
		Component,
		control::{ClientMessage, ServerMessage},
	},
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream::BoxStream},
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
		let kind = Kind {
			component: self,
			destroy,
			get,
			id,
			processes,
			session,
		};
		tangram_server_core::holder::request(&kind, arg, index_required).await
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
		let component = self.clone();
		let server = server.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			while let Some(message) = messages
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the message"))?
			{
				// Answer a duplicate of a request from the replies that the holder sent, without passing it to the holder again, and drop a duplicate of a request that the holder is acting on, whose reply will reach its sender.
				let message = message.payload.0;
				if let tg::sandbox::control::ServerMessage::Request(request) = &message {
					match component.0.holder_replies.begin(&request.id) {
						tangram_server_core::holder::Begin::New => (),
						tangram_server_core::holder::Begin::Pending => continue,
						tangram_server_core::holder::Begin::Replied(response) => {
							component
								.publish_sandbox_holder_response(&server, &id, response)
								.await?;
							continue;
						},
					}
				}
				handle(message).await?;
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
		self.0.holder_replies.acknowledge(&ack.id);
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
		// Remember the reply, so that a duplicate of the request is answered with it, unless the reply asks the sender to send the request again, after which a copy of the request is acted on again.
		if response
			.output
			.as_ref()
			.is_some_and(tg::sandbox::control::ClientResponseOutput::is_deferral)
		{
			self.0.holder_replies.forget(&response.id);
		} else {
			self.0.holder_replies.finish(&response.id, response.clone());
		}
		let kind = match response.output.as_ref() {
			Some(tg::sandbox::control::ClientResponseOutput::Destroy(_)) => "destroy",
			Some(tg::sandbox::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::sandbox::control::ClientResponseOutput::GetProcesses(_)) => "get_processes",
			Some(tg::sandbox::control::ClientResponseOutput::NotHolder(_)) => "not_holder",
			Some(tg::sandbox::control::ClientResponseOutput::NotReady(_)) => "not_ready",
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
		id: String,
		options: tangram_server_core::control::Options,
		resend: Option<BoxStream<'static, ()>>,
	) -> BoxFuture<'_, tg::Result<Self::Reply>> {
		async move {
			if self.destroy {
				tangram_server_core::checkpoint!(self.session.server, "sandbox.destroy.control", sandbox = %self.id)
					.await;
			}
			let output = async {
				self.component
					.send_sandbox_control_request(self.session, self.id, id, request, options, resend)
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

	fn index_required(&self) -> BoxFuture<'_, ()> {
		async {}.boxed()
	}

	/// The index answers for a sandbox that is stopped or destroyed, that is being destroyed while it is stopped, or that lives in a remote, and for the processes that it has. The scheduler answers for a starting sandbox.
	fn is_decisive(&self, indexed: Option<&Self::Indexed>, _fallback: bool) -> bool {
		indexed.is_some_and(|sandbox| {
			self.processes
				|| sandbox.data.as_ref().is_some_and(|data| {
					data.data.status.is_stopped_or_destroyed() || is_destroying_stopped(&data.data)
				}) || sandbox
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		})
	}

	fn is_complete(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|sandbox| {
			self.processes
				|| sandbox.data.as_ref().is_some_and(|data| {
					data.data.status.is_starting()
						|| data.data.status.is_stopped_or_destroyed()
						|| is_destroying_stopped(&data.data)
				})
		})
	}

	fn has_status(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|sandbox| sandbox.data.is_some())
	}
}

/// Whether a sandbox is being destroyed while it is stopped, in which case the index holds it and the runner that has its directory removes the directory.
pub(crate) fn is_destroying_stopped(data: &tg::sandbox::Data) -> bool {
	data.status.is_destroying() && data.stopped_at.is_some()
}
