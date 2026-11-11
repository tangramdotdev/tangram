use {
	crate::{
		Component,
		control::{ClientMessage, ServerMessage},
	},
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::Messenger as _,
	tangram_server_core::{Server, Session},
};

/// The record of a process in the index and its data.
pub(crate) type Indexed = (tangram_index::process::Process, Option<tg::process::Data>);

pub(crate) type Arg<'a> =
	tangram_server_core::holder::Arg<'a, tg::process::control::ServerRequestArg>;

pub(crate) type Output =
	tangram_server_core::holder::Output<Indexed, tg::process::control::ClientResponseOutput>;

/// The process whose holder a request is sent to.
struct Kind<'a> {
	component: &'a Component,
	get: bool,
	id: &'a tg::process::Id,
	session: &'a Session,
}

impl Component {
	/// Send a request to the holder of a process while reading the process from the index, and return the holder's reply or the index's answer, whichever is decisive first. Return `None` if the wakeups end.
	pub(crate) async fn request_process_holder(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: Arg<'_>,
		index_required: impl Fn(&tg::process::control::ClientResponseOutput) -> bool + Send,
	) -> tg::Result<Option<Output>> {
		let get = matches!(arg.request, tg::process::control::ServerRequestArg::Get(_));
		let kind = Kind {
			component: self,
			get,
			id,
			session,
		};
		tangram_server_core::holder::request(&kind, arg, index_required).await
	}

	/// Serve the requests to the holder of a process by passing each message to the holder, which answers a request or replies that it does not hold the process.
	pub(crate) async fn serve_process_holder<F>(
		&self,
		server: &Server,
		id: &tg::process::Id,
		mut handle: F,
	) -> tg::Result<Task<tg::Result<()>>>
	where
		F: FnMut(tg::process::control::ServerMessage) -> BoxFuture<'static, tg::Result<()>>
			+ Send
			+ 'static,
	{
		let subject = format!("processes.{id}.control.server");
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
	pub(crate) async fn publish_process_holder_ack(
		&self,
		server: &Server,
		id: &tg::process::Id,
		ack: tg::process::control::ClientAck,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.control.client.{}", ack.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Ack(ack));
		server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to publish the process control acknowledgement"
				)
			})?;

		Ok(())
	}

	/// Publish a holder's response to a request to the server that sent the request.
	pub(crate) async fn publish_process_holder_response(
		&self,
		server: &Server,
		id: &tg::process::Id,
		response: tg::process::control::ClientResponse,
	) -> tg::Result<()> {
		let kind = match response.output.as_ref() {
			Some(tg::process::control::ClientResponseOutput::AcquireLease(_)) => "acquire_lease",
			Some(tg::process::control::ClientResponseOutput::Finish(_)) => "finish",
			Some(tg::process::control::ClientResponseOutput::Get(_)) => "get",
			Some(tg::process::control::ClientResponseOutput::GetChildren(_)) => "get_children",
			Some(tg::process::control::ClientResponseOutput::NotHolder(_)) => "not_holder",
			Some(tg::process::control::ClientResponseOutput::Open(_)) => "open",
			Some(tg::process::control::ClientResponseOutput::ReleaseLease(_)) => "release_lease",
			Some(tg::process::control::ClientResponseOutput::Signal(_)) => "signal",
			Some(tg::process::control::ClientResponseOutput::Tty(_)) => "tty",
			None => "error",
		};
		let request = response.id.clone();
		tangram_server_core::checkpoint!(
			server,
			"process.control.response.publish",
			kind = %kind,
			process = %id,
			request = request.clone(),
		)
		.await;
		let subject = format!("processes.{id}.control.client.{}", response.id);
		let payload = ClientMessage(tg::process::control::ClientMessage::Response(response));
		server
			.messenger
			.publish(subject, payload)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to publish the process control response")
			})?;
		tangram_server_core::checkpoint!(
			server,
			"process.control.response.published",
			kind = %kind,
			process = %id,
			request,
		)
		.await;

		Ok(())
	}
}

impl tangram_server_core::holder::Kind for Kind<'_> {
	type Indexed = Indexed;

	type Reply = tg::process::control::ClientResponseOutput;

	type Request = tg::process::control::ServerRequestArg;

	const HOLD: bool = true;

	fn index(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		async move {
			let indexed = self.reindex(exhaustive).await;
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "process.get.index", process = %self.id)
					.await;
			}
			indexed
		}
		.boxed()
	}

	fn reindex(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		self.component
			.try_get_process_with_data_from_index_with_exhaustive(self.session, self.id, exhaustive)
			.boxed()
	}

	fn send(
		&self,
		request: Self::Request,
		options: tangram_server_core::control::Options,
		hold: Option<BoxFuture<'static, ()>>,
	) -> BoxFuture<'_, tg::Result<Self::Reply>> {
		async move {
			let output = async {
				self.component
					.send_process_control_request(self.session, self.id, request, options, hold)
					.boxed()
					.await?
					.await?
			}
			.await;
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "process.get.control", process = %self.id)
					.await;
			}
			output
		}
		.boxed()
	}

	fn hold(&self, indexed: Option<&Self::Indexed>) -> Option<BoxFuture<'static, ()>> {
		let starting = indexed
			.and_then(|(process, _)| process.status)
			.is_some_and(|status| status.is_starting());
		starting.then(|| {
			self.component
				.create_wait_process_start_future(self.session, self.id)
				.map(|_| ())
				.boxed()
		})
	}

	fn index_required(&self) -> BoxFuture<'_, ()> {
		async move {
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "process.get.index.required", process = %self.id)
					.await;
			}
		}
		.boxed()
	}

	/// The index answers for a process that finished or that lives in a remote, and a read is also answered for a starting process.
	fn is_decisive(indexed: Option<&Self::Indexed>, fallback: bool) -> bool {
		indexed.is_some_and(|(process, _)| {
			process
				.status
				.is_some_and(|status| status.is_finished() || (fallback && status.is_starting()))
				|| process
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote)
		})
	}

	fn is_complete(indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| {
			process
				.status
				.is_some_and(|status| status.is_finished() || status.is_starting())
		})
	}

	fn has_status(indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| process.status.is_some())
	}
}
