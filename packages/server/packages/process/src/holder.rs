use {
	crate::{
		Component,
		control::{ClientMessage, ServerMessage},
	},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future::BoxFuture, stream::BoxStream,
	},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_messenger::{Messenger as _, Payload},
	tangram_server_core::{Server, Session},
};

/// The record of a process in the index and its data.
pub(crate) type Indexed = (tangram_index::process::Process, Option<tg::process::Data>);

pub(crate) type Arg<'a> =
	tangram_server_core::holder::Arg<'a, tg::process::control::ServerRequestArg>;

pub(crate) type Output =
	tangram_server_core::holder::Output<Indexed, tg::process::control::ClientResponseOutput>;

/// The process whose holder a request is sent to.
struct Item<'a> {
	cancel: bool,
	children: bool,
	component: &'a Component,
	get: bool,
	id: &'a tg::process::Id,
	session: &'a Session,
	signal: bool,
}

/// A stream of a process's stdio that the process's holder relays.
pub(crate) struct StreamArg<'a, M> {
	pub(crate) id: &'a tg::process::Id,

	/// The method that relays the stream's data on each attempt.
	pub(crate) method: M,

	/// The relay that the runner opened when the process started, which the first attempt claims instead of sending an open request.
	pub(crate) open: Option<String>,

	/// The future that resolves once the process leaves the starting status, while which the relay waits for its holder without timing out.
	pub(crate) start: Option<crate::stdio::read::Start>,
}

/// The relay of one attempt of a stream, opened at the process's holder.
pub(crate) struct Opened<S, R> {
	/// Whether the attempt claimed the relay that the runner opened when the process started.
	pub(crate) claimed: bool,

	pub(crate) receiver: tangram_messenger::relay::Receiver<R>,

	/// The holder's response to the open request, which never resolves for a claimed relay.
	pub(crate) response: BoxFuture<'static, tg::Result<()>>,

	pub(crate) sender: tangram_messenger::relay::Sender<S>,

	pub(crate) start: Option<crate::stdio::read::Start>,
}

/// The side of a process's stdio that a stream relays.
#[derive(Clone, Copy)]
pub(crate) enum StreamSide {
	Read,
	Write,
}

/// The holder stream of a process, which opens each attempt's relay at the process's holder.
struct Stream<'a, M> {
	component: &'a Component,
	id: &'a tg::process::Id,
	method: M,
	open: Option<String>,
	session: &'a Session,
	start: Option<crate::stdio::read::Start>,
}

/// A method whose data a process's holder relays, such as a stdio read or a stdio write.
pub(crate) trait StreamMethod: Send {
	type Receive: Payload + Send + 'static;

	type Send: Payload + Send + 'static;

	const SIDE: StreamSide;

	/// Relay the data of one attempt on a relay that the holder opened.
	fn attempt<'a>(
		&'a mut self,
		component: &'a Component,
		session: &'a Session,
		opened: Opened<Self::Send, Self::Receive>,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), tg::Error>>>;

	/// Whether the stream's consumer is gone.
	fn is_closed(&self) -> bool;

	/// The read that the open request asks the holder to start, from the stream's acknowledged position.
	fn open_arg(&self) -> tg::process::stdio::read::Arg;

	/// The position through which the stream has delivered its data.
	fn position(&self) -> u64;
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
		let children = matches!(
			arg.request,
			tg::process::control::ServerRequestArg::GetChildren(_)
		);
		let get = children || matches!(arg.request, tg::process::control::ServerRequestArg::Get(_));
		let cancel = matches!(
			arg.request,
			tg::process::control::ServerRequestArg::ReleaseLease(_)
		);
		let signal = matches!(
			arg.request,
			tg::process::control::ServerRequestArg::Signal(_)
		);
		let item = Item {
			cancel,
			children,
			component: self,
			get,
			id,
			session,
			signal,
		};
		tangram_server_core::holder::request(&item, arg, index_required).await
	}

	/// Relay a stream of a process's stdio through the process's holder. Each attempt opens a relay and asks the holder to open its end, which only the holder accepts, and the stream reopens from its acknowledged position after a handoff or a failure, until it ends.
	pub(crate) async fn stream_process_holder<M: StreamMethod>(
		&self,
		session: &Session,
		arg: StreamArg<'_, M>,
	) -> tg::Result<()> {
		let StreamArg {
			id,
			method,
			open,
			start,
		} = arg;
		let mut stream = Stream {
			component: self,
			id,
			method,
			open,
			session,
			start,
		};
		let retry = session
			.server
			.config
			.process
			.stdio_relay_retry
			.clone()
			.into();
		tangram_server_core::holder::stream(&mut stream, retry).await?;

		Ok(())
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
				if let tg::process::control::ServerMessage::Request(request) = &message {
					match component.0.holder_replies.begin(&request.id) {
						tangram_server_core::holder::Begin::New => (),
						tangram_server_core::holder::Begin::Pending => continue,
						tangram_server_core::holder::Begin::Replied(response) => {
							component
								.publish_process_holder_response(&server, &id, response)
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
	pub(crate) async fn publish_process_holder_ack(
		&self,
		server: &Server,
		id: &tg::process::Id,
		ack: tg::process::control::ClientAck,
	) -> tg::Result<()> {
		self.0.holder_replies.acknowledge(&ack.id);
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
		// Remember the reply, so that a duplicate of the request is answered with it, unless the reply asks the sender to send the request again, after which a copy of the request is acted on again.
		if response.retry.is_some() {
			self.0.holder_replies.forget(&response.id);
		} else {
			self.0.holder_replies.finish(&response.id, response.clone());
		}
		let kind = match (response.retry, response.output.as_ref()) {
			(Some(tg::control::Retry::NotHolder), _) => "not_holder",
			(Some(tg::control::Retry::NotReady), _) => "not_ready",
			(None, Some(tg::process::control::ClientResponseOutput::AcquireLease(_))) => {
				"acquire_lease"
			},
			(None, Some(tg::process::control::ClientResponseOutput::Finish(_))) => "finish",
			(None, Some(tg::process::control::ClientResponseOutput::Get(_))) => "get",
			(None, Some(tg::process::control::ClientResponseOutput::GetChildren(_))) => {
				"get_children"
			},
			(None, Some(tg::process::control::ClientResponseOutput::Open(_))) => "open",
			(None, Some(tg::process::control::ClientResponseOutput::ReleaseLease(_))) => {
				"release_lease"
			},
			(None, Some(tg::process::control::ClientResponseOutput::Signal(_))) => "signal",
			(None, Some(tg::process::control::ClientResponseOutput::Tty(_))) => "tty",
			(None, None) => "error",
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

impl tangram_server_core::holder::Item for Item<'_> {
	type Indexed = Indexed;

	type Reply = tg::process::control::ClientResponseOutput;

	type Request = tg::process::control::ServerRequestArg;

	fn index(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		async move {
			let indexed = self.reindex(exhaustive).await;
			if self.get {
				tangram_server_core::checkpoint!(self.session.server, "process.get.index", process = %self.id)
					.await;
			}
			if self.cancel {
				tangram_server_core::checkpoint!(self.session.server, "process.cancel.index", process = %self.id)
					.await;
			}
			if self.signal {
				tangram_server_core::checkpoint!(self.session.server, "process.signal.index", process = %self.id)
					.await;
			}
			indexed
		}
		.boxed()
	}

	fn reindex(&self, exhaustive: bool) -> BoxFuture<'_, tg::Result<Option<Self::Indexed>>> {
		async move {
			// A request for a process's children requires a record that has them, or a starting process, which has none yet.
			let indexed = self
				.component
				.try_get_process_with_data_from_index_with_exhaustive(
					self.session,
					self.id,
					exhaustive,
				)
				.await?;
			Ok(indexed.filter(|(process, _)| {
				!self.children
					|| process.set.children
					|| process.status.is_some_and(|status| status.is_starting())
			}))
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
			if self.cancel {
				tangram_server_core::checkpoint!(self.session.server, "process.cancel.control", process = %self.id)
					.await;
			}
			let output = async {
				self.component
					.send_process_control_request(self.session, self.id, id, request, options, resend)
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
	fn is_decisive(&self, indexed: Option<&Self::Indexed>, fallback: bool) -> bool {
		indexed.is_some_and(|(process, _)| {
			self.children
				|| process.status.is_some_and(|status| {
					status.is_finished() || (fallback && status.is_starting())
				}) || process
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		})
	}

	fn is_complete(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| {
			self.children
				|| process
					.status
					.is_some_and(|status| status.is_finished() || status.is_starting())
		})
	}

	fn has_status(&self, indexed: Option<&Self::Indexed>) -> bool {
		indexed.is_some_and(|(process, _)| self.children || process.status.is_some())
	}
}

impl<M: StreamMethod> tangram_server_core::holder::Stream for Stream<'_, M> {
	fn attempt<'a>(
		&'a mut self,
		previous: Option<&'a tg::Error>,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), tg::Error>>> {
		async move {
			let id = self.id;
			let session = self.session;

			// Report that the stream reopens after a handoff or a failure.
			if let Some(error) = previous {
				match M::SIDE {
					StreamSide::Read => {
						tracing::warn!(error = %error.trace(), %id, "reopening the stdio relay");
						tangram_server_core::checkpoint!(session.server, "process.stdio.read.reopen", process = %id).await;
					},
					StreamSide::Write => {
						tracing::warn!(error = %error.trace(), %id, "reopening the stdin relay");
						tangram_server_core::checkpoint!(session.server, "process.stdio.write.reopen", process = %id).await;
					},
				}
			}

			// Open the relay before the holder's end, so that the holder's first data is not missed. A claimed open names the relay that the runner opened when the process started.
			let claimed = self.open.is_some();
			let relay = self
				.open
				.take()
				.unwrap_or_else(tangram_server_core::control::id);
			let data_subject = crate::stdio::relay::data_subject(id, &relay);
			let ack_subject = crate::stdio::relay::ack_subject(id, &relay);
			let (send_subject, receive_subject) = match M::SIDE {
				StreamSide::Read => (ack_subject, data_subject),
				StreamSide::Write => (data_subject, ack_subject),
			};
			let options = session.server.config.process.stdio_relay;
			let hold = self.start.clone().map(|start| start.map(|_| ()).boxed());
			let result = tangram_messenger::relay::open_with_hold::<M::Send, M::Receive, _>(
				&session.server.messenger,
				send_subject,
				receive_subject,
				options,
				hold,
			)
			.await;
			let (sender, receiver) = crate::stdio::relay::attempt!(result);

			// Ask the holder to open its end, unless the runner opened it when the process started.
			let response = if claimed {
				match M::SIDE {
					StreamSide::Read => {
						tangram_server_core::checkpoint!(session.server, "process.stdio.read.open.claimed", process = %id).await;
					},
					StreamSide::Write => {
						tangram_server_core::checkpoint!(session.server, "process.stdio.write.open.claimed", process = %id).await;
					},
				}
				futures::future::pending::<tg::Result<()>>().boxed()
			} else {
				let arg = self.method.open_arg();
				let request = tg::process::control::ServerRequest {
					arg: tg::process::control::ServerRequestArg::Open(
						tg::process::control::OpenServerRequestArg { arg },
					),
					id: relay,
				};
				let resend = self
					.start
					.clone()
					.map(|start| start.map(|_| ()).into_stream().boxed());
				self.component
					.send_process_control_open(session, id, request, resend)
					.await?
			};

			// Relay the attempt's data.
			let opened = Opened {
				claimed,
				receiver,
				response,
				sender,
				start: self.start.clone(),
			};
			self.method
				.attempt(self.component, session, opened)
				.await
		}
		.boxed()
	}

	fn is_closed(&self) -> bool {
		self.method.is_closed()
	}

	fn position(&self) -> u64 {
		self.method.position()
	}
}
