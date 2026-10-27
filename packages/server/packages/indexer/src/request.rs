use {
	super::{Indexer, IndexerState, queue, wait},
	crate::Component,
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::{Messenger as _, Payload},
	tangram_server_core::Server,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
enum ClientMessage {
	#[tangram_serialize(id = 0)]
	Ack(Ack),

	#[tangram_serialize(id = 1)]
	Response(Response),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
enum ServerMessage {
	#[tangram_serialize(id = 0)]
	Ack(Ack),

	#[tangram_serialize(id = 1)]
	Request(Request),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Ack {
	#[tangram_serialize(id = 0)]
	id: String,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Request {
	#[tangram_serialize(id = 0)]
	arg: RequestArg,

	#[tangram_serialize(id = 1)]
	id: String,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum RequestArg {
	#[tangram_serialize(id = 0)]
	Enqueue(EnqueueRequestArg),

	#[tangram_serialize(id = 1)]
	Wait,
}

/// Fragments of archive and index batches to write to the cache's queues.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct EnqueueRequestArg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub archive: Vec<queue::Fragment>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub index: Vec<queue::Fragment>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Response {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	error: Option<tg::error::Data>,

	#[tangram_serialize(id = 1)]
	id: String,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	output: Option<ResponseOutput>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub(crate) enum ResponseOutput {
	#[tangram_serialize(id = 0)]
	Enqueue,

	#[tangram_serialize(id = 1)]
	Wait,
}

pub(super) struct RequestTaskArgs {
	pub changed: Arc<tokio::sync::Notify>,
	pub ready: tokio::sync::oneshot::Sender<()>,
	pub state: Arc<Mutex<IndexerState>>,
	pub stopper: Stopper,
	pub wait_sender: wait::Sender,
	pub write_sender: queue::write::Sender,
}

struct HandleIndexerRequestArgs {
	guard: Guard,
	request: Request,
	sender: tangram_server_core::control::Sender<ServerMessage, ClientMessage>,
	wait_sender: wait::Sender,
	write_sender: queue::write::Sender,
}

struct HandleRequestsArgs {
	changed: Arc<tokio::sync::Notify>,
	control: tangram_server_core::control::Stream<ServerMessage, ClientMessage>,
	state: Arc<Mutex<IndexerState>>,
	stopper: Stopper,
	wait_sender: wait::Sender,
	write_sender: queue::write::Sender,
}

struct Guard {
	changed: Arc<tokio::sync::Notify>,
	state: Arc<Mutex<IndexerState>>,
	writing: bool,
}

impl Component {
	pub(crate) async fn send_indexer_request(
		&self,
		server: &Server,
		indexer: Option<&tg::indexer::Id>,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		// Reuse the ID so a retry does not replace an active wait with a later barrier.
		let id = tangram_server_core::control::id();
		let retry = server.config.indexer.request.retry.clone().into();
		let timeout =
			matches!(arg, RequestArg::Wait).then_some(server.config.indexer.request.response_ttl);
		tangram_futures::retry(&retry, || {
			let arg = arg.clone();
			let id = &id;
			async move {
				let request = self.send_indexer_request_inner(server, indexer, id, arg);
				let response = if let Some(timeout) = timeout {
					match tokio::time::timeout(timeout, request).await {
						Ok(response) => response?,
						Err(source) => {
							return Ok(ControlFlow::Continue(tg::error!(
								!source,
								"timed out waiting for the indexer response"
							)));
						},
					}
				} else {
					request.await?
				};
				Ok(ControlFlow::Break(response))
			}
		})
		.await
	}

	async fn send_indexer_request_inner(
		&self,
		server: &Server,
		indexer: Option<&tg::indexer::Id>,
		id: &str,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let request = ServerMessage::Request(Request {
			arg,
			id: id.to_owned(),
		});
		let config = server.config.indexer.request.clone();
		let options = tangram_server_core::control::Options {
			retry: config.retry.into(),
			timeout: config.timeout,
		};
		let arg = tangram_server_core::control::SendControlRequestArg {
			ack: |id| ServerMessage::Ack(Ack { id }),
			client_subject: Indexer::client_subject(id),
			is_ack: |message: &ClientMessage| matches!(message, ClientMessage::Ack(_)),
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: ClientMessage| {
				let ClientMessage::Response(message) = message else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error).map_err(|source| {
						tg::error!(!source, "failed to deserialize the indexer error")
					})?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing indexer response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: Indexer::server_subject(indexer),
		};
		server.request_control(arg).await
	}
}

impl Indexer {
	pub(super) async fn request_task(&self, args: RequestTaskArgs) -> tg::Result<()> {
		let RequestTaskArgs {
			changed,
			ready,
			state,
			stopper,
			wait_sender,
			write_sender,
		} = args;
		let messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(Self::server_subject(self.id.as_ref()))
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to subscribe to the indexer request stream")
			})?
			.map_err(|source| tg::error!(!source, "failed to receive an indexer message"))
			.map_ok(|message| message.payload)
			.boxed();
		let capacity = self.server.config.indexer.request.channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let mut options = tangram_server_core::control::stream_options(&self.server.config.control);
		options.outbox_ttl = Some(self.server.config.indexer.request.response_ttl);
		let control = tangram_server_core::control::Stream::new(messages, sender, options);
		ready
			.send(())
			.map_err(|()| tg::error!("failed to signal indexer request readiness"))?;
		let args = HandleRequestsArgs {
			changed,
			control,
			state,
			stopper,
			wait_sender,
			write_sender,
		};
		let requests = self.handle_requests(args);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(&self, args: HandleRequestsArgs) -> tg::Result<()> {
		let HandleRequestsArgs {
			changed,
			mut control,
			state,
			stopper,
			wait_sender,
			write_sender,
		} = args;

		// Accept each request once and track its handler until it finishes.
		let mut requests = tokio::task::JoinSet::new();
		loop {
			let message = tokio::select! {
				biased;
				() = stopper.wait() => break,
				result = requests.join_next(), if !requests.is_empty() => {
					result.unwrap().map_err(|source| tg::error!(!source, "an indexer request handler panicked"))??;
					continue;
				},
				message = control.recv_without_ack() => message?,
			};
			let Some(message) = message else {
				return Err(tg::error!("the indexer request stream ended"));
			};
			let ServerMessage::Request(request) = message else {
				continue;
			};
			let writing = !matches!(request.arg, RequestArg::Wait);
			{
				let mut state = state.lock().unwrap();
				if writing && !state.available {
					drop(state);
					let error = tg::error!("the indexer is unavailable");
					control
						.sender()
						.try_send_untracked(ClientMessage::Response(Self::response(
							request.id,
							Err(error),
						)));
					continue;
				}
				if writing {
					state.writes += 1;
				}
			}
			control.acknowledge_now(request.id.clone());
			let guard = Guard {
				changed: changed.clone(),
				state: state.clone(),
				writing,
			};
			let wait_sender = wait_sender.clone();
			let write_sender = write_sender.clone();
			let indexer = self.clone();
			let sender = control.sender();
			let args = HandleIndexerRequestArgs {
				guard,
				request,
				sender,
				wait_sender,
				write_sender,
			};
			requests.spawn(async move { indexer.handle_indexer_request(args).await });
		}

		// The queues are drained and indexing has finished when the shutdown task stops reception.
		requests.shutdown().await;
		Ok(())
	}

	async fn handle_indexer_request(&self, args: HandleIndexerRequestArgs) -> tg::Result<()> {
		let HandleIndexerRequestArgs {
			mut guard,
			request,
			sender,
			wait_sender,
			write_sender,
		} = args;
		let result = match request.arg {
			RequestArg::Enqueue(arg) => Self::handle_enqueue_request(&write_sender, arg)
				.await
				.map(|()| ResponseOutput::Enqueue),
			RequestArg::Wait => self
				.wait_for_indexing(&wait_sender, request.id.clone())
				.await
				.map(|()| ResponseOutput::Wait),
		};
		guard.finish_write();
		sender.send_now(ClientMessage::Response(Self::response(request.id, result)));
		Ok(())
	}

	fn response(id: String, result: tg::Result<ResponseOutput>) -> Response {
		match result {
			Ok(output) => Response {
				error: None,
				id,
				output: Some(output),
			},
			Err(error) => Response {
				error: Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				id,
				output: None,
			},
		}
	}

	#[tracing::instrument(name = "indexer.enqueue", level = "debug", skip_all, fields(archive = arg.archive.len(), index = arg.index.len()), err(level = "debug"))]
	async fn handle_enqueue_request(
		write_sender: &queue::write::Sender,
		arg: EnqueueRequestArg,
	) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = queue::write::Request {
			archive: arg.archive,
			index: arg.index,
			sender,
		};
		write_sender
			.send(request)
			.await
			.map_err(|_| tg::error!("the indexer write task stopped"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the indexer write task stopped"))??;

		Ok(())
	}

	async fn publish_client_messages(
		&self,
		receiver: tokio::sync::mpsc::Receiver<ClientMessage>,
	) -> tg::Result<()> {
		tokio_stream::wrappers::ReceiverStream::new(receiver)
			.map(Ok::<_, tg::Error>)
			.try_for_each_concurrent(
				self.server.config.indexer.request.concurrency,
				|message| async move {
					let id = message.id().to_owned();
					if let Err(error) = self
						.server
						.messenger
						.publish(Self::client_subject(&id), message)
						.await
					{
						tracing::error!(%error, "failed to publish an indexer client message");
					}
					Ok(())
				},
			)
			.await?;
		Ok(())
	}

	fn client_subject(id: &str) -> String {
		format!("indexers.client.{id}")
	}

	fn server_subject(id: Option<&tg::indexer::Id>) -> String {
		id.map_or_else(
			|| "indexers.server".to_owned(),
			|id| format!("indexers.{id}.server"),
		)
	}
}

impl ClientMessage {
	fn id(&self) -> &str {
		match self {
			Self::Ack(ack) => &ack.id,
			Self::Response(response) => &response.id,
		}
	}
}

impl Guard {
	fn finish_write(&mut self) {
		if self.writing {
			self.state
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner)
				.writes -= 1;
			self.writing = false;
			self.changed.notify_waiters();
		}
	}
}

impl Drop for Guard {
	fn drop(&mut self) {
		self.finish_write();
	}
}

impl tangram_server_core::control::Input<ClientMessage> for ServerMessage {
	fn kind(&self) -> tangram_server_core::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => tangram_server_core::control::InputKind::Ack { id: &ack.id },
			Self::Request(request) => tangram_server_core::control::InputKind::Message {
				id: Some(&request.id),
			},
		}
	}

	fn create_ack_message(id: String) -> ClientMessage {
		ClientMessage::Ack(Ack { id })
	}
}

impl tangram_server_core::control::Output for ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) => None,
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}

impl Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}
