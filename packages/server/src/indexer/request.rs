use {
	super::{Indexer, State, queue, wait},
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::{Messenger as _, Payload},
	tangram_store::Store as _,
};

pub(super) mod limits;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
enum ClientMessage {
	Ack(Ack),
	Response(Response),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
enum ServerMessage {
	Ack(Ack),
	Request(Request),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Ack {
	id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Request {
	arg: RequestArg,
	id: String,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum RequestArg {
	Archive(ArchiveRequestArg),
	Index(IndexRequestArg),
	Wait,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ArchiveRequestArg {
	pub object: tg::object::Id,
	pub put: [u8; 16],
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct IndexRequestArg {
	pub batch: crate::store::index::queue::batch::Id,
	pub fragment: u64,
	pub fragments: u64,
	#[serde(with = "bytes_base64")]
	pub payload: bytes::Bytes,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Response {
	error: Option<tg::error::Data>,
	id: String,
	output: Option<ResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResponseOutput {
	Archive,
	Busy,
	Index,
	Wait,
}

pub(super) struct RequestTaskArgs {
	pub archive_sender: queue::ArchiveMessageSender,
	pub changed: Arc<tokio::sync::Notify>,
	pub index_sender: queue::IndexMessageSender,
	pub ready: tokio::sync::oneshot::Sender<()>,
	pub state: Arc<Mutex<State>>,
	pub stopper: Stopper,
	pub wait_sender: wait::Sender,
}

struct HandleIndexerRequestArgs {
	archive_sender: queue::ArchiveMessageSender,
	guard: Guard,
	index_sender: queue::IndexMessageSender,
	request: Request,
	sender: crate::control::Sender<ServerMessage, ClientMessage>,
	wait_sender: wait::Sender,
}

struct HandleRequestsArgs {
	archive_sender: queue::ArchiveMessageSender,
	changed: Arc<tokio::sync::Notify>,
	control: crate::control::Stream<ServerMessage, ClientMessage>,
	index_sender: queue::IndexMessageSender,
	state: Arc<Mutex<State>>,
	stopper: Stopper,
	wait_sender: wait::Sender,
}

struct Guard {
	changed: Arc<tokio::sync::Notify>,
	id: String,
	state: Arc<Mutex<State>>,
	writing: bool,
}

impl Session {
	pub(crate) async fn send_indexer_request(
		&self,
		indexer: Option<&tg::indexer::Id>,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		self.server.send_indexer_request(indexer, arg).await
	}
}

impl crate::Server {
	pub(crate) async fn send_indexer_request(
		&self,
		indexer: Option<&tg::indexer::Id>,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		// Reuse the ID so a retry does not replace an active wait with a later barrier.
		let id = crate::control::id();
		let retry = self.config.indexer.request.retry.clone().into();
		let timeout =
			matches!(arg, RequestArg::Wait).then_some(self.config.indexer.request.response_ttl);
		tangram_futures::retry(&retry, || {
			let arg = arg.clone();
			let id = &id;
			async move {
				let request = self.send_indexer_request_inner(indexer, id, arg);
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
				if matches!(response, Ok(ResponseOutput::Busy)) {
					return Ok(ControlFlow::Continue(tg::error!("the indexer is busy")));
				}
				Ok(ControlFlow::Break(response))
			}
		})
		.await
	}

	async fn send_indexer_request_inner(
		&self,
		indexer: Option<&tg::indexer::Id>,
		id: &str,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let request = ServerMessage::Request(Request {
			arg,
			id: id.to_owned(),
		});
		let config = self.config.indexer.request.clone();
		let options = crate::control::Options {
			retry: config.retry.into(),
			timeout: config.timeout,
		};
		let arg = crate::control::SendControlRequestArg {
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
		self.send_control_request(arg).await
	}
}

impl Indexer {
	pub(super) async fn request_task(&self, args: RequestTaskArgs) -> tg::Result<()> {
		let RequestTaskArgs {
			archive_sender,
			changed,
			index_sender,
			ready,
			state,
			stopper,
			wait_sender,
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
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let mut options = crate::control::stream_options();
		options.outbox_ttl = Some(self.server.config.indexer.request.response_ttl);
		let control = crate::control::Stream::new(messages, sender, options);
		ready
			.send(())
			.map_err(|()| tg::error!("failed to signal indexer request readiness"))?;
		let args = HandleRequestsArgs {
			archive_sender,
			changed,
			control,
			index_sender,
			state,
			stopper,
			wait_sender,
		};
		let requests = self.handle_requests(args);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(&self, args: HandleRequestsArgs) -> tg::Result<()> {
		let HandleRequestsArgs {
			archive_sender,
			changed,
			mut control,
			index_sender,
			state,
			stopper,
			wait_sender,
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
			let accepted = {
				let mut state = state.lock().unwrap();
				if !state.available && !matches!(request.arg, RequestArg::Wait) {
					Err(tg::error!("the indexer is unavailable"))
				} else {
					let accepted = state
						.limits
						.try_insert(&request, &self.server.config.indexer.request);
					if matches!(accepted, Ok(true)) && !matches!(request.arg, RequestArg::Wait) {
						state.writes += 1;
					}
					accepted
				}
			};
			if !matches!(accepted, Ok(true)) {
				let result = accepted.map(|_| ResponseOutput::Busy);
				control
					.sender()
					.try_send_untracked(ClientMessage::Response(Self::response(
						request.id, result,
					)));
				continue;
			}
			control.acknowledge_now(request.id.clone());
			let guard = Guard {
				changed: changed.clone(),
				id: request.id.clone(),
				state: state.clone(),
				writing: !matches!(request.arg, RequestArg::Wait),
			};
			let wait_sender = wait_sender.clone();
			let archive_sender = archive_sender.clone();
			let index_sender = index_sender.clone();
			let indexer = self.clone();
			let sender = control.sender();
			let args = HandleIndexerRequestArgs {
				archive_sender,
				guard,
				index_sender,
				request,
				sender,
				wait_sender,
			};
			requests.spawn(async move { indexer.handle_indexer_request(args).await });
		}

		// The queues are drained and indexing has finished when the shutdown task stops reception.
		requests.shutdown().await;
		Ok(())
	}

	async fn handle_indexer_request(&self, args: HandleIndexerRequestArgs) -> tg::Result<()> {
		let HandleIndexerRequestArgs {
			archive_sender,
			mut guard,
			index_sender,
			request,
			sender,
			wait_sender,
		} = args;
		let result = match request.arg {
			RequestArg::Archive(arg) => self
				.handle_archive_request(&guard.state, &guard.changed, &archive_sender, arg)
				.await
				.map(|()| ResponseOutput::Archive),
			RequestArg::Index(arg) => {
				let result = self
					.handle_index_request(&guard.state, &guard.changed, &index_sender, arg)
					.await;
				guard.finish_write();
				match result {
					Ok(receiver) => receiver
						.await
						.map_err(|_| tg::error!("the index batch response channel closed"))?
						.map(|()| ResponseOutput::Index),
					Err(error) => Err(error),
				}
			},
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

	async fn handle_archive_request(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		archive_sender: &queue::ArchiveMessageSender,
		arg: ArchiveRequestArg,
	) -> tg::Result<()> {
		let sequence = Self::allocate_sequence(state, changed, queue::Kind::Archive).await?;
		let entry = crate::store::archive::queue::Entry {
			indexer: self.id().clone(),
			object: arg.object,
			put: arg.put,
			sequence,
		};
		let arg = crate::store::archive::queue::put::Arg {
			entry: entry.clone(),
		};
		let result = self
			.server
			.store
			.put_archive_queue_entry(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put an archive queue entry"));
		let message = if result.is_ok() {
			queue::ArchiveMessage::Process(entry)
		} else {
			queue::ArchiveMessage::Delete(sequence)
		};
		archive_sender
			.send(message)
			.await
			.map_err(|_| tg::error!("the archive queue task stopped"))?;
		result?;
		Ok(())
	}

	async fn handle_index_request(
		&self,
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		index_sender: &queue::IndexMessageSender,
		arg: IndexRequestArg,
	) -> tg::Result<tokio::sync::oneshot::Receiver<tg::Result<()>>> {
		let sequence = Self::allocate_sequence(state, changed, queue::Kind::Index).await?;
		let fragment = crate::store::index::queue::Fragment {
			batch: arg.batch,
			fragment: arg.fragment,
			fragments: arg.fragments,
			indexer: self.id().clone(),
			payload: arg.payload,
			sequence,
		};
		let arg = crate::store::index::queue::put::Arg {
			fragment: fragment.clone(),
		};
		if let Err(source) = self.server.store.put_index_queue_fragment(arg).await {
			index_sender
				.send(queue::IndexMessage::Delete(vec![sequence]))
				.await
				.map_err(|_| tg::error!("the index queue task stopped"))?;
			return Err(tg::error!(!source, "failed to put an index queue fragment"));
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let output = {
			let mut state = state.lock().unwrap();
			state.queues.insert_index_fragment(
				fragment,
				Some(sender),
				self.server.config.object.index_queue.batch_timeout,
			)
		};
		changed.notify_waiters();
		for message in output.messages {
			index_sender
				.send(message)
				.await
				.map_err(|_| tg::error!("the index queue task stopped"))?;
		}
		for (sender, result) in output.responses {
			sender.send(result).ok();
		}
		Ok(receiver)
	}

	async fn allocate_sequence(
		state: &Mutex<State>,
		changed: &tokio::sync::Notify,
		kind: queue::Kind,
	) -> tg::Result<u64> {
		loop {
			let notified = changed.notified();
			let sequence = state.lock().unwrap().queues.try_allocate_sequence(kind);
			if let Some(sequence) = sequence {
				changed.notify_waiters();
				return Ok(sequence);
			}
			notified.await;
		}
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
		let mut state = self
			.state
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		state.limits.remove(&self.id);
		drop(state);
		self.changed.notify_waiters();
	}
}

impl crate::control::Input<ClientMessage> for ServerMessage {
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
		}
	}

	fn create_ack_message(id: String) -> ClientMessage {
		ClientMessage::Ack(Ack { id })
	}
}

impl crate::control::Output for ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) => None,
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}
}

impl Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}
}

mod bytes_base64 {
	use serde::{Deserialize as _, Deserializer, Serializer};

	pub fn deserialize<'de, D>(deserializer: D) -> Result<bytes::Bytes, D::Error>
	where
		D: Deserializer<'de>,
	{
		let value = String::deserialize(deserializer)?;
		let bytes = data_encoding::BASE64
			.decode(value.as_bytes())
			.map_err(serde::de::Error::custom)?;

		Ok(bytes.into())
	}

	pub fn serialize<S>(value: &bytes::Bytes, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(&data_encoding::BASE64.encode(value))
	}
}
