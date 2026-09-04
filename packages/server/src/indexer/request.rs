use {
	self::state::{State, WaitRequest, WaitRequestState},
	super::{Indexer, RETRY_OPTIONS, queue},
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{collections::VecDeque, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::{Messenger as _, Payload},
	tangram_store::Store as _,
};

mod state;

pub(super) type CommandReceiver = tokio::sync::mpsc::UnboundedReceiver<Command>;
type Operations = futures::stream::FuturesUnordered<futures::future::BoxFuture<'static, Operation>>;

pub(super) struct Inputs {
	pub(super) command_receiver: CommandReceiver,
	pub(super) completion_receiver: queue::CompletionReceiver,
	pub(super) queue_sender: queue::MessageSender,
	pub(super) queues: queue::Queues,
}

pub(super) enum Command {
	Checkpoint {
		sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
	},
	Drain {
		sender: tokio::sync::oneshot::Sender<()>,
	},
	SetQueueRequestsEnabled {
		enabled: bool,
		sender: tokio::sync::oneshot::Sender<()>,
	},
}

enum Event {
	BatchExpiration,
	Checkpoint,
	Command(Command),
	Completion(queue::Completion),
	Message(ServerMessage),
	Operation(Operation),
	Poll,
	Stop,
	TaskWait(Vec<String>),
}

enum Operation {
	Archive {
		id: String,
		result: tg::Result<crate::store::object::archive::queue::Entry>,
		sequence: u64,
	},
	Checkpoint {
		result: tg::Result<()>,
	},
	Index {
		id: String,
		result: tg::Result<crate::store::object::index::queue::Fragment>,
		sequence: u64,
	},
	Reserve {
		reservation: queue::Reservation,
		result: tg::Result<()>,
	},
}

struct Drain {
	abandoned: bool,
	archive_target: u64,
	index_target: u64,
	sender: tokio::sync::oneshot::Sender<()>,
}

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
	pub batch: crate::store::object::index::queue::batch::Id,
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
	Index,
	Wait,
}

impl Session {
	pub(crate) async fn send_indexer_request(
		&self,
		indexer: &tg::indexer::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		self.server.send_indexer_request(indexer, arg).await
	}
}

impl crate::Server {
	pub(crate) async fn send_indexer_request(
		&self,
		indexer: &tg::indexer::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let id = crate::control::id();
		let request = ServerMessage::Request(Request {
			arg,
			id: id.clone(),
		});
		let config = self.config.indexer.request.clone();
		let options = crate::control::Options {
			retry: config.retry.into(),
			timeout: config.timeout,
		};
		self.send_control_request(crate::control::SendControlRequestArg {
			ack: |id| ServerMessage::Ack(Ack { id }),
			client_subject: Indexer::client_subject(&id),
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
		})
		.await
	}
}

impl Indexer {
	pub(super) async fn request_task(
		&self,
		inputs: Inputs,
		poll_interval: std::time::Duration,
		ready: tokio::sync::oneshot::Sender<()>,
		stopper: Stopper,
	) -> tg::Result<()> {
		let messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(Self::server_subject(&self.id))
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to subscribe to the indexer request stream")
			})?
			.map_err(|source| tg::error!(!source, "failed to receive an indexer message"))
			.map_ok(|message| message.payload)
			.boxed();
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let mut options = crate::control::stream_options();
		options.outbox_ttl = Some(std::time::Duration::from_mins(1));
		let control = crate::control::Stream::new(messages, sender, options);
		ready
			.send(())
			.map_err(|()| tg::error!("failed to signal indexer request readiness"))?;
		let requests = self.handle_requests(control, inputs, poll_interval, stopper);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(
		&self,
		mut control: crate::control::Stream<ServerMessage, ClientMessage>,
		inputs: Inputs,
		poll_interval: std::time::Duration,
		stopper: Stopper,
	) -> tg::Result<()> {
		let Inputs {
			mut command_receiver,
			mut completion_receiver,
			queue_sender,
			queues,
		} = inputs;
		let mut checkpoint_interval =
			tokio::time::interval(self.server.config.object.queue_checkpoint_interval);
		checkpoint_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut poll_interval = tokio::time::interval(poll_interval);
		poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut drain = None;
		let mut checkpoint_pending = false;
		let mut operations = Operations::new();
		let mut pending_requests = VecDeque::new();
		let mut queue_requests_enabled = false;
		let mut state = State::new(queues);
		loop {
			let batch_deadline = state.queues.next_batch_deadline();
			let batch_expiration = async move {
				if let Some(deadline) = batch_deadline {
					tokio::time::sleep_until(deadline).await;
				} else {
					future::pending::<()>().await;
				}
			};
			tokio::pin!(batch_expiration);
			let event = tokio::select! {
				biased;
				command = command_receiver.recv() => {
					let command = command.ok_or_else(|| tg::error!("the indexer command stream ended"))?;
					Event::Command(command)
				},
				() = &mut batch_expiration => Event::BatchExpiration,
				_ = checkpoint_interval.tick() => Event::Checkpoint,
				completion = completion_receiver.recv() => {
					let completion = completion.ok_or_else(|| tg::error!("the object queue task stopped"))?;
					Event::Completion(completion)
				},
				task_wait = state.task_waits.next(), if !state.task_waits.is_empty() => {
					Event::TaskWait(task_wait.unwrap())
				},
				operation = operations.next(), if !operations.is_empty() => {
					Event::Operation(operation.unwrap())
				},
				message = control.recv_with_ack() => {
					let message = message?
						.ok_or_else(|| tg::error!("the indexer request stream ended"))?;
					Event::Message(message)
				},
				_ = poll_interval.tick(), if state.needs_poll() => Event::Poll,
				() = stopper.wait() => Event::Stop,
			};
			match event {
				Event::BatchExpiration => {
					let timeout = self.server.config.object.index_queue.batch_timeout;
					let actions = state.queues.expire_index_batches(timeout);
					Self::dispatch_actions(actions, &queue_sender, &control.sender())?;
				},
				Event::Checkpoint => {
					if !checkpoint_pending {
						let (archive_read_sequence, index_read_sequence) =
							state.queues.read_sequences();
						let indexer = self.clone();
						operations.push(
							async move {
								let result = indexer
									.checkpoint_read_sequences(
										archive_read_sequence,
										index_read_sequence,
									)
									.await;

								Operation::Checkpoint { result }
							}
							.boxed(),
						);
						checkpoint_pending = true;
					}
				},
				Event::Command(Command::Checkpoint { sender }) => {
					let result = state.queues.checkpoint(self).await;
					sender.send(result).ok();
				},
				Event::Command(Command::Drain { sender }) => {
					queue_requests_enabled = false;
					let (archive_target, index_target) = state.queues.targets();
					drain = Some(Drain {
						abandoned: false,
						archive_target,
						index_target,
						sender,
					});
				},
				Event::Command(Command::SetQueueRequestsEnabled { enabled, sender }) => {
					queue_requests_enabled = enabled;
					sender.send(()).ok();
				},
				Event::Completion(completion) => state.queues.complete(completion),
				Event::Message(ServerMessage::Ack(_)) => unreachable!(),
				Event::Message(ServerMessage::Request(request)) => match request.arg {
					RequestArg::Archive(arg) => {
						if queue_requests_enabled {
							let request = self.start_archive(
								arg,
								request.id,
								&mut state,
								&mut operations,
								&control.sender(),
							);
							if let Some(request) = request {
								pending_requests.push_back(request);
							}
						} else {
							let error = tg::error!("the indexer is unavailable");
							State::send_response(request.id, Err(error), &control.sender());
						}
					},
					RequestArg::Index(arg) => {
						if queue_requests_enabled {
							let request = self.start_index(
								arg,
								request.id,
								&mut state,
								&mut operations,
								&control.sender(),
							);
							if let Some(request) = request {
								pending_requests.push_back(request);
							}
						} else {
							let error = tg::error!("the indexer is unavailable");
							State::send_response(request.id, Err(error), &control.sender());
						}
					},
					RequestArg::Wait => {
						let id = request.id.clone();
						let wait = WaitRequest {
							state: WaitRequestState::Tasks,
						};
						state.waits.insert(request.id, wait);
						crate::checkpoint!(self.server, "indexer.request.receive", request = id,)
							.await;
						state.start_task_wait(&self.server);
					},
				},
				Event::Operation(Operation::Archive {
					id,
					result,
					sequence,
				}) => match result {
					Ok(entry) => {
						queue_sender
							.send(queue::Message::Archive(entry))
							.map_err(|_| tg::error!("the object queue task stopped"))?;
						State::send_response(id, Ok(ResponseOutput::Archive), &control.sender());
					},
					Err(error) => {
						queue_sender
							.send(queue::Message::DeleteArchive(sequence))
							.map_err(|_| tg::error!("the object queue task stopped"))?;
						State::send_response(id, Err(error), &control.sender());
					},
				},
				Event::Operation(Operation::Checkpoint { result }) => {
					checkpoint_pending = false;
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), "failed to checkpoint the object queues");
					}
				},
				Event::Operation(Operation::Index {
					id,
					result,
					sequence,
				}) => match result {
					Ok(fragment) => {
						let timeout = self.server.config.object.index_queue.batch_timeout;
						let actions =
							state
								.queues
								.insert_index_fragment(fragment, Some(id), timeout);
						Self::dispatch_actions(actions, &queue_sender, &control.sender())?;
					},
					Err(error) => {
						queue_sender
							.send(queue::Message::DeleteIndex(vec![sequence]))
							.map_err(|_| tg::error!("the object queue task stopped"))?;
						State::send_response(id, Err(error), &control.sender());
					},
				},
				Event::Operation(Operation::Reserve {
					reservation,
					result,
				}) => {
					match result {
						Ok(()) => state.queues.finish_reservation(reservation),
						Err(error) => {
							state.queues.cancel_reservation(reservation);
							tracing::error!(error = %error.trace(), "failed to reserve object queue sequences");
						},
					}
					self.start_pending_requests(
						&mut pending_requests,
						&mut state,
						&mut operations,
						&control.sender(),
					);
				},
				Event::Poll => {
					let sender = control.sender();
					if let Err(error) = state.poll(self, &sender).await {
						state.fail(&error, &sender);
					}
				},
				Event::Stop => break,
				Event::TaskWait(ids) => {
					state.handle_task_wait(ids);
					state.start_task_wait(&self.server);
				},
			}
			Self::progress_drain(
				&mut drain,
				operations.is_empty() && pending_requests.is_empty(),
				&mut state,
				&queue_sender,
				&control.sender(),
			)?;
		}

		Ok(())
	}

	fn start_archive(
		&self,
		arg: ArchiveRequestArg,
		id: String,
		state: &mut State,
		operations: &mut Operations,
		response_sender: &crate::control::Sender<ServerMessage, ClientMessage>,
	) -> Option<Request> {
		let Some(sequence) = state.queues.try_allocate_sequence(queue::Kind::Archive) else {
			if let Err(error) = self.start_reservation(state, queue::Kind::Archive, operations) {
				State::send_response(id, Err(error), response_sender);

				return None;
			}
			let arg = RequestArg::Archive(arg);
			let request = Request { arg, id };

			return Some(request);
		};
		if let Err(error) = self.start_reservation(state, queue::Kind::Archive, operations) {
			tracing::error!(error = %error.trace(), "failed to start an archive queue reservation");
		}
		let entry = crate::store::object::archive::queue::Entry {
			indexer: self.id.clone(),
			object: arg.object,
			put: arg.put,
			sequence,
		};
		let server = self.server.clone();
		operations.push(
			async move {
				let arg = crate::store::object::archive::queue::put::Arg {
					entry: entry.clone(),
				};
				let result = server
					.store
					.put_object_archive_queue_entry(arg)
					.await
					.map(|()| entry)
					.map_err(|error| tg::error!(!error, "failed to put an archive queue entry"));

				Operation::Archive {
					id,
					result,
					sequence,
				}
			}
			.boxed(),
		);

		None
	}

	fn start_index(
		&self,
		arg: IndexRequestArg,
		id: String,
		state: &mut State,
		operations: &mut Operations,
		response_sender: &crate::control::Sender<ServerMessage, ClientMessage>,
	) -> Option<Request> {
		if arg.fragments == 0 || arg.fragment >= arg.fragments {
			let fragment = arg.fragment;
			let fragments = arg.fragments;
			let error = tg::error!(%fragment, %fragments, "invalid index queue fragment");
			State::send_response(id, Err(error), response_sender);

			return None;
		}
		let Some(sequence) = state.queues.try_allocate_sequence(queue::Kind::Index) else {
			if let Err(error) = self.start_reservation(state, queue::Kind::Index, operations) {
				State::send_response(id, Err(error), response_sender);

				return None;
			}
			let arg = RequestArg::Index(arg);
			let request = Request { arg, id };

			return Some(request);
		};
		if let Err(error) = self.start_reservation(state, queue::Kind::Index, operations) {
			tracing::error!(error = %error.trace(), "failed to start an index queue reservation");
		}
		let fragment = crate::store::object::index::queue::Fragment {
			batch: arg.batch,
			fragment: arg.fragment,
			fragments: arg.fragments,
			indexer: self.id.clone(),
			payload: arg.payload,
			sequence,
		};
		let server = self.server.clone();
		operations.push(
			async move {
				let arg = crate::store::object::index::queue::put::Arg {
					fragment: fragment.clone(),
				};
				let result = server
					.store
					.put_object_index_queue_fragment(arg)
					.await
					.map(|()| fragment)
					.map_err(|error| tg::error!(!error, "failed to put an index queue fragment"));

				Operation::Index {
					id,
					result,
					sequence,
				}
			}
			.boxed(),
		);

		None
	}

	fn start_reservation(
		&self,
		state: &mut State,
		kind: queue::Kind,
		operations: &mut Operations,
	) -> tg::Result<()> {
		let Some(reservation) = state.queues.start_reservation(self, kind)? else {
			return Ok(());
		};
		let indexer = self.clone();
		operations.push(
			async move {
				let result = indexer.persist_reservation_with_retry(reservation).await;

				Operation::Reserve {
					reservation,
					result,
				}
			}
			.boxed(),
		);

		Ok(())
	}

	fn start_pending_requests(
		&self,
		requests: &mut VecDeque<Request>,
		state: &mut State,
		operations: &mut Operations,
		response_sender: &crate::control::Sender<ServerMessage, ClientMessage>,
	) {
		let len = requests.len();
		for _ in 0..len {
			let request = requests.pop_front().unwrap();
			let request = match request.arg {
				RequestArg::Archive(arg) => {
					self.start_archive(arg, request.id, state, operations, response_sender)
				},
				RequestArg::Index(arg) => {
					self.start_index(arg, request.id, state, operations, response_sender)
				},
				RequestArg::Wait => unreachable!(),
			};
			let Some(request) = request else {
				continue;
			};
			requests.push_back(request);
		}
	}

	fn dispatch_actions(
		actions: queue::Actions,
		queue_sender: &queue::MessageSender,
		response_sender: &crate::control::Sender<ServerMessage, ClientMessage>,
	) -> tg::Result<()> {
		for message in actions.messages {
			queue_sender
				.send(message)
				.map_err(|_| tg::error!("the object queue task stopped"))?;
		}
		for (id, result) in actions.responses {
			let result = result.map(|()| ResponseOutput::Index);
			State::send_response(id, result, response_sender);
		}

		Ok(())
	}

	fn progress_drain(
		drain: &mut Option<Drain>,
		enqueues_finished: bool,
		state: &mut State,
		queue_sender: &queue::MessageSender,
		response_sender: &crate::control::Sender<ServerMessage, ClientMessage>,
	) -> tg::Result<()> {
		let Some(current) = drain.as_mut() else {
			return Ok(());
		};
		if enqueues_finished && !current.abandoned {
			let actions = state.queues.abandon_incomplete_batches();
			Self::dispatch_actions(actions, queue_sender, response_sender)?;
			current.abandoned = true;
		}
		if !current.abandoned
			|| !state
				.queues
				.drained(current.archive_target, current.index_target)
		{
			return Ok(());
		}
		let current = drain.take().unwrap();
		current.sender.send(()).ok();

		Ok(())
	}

	async fn publish_client_messages(
		&self,
		mut receiver: tokio::sync::mpsc::Receiver<ClientMessage>,
	) -> tg::Result<()> {
		while let Some(message) = receiver.recv().await {
			let id = message.id().to_owned();
			let server = self.server.clone();
			tokio::spawn(async move {
				let result = server
					.messenger
					.publish(Self::client_subject(&id), message)
					.await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish an indexer client message");
				}
			});
		}

		Ok(())
	}

	pub(super) async fn set_queue_requests_enabled(&self, enabled: bool) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let command = Command::SetQueueRequestsEnabled { enabled, sender };
		self.command_sender
			.send(command)
			.map_err(|_| tg::error!("the indexer request task stopped"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the indexer request task stopped"))?;

		Ok(())
	}

	pub(super) async fn drain_queues(&self) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let command = Command::Drain { sender };
		self.command_sender
			.send(command)
			.map_err(|_| tg::error!("the indexer request task stopped"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the indexer request task stopped"))?;

		Ok(())
	}

	pub(super) async fn checkpoint_queues_with_retry(&self) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let (sender, receiver) = tokio::sync::oneshot::channel();
			let command = Command::Checkpoint { sender };
			let result = match self.command_sender.send(command) {
				Ok(()) => receiver
					.await
					.map_err(|_| tg::error!("the indexer request task stopped"))?,
				Err(_) => Err(tg::error!("the indexer request task stopped")),
			};
			match result {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to checkpoint the object queues");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	fn client_subject(id: &str) -> String {
		format!("indexers.client.{id}")
	}

	fn server_subject(id: &tg::indexer::Id) -> String {
		format!("indexers.{id}.server")
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
