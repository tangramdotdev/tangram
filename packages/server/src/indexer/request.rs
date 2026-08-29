use {
	self::state::{IndexRequest, IndexRequestState, State},
	super::Indexer,
	crate::Session,
	futures::{StreamExt as _, TryStreamExt as _, future},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
};

mod state;

enum Event {
	Barrier(Vec<String>),
	Message(ServerMessage),
	Poll,
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
	Index,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct Response {
	error: Option<tg::error::Data>,
	id: String,
	output: Option<ResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ResponseOutput {
	Index,
}

impl Session {
	pub(crate) async fn send_indexer_request(
		&self,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let id = crate::control::id();
		let request = ServerMessage::Request(Request {
			arg,
			id: id.clone(),
		});
		let options = self.indexer_message_options();
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
			server_subject: Indexer::server_subject(),
		})
		.await
	}

	fn indexer_message_options(&self) -> crate::control::Options {
		let config = self.server.config.indexer.clone();
		crate::control::Options {
			retry: config.message_retry.into(),
			timeout: config.message_timeout,
		}
	}
}

impl Indexer {
	pub(super) async fn request_task(&self, poll_interval: Duration) -> tg::Result<()> {
		loop {
			let result = self.request_task_inner(poll_interval).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the indexer request task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn request_task_inner(&self, poll_interval: Duration) -> tg::Result<()> {
		let messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(Self::server_subject())
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to subscribe to the indexer request stream")
			})?
			.map_err(|source| tg::error!(!source, "failed to receive an indexer message"))
			.map_ok(|message| message.payload)
			.boxed();
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let control =
			crate::control::Stream::new(messages, sender, crate::control::stream_options());
		let requests = self.handle_requests(control, poll_interval);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(
		&self,
		mut control: crate::control::Stream<ServerMessage, ClientMessage>,
		poll_interval: Duration,
	) -> tg::Result<()> {
		let mut interval = tokio::time::interval(poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut state = State::new();
		loop {
			let event = tokio::select! {
				barrier = state.barriers.next(), if !state.barriers.is_empty() => {
					Event::Barrier(barrier.unwrap())
				},
				message = control.recv_with_ack() => {
					let message = message?
						.ok_or_else(|| tg::error!("the indexer request stream ended"))?;
					Event::Message(message)
				},
				_ = interval.tick(), if state.needs_poll() => Event::Poll,
			};
			match event {
				Event::Barrier(ids) => {
					state.handle_barrier(ids, !self.server.config.advanced.single_process);
					state.start_barrier(&self.server);
				},
				Event::Message(ServerMessage::Ack(_)) => unreachable!(),
				Event::Message(ServerMessage::Request(request)) => match request.arg {
					RequestArg::Index => {
						let id = request.id.clone();
						let entry = IndexRequest {
							state: IndexRequestState::Tasks,
						};
						state.requests.insert(request.id, entry);
						crate::checkpoint!(self.server, "indexer.request.receive", request = id,)
							.await;
						state.start_barrier(&self.server);
					},
				},
				Event::Poll => {
					let sender = control.sender();
					if let Err(error) = state.poll(&self.server, &sender).await {
						state.fail(&error, &sender);
					}
				},
			}
		}
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

		Err(tg::error!("the indexer client message stream ended"))
	}

	fn client_subject(id: &str) -> String {
		format!("indexer.client.{id}")
	}

	fn server_subject() -> String {
		"indexer.server".to_owned()
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
