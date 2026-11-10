use {
	crate::{Component, Scheduler},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{
		ops::ControlFlow,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
	tangram_server_core::Session,
	tangram_server_scheduler_core::{RequestArg, ResponseOutput},
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ClientMessage {
	Ack(Ack),
	Response(Response),
	Unavailable(Ack),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ServerMessage {
	Ack(Ack),
	Request(Request),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Ack {
	id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Request {
	pub arg: RequestArg,
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Response {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	error: Option<tg::error::Data>,

	id: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	output: Option<ResponseOutput>,
}

pub(crate) struct RequestTaskArgs {
	pub available: Arc<AtomicBool>,
	pub control: tangram_server_core::control::Stream<ServerMessage, ClientMessage>,
	pub receiver: tokio::sync::mpsc::Receiver<ClientMessage>,
	pub request_sender: tokio::sync::mpsc::Sender<Request>,
}

struct HandleRequestsArgs {
	available: Arc<AtomicBool>,
	control: tangram_server_core::control::Stream<ServerMessage, ClientMessage>,
	request_sender: tokio::sync::mpsc::Sender<Request>,
}

impl Component {
	pub(crate) async fn send_scheduler_request(
		&self,
		session: &Session,
		arg: RequestArg,
	) -> tg::Result<(tg::scheduler::Id, tg::Result<ResponseOutput>)> {
		// Send the request to one scheduler, and retry it with another scheduler on failure.
		let server = &session.server;
		let start = rand::random::<u64>();
		let mut attempt = 0u64;
		let retry =
			tangram_futures::retry::Options::from(server.config.scheduler.request.retry.clone());
		tangram_futures::retry(&retry, || {
			let selection = start.wrapping_add(attempt);
			let refresh = attempt > 0;
			attempt = attempt.wrapping_add(1);
			let arg = arg.clone();
			async move {
				let scheduler = match self.select_scheduler(server, selection, refresh).await {
					Ok(scheduler) => scheduler,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				match self
					.send_scheduler_request_inner(session, &scheduler, arg)
					.await
				{
					Ok(Some(response)) => Ok(ControlFlow::Break((scheduler, response))),
					Ok(None) => {
						let error = tg::error!(%scheduler, "the scheduler is unavailable");
						Ok(ControlFlow::Continue(error))
					},
					Err(error) => Ok(ControlFlow::Continue(error)),
				}
			}
		})
		.await
	}

	pub(crate) async fn send_scheduler_request_to(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		// Send the request to the scheduler, and retry it while the scheduler is unavailable.
		let retry = tangram_futures::retry::Options::from(
			session.server.config.scheduler.request.retry.clone(),
		);
		tangram_futures::retry(&retry, || {
			let arg = arg.clone();
			async move {
				let response = self
					.send_scheduler_request_inner(session, scheduler, arg)
					.await?;
				let Some(response) = response else {
					let error = tg::error!(%scheduler, "the scheduler is unavailable");
					return Ok(ControlFlow::Continue(error));
				};
				Ok(ControlFlow::Break(response))
			}
		})
		.await
	}

	/// Send a request to a scheduler, returning `None` if the scheduler is unavailable.
	async fn send_scheduler_request_inner(
		&self,
		session: &Session,
		scheduler: &tg::scheduler::Id,
		arg: RequestArg,
	) -> tg::Result<Option<tg::Result<ResponseOutput>>> {
		let id = tangram_server_core::control::id();
		let request = ServerMessage::Request(Request {
			arg,
			id: id.clone(),
		});
		let config = session.server.config.scheduler.request.clone();
		let options = tangram_server_core::control::Options {
			retry: config.retry.into(),
			timeout: config.timeout,
		};
		let arg = tangram_server_core::control::SendControlRequestArg {
			ack: |id| ServerMessage::Ack(Ack { id }),
			client_subject: Scheduler::client_subject(&id),
			hold: None,
			is_ack: |message: &ClientMessage| matches!(message, ClientMessage::Ack(_)),
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: ClientMessage| match message {
				ClientMessage::Ack(_) => Ok(None),
				ClientMessage::Response(message) => {
					if let Some(error) = message.error {
						let error = tg::Error::try_from(error).map_err(|source| {
							tg::error!(!source, "failed to deserialize the scheduler error")
						})?;
						return Ok(Some((message.id, Some(Err(error)))));
					}
					let Some(output) = message.output else {
						return Err(tg::error!("missing scheduler response output"));
					};
					Ok(Some((message.id, Some(Ok(output)))))
				},
				ClientMessage::Unavailable(message) => Ok(Some((message.id, None))),
			},
			server_subject: Scheduler::server_subject(scheduler),
		};
		let output = session.server.request_control(arg).boxed().await?;

		Ok(output)
	}
}

impl Scheduler {
	pub(crate) async fn request_task(&self, args: RequestTaskArgs) -> tg::Result<()> {
		let RequestTaskArgs {
			available,
			control,
			receiver,
			request_sender,
		} = args;
		let args = HandleRequestsArgs {
			available,
			control,
			request_sender,
		};
		let requests = self.handle_requests(args);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(&self, args: HandleRequestsArgs) -> tg::Result<()> {
		let HandleRequestsArgs {
			available,
			mut control,
			request_sender,
		} = args;

		// Accept each request once and send it to the scheduling task, which bounds the requests that wait for it.
		loop {
			let message = control.recv_without_ack().await?;
			let Some(message) = message else {
				return Err(tg::error!("the scheduler request stream ended"));
			};
			let ServerMessage::Request(request) = message else {
				continue;
			};
			if !available.load(Ordering::SeqCst) {
				let message = ClientMessage::Unavailable(Ack { id: request.id });
				control.sender().try_send_untracked(message);
				continue;
			}
			control.acknowledge_now(request.id.clone());
			request_sender
				.send(request)
				.await
				.map_err(|_| tg::error!("the scheduling task stopped"))?;
		}
	}

	async fn publish_client_messages(
		&self,
		receiver: tokio::sync::mpsc::Receiver<ClientMessage>,
	) -> tg::Result<()> {
		tokio_stream::wrappers::ReceiverStream::new(receiver)
			.map(Ok::<_, tg::Error>)
			.try_for_each_concurrent(
				self.server.config.scheduler.request.concurrency,
				|message| async move {
					let id = message.id().to_owned();
					if let Err(error) = self
						.server
						.messenger
						.publish(Self::client_subject(&id), message)
						.await
					{
						tracing::error!(%error, "failed to publish a scheduler client message");
					}
					Ok(())
				},
			)
			.await?;
		Ok(())
	}

	pub(crate) fn response(id: String, result: tg::Result<ResponseOutput>) -> Response {
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

	pub(crate) fn client_subject(id: &str) -> String {
		format!("schedulers.client.{id}")
	}

	pub(crate) fn server_subject(id: &tg::scheduler::Id) -> String {
		format!("schedulers.{id}.server")
	}
}

impl ClientMessage {
	fn id(&self) -> &str {
		match self {
			Self::Ack(ack) | Self::Unavailable(ack) => &ack.id,
			Self::Response(response) => &response.id,
		}
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
			Self::Ack(_) | Self::Unavailable(_) => None,
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}

impl Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		serde_json::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}
