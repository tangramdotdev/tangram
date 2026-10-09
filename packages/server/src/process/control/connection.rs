use {
	futures::{StreamExt as _, stream::BoxStream},
	tangram_client::{
		prelude::*,
		process::control::{
			ClientMessage, ClientRequest, ClientRequestArg, ClientResponse, ClientResponseOutput,
			Priority, ServerAck, ServerMessage,
		},
	},
	tangram_futures::task::Task,
	tokio::sync::{mpsc, oneshot},
};

mod task;

pub(crate) struct Connection {
	receiver: mpsc::Receiver<tg::Result<tg::control::Event<ClientMessage>>>,
	sender: Sender,
	task: Task<()>,
}

#[derive(Clone)]
pub(crate) struct Sender {
	sender: mpsc::Sender<task::Message>,
	updates: mpsc::UnboundedSender<task::Update>,
}

impl Connection {
	#[must_use]
	pub(crate) fn new(
		input: BoxStream<'static, tg::Result<ClientMessage>>,
		high: mpsc::Sender<ServerMessage>,
		low: mpsc::Sender<ServerMessage>,
	) -> Self {
		Self::new_reconnecting(
			input
				.map(|message| message.map(tg::control::Event::Message))
				.boxed(),
			high,
			low,
		)
	}

	#[must_use]
	pub(crate) fn new_reconnecting(
		input: BoxStream<'static, tg::Result<tg::control::Event<ClientMessage>>>,
		high: mpsc::Sender<ServerMessage>,
		low: mpsc::Sender<ServerMessage>,
	) -> Self {
		let (sender, messages) = mpsc::channel(256);
		let (updates, update_receiver) = mpsc::unbounded_channel();
		let (events, receiver) = mpsc::channel(64);
		let state = task::State::new(input, high, low, events);
		let task = Task::spawn(move |_| async move {
			state.run(messages, update_receiver).await;
		});
		Self {
			receiver,
			sender: Sender { sender, updates },
			task,
		}
	}

	pub(crate) async fn recv_without_ack(&mut self) -> tg::Result<Option<ClientMessage>> {
		loop {
			match self.receiver.recv().await.transpose()? {
				Some(tg::control::Event::Message(message)) => return Ok(Some(message)),
				Some(tg::control::Event::Reconnect) => {},
				None => return Ok(None),
			}
		}
	}

	pub(crate) fn acknowledge_now(&mut self, id: String) {
		self.sender
			.updates
			.send(task::Update::Acknowledge {
				id,
				priority: Priority::High,
			})
			.ok();
	}

	pub(crate) async fn acknowledge_with_priority(
		&mut self,
		id: String,
		priority: Priority,
	) -> tg::Result<()> {
		self.sender
			.updates
			.send(task::Update::Acknowledge { id, priority })
			.map_err(|_| tg::error!("the process control connection closed"))?;
		Ok(())
	}

	#[must_use]
	pub(crate) fn sender(&self) -> Sender {
		self.sender.clone()
	}
}

impl Drop for Connection {
	fn drop(&mut self) {
		self.task.abort();
	}
}

impl Sender {
	pub(crate) async fn send(&self, message: ServerMessage) -> tg::Result<()> {
		self.send_with_priority(message, Priority::High).await?;
		Ok(())
	}

	pub(crate) async fn send_low(&self, message: ServerMessage) -> tg::Result<()> {
		self.send_with_priority(message, Priority::Low).await?;
		Ok(())
	}

	async fn send_with_priority(
		&self,
		message: ServerMessage,
		priority: Priority,
	) -> tg::Result<()> {
		self.sender
			.send(task::Message::Send {
				message,
				priority,
				response: None,
			})
			.await
			.map_err(|_| tg::error!("the process control connection closed"))?;
		Ok(())
	}
}

fn incoming_priority(message: &ClientMessage) -> Priority {
	if matches!(
		message,
		ClientMessage::Sync(_)
			| ClientMessage::Request(ClientRequest {
				arg: ClientRequestArg::Write(_),
				..
			}) | ClientMessage::Response(ClientResponse {
			output: Some(ClientResponseOutput::Read(_) | ClientResponseOutput::Write(_)),
			..
		})
	) {
		Priority::Low
	} else {
		Priority::High
	}
}
