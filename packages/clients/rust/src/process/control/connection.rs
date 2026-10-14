use {
	super::*,
	futures::{StreamExt as _, stream::BoxStream},
	std::{
		pin::Pin,
		task::{Context, Poll},
	},
	tangram_futures::task::Task,
	tokio::sync::{mpsc, oneshot, watch},
};

mod task;

pub struct Connection {
	receiver: mpsc::Receiver<tg::Result<tg::control::Event<ServerMessage>>>,
	sender: Sender,
	task: Task<()>,
}

#[derive(Clone)]
pub struct Sender {
	messages: mpsc::Sender<task::Message>,
	outcome_sync: watch::Receiver<Option<tg::Referent<tg::sync::Id>>>,
	updates: mpsc::UnboundedSender<task::Update>,
}

pub struct Response {
	id: String,
	receiver: oneshot::Receiver<ServerMessage>,
	updates: mpsc::UnboundedSender<task::Update>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Priority {
	High,
	Low,
}

impl Connection {
	#[must_use]
	pub fn new(
		input: BoxStream<'static, tg::Result<ServerMessage>>,
		high: mpsc::Sender<ClientMessage>,
		low: mpsc::Sender<ClientMessage>,
		config: Config,
	) -> Self {
		let input = input
			.map(|message| message.map(tg::control::Event::Message))
			.boxed();
		Self::with_input_and_output(input, Some(high), Some(low), config)
	}

	#[must_use]
	pub fn with_input(
		input: BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
		config: Config,
	) -> Self {
		Self::with_input_and_output(input, None, None, config)
	}

	fn with_input_and_output(
		input: BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
		high: Option<mpsc::Sender<ClientMessage>>,
		low: Option<mpsc::Sender<ClientMessage>>,
		config: Config,
	) -> Self {
		let (messages, message_receiver) = mpsc::channel(config.message_capacity);
		let (updates, update_receiver) = mpsc::unbounded_channel();
		let (events, receiver) = mpsc::channel(config.event_capacity);
		let (outcome_sync_sender, outcome_sync) = watch::channel(None);
		let state = task::State::new(input, high, low, events, outcome_sync_sender, config);
		let task = Task::spawn(move |_| async move {
			state.run(message_receiver, update_receiver).await;
		});
		let sender = Sender {
			messages,
			outcome_sync,
			updates,
		};
		Self {
			receiver,
			sender,
			task,
		}
	}

	pub async fn open_stream<I: tg::Instance>(
		instance: &I,
		arg: Arg,
		sender: Sender,
		reconnect: impl FnOnce(&Header) -> I + Send,
	) -> tg::Result<
		Option<(
			Header,
			BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
		)>,
	> {
		task::open_stream(instance, arg, sender, reconnect).await
	}

	pub async fn recv_event_with_ack(
		&mut self,
	) -> tg::Result<Option<tg::control::Event<ServerMessage>>> {
		loop {
			let Some(event) = self.receiver.recv().await.transpose()? else {
				return Ok(None);
			};
			if let tg::control::Event::Message(message) = &event {
				match message {
					ServerMessage::Ack(_) => continue,
					ServerMessage::Request(request) => {
						self.acknowledge_with_priority(
							request.id.clone(),
							incoming_priority(message),
						)
						.await?;
					},
					ServerMessage::Response(response) => {
						self.sender
							.send_with_priority(
								ClientMessage::Ack(ClientAck {
									id: response.id.clone(),
								}),
								incoming_priority(message),
							)
							.await?;
					},
					ServerMessage::Notification(_) => {},
				}
			}
			return Ok(Some(event));
		}
	}

	pub async fn recv_without_ack(&mut self) -> tg::Result<Option<ServerMessage>> {
		loop {
			match self.receiver.recv().await.transpose()? {
				Some(tg::control::Event::Message(message)) => return Ok(Some(message)),
				Some(tg::control::Event::Reconnect) => {},
				None => return Ok(None),
			}
		}
	}

	pub fn acknowledge_now(&mut self, id: String) {
		self.sender
			.updates
			.send(task::Update::Acknowledge {
				id,
				priority: Priority::High,
			})
			.ok();
	}

	pub async fn acknowledge_with_priority(
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
	pub fn sender(&self) -> Sender {
		self.sender.clone()
	}
}

impl Sender {
	async fn attach(
		&self,
	) -> tg::Result<(
		Option<String>,
		BoxStream<'static, tg::Result<ClientMessage>>,
	)> {
		let (high, high_receiver) = mpsc::channel(1);
		let (low, low_receiver) = mpsc::channel(1);
		let (sender, receiver) = oneshot::channel();
		self.updates
			.send(task::Update::Attach { high, low, sender })
			.map_err(|_| tg::error!("the process control connection closed"))?;
		let lease = receiver
			.await
			.map_err(|_| tg::error!("the process control connection closed"))?;
		let high = tokio_stream::wrappers::ReceiverStream::new(high_receiver);
		let low = tokio_stream::wrappers::ReceiverStream::new(low_receiver);
		let stream = futures::stream::select_with_strategy(high, low, |(): &mut ()| {
			futures::stream::PollNext::Left
		});
		Ok((lease, stream.map(Ok).boxed()))
	}

	pub async fn send(&self, message: ClientMessage) -> tg::Result<()> {
		self.send_with_priority(message, Priority::High).await?;
		Ok(())
	}

	pub async fn send_low(&self, message: ClientMessage) -> tg::Result<()> {
		self.send_with_priority(message, Priority::Low).await?;
		Ok(())
	}

	async fn send_with_priority(
		&self,
		message: ClientMessage,
		priority: Priority,
	) -> tg::Result<()> {
		self.messages
			.send(task::Message::Send {
				message,
				priority,
				response: None,
			})
			.await
			.map_err(|_| tg::error!("the process control connection closed"))?;
		Ok(())
	}

	pub async fn request(
		&self,
		message: ClientMessage,
		priority: Priority,
	) -> tg::Result<Response> {
		let ClientMessage::Request(request) = &message else {
			return Err(tg::error!("expected a process control request"));
		};
		let id = request.id.clone();
		let (response, receiver) = oneshot::channel();
		self.messages
			.send(task::Message::Send {
				message,
				priority,
				response: Some(response),
			})
			.await
			.map_err(|_| tg::error!("the process control connection closed"))?;
		Ok(Response {
			id,
			receiver,
			updates: self.updates.clone(),
		})
	}

	/// Get the most recently refreshed outcome sync, if the server has sent one.
	#[must_use]
	pub fn outcome_sync(&self) -> Option<tg::Referent<tg::sync::Id>> {
		self.outcome_sync.borrow().clone()
	}

	pub async fn wait_for_empty(&self) {
		let (sender, receiver) = oneshot::channel();
		self.messages.send(task::Message::Wait(sender)).await.ok();
		receiver.await.ok();
	}
}

impl Drop for Connection {
	fn drop(&mut self) {
		self.task.abort();
	}
}

impl Future for Response {
	type Output = Result<ServerMessage, oneshot::error::RecvError>;
	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		Pin::new(&mut self.receiver).poll(cx)
	}
}

impl Drop for Response {
	fn drop(&mut self) {
		self.updates
			.send(task::Update::Remove(self.id.clone()))
			.ok();
	}
}

fn incoming_priority(message: &ServerMessage) -> Priority {
	if matches!(
		message,
		ServerMessage::Request(ServerRequest {
			arg: ServerRequestArg::Open(_),
			..
		})
	) {
		Priority::Low
	} else {
		Priority::High
	}
}
