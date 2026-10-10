use {
	super::*,
	futures::TryStreamExt as _,
	indexmap::IndexMap,
	std::{
		collections::{BTreeMap, VecDeque},
		time::Duration,
	},
	tokio::time::Instant,
};

pub(super) struct State {
	deliveries: VecDeque<tg::control::Event<ClientMessage>>,
	events: mpsc::Sender<tg::Result<tg::control::Event<ClientMessage>>>,
	high: mpsc::Sender<ServerMessage>,
	inbox: BTreeMap<String, Option<Instant>>,
	input: BoxStream<'static, tg::Result<tg::control::Event<ClientMessage>>>,
	low: mpsc::Sender<ServerMessage>,
	pending: IndexMap<String, Pending>,
	untracked: VecDeque<(ServerMessage, Priority)>,
}

pub(super) enum Message {
	Send {
		message: ServerMessage,
		priority: Priority,
		response: Option<oneshot::Sender<ClientMessage>>,
	},
}

pub(super) enum Update {
	Acknowledge { id: String, priority: Priority },
}

struct Pending {
	acknowledged: bool,
	message: ServerMessage,
	priority: Priority,
	response: Option<oneshot::Sender<ClientMessage>>,
	sent: bool,
}

impl State {
	pub(super) fn new(
		input: BoxStream<'static, tg::Result<tg::control::Event<ClientMessage>>>,
		high: mpsc::Sender<ServerMessage>,
		low: mpsc::Sender<ServerMessage>,
		events: mpsc::Sender<tg::Result<tg::control::Event<ClientMessage>>>,
	) -> Self {
		Self {
			deliveries: VecDeque::new(),
			events,
			high,
			inbox: BTreeMap::new(),
			input,
			low,
			pending: IndexMap::new(),
			untracked: VecDeque::new(),
		}
	}

	pub(super) async fn run(
		mut self,
		mut messages: mpsc::Receiver<Message>,
		mut updates: mpsc::UnboundedReceiver<Update>,
	) {
		let result = self.run_inner(&mut messages, &mut updates).await;
		if let Err(error) = result {
			self.events.send(Err(error)).await.ok();
		}
	}

	async fn run_inner(
		&mut self,
		messages: &mut mpsc::Receiver<Message>,
		updates: &mut mpsc::UnboundedReceiver<Update>,
	) -> tg::Result<()> {
		let mut retry = tokio::time::interval(Duration::from_secs(1));
		retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			// Never wait for output capacity without also servicing incoming responses.
			let high = self.has_message(Priority::High);
			let low = self.has_message(Priority::Low);
			tokio::select! {
				biased;
				() = self.events.closed() => break,
				Some(update) = updates.recv() => self.handle_update(update),
				event = self.input.try_next(), if self.deliveries.len() < 64 => {
					let Some(event) = event? else { break; };
					self.handle_server_message(event);
				},
				permit = self.events.clone().reserve_owned(), if !self.deliveries.is_empty() => {
					permit.map_err(|_| tg::error!("the process control receiver closed"))?
						.send(Ok(self.deliveries.pop_front().unwrap()));
				},
				permit = self.high.clone().reserve_owned(), if high => {
					let permit = permit.map_err(|_| tg::error!("the process control stream closed"))?;
					permit.send(self.take_message(Priority::High).unwrap());
				},
				permit = self.low.clone().reserve_owned(), if low => {
					let permit = permit.map_err(|_| tg::error!("the process control stream closed"))?;
					permit.send(self.take_message(Priority::Low).unwrap());
				},
				Some(message) = messages.recv(), if self.untracked.len() < 256 => self.handle_message(message)?,
				_ = retry.tick() => {
					for pending in self.pending.values_mut() {
						if !pending.acknowledged { pending.sent = false; }
					}
					let now = Instant::now();
					self.inbox.retain(|_, deadline| deadline.is_none_or(|deadline| deadline > now));
				},
			}
		}
		Ok(())
	}

	fn handle_message(&mut self, message: Message) -> tg::Result<()> {
		match message {
			Message::Send {
				message,
				priority,
				response,
			} => {
				if response.as_ref().is_some_and(oneshot::Sender::is_closed) {
					return Ok(());
				}
				let id = match &message {
					ServerMessage::Request(request) => Some(request.id.clone()),
					ServerMessage::Response(response) => Some(response.id.clone()),
					ServerMessage::Ack(_)
					| ServerMessage::Notification(_)
					| ServerMessage::Sync(_)
					| ServerMessage::SyncConfig(_)
					| ServerMessage::SyncConsumption(_) => None,
				};
				if let Some(id) = id {
					if let Some(pending) = self.pending.get_mut(&id) {
						// A repeated submission keeps its original place in the pending map.
						pending.sent = false;
						pending.acknowledged = false;
						if response.is_some() {
							return Err(tg::error!("duplicate process control request id"));
						}
					} else {
						self.pending.insert(
							id,
							Pending {
								acknowledged: false,
								message,
								priority,
								response,
								sent: false,
							},
						);
					}
				} else {
					self.untracked.push_back((message, priority));
				}
			},
		}
		Ok(())
	}

	fn handle_update(&mut self, update: Update) {
		match update {
			Update::Acknowledge { id, priority } => {
				self.inbox.insert(id.clone(), None);
				self.untracked
					.push_back((ServerMessage::Ack(ServerAck { id }), priority));
			},
		}
	}

	fn handle_server_message(&mut self, event: tg::control::Event<ClientMessage>) {
		let message = match event {
			tg::control::Event::Reconnect => {
				self.disconnect();
				self.deliveries.push_back(tg::control::Event::Reconnect);
				return;
			},
			tg::control::Event::Message(message) => message,
		};
		match &message {
			ClientMessage::Ack(ack) => {
				if let Some(pending) = self.pending.get_mut(&ack.id) {
					if matches!(pending.message, ServerMessage::Request(_)) {
						pending.acknowledged = true;
					} else {
						self.pending.shift_remove(&ack.id);
						if let Some(deadline) = self.inbox.get_mut(&ack.id) {
							*deadline = Some(Instant::now() + Duration::from_secs(60));
						}
					}
				}
			},
			ClientMessage::Request(request) => {
				if self.inbox.contains_key(&request.id) {
					self.untracked.push_back((
						ServerMessage::Ack(ServerAck {
							id: request.id.clone(),
						}),
						incoming_priority(&message),
					));
					return;
				}
				self.inbox.insert(request.id.clone(), None);
			},
			ClientMessage::Response(response) => {
				if let Some(pending) = self.pending.shift_remove(&response.id)
					&& let Some(sender) = pending.response
				{
					self.untracked.push_back((
						ServerMessage::Ack(ServerAck {
							id: response.id.clone(),
						}),
						pending.priority,
					));
					sender.send(message).ok();
					return;
				}
			},
			ClientMessage::Notification(_)
			| ClientMessage::Sync(_)
			| ClientMessage::SyncConfig(_)
			| ClientMessage::SyncConsumption(_) => {},
		}
		self.deliveries
			.push_back(tg::control::Event::Message(message));
	}

	fn disconnect(&mut self) {
		for pending in self.pending.values_mut() {
			pending.acknowledged = false;
			pending.sent = false;
		}
	}

	fn has_message(&self, priority: Priority) -> bool {
		self.pending
			.values()
			.any(|pending| !pending.sent && !pending.acknowledged && pending.priority == priority)
			|| self.untracked.iter().any(|(_, p)| *p == priority)
	}

	fn take_message(&mut self, priority: Priority) -> Option<ServerMessage> {
		// Acknowledgments must be able to release the peer's capacity before replay.
		if let Some(index) = self
			.untracked
			.iter()
			.position(|(message, p)| *p == priority && matches!(message, ServerMessage::Ack(_)))
		{
			return self.untracked.remove(index).map(|(message, _)| message);
		}
		if let Some(pending) = self
			.pending
			.values_mut()
			.find(|pending| !pending.sent && !pending.acknowledged && pending.priority == priority)
		{
			pending.sent = true;
			return Some(pending.message.clone());
		}
		let index = self.untracked.iter().position(|(_, p)| *p == priority)?;
		self.untracked.remove(index).map(|(message, _)| message)
	}
}
