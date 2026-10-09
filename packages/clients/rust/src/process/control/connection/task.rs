use {
	super::*,
	futures::{TryStreamExt as _, stream},
	indexmap::IndexMap,
	std::{
		collections::{BTreeMap, VecDeque},
		time::Duration,
	},
	tokio::time::Instant,
};

#[cfg(test)]
mod tests;

pub(super) struct State {
	attached: bool,
	deliveries: VecDeque<tg::control::Event<ServerMessage>>,
	events: mpsc::Sender<tg::Result<tg::control::Event<ServerMessage>>>,
	high: Option<mpsc::Sender<ClientMessage>>,
	inbox: BTreeMap<String, Option<Instant>>,
	input: BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
	lease: Option<String>,
	low: Option<mpsc::Sender<ClientMessage>>,
	pending: IndexMap<String, Pending>,
	start: Option<ClientRequest>,
	untracked: VecDeque<(ClientMessage, Priority)>,
	waiters: Vec<oneshot::Sender<()>>,
}

pub(super) enum Message {
	Send {
		message: ClientMessage,
		priority: Priority,
		response: Option<oneshot::Sender<ServerMessage>>,
	},
	Wait(oneshot::Sender<()>),
}

pub(super) enum Update {
	Acknowledge {
		id: String,
		priority: Priority,
	},
	Attach {
		high: mpsc::Sender<ClientMessage>,
		low: mpsc::Sender<ClientMessage>,
		sender: oneshot::Sender<Option<String>>,
	},
	Remove(String),
}

struct Pending {
	acknowledged: bool,
	message: ClientMessage,
	priority: Priority,
	response: Option<oneshot::Sender<ServerMessage>>,
	sent: bool,
}

pub(super) async fn open_stream<I: tg::Instance>(
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
	let instance = instance.clone();
	let (_, input) = sender.attach().await?;

	// Get the initial header and stream.
	let Some((header, output_stream)) = instance
		.try_get_process_control_stream(arg.clone(), input)
		.await?
	else {
		return Ok(None);
	};
	let instance = reconnect(&header);
	let mut arg = arg;
	arg.id = Some(header.process.node.clone());
	if let Mode::Start(start) = arg.mode {
		arg.mode = Mode::Resume { lease: start.lease };
	}

	// Yield events from the stream, reconnecting with backoff when the stream ends or returns an error.
	struct State {
		retries: Option<BoxStream<'static, ()>>,
		stream: Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>,
	}
	let state = State {
		retries: None,
		stream: Some(output_stream.boxed()),
	};
	let stream = stream::unfold(state, move |mut state| {
		let instance = instance.clone();
		let mut arg = arg.clone();
		let sender = sender.clone();
		async move {
			loop {
				if state.stream.is_none() {
					let (lease, input) = match sender.attach().await {
						Ok(output) => output,
						Err(error) => return Some((Err(error), state)),
					};
					if let Some(lease) = lease {
						arg.mode = Mode::Resume { lease };
					}
					let retries = state.retries.get_or_insert_with(|| {
						let options = tangram_futures::retry::Options {
							max_retries: u64::MAX,
							..Default::default()
						};
						tangram_futures::retry::stream(options).boxed()
					});
					retries.next().await?;
					match instance
						.try_get_process_control_stream(arg.clone(), input)
						.await
					{
						Ok(Some((_, stream))) => {
							state.stream.replace(stream.boxed());
							return Some((Ok(tg::control::Event::Reconnect), state));
						},
						Ok(None) => {
							let error = tg::error!("failed to find the process");
							return Some((Err(error), state));
						},
						Err(error) => {
							tracing::error!(error = %error.trace(), "failed to reconnect the control stream");
							continue;
						},
					}
				}
				match state.stream.as_mut().unwrap().next().await {
					Some(Ok(event)) => {
						state.retries.take();
						return Some((Ok(tg::control::Event::Message(event)), state));
					},
					Some(Err(error)) => {
						tracing::error!(error = %error.trace(), "the control stream returned an error");
						state.stream.take();
					},
					None => {
						state.stream.take();
					},
				}
			}
		}
	});

	Ok(Some((header, stream.boxed())))
}

impl State {
	pub(super) fn new(
		input: BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
		high: Option<mpsc::Sender<ClientMessage>>,
		low: Option<mpsc::Sender<ClientMessage>>,
		events: mpsc::Sender<tg::Result<tg::control::Event<ServerMessage>>>,
	) -> Self {
		Self {
			attached: false,
			deliveries: VecDeque::new(),
			events,
			high,
			inbox: BTreeMap::new(),
			input,
			lease: None,
			low,
			pending: IndexMap::new(),
			start: None,
			untracked: VecDeque::new(),
			waiters: Vec::new(),
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
			if self.pending.is_empty() && self.untracked.is_empty() {
				for waiter in self.waiters.drain(..) {
					waiter.send(()).ok();
				}
			}
			// Never wait for output capacity without also servicing incoming responses.
			let high = self.has_message(Priority::High) && self.high.is_some();
			let high_sender = self.high.clone();
			let low = self.has_message(Priority::Low) && self.low.is_some();
			let low_sender = self.low.clone();
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
				permit = async { high_sender.unwrap().reserve_owned().await }, if high => {
					let Ok(permit) = permit else {
						self.high = None;
						self.low = None;
						continue;
					};
					permit.send(self.take_message(Priority::High).unwrap());
				},
				permit = async { low_sender.unwrap().reserve_owned().await }, if low => {
					let Ok(permit) = permit else {
						self.high = None;
						self.low = None;
						continue;
					};
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
				if let ClientMessage::Request(request) = &message
					&& matches!(request.arg, ClientRequestArg::Start(_))
				{
					self.start = Some(request.clone());
				}
				let id = match &message {
					ClientMessage::Request(request) => Some(request.id.clone()),
					ClientMessage::Response(response) => Some(response.id.clone()),
					ClientMessage::Ack(_)
					| ClientMessage::Notification(_)
					| ClientMessage::Sync(_) => None,
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
			Message::Wait(sender) => self.waiters.push(sender),
		}
		Ok(())
	}

	fn handle_update(&mut self, update: Update) {
		match update {
			Update::Acknowledge { id, priority } => {
				self.inbox.insert(id.clone(), None);
				self.untracked
					.push_back((ClientMessage::Ack(ClientAck { id }), priority));
			},
			Update::Attach { high, low, sender } => {
				self.high = Some(high);
				self.low = Some(low);
				self.attached = true;
				self.disconnect();
				if let Some(start) = &self.start
					&& !self.pending.contains_key(&start.id)
				{
					self.untracked
						.push_front((ClientMessage::Request(start.clone()), Priority::High));
				}
				sender.send(self.lease.clone()).ok();
			},
			Update::Remove(id) => {
				self.pending.shift_remove(&id);
			},
		}
	}

	fn handle_server_message(&mut self, event: tg::control::Event<ServerMessage>) {
		let message = match event {
			tg::control::Event::Reconnect => {
				if !self.attached {
					self.disconnect();
				}
				self.deliveries.push_back(tg::control::Event::Reconnect);
				return;
			},
			tg::control::Event::Message(message) => message,
		};
		match &message {
			ServerMessage::Ack(ack) => {
				if let Some(pending) = self.pending.get_mut(&ack.id) {
					if matches!(pending.message, ClientMessage::Request(_)) {
						pending.acknowledged = pending.sent;
					} else {
						self.pending.shift_remove(&ack.id);
						if let Some(deadline) = self.inbox.get_mut(&ack.id) {
							*deadline = Some(Instant::now() + Duration::from_secs(60));
						}
					}
				}
			},
			ServerMessage::Request(request) => {
				if self.inbox.contains_key(&request.id) {
					self.untracked.push_back((
						ClientMessage::Ack(ClientAck {
							id: request.id.clone(),
						}),
						incoming_priority(&message),
					));
					return;
				}
				self.inbox.insert(request.id.clone(), None);
			},
			ServerMessage::Response(response) => {
				if response.error.is_none()
					&& matches!(response.output, Some(ServerResponseOutput::Start(_)))
					&& let Some(start) = &self.start
					&& start.id == response.id
				{
					let ClientRequestArg::Start(start) = &start.arg else {
						unreachable!()
					};
					self.lease = Some(start.lease.clone());
					self.start = None;
				}
				if let Some(pending) = self.pending.shift_remove(&response.id)
					&& let Some(sender) = pending.response
				{
					self.untracked.push_back((
						ClientMessage::Ack(ClientAck {
							id: response.id.clone(),
						}),
						pending.priority,
					));
					sender.send(message).ok();
					return;
				}
			},
			ServerMessage::Notification(_) | ServerMessage::Sync(_) => {},
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

	fn take_message(&mut self, priority: Priority) -> Option<ClientMessage> {
		if priority == Priority::High
			&& self.untracked.front().is_some_and(|(message, _)| {
				matches!(
					message,
					ClientMessage::Request(ClientRequest {
						arg: ClientRequestArg::Start(_),
						..
					})
				)
			}) {
			return self.untracked.pop_front().map(|(message, _)| message);
		}
		// Acknowledgments must be able to release the peer's capacity before replay.
		if let Some(index) = self
			.untracked
			.iter()
			.position(|(message, p)| *p == priority && matches!(message, ClientMessage::Ack(_)))
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
