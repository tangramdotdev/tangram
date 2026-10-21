use {
	super::*,
	futures::{TryStreamExt as _, stream},
	indexmap::IndexMap,
	std::collections::{BTreeMap, VecDeque},
	tokio::time::Instant,
};

#[cfg(test)]
mod tests;

pub(super) struct State {
	attached: bool,
	config: Config,
	deliveries: VecDeque<tg::control::Event<ServerMessage>>,
	events: mpsc::Sender<tg::Result<tg::control::Event<ServerMessage>>>,
	high: Option<mpsc::Sender<ClientMessage>>,
	inbox: BTreeMap<String, Option<Instant>>,
	input: BoxStream<'static, tg::Result<tg::control::Event<ServerMessage>>>,
	lease: Option<tg::process::Lease>,
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
		sender: oneshot::Sender<Option<tg::process::Lease>>,
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
	let stream_retry = instance.arg().stream_retry;

	// Get the initial header and stream, retrying with backoff while the request cannot reach a server, so that a start whose header the client has not received is replayed.
	let mut retries = tangram_futures::retry::stream(stream_retry.clone()).boxed();
	let (header, output_stream) = loop {
		let (_, input) = sender.attach().await?;
		match instance
			.try_get_process_control_stream(arg.clone(), input)
			.await?
		{
			ControlFlow::Break(Some(output)) => break output,
			ControlFlow::Break(None) => return Ok(None),
			ControlFlow::Continue(error) => {
				tracing::error!(error = %error.trace(), "failed to connect the control stream");
				if retries.next().await.is_none() {
					return Err(error);
				}
			},
		}
	};
	let instance = reconnect(&header);
	let mut arg = arg;
	arg.proof = None;
	if let Mode::Start(start) = arg.mode {
		arg.mode = Mode::Resume { lease: start.lease };
	}

	// Yield events from the stream, reconnecting with backoff when the stream ends before the connection closes, which is how a stream reports a lost connection.
	struct StreamState {
		arg: Arg,
		ended: bool,
		error: Option<tg::Error>,
		retries: Option<BoxStream<'static, ()>>,
		stream: Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>,
	}
	let state = StreamState {
		arg,
		ended: false,
		error: None,
		retries: None,
		stream: Some(output_stream.boxed()),
	};
	let stream = stream::unfold(state, move |mut state| {
		let instance = instance.clone();
		let sender = sender.clone();
		let stream_retry = stream_retry.clone();
		async move {
			// End the stream after it yields an error.
			if state.ended {
				return None;
			}
			loop {
				if state.stream.is_none() {
					let (lease, input) = match sender.attach().await {
						Ok(output) => output,
						Err(error) => {
							state.ended = true;
							return Some((Err(error), state));
						},
					};
					if let Some(lease) = lease {
						state.arg.mode = Mode::Resume { lease };
					}

					// Fail with the last error once the retries are exhausted.
					let retries = state.retries.get_or_insert_with(|| {
						tangram_futures::retry::stream(stream_retry.clone()).boxed()
					});
					if retries.next().await.is_none() {
						let error = state
							.error
							.take()
							.unwrap_or_else(|| tg::error!("the control stream ended"));
						state.ended = true;
						return Some((Err(error), state));
					}
					match instance
						.try_get_process_control_stream(state.arg.clone(), input)
						.await
					{
						Ok(ControlFlow::Break(Some((_, stream)))) => {
							state.error = None;
							state.stream.replace(stream.boxed());
							return Some((Ok(tg::control::Event::Reconnect), state));
						},
						Ok(ControlFlow::Break(None)) => {
							let error = tg::error!("failed to find the process");
							state.ended = true;
							return Some((Err(error), state));
						},
						Ok(ControlFlow::Continue(error)) => {
							tracing::error!(error = %error.trace(), "failed to reconnect the control stream");
							state.error = Some(error);
							continue;
						},
						Err(error) => {
							state.ended = true;
							return Some((Err(error), state));
						},
					}
				}
				match state.stream.as_mut().unwrap().next().await {
					Some(Ok(event)) => {
						state.retries.take();
						return Some((Ok(tg::control::Event::Message(event)), state));
					},
					Some(Err(error)) => {
						state.ended = true;
						return Some((Err(error), state));
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
		config: Config,
	) -> Self {
		Self {
			attached: false,
			config,
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
		let mut retry = tokio::time::interval(self.config.resend_interval);
		retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			if self.pending.is_empty() && self.untracked.is_empty() {
				for waiter in self.waiters.drain(..) {
					waiter.send(()).ok();
				}
			}
			// Select the next event, servicing incoming messages while waiting for output capacity.
			let high = self.has_message(Priority::High) && self.high.is_some();
			let high_sender = self.high.clone();
			let low = self.has_message(Priority::Low) && self.low.is_some();
			let low_sender = self.low.clone();
			tokio::select! {
				biased;
				() = self.events.closed() => break,
				Some(update) = updates.recv() => self.handle_update(update),
				event = self.input.try_next(), if self.deliveries.len() < self.config.max_deliveries => {
					let Some(event) = event? else { break; };
					self.handle_server_message(event)?;
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
				Some(message) = messages.recv(), if self.untracked.len() < self.config.max_untracked_messages => self.handle_message(message)?,
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
					ClientMessage::Ack(_) | ClientMessage::Notification(_) => None,
				};
				if let Some(id) = id {
					if let Some(pending) = self.pending.get_mut(&id) {
						// Mark a repeated submission unsent, keeping its original place in the pending map.
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
				// Expire an acknowledged id that did not arrive on the connection, because no tracked response will set its deadline.
				let inbox_ttl = self.config.inbox_ttl;
				self.inbox
					.entry(id.clone())
					.or_insert_with(|| Some(Instant::now() + inbox_ttl));
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
				sender.send(self.lease).ok();
			},
			Update::Remove(id) => {
				self.pending.shift_remove(&id);
			},
		}
	}

	fn handle_server_message(
		&mut self,
		event: tg::control::Event<ServerMessage>,
	) -> tg::Result<()> {
		let message = match event {
			tg::control::Event::Reconnect => {
				if !self.attached {
					self.disconnect();
				}
				self.deliveries.push_back(tg::control::Event::Reconnect);
				return Ok(());
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
							*deadline = Some(Instant::now() + self.config.inbox_ttl);
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
					return Ok(());
				}
				self.inbox.insert(request.id.clone(), None);
			},
			ServerMessage::Response(response) => {
				// Clear the start request when the server responds to it, and store its lease if it succeeded.
				let mut rejected = None;
				if let Some(start) = &self.start
					&& start.id == response.id
				{
					if response.error.is_none()
						&& matches!(response.output, Some(ServerResponseOutput::Start(_)))
					{
						let ClientRequestArg::Start(start) = &start.arg else {
							unreachable!()
						};
						self.lease = Some(start.lease);
					} else {
						rejected = Some(response.error.clone());
					}
					self.start = None;
				}
				let pending = self.pending.shift_remove(&response.id);
				let delivered = if let Some(pending) = pending
					&& let Some(sender) = pending.response
				{
					self.untracked.push_back((
						ClientMessage::Ack(ClientAck {
							id: response.id.clone(),
						}),
						pending.priority,
					));
					sender.send(message.clone()).ok();
					true
				} else {
					false
				};

				// End the connection when the server rejects the start, because a reconnect cannot resume a process that did not start.
				if let Some(error) = rejected {
					let error = error
						.map(tg::Error::try_from)
						.transpose()
						.map_err(|source| tg::error!(!source, "failed to deserialize the error"))?;
					let error = if let Some(error) = error {
						tg::error!(!error, "the server rejected the process start")
					} else {
						tg::error!("the server rejected the process start")
					};
					return Err(error);
				}
				if delivered {
					return Ok(());
				}
			},
			ServerMessage::Notification(notification) => match *notification {},
		}
		self.deliveries
			.push_back(tg::control::Event::Message(message));
		Ok(())
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
			|| self
				.untracked
				.iter()
				.any(|(_, untracked_priority)| *untracked_priority == priority)
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
		// Send acknowledgments before replayed messages, so that they can release the peer's capacity.
		if let Some(index) = self
			.untracked
			.iter()
			.position(|(message, untracked_priority)| {
				*untracked_priority == priority && matches!(message, ClientMessage::Ack(_))
			}) {
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
		let index = self
			.untracked
			.iter()
			.position(|(_, untracked_priority)| *untracked_priority == priority)?;
		self.untracked.remove(index).map(|(message, _)| message)
	}
}
