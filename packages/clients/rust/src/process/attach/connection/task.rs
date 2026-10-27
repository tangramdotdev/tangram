use {
	super::*,
	futures::TryStreamExt as _,
	indexmap::IndexMap,
	std::{collections::VecDeque, ops::ControlFlow},
	tokio::sync::{mpsc, oneshot, watch},
	tokio_stream::wrappers::ReceiverStream,
};

#[cfg(test)]
mod tests;

pub(super) struct State {
	config: tg::process::attach::Config,
	high: VecDeque<ClientMessage>,
	input: BoxStream<'static, tg::Result<ServerMessage>>,
	next_id: u64,
	outcome: watch::Sender<Option<tg::Result<tg::process::outcome::Data>>>,
	output: watch::Sender<Option<tg::process::spawn::Output>>,
	pending: IndexMap<u64, Pending>,
	progress: Option<mpsc::Sender<tg::Result<tg::progress::Event<tg::process::spawn::Output>>>>,
	ready: bool,
	sender: mpsc::Sender<tg::Result<ClientMessage>>,
	status: watch::Sender<Status>,
}

pub(super) enum Message {
	Ready,
	Request {
		arg: ClientRequestArg,
		sender: oneshot::Sender<tg::Result<Response>>,
	},
}

#[derive(Clone, Copy)]
pub(super) enum Update {
	Remove(u64),
}

#[derive(Clone, Copy)]
enum Next {
	Notification,
	Request(u64),
}

struct Pending {
	request: ClientRequest,
	response: Option<oneshot::Sender<tg::Result<ServerResponseOutput>>>,
	sent: bool,
}

impl State {
	pub(super) async fn open<I: tg::Instance>(
		instance: &I,
		arg: Arg,
		config: tg::process::attach::Config,
		progress: mpsc::Sender<tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		output: watch::Sender<Option<tg::process::spawn::Output>>,
		outcome: watch::Sender<Option<tg::Result<tg::process::outcome::Data>>>,
		status: watch::Sender<Status>,
	) -> tg::Result<ControlFlow<Self, tg::Error>> {
		let (sender, receiver) = mpsc::channel(config.input_capacity);
		let input = match instance
			.try_get_process_attach_stream(arg, ReceiverStream::new(receiver).boxed())
			.await?
		{
			ControlFlow::Break(Some((_, input))) => input,
			ControlFlow::Break(None) => return Err(tg::error!("failed to find the process")),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let state = Self {
			config,
			high: VecDeque::new(),
			input,
			next_id: 1,
			outcome,
			output,
			pending: IndexMap::new(),
			progress: Some(progress),
			ready: false,
			sender,
			status,
		};

		Ok(ControlFlow::Break(state))
	}

	pub(super) async fn run(
		mut self,
		mut messages: mpsc::Receiver<Message>,
		mut updates: mpsc::UnboundedReceiver<Update>,
		update_sender: mpsc::UnboundedSender<Update>,
	) {
		let result = self
			.run_inner(&mut messages, &mut updates, &update_sender)
			.await;
		self.disconnect(result.err());

		// Fail requests that arrive after the transport closes.
		while let Some(message) = messages.recv().await {
			match message {
				Message::Ready => {},
				Message::Request { sender, .. } => {
					sender
						.send(Err(tg::error!("the process connection closed")))
						.ok();
				},
			}
		}
	}

	async fn run_inner(
		&mut self,
		messages: &mut mpsc::Receiver<Message>,
		updates: &mut mpsc::UnboundedReceiver<Update>,
		update_sender: &mpsc::UnboundedSender<Update>,
	) -> tg::Result<()> {
		let mut writable = true;
		loop {
			let next = self.next_message();
			tokio::select! {
				biased;
				Some(update) = updates.recv() => self.handle_update(update),
				message = self.input.try_next() => {
					let Some(message) = message? else {
						break;
					};
					self.handle_server_message(message).await?;
				},
				permit = self.sender.clone().reserve_owned(), if writable && next.is_some() => {
					let Ok(permit) = permit else {
						// Drain the response body after the request body closes.
						writable = false;
						continue;
					};
					let message = self.take_message(next.unwrap());
					permit.send(Ok(message));
				},
				message = messages.recv(), if self.high.len() < self.config.max_messages => {
					let Some(message) = message else { break; };
					self.handle_message(message, update_sender);
				},
			}
		}
		Ok(())
	}

	fn handle_message(&mut self, message: Message, updates: &mpsc::UnboundedSender<Update>) {
		match message {
			Message::Ready => self.ready(),
			Message::Request { arg, sender } => {
				if sender.is_closed() {
					return;
				}
				let (response, receiver) = oneshot::channel();
				let output = self
					.enqueue_request(arg, Some(response))
					.map(|id| Response {
						id,
						receiver,
						updates: updates.clone(),
					});
				sender.send(output).ok();
				self.ready();
			},
		}
	}

	fn enqueue_request(
		&mut self,
		arg: ClientRequestArg,
		response: Option<oneshot::Sender<tg::Result<ServerResponseOutput>>>,
	) -> tg::Result<u64> {
		if self.pending.len() >= self.config.max_requests {
			return Err(tg::error!("too many pending process requests"));
		}
		let id = self.next_id;
		self.next_id = id
			.checked_add(1)
			.ok_or_else(|| tg::error!("the process request id overflowed"))?;
		let request = ClientRequest { arg, id };
		self.pending.insert(
			id,
			Pending {
				request,
				response,
				sent: false,
			},
		);
		Ok(id)
	}

	async fn handle_server_message(&mut self, message: ServerMessage) -> tg::Result<()> {
		match message {
			ServerMessage::Ack(_) => {},
			ServerMessage::Notification(ServerNotification::Outcome(outcome)) => {
				self.outcome.send_replace(Some(Ok(outcome)));
			},
			ServerMessage::Notification(ServerNotification::Progress(event)) => {
				if let tg::progress::Event::Output(output) = &event {
					self.output.send_replace(Some(output.clone()));
				}
				let selected = matches!(event, tg::progress::Event::Output(_));
				if let Some(progress) = &self.progress {
					progress
						.send(Ok(event))
						.await
						.map_err(|_| tg::error!("the spawn receiver closed"))?;
				}
				if selected {
					self.progress = None;
				}
			},
			ServerMessage::Response(response) => self.complete_request(response)?,
		}
		Ok(())
	}

	fn complete_request(&mut self, response: ServerResponse) -> tg::Result<()> {
		let result = match (response.error, response.output) {
			(None, Some(output)) => Ok(output),
			(Some(error), None) => Err(error.try_into()?),
			_ => Err(tg::error!("invalid process response")),
		};
		let pending = self.pending.shift_remove(&response.id);
		self.acknowledge(response.id);
		if let Some(sender) = pending.and_then(|pending| pending.response) {
			sender.send(result).ok();
		}
		Ok(())
	}

	fn handle_update(&mut self, update: Update) {
		match update {
			Update::Remove(id) => {
				self.pending.shift_remove(&id);
			},
		}
	}

	fn ready(&mut self) {
		if !self.ready {
			self.ready = true;
			// Send the ready notification after the operation that caused a reconnect.
			self.high
				.push_back(ClientMessage::Notification(ClientNotification::Ready));
		}
	}

	fn acknowledge(&mut self, id: u64) {
		self.high.push_front(ClientMessage::Ack(Ack { id }));
	}

	fn next_message(&self) -> Option<Next> {
		// Send acknowledgments before ordinary requests.
		if self.high.front().is_some_and(|message| {
			!matches!(
				message,
				ClientMessage::Notification(ClientNotification::Ready)
			)
		}) {
			return Some(Next::Notification);
		}
		let pending = self.pending.iter().filter(|(_, pending)| !pending.sent);
		if let Some((&id, _)) = pending
			.clone()
			.find(|(_, pending)| matches!(pending.request.arg, ClientRequestArg::Detach))
			.or_else(|| pending.into_iter().next())
		{
			return Some(Next::Request(id));
		}
		(!self.high.is_empty()).then_some(Next::Notification)
	}

	fn take_message(&mut self, id: Next) -> ClientMessage {
		if let Next::Request(id) = id {
			let pending = self.pending.get_mut(&id).unwrap();
			pending.sent = true;
			ClientMessage::Request(pending.request.clone())
		} else {
			self.high.pop_front().unwrap()
		}
	}

	fn disconnect(&mut self, error: Option<tg::Error>) {
		self.status.send_replace(Status {
			closed: true,
			error: error.clone(),
		});
		if let Some(progress) = self.progress.take() {
			progress
				.try_send(Err(error.clone().unwrap_or_else(|| {
					tg::error!("the process connection closed before its response")
				})))
				.ok();
		}
		for (_, pending) in std::mem::take(&mut self.pending) {
			if let (Some(error), Some(sender)) = (&error, pending.response) {
				sender.send(Err(error.clone())).ok();
			}
		}
		if self.outcome.borrow().is_none() {
			self.outcome.send_replace(Some(Err(
				error.unwrap_or_else(|| tg::error!("the process connection closed"))
			)));
		}
	}
}
