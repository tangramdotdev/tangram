use {
	super::*,
	indexmap::IndexMap,
	std::collections::{BTreeMap, VecDeque},
	tokio::sync::{mpsc, oneshot, watch},
	tokio_stream::wrappers::ReceiverStream,
};

#[cfg(test)]
mod tests;

const MAX_MESSAGES: usize = 128;
const MAX_REQUESTS: usize = 64;

pub(super) struct State {
	high: VecDeque<ClientMessage>,
	initial: Vec<(
		u64,
		read::Arg,
		mpsc::Receiver<tg::Result<read::ServerMessage>>,
	)>,
	input: BoxStream<'static, tg::Result<ServerMessage>>,
	next_id: u64,
	outcome: watch::Sender<Option<tg::Result<tg::process::outcome::Data>>>,
	output: watch::Sender<Option<tg::process::spawn::Output>>,
	pending: IndexMap<u64, Pending>,
	progress: Option<mpsc::Sender<tg::Result<tg::progress::Event<tg::process::spawn::Output>>>>,
	reads: BTreeMap<u64, mpsc::Sender<tg::Result<read::ServerMessage>>>,
	ready: bool,
	sender: mpsc::Sender<tg::Result<ClientMessage>>,
	status: watch::Sender<Status>,
}

pub(super) enum Message {
	CloseInitial(tg::process::stdio::Stream),
	Read {
		arg: read::Arg,
		sender: oneshot::Sender<tg::Result<(u64, mpsc::Receiver<tg::Result<read::ServerMessage>>)>>,
	},
	Ready,
	Request {
		arg: ClientRequestArg,
		sender: oneshot::Sender<tg::Result<Response>>,
	},
}

pub(super) enum Update {
	Close(u64),
	Read {
		id: u64,
		message: tg::Result<read::ClientMessage>,
	},
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
		progress: mpsc::Sender<tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		output: watch::Sender<Option<tg::process::spawn::Output>>,
		outcome: watch::Sender<Option<tg::Result<tg::process::outcome::Data>>>,
		status: watch::Sender<Status>,
	) -> tg::Result<Self> {
		let (sender, receiver) = mpsc::channel(64);
		let mut initial = Vec::new();
		let mut reads = BTreeMap::new();
		for (&id, arg) in &arg.reads {
			let (sender, receiver) = mpsc::channel(tg::process::stdio::flow::CHANNEL_CAPACITY);
			reads.insert(id, sender);
			initial.push((id, arg.clone(), receiver));
		}
		let next_id = arg
			.reads
			.keys()
			.last()
			.copied()
			.unwrap_or(0)
			.checked_add(1)
			.ok_or_else(|| tg::error!("the process request id overflowed"))?;
		let (_, input) = instance
			.get_process_connect_stream(arg, ReceiverStream::new(receiver).boxed())
			.await?;
		Ok(Self {
			high: VecDeque::new(),
			initial,
			input,
			next_id,
			outcome,
			output,
			pending: IndexMap::new(),
			progress: Some(progress),
			reads,
			ready: false,
			sender,
			status,
		})
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

		// Keep buffered initial reads available after the transport closes.
		while let Some(message) = messages.recv().await {
			match message {
				Message::CloseInitial(stream) => {
					self.initial.retain(|(_, arg, _)| arg.streams != [stream]);
				},
				Message::Read { arg, sender } => {
					let output = self
						.initial
						.iter()
						.position(|(_, initial, _)| matches_read(initial, &arg))
						.map(|index| {
							let (id, _, receiver) = self.initial.remove(index);
							(id, receiver)
						})
						.ok_or_else(|| tg::error!("the process connection closed"));
					sender.send(output).ok();
				},
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
				Some(update) = updates.recv() => self.handle_update(update)?,
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
				message = messages.recv(), if self.high.len() < MAX_MESSAGES => {
					let Some(message) = message else { break; };
					self.handle_message(message, update_sender)?;
				},
			}
		}
		Ok(())
	}

	fn handle_message(
		&mut self,
		message: Message,
		updates: &mpsc::UnboundedSender<Update>,
	) -> tg::Result<()> {
		match message {
			Message::CloseInitial(stream) => {
				if let Some(index) = self
					.initial
					.iter()
					.position(|(_, arg, _)| arg.streams == [stream])
				{
					let (id, _, _) = self.initial.remove(index);
					self.close(id)?;
				}
			},
			Message::Read { arg, sender } => {
				if sender.is_closed() {
					return Ok(());
				}
				let output = if let Some(index) = self
					.initial
					.iter()
					.position(|(_, initial, _)| matches_read(initial, &arg))
				{
					let (id, _, receiver) = self.initial.remove(index);
					Ok((id, receiver))
				} else {
					let (read_sender, receiver) =
						mpsc::channel(tg::process::stdio::flow::CHANNEL_CAPACITY);
					self.enqueue_request(ClientRequestArg::Read(arg), None)
						.map(|id| {
							self.reads.insert(id, read_sender);
							(id, receiver)
						})
				};
				sender.send(output).ok();
				self.ready();
			},
			Message::Ready => self.ready(),
			Message::Request { arg, sender } => {
				if sender.is_closed() {
					return Ok(());
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
		Ok(())
	}

	fn enqueue_request(
		&mut self,
		arg: ClientRequestArg,
		response: Option<oneshot::Sender<tg::Result<ServerResponseOutput>>>,
	) -> tg::Result<u64> {
		let kind = std::mem::discriminant(&arg);
		let count = self
			.pending
			.values()
			.filter(|pending| std::mem::discriminant(&pending.request.arg) == kind)
			.count();
		let limit = match &arg {
			ClientRequestArg::Write(_) => tg::process::stdio::flow::MAX_CHUNKS + 1,
			_ => MAX_REQUESTS,
		};
		if count >= limit
			|| matches!(arg, ClientRequestArg::Read(_)) && self.reads.len() >= MAX_REQUESTS
		{
			return Err(tg::error!("too many pending process requests of this kind"));
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
			ServerMessage::Notification(ServerNotification::Read(notification)) => {
				if let Some(sender) = self.reads.get(&notification.id) {
					let message = read::ServerMessage::Notification(notification.event);
					if let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send(Ok(message)) {
						return Err(tg::error!("the process read buffer is full"));
					}
				}
			},
			ServerMessage::Response(response) => self.complete_request(response)?,
			ServerMessage::Sync(_) => return Err(tg::error!("unexpected process sync message")),
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
		if let Some(sender) = self.reads.get(&response.id) {
			let result = match result {
				Ok(ServerResponseOutput::Read(output)) => Ok(read::ServerMessage::Response(output)),
				Ok(_) => Err(tg::error!("expected a read response")),
				Err(error) => Err(error),
			};
			let failed = result.is_err();
			match sender.try_send(result) {
				// A read is acknowledged only after the caller consumes its buffered chunks.
				Ok(()) if !failed => return Ok(()),
				Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {},
				Err(mpsc::error::TrySendError::Full(_)) => {
					return Err(tg::error!("the process read buffer is full"));
				},
			}
			self.reads.remove(&response.id);
			self.acknowledge(response.id);
		} else {
			self.acknowledge(response.id);
			if let Some(sender) = pending.and_then(|pending| pending.response) {
				sender.send(result).ok();
			}
		}
		Ok(())
	}

	fn handle_update(&mut self, update: Update) -> tg::Result<()> {
		match update {
			Update::Close(id) => self.close(id)?,
			Update::Read { id, message } => match message {
				Ok(read::ClientMessage::Ack) => {
					self.acknowledge(id);
					self.reads.remove(&id);
				},
				Ok(read::ClientMessage::Notification(progress)) => {
					let notification = ReadClientNotification { id, progress };
					self.high
						.push_back(ClientMessage::Notification(ClientNotification::Read(
							notification,
						)));
				},
				Err(error) => {
					if let Some(sender) = self.reads.remove(&id) {
						sender.try_send(Err(error)).ok();
					}
					self.close(id)?;
				},
			},
			Update::Remove(id) => {
				self.pending.shift_remove(&id);
			},
		}
		Ok(())
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

	fn close(&mut self, id: u64) -> tg::Result<()> {
		self.reads.remove(&id);
		self.pending.shift_remove(&id);
		self.enqueue_request(ClientRequestArg::Close(id), None)?;
		Ok(())
	}

	fn next_message(&self) -> Option<Next> {
		// Send acknowledgments and read progress before ordinary requests.
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
			let message = ClientMessage::Request(pending.request.clone());
			if pending.response.is_none()
				&& matches!(pending.request.arg, ClientRequestArg::Close(_))
			{
				self.pending.shift_remove(&id);
			}
			message
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
		self.reads.clear();
		if self.outcome.borrow().is_none() {
			self.outcome.send_replace(Some(Err(
				error.unwrap_or_else(|| tg::error!("the process connection closed"))
			)));
		}
	}
}

pub(super) fn matches_read(initial: &read::Arg, arg: &read::Arg) -> bool {
	initial.streams == arg.streams
		&& initial.position.unwrap_or(std::io::SeekFrom::Start(0))
			== arg.position.unwrap_or(std::io::SeekFrom::Start(0))
		&& initial.length == arg.length
		&& initial.size == arg.size
		&& initial.timeout == arg.timeout
}
