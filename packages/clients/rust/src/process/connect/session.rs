use {
	super::*,
	futures::{FutureExt as _, TryStreamExt as _, stream},
	std::{
		collections::BTreeMap,
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, AtomicU64, Ordering},
		},
	},
	tangram_futures::{stream::Ext as _, task::Task},
	tokio::sync::{mpsc, oneshot, watch},
	tokio_stream::wrappers::ReceiverStream,
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(super) struct Session {
	state: Arc<State>,
	task: Arc<Task<()>>,
}

struct State {
	acks: mpsc::Sender<tg::Result<ClientMessage>>,
	closed: AtomicBool,
	confirmed: AtomicBool,
	error: Mutex<Option<tg::Error>>,
	initial: Mutex<
		Vec<(
			u64,
			tg::process::stdio::read::Arg,
			mpsc::Receiver<tg::Result<tg::process::stdio::read::ServerMessage>>,
		)>,
	>,
	next_id: AtomicU64,
	output: Mutex<Option<tg::process::spawn::Output>>,
	reads: Mutex<BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>>>,
	requests: Mutex<BTreeMap<u64, oneshot::Sender<tg::Result<ServerResponseOutput>>>>,
	sender: mpsc::Sender<tg::Result<ClientMessage>>,
	wait: watch::Sender<Option<tg::Result<tg::process::wait::Output>>>,
}

struct ReadGuard {
	ended: Arc<AtomicBool>,
	id: u64,
	state: Arc<State>,
	task: Option<Task<()>>,
}

impl Session {
	pub(crate) async fn open<H: tg::Handle>(
		handle: &H,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		// Create the request and read channels.
		let (acks, ack_receiver) = mpsc::channel(64);
		let (sender, receiver) = mpsc::channel(64);
		let (progress, progress_receiver) = mpsc::channel(64);
		let (wait, _) = watch::channel(None);
		let mut initial = Vec::new();
		let mut reads = BTreeMap::new();
		for (&id, arg) in &arg.reads {
			let (sender, receiver) = mpsc::channel(tg::process::stdio::flow::CHANNEL_CAPACITY);
			reads.insert(id, sender);
			initial.push((id, arg.clone(), receiver));
		}

		// Send the opening request.
		let next_id = arg.reads.keys().last().copied().unwrap_or(0) + 1;
		let request = ClientRequest {
			arg: ClientRequestArg::Connect(arg),
			id: 0,
		};
		sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.unwrap();
		let input = stream::select(
			ReceiverStream::new(ack_receiver),
			ReceiverStream::new(receiver),
		);
		let output = handle.connect_process(input.boxed()).await?;

		// Receive the process responses and notifications.
		let state = State {
			acks,
			closed: AtomicBool::new(false),
			confirmed: AtomicBool::new(false),
			error: Mutex::new(None),
			initial: Mutex::new(initial),
			next_id: AtomicU64::new(next_id),
			output: Mutex::new(None),
			reads: Mutex::new(reads),
			requests: Mutex::new(BTreeMap::new()),
			sender,
			wait,
		};
		let state = Arc::new(state);
		let state_task = state.clone();
		let task = Task::spawn(move |_| async move {
			let mut progress = Some(progress);
			let result = Self::task(&state_task, output, &mut progress).await;
			let error = result.err();
			if let Some(progress) = progress {
				progress
					.send(Err(error.clone().unwrap_or_else(|| {
						tg::error!("the process connection closed before its response")
					})))
					.await
					.ok();
			}
			state_task.fail(error);
		});

		let connection = Self {
			state,
			task: Arc::new(task),
		};
		let progress = ReceiverStream::new(progress_receiver).boxed();

		Ok((connection, progress))
	}

	async fn task(
		state: &State,
		mut output: BoxStream<'static, tg::Result<ServerMessage>>,
		progress: &mut Option<
			mpsc::Sender<tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
		>,
	) -> tg::Result<()> {
		while let Some(message) = output.try_next().await? {
			match message {
				ServerMessage::Ack(_) => (),

				ServerMessage::Notification(ServerNotification::Progress(event)) => {
					let event =
						event.try_map_output(|()| Err(tg::error!("unexpected progress output")))?;
					if let Some(progress) = progress.as_ref() {
						progress
							.send(Ok(event))
							.await
							.map_err(|_| tg::error!("the spawn receiver closed"))?;
					}
				},
				ServerMessage::Notification(ServerNotification::Read(notification)) => {
					let reads = state.reads.lock().unwrap();
					let message =
						tg::process::stdio::read::ServerMessage::Notification(notification.event);
					if let Some(sender) = reads.get(&notification.id)
						&& let Err(mpsc::error::TrySendError::Full(_)) =
							sender.try_send(Ok(message))
					{
						return Err(tg::error!("the process read buffer is full"));
					}
				},
				ServerMessage::Notification(ServerNotification::Wait(output)) => {
					state.wait.send_replace(Some(Ok(output)));
				},
				ServerMessage::Response(response) => {
					let result = match (response.error, response.output) {
						(None, Some(output)) => Ok(output),
						(Some(error), None) => Err(error.try_into()?),
						_ => Err(tg::error!("invalid process response")),
					};
					let read = state.reads.lock().unwrap().get(&response.id).cloned();
					if let Some(sender) = read {
						let result = match result {
							Ok(ServerResponseOutput::Read(output)) => {
								Ok(tg::process::stdio::read::ServerMessage::Response(output))
							},
							Ok(_) => Err(tg::error!("expected a read response")),
							Err(error) => Err(error),
						};
						let failed = result.is_err();
						match sender.try_send(result) {
							Ok(()) if !failed => {
								// A read response is acknowledged only after its buffered chunks are consumed.
								continue;
							},
							Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => (),
							Err(mpsc::error::TrySendError::Full(_)) => {
								return Err(tg::error!("the process read buffer is full"));
							},
						}
						state.reads.lock().unwrap().remove(&response.id);
						state
							.acks
							.send(Ok(ClientMessage::Ack(Ack { id: response.id })))
							.await
							.ok();
						continue;
					}
					if response.id == 0 {
						let ServerResponseOutput::Connect(output) = result? else {
							return Err(tg::error!("expected a connect response"));
						};
						*state.output.lock().unwrap() = Some(output.clone());
						let progress = progress
							.take()
							.ok_or_else(|| tg::error!("duplicate connect response"))?;
						progress
							.send(Ok(tg::progress::Event::Output(output)))
							.await
							.map_err(|_| tg::error!("the spawn receiver closed"))?;
					} else {
						state
							.acks
							.send(Ok(ClientMessage::Ack(Ack { id: response.id })))
							.await
							.ok();
						if let Some(sender) = state.requests.lock().unwrap().remove(&response.id) {
							sender.send(result).ok();
						}
					}
				},
			}
		}
		Ok(())
	}

	pub(super) async fn start_request(
		&self,
		arg: ClientRequestArg,
	) -> tg::Result<futures::future::BoxFuture<'static, tg::Result<Option<ServerResponseOutput>>>>
	{
		let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
		let (sender, receiver) = oneshot::channel();
		{
			let mut requests = self.state.requests.lock().unwrap();
			if self.closed() {
				return Ok(futures::future::ready(self.error().map_or(Ok(None), Err)).boxed());
			}
			if requests.len() >= 128 && !matches!(arg, ClientRequestArg::Detach) {
				return Err(tg::error!("too many process requests"));
			}
			requests.insert(id, sender);
		}
		let guard = scopeguard::guard(self.state.clone(), move |state| {
			state.requests.lock().unwrap().remove(&id);
		});
		let request = ClientRequest { arg, id };
		self.state
			.sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.ok();
		self.confirm().await;
		let state = self.state.clone();
		let future = async move {
			let _guard = guard;
			match receiver.await {
				Ok(result) => result.map(Some),
				Err(_) => state.error.lock().unwrap().clone().map_or(Ok(None), Err),
			}
		};
		Ok(future.boxed())
	}

	pub(super) async fn confirm(&self) {
		if !self.state.confirmed.swap(true, Ordering::SeqCst) {
			// Keep the opening acknowledgment behind the operation that caused a reconnect.
			self.state
				.sender
				.send(Ok(ClientMessage::Ack(Ack { id: 0 })))
				.await
				.ok();
		}
	}

	#[must_use]
	pub(super) fn closed(&self) -> bool {
		self.state.closed.load(Ordering::SeqCst)
	}

	#[must_use]
	pub(super) fn error(&self) -> Option<tg::Error> {
		self.state.error.lock().unwrap().clone()
	}

	pub(super) fn arg(&self) -> tg::Result<Arg> {
		let output = self.state.output.lock().unwrap();
		let output = output
			.as_ref()
			.ok_or_else(|| tg::error!("the process was not selected"))?;
		let id = output
			.process
			.as_ref()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("expected a sandboxed process"))?;
		let arg = Arg {
			lease: output.lease.clone(),
			location: output.location.clone().map(Into::into),
			mode: Mode::Run,
			process: tg::Either::Right(id),
			reads: BTreeMap::new(),
			tokens: output.tokens.clone(),
		};
		Ok(arg)
	}

	#[must_use]
	pub(super) fn has_initial(&self, arg: &tg::process::stdio::read::Arg) -> bool {
		self.state
			.initial
			.lock()
			.unwrap()
			.iter()
			.any(|(_, initial, _)| matches_read(initial, arg))
	}

	pub(super) async fn close_initial(&self, stream: tg::process::stdio::Stream) {
		let arg = tg::process::stdio::read::Arg {
			streams: vec![stream],
			..Default::default()
		};
		let initial = {
			let mut initial = self.state.initial.lock().unwrap();
			initial
				.iter()
				.position(|(_, initial, _)| matches_read(initial, &arg))
				.map(|index| initial.remove(index))
		};
		if let Some((id, _, _)) = initial {
			self.state.close(id).await;
		}
	}

	pub(crate) async fn wait(&self) -> tg::Result<tg::process::wait::Output> {
		let mut receiver = self.state.wait.subscribe();
		loop {
			if let Some(result) = receiver.borrow_and_update().clone() {
				return result;
			}
			receiver
				.changed()
				.await
				.map_err(|_| tg::error!("the process connection closed before completion"))?;
		}
	}

	pub(crate) async fn detach(&self) -> tg::Result<()> {
		if !self.state.wait.borrow().as_ref().is_some_and(Result::is_ok) {
			match self.start_request(ClientRequestArg::Detach).await?.await {
				Ok(Some(ServerResponseOutput::Detach)) => (),
				Ok(_) | Err(_) if self.state.wait.borrow().as_ref().is_some_and(Result::is_ok) => {
				},
				Ok(_) => {
					return Err(tg::error!(
						"the process connection closed before the detach response"
					));
				},
				Err(error) => return Err(error),
			}
		}
		self.state
			.fail(Some(tg::error!("the process connection was detached")));
		self.task.abort();
		Ok(())
	}

	pub(crate) async fn read(
		&self,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>> {
		// Open the read request.
		let initial = {
			let mut initial = self.state.initial.lock().unwrap();
			initial
				.iter()
				.position(|(_, initial, _)| matches_read(initial, &arg))
				.map(|index| initial.remove(index))
		};
		let (id, receiver) = if let Some((id, _, receiver)) = initial {
			(id, receiver)
		} else {
			let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
			let (sender, receiver) = mpsc::channel(tg::process::stdio::flow::CHANNEL_CAPACITY);
			{
				let mut reads = self.state.reads.lock().unwrap();
				if self.closed() {
					return self
						.error()
						.map_or_else(|| Ok(stream::empty().boxed()), Err);
				}
				reads.insert(id, sender);
			}
			let request = ClientRequest {
				arg: ClientRequestArg::Read(arg),
				id,
			};
			if self
				.state
				.sender
				.send(Ok(ClientMessage::Request(request)))
				.await
				.is_err()
			{
				self.state.reads.lock().unwrap().remove(&id);
				return Err(tg::error!("the process connection closed"));
			}

			(id, receiver)
		};

		self.confirm().await;

		// Send the stdio messages.
		let state = self.state.clone();
		let task = Task::spawn(move |_| async move {
			Self::read_task(state, id, input).await;
		});
		let state = self.state.clone();
		let errors = stream::once(async move { state.error.lock().unwrap().clone().map(Err) })
			.filter_map(futures::future::ready);
		let ended = Arc::new(AtomicBool::new(false));
		let ended_stream = ended.clone();
		let output = ReceiverStream::new(receiver).inspect(move |result| {
			if matches!(
				result,
				Ok(tg::process::stdio::read::ServerMessage::Response(_))
			) {
				ended_stream.store(true, Ordering::SeqCst);
			}
		});
		let guard = ReadGuard {
			ended,
			id,
			state: self.state.clone(),
			task: Some(task),
		};
		let output = output.chain(errors).attach(guard).boxed();

		Ok(output)
	}

	async fn read_task(
		state: Arc<State>,
		id: u64,
		mut input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) {
		loop {
			let message = tokio::select! {
				message = input.next() => message,
				() = state.sender.closed() => return,
			};
			let Some(message) = message else {
				state.close(id).await;
				return;
			};
			let message = match message {
				Err(error) => {
					if let Some(sender) = state.reads.lock().unwrap().remove(&id) {
						sender.try_send(Err(error)).ok();
					}
					state.close(id).await;
					return;
				},
				Ok(message) => message,
			};
			match message {
				tg::process::stdio::read::ClientMessage::Ack => {
					state
						.acks
						.send(Ok(ClientMessage::Ack(Ack { id })))
						.await
						.ok();
					state.reads.lock().unwrap().remove(&id);
					return;
				},
				tg::process::stdio::read::ClientMessage::Notification(progress) => {
					let notification = ReadClientNotification { id, progress };
					let message =
						ClientMessage::Notification(ClientNotification::Read(notification));
					if state.acks.send(Ok(message)).await.is_err() {
						return;
					}
				},
			}
		}
	}
}

impl State {
	async fn close(&self, id: u64) {
		self.reads.lock().unwrap().remove(&id);
		if self.closed.load(Ordering::SeqCst) {
			return;
		}
		let request = ClientRequest {
			arg: ClientRequestArg::Close(id),
			id: self.next_id.fetch_add(1, Ordering::Relaxed),
		};
		self.sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.ok();
	}

	fn fail(&self, error: Option<tg::Error>) {
		*self.error.lock().unwrap() = error.clone();
		// Close under the same lock used to register requests.
		let mut requests = self.requests.lock().unwrap();
		self.closed.store(true, Ordering::SeqCst);
		for (_, sender) in std::mem::take(&mut *requests) {
			if let Some(error) = &error {
				sender.send(Err(error.clone())).ok();
			}
		}
		self.reads.lock().unwrap().clear();
		if self.wait.borrow().is_none() {
			self.wait.send_replace(Some(Err(
				error.unwrap_or_else(|| tg::error!("the process connection closed"))
			)));
		}
	}
}

impl Drop for ReadGuard {
	fn drop(&mut self) {
		if self.ended.load(Ordering::SeqCst) {
			// Finish forwarding the caller's EOF response before dropping the input task.
			self.task.take().unwrap().detach();
		} else {
			let state = self.state.clone();
			let id = self.id;
			tokio::spawn(async move {
				state.close(id).await;
			});
		}
	}
}

fn matches_read(
	initial: &tg::process::stdio::read::Arg,
	arg: &tg::process::stdio::read::Arg,
) -> bool {
	initial.streams == arg.streams
		&& initial.position.unwrap_or(std::io::SeekFrom::Start(0))
			== arg.position.unwrap_or(std::io::SeekFrom::Start(0))
		&& initial.length == arg.length
		&& initial.size == arg.size
		&& initial.timeout == arg.timeout
}
