use {
	super::*,
	futures::{TryStreamExt as _, stream},
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

#[derive(Clone)]
pub struct Connection {
	state: Arc<State>,
	task: Arc<Task<()>>,
}

struct State {
	detached: AtomicBool,
	error: Mutex<Option<tg::Error>>,
	initial: Mutex<
		Vec<(
			u64,
			tg::process::stdio::read::Arg,
			mpsc::Receiver<tg::Result<tg::process::stdio::read::ServerMessage>>,
		)>,
	>,
	next_id: AtomicU64,
	reads: Mutex<BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>>>,
	requests: Mutex<BTreeMap<u64, oneshot::Sender<tg::Result<ServerResponseOutput>>>>,
	sender: mpsc::Sender<tg::Result<ClientMessage>>,
	wait: watch::Sender<Option<tg::Result<tg::process::wait::Output>>>,
	writes:
		Mutex<BTreeMap<u64, mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>>>,
}

struct ReadGuard {
	ended: Arc<AtomicBool>,
	id: u64,
	state: Arc<State>,
	task: Option<Task<()>>,
}

impl Connection {
	pub(crate) async fn open<H: tg::Handle>(
		handle: &H,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		let (sender, receiver) = mpsc::channel(64);
		let (progress, progress_receiver) = mpsc::channel(64);
		let (wait, _) = watch::channel(None);
		let mut initial = Vec::new();
		let mut reads = BTreeMap::new();
		for (&id, arg) in &arg.reads {
			let (sender, receiver) = mpsc::channel(4);
			reads.insert(id, sender);
			initial.push((id, arg.clone(), receiver));
		}
		let next_id = arg.reads.keys().last().copied().unwrap_or(0) + 1;
		let request = ClientRequest {
			arg: ClientRequestArg::Connect(arg),
			id: 0,
		};
		sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.unwrap();
		let output = handle
			.connect_process(ReceiverStream::new(receiver).boxed())
			.await?;
		let state = Arc::new(State {
			detached: AtomicBool::new(false),
			error: Mutex::new(None),
			initial: Mutex::new(initial),
			next_id: AtomicU64::new(next_id),
			reads: Mutex::new(reads),
			requests: Mutex::new(BTreeMap::new()),
			sender,
			wait,
			writes: Mutex::new(BTreeMap::new()),
		});
		let state_task = state.clone();
		let task = Task::spawn(move |_| async move {
			let mut progress = Some(progress);
			let result = Self::task(&state_task, output, &mut progress).await;
			let error = result
				.err()
				.unwrap_or_else(|| tg::error!("the process connection closed"));
			if let Some(progress) = progress {
				progress.send(Err(error.clone())).await.ok();
			}
			state_task.fail(error);
		});
		let connection = Self {
			state,
			task: Arc::new(task),
		};
		Ok((connection, ReceiverStream::new(progress_receiver).boxed()))
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
				ServerMessage::Notification(ServerNotification::Error(notification)) => {
					let error: tg::Error = notification.error.try_into()?;
					if let Some(sender) = state.reads.lock().unwrap().remove(&notification.id) {
						sender.try_send(Err(error.clone())).ok();
					}
					if let Some(sender) = state.writes.lock().unwrap().remove(&notification.id) {
						sender.try_send(Err(error)).ok();
					}
				},
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
					if let Some(sender) = reads.get(&notification.id)
						&& let Err(mpsc::error::TrySendError::Full(_)) =
							sender.try_send(Ok(notification.message))
					{
						return Err(tg::error!("the process read buffer is full"));
					}
				},
				ServerMessage::Notification(ServerNotification::Wait(output)) => {
					state.wait.send_replace(Some(Ok(output)));
				},
				ServerMessage::Notification(ServerNotification::Write(notification)) => {
					let mut writes = state.writes.lock().unwrap();
					let end = matches!(
						&notification.message,
						tg::process::stdio::write::ServerMessage::Response(
							tg::process::stdio::write::ServerResponse::End
								| tg::process::stdio::write::ServerResponse::Write(
									tg::process::stdio::write::Output { closed: true, .. }
								)
						)
					);
					if let Some(sender) = writes.get(&notification.id)
						&& let Err(mpsc::error::TrySendError::Full(_)) =
							sender.try_send(Ok(notification.message))
					{
						return Err(tg::error!("the process write buffer is full"));
					}
					if end {
						writes.remove(&notification.id);
					}
				},
				ServerMessage::Response(response) => {
					state
						.sender
						.try_send(Ok(ClientMessage::Ack(Ack { id: response.id })))
						.ok();
					let result = match (response.error, response.output) {
						(None, Some(output)) => Ok(output),
						(Some(error), None) => Err(error.try_into()?),
						_ => Err(tg::error!("invalid process response")),
					};
					if response.id == 0 {
						let ServerResponseOutput::Connect(output) = result? else {
							return Err(tg::error!("expected a connect response"));
						};
						let progress = progress
							.take()
							.ok_or_else(|| tg::error!("duplicate connect response"))?;
						progress
							.send(Ok(tg::progress::Event::Output(output)))
							.await
							.map_err(|_| tg::error!("the spawn receiver closed"))?;
					} else if let Some(sender) = state.requests.lock().unwrap().remove(&response.id)
					{
						sender.send(result).ok();
					}
				},
			}
		}
		Ok(())
	}

	pub(crate) async fn request(&self, arg: ClientRequestArg) -> tg::Result<ServerResponseOutput> {
		let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
		self.request_with_id(id, arg).await
	}

	async fn request_with_id(
		&self,
		id: u64,
		arg: ClientRequestArg,
	) -> tg::Result<ServerResponseOutput> {
		let subscription = matches!(arg, ClientRequestArg::Read(_) | ClientRequestArg::Write(_));
		let (sender, receiver) = oneshot::channel();
		{
			let error = self.state.error.lock().unwrap();
			if let Some(error) = &*error {
				return Err(error.clone());
			}
			let mut requests = self.state.requests.lock().unwrap();
			if requests.len() >= 64 && !matches!(arg, ClientRequestArg::Detach) {
				return Err(tg::error!("too many process requests"));
			}
			requests.insert(id, sender);
		}
		let guard = scopeguard::guard(self.state.clone(), move |state| {
			state.requests.lock().unwrap().remove(&id);
			if subscription {
				tokio::spawn(async move {
					state.close(id).await;
				});
			}
		});
		let request = ClientRequest { arg, id };
		self.state
			.sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		let output = receiver
			.await
			.map_err(|_| tg::error!("the process connection closed before the response"))??;
		scopeguard::ScopeGuard::into_inner(guard);
		Ok(output)
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

	#[must_use]
	pub(crate) fn detached(&self) -> bool {
		self.state.detached.load(Ordering::SeqCst)
	}

	pub(crate) async fn detach(&self) -> tg::Result<()> {
		if self.detached() {
			return Ok(());
		}
		if !self.state.wait.borrow().as_ref().is_some_and(Result::is_ok) {
			match self.request(ClientRequestArg::Detach).await {
				Ok(ServerResponseOutput::Detach) => (),
				Ok(_) => return Err(tg::error!("expected a detach response")),
				Err(_) if self.state.wait.borrow().as_ref().is_some_and(Result::is_ok) => (),
				Err(error) => return Err(error),
			}
		}
		self.state.detached.store(true, Ordering::SeqCst);
		self.state
			.fail(tg::error!("the process connection was detached"));
		self.task.abort();
		Ok(())
	}

	pub(crate) async fn read(
		&self,
		arg: tg::process::stdio::read::Arg,
		mut input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>> {
		let initial = {
			let mut initial = self.state.initial.lock().unwrap();
			initial
				.iter()
				.position(|(_, initial, _)| {
					initial.streams == arg.streams
						&& initial.position == arg.position
						&& initial.length == arg.length
						&& initial.size == arg.size
						&& initial.timeout == arg.timeout
				})
				.map(|index| initial.remove(index))
		};
		let (id, receiver) = if let Some((id, _, receiver)) = initial {
			(id, receiver)
		} else {
			let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
			let (sender, receiver) = mpsc::channel(4);
			self.state.reads.lock().unwrap().insert(id, sender);
			if let Err(error) = self.request_with_id(id, ClientRequestArg::Read(arg)).await {
				self.state.reads.lock().unwrap().remove(&id);
				return Err(error);
			}
			(id, receiver)
		};
		let state = self.state.clone();
		let task =
			Task::spawn(move |_| async move {
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
						Ok(message) => message,
						Err(error) => {
							if let Some(sender) = state.reads.lock().unwrap().remove(&id) {
								sender.try_send(Err(error)).ok();
							}
							state.close(id).await;
							return;
						},
					};
					let end = matches!(
						&message,
						tg::process::stdio::read::ClientMessage::Response(
							tg::process::stdio::read::ClientResponse::End
						)
					);
					let message = ClientMessage::Notification(ClientNotification::Read(
						ReadClientNotification { id, message },
					));
					if state.sender.send(Ok(message)).await.is_err() {
						return;
					}
					if end {
						state.reads.lock().unwrap().remove(&id);
						return;
					}
				}
			});
		let state = self.state.clone();
		let errors = stream::once(async move {
			Err(state
				.error
				.lock()
				.unwrap()
				.clone()
				.unwrap_or_else(|| tg::error!("the process read closed before EOF")))
		});
		let ended = Arc::new(AtomicBool::new(false));
		let ended_stream = ended.clone();
		let output = ReceiverStream::new(receiver).inspect(move |result| {
			if matches!(
				result,
				Ok(tg::process::stdio::read::ServerMessage::Request(
					tg::process::stdio::read::ServerRequest::End
				))
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
		Ok(output.chain(errors).attach(guard).boxed())
	}

	pub(crate) async fn write(
		&self,
		arg: tg::process::stdio::write::Arg,
		mut input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>> {
		let id = self.state.next_id.fetch_add(1, Ordering::Relaxed);
		let (sender, receiver) = mpsc::channel(4);
		self.state.writes.lock().unwrap().insert(id, sender);
		if let Err(error) = self.request_with_id(id, ClientRequestArg::Write(arg)).await {
			self.state.writes.lock().unwrap().remove(&id);
			return Err(error);
		}
		let state = self.state.clone();
		let task = Task::spawn(move |_| async move {
			while let Some(message) = input.next().await {
				let message = match message {
					Ok(message) => message,
					Err(error) => {
						if let Some(sender) = state.writes.lock().unwrap().remove(&id) {
							sender.try_send(Err(error)).ok();
						}
						break;
					},
				};
				let end = matches!(
					&message,
					tg::process::stdio::write::ClientMessage::Request(
						tg::process::stdio::write::ClientRequest::End { .. }
					)
				);
				let message = ClientMessage::Notification(ClientNotification::Write(
					WriteClientNotification { id, message },
				));
				if state.sender.send(Ok(message)).await.is_err() || end {
					return;
				}
			}
			state.close(id).await;
		});
		let state = self.state.clone();
		let guard = scopeguard::guard((), move |()| {
			if state.writes.lock().unwrap().contains_key(&id) {
				tokio::spawn(async move {
					state.close(id).await;
				});
			}
		});
		let state = self.state.clone();
		let errors = stream::once(async move {
			Err(state
				.error
				.lock()
				.unwrap()
				.clone()
				.unwrap_or_else(|| tg::error!("the process write closed before EOF")))
		});
		Ok(ReceiverStream::new(receiver)
			.chain(errors)
			.attach((task, guard))
			.boxed())
	}
}

impl State {
	async fn close(&self, id: u64) {
		self.reads.lock().unwrap().remove(&id);
		self.writes.lock().unwrap().remove(&id);
		let request = ClientRequest {
			arg: ClientRequestArg::Close(id),
			id: self.next_id.fetch_add(1, Ordering::Relaxed),
		};
		self.sender
			.send(Ok(ClientMessage::Request(request)))
			.await
			.ok();
	}

	fn fail(&self, error: tg::Error) {
		*self.error.lock().unwrap() = Some(error.clone());
		for (_, sender) in std::mem::take(&mut *self.requests.lock().unwrap()) {
			sender.send(Err(error.clone())).ok();
		}
		self.reads.lock().unwrap().clear();
		self.writes.lock().unwrap().clear();
		if self.wait.borrow().is_none() {
			self.wait.send_replace(Some(Err(error)));
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
