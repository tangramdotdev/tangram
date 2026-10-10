use {
	super::*,
	crate::process::stdio::{read, write},
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_futures::{stream::Ext as _, task::Task},
	tokio::sync::{Mutex, mpsc, oneshot, watch},
	tokio_stream::wrappers::ReceiverStream,
};

mod task;

#[derive(Clone)]
pub struct Connection {
	inner: Arc<Inner>,
}

struct Inner {
	detached: AtomicBool,
	initial: Session,
	instance: tg::instance::dynamic::Instance,
	session: Mutex<Session>,
}

#[derive(Clone)]
struct Session {
	initial: Arc<std::sync::Mutex<Vec<read::Arg>>>,
	outcome: watch::Receiver<Option<tg::Result<tg::process::outcome::Data>>>,
	output: watch::Receiver<Option<tg::process::spawn::Output>>,
	sender: mpsc::Sender<task::Message>,
	status: watch::Sender<Status>,
	task: Arc<Task<()>>,
	updates: mpsc::UnboundedSender<task::Update>,
}

#[derive(Clone, Default)]
struct Status {
	closed: bool,
	error: Option<tg::Error>,
}

struct Response {
	id: u64,
	receiver: oneshot::Receiver<tg::Result<ServerResponseOutput>>,
	updates: mpsc::UnboundedSender<task::Update>,
}

struct ReadGuard {
	ended: Arc<AtomicBool>,
	id: u64,
	task: Option<Task<()>>,
	updates: mpsc::UnboundedSender<task::Update>,
}

impl Connection {
	pub(crate) async fn open<I: tg::Instance>(
		instance: &I,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		let (session, progress) = Session::open(instance, arg).await?;
		let connection = Self::with_session(instance, session.clone());
		let progress = progress
			.then(move |event| {
				let session = session.clone();
				async move {
					if matches!(event, Ok(tg::progress::Event::Output(_))) {
						session.ready().await;
					}
					event
				}
			})
			.boxed();
		Ok((connection, progress))
	}

	#[must_use]
	fn with_session<I: tg::Instance>(instance: &I, session: Session) -> Self {
		let inner = Inner {
			detached: AtomicBool::new(false),
			initial: session.clone(),
			instance: tg::instance::dynamic::Instance::new(instance.clone()),
			session: Mutex::new(session),
		};
		Self {
			inner: Arc::new(inner),
		}
	}

	pub(crate) async fn request(&self, arg: ClientRequestArg) -> tg::Result<ServerResponseOutput> {
		let response = {
			let mut session = self.inner.session.lock().await;
			self.ensure_session(&mut session, None).await?;
			session.start_request(arg).await?
		};
		let output = response
			.await?
			.ok_or_else(|| tg::error!("the process connection closed before the response"))?;
		Ok(output)
	}

	async fn ensure_session(
		&self,
		session: &mut Session,
		read: Option<read::Arg>,
	) -> tg::Result<()> {
		if self.detached() {
			return Err(tg::error!("the process connection was detached"));
		}
		if !session.closed() {
			return Ok(());
		}
		// Reopen the selected process, and include the read that requires this connection.
		let mut arg = session.arg()?;
		arg.reads = read.into_iter().map(|arg| (1, arg)).collect();
		let (next, progress) = Session::open(&self.inner.instance, arg).await?;
		progress
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("missing the connect output"))?;
		*session = next;
		Ok(())
	}

	pub(crate) async fn wait(&self) -> tg::Result<tg::process::outcome::Data> {
		loop {
			let session = self.inner.session.lock().await.clone();
			session.ready().await;
			match session.wait().await {
				Ok(outcome) => return Ok(outcome),
				Err(error) if session.error().is_some() || self.detached() => return Err(error),
				Err(_) => {},
			}
			let mut session = self.inner.session.lock().await;
			self.ensure_session(&mut session, None).await?;
		}
	}

	pub(crate) async fn detach(&self) -> tg::Result<()> {
		if self.detached() {
			return Ok(());
		}
		let session = self.inner.session.lock().await;
		if !session.closed() {
			session.detach().await?;
		}
		self.inner.detached.store(true, Ordering::SeqCst);
		Ok(())
	}

	#[must_use]
	pub(crate) fn detached(&self) -> bool {
		self.inner.detached.load(Ordering::SeqCst)
	}

	pub(crate) async fn read(
		&self,
		arg: read::Arg,
		input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<read::ServerMessage>>> {
		if self.inner.initial.has_initial(&arg) {
			return self.inner.initial.read(arg, input).await;
		}
		let mut session = self.inner.session.lock().await;
		self.ensure_session(&mut session, Some(arg.clone())).await?;
		session.read(arg, input).await
	}

	pub(crate) async fn close_initial(&self, stream: tg::process::stdio::Stream) {
		self.inner.initial.close_initial(stream).await;
	}

	pub(crate) async fn write(
		&self,
		arg: write::stream::Arg,
		input: BoxStream<'static, tg::Result<write::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<write::ServerMessage>>> {
		let mut input = input
			.try_filter_map(|message| async move {
				match message {
					write::ClientMessage::Ack(_) => Ok(None),
					write::ClientMessage::Request(request) => Ok(Some(request)),
				}
			})
			.boxed();
		let connection = self.clone();
		let output = stream::once(async move {
			let Some(request) = input.try_next().await? else {
				return Ok::<_, tg::Error>(stream::empty().boxed());
			};
			// Bind this write attempt to one session so its retry preserves request order.
			let (session, first) = {
				let mut session = connection.inner.session.lock().await;
				connection.ensure_session(&mut session, None).await?;
				let first = Self::start_write(&session, &arg, request).await;
				(session.clone(), first)
			};
			let remaining = input.and_then(move |request| {
				let session = session.clone();
				let arg = arg.clone();
				async move { Ok(Self::start_write(&session, &arg, request).await) }
			});
			let output = stream::once(futures::future::ok(first))
				.chain(remaining)
				.try_buffered(tg::process::stdio::Config::default().message_capacity())
				.take_while(|result| futures::future::ready(!matches!(result, Ok(None))))
				.try_filter_map(|message| futures::future::ready(Ok(message)))
				.boxed();
			Ok(output)
		})
		.try_flatten()
		.boxed();
		Ok(output)
	}

	async fn start_write(
		session: &Session,
		arg: &write::stream::Arg,
		request: write::Request,
	) -> BoxFuture<'static, tg::Result<Option<write::ServerMessage>>> {
		let arg = write::Arg {
			data: request.arg,
			location: arg.location.clone(),
			tokens: arg.tokens.clone(),
		};
		let response = session.start_request(ClientRequestArg::Write(arg)).await;
		// Deliver request errors in write order alongside the pending responses.
		let future = async move {
			let Some(output) = response?.await? else {
				return Ok(None);
			};
			let ServerResponseOutput::Write(output) = output else {
				return Err(tg::error!("expected a write response"));
			};
			let response = write::Response {
				error: None,
				id: request.id,
				output: Some(output),
			};
			Ok(Some(write::ServerMessage::Response(response)))
		};
		future.boxed()
	}
}

impl Session {
	async fn open<I: tg::Instance>(
		instance: &I,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		let initial = Arc::new(std::sync::Mutex::new(arg.reads.values().cloned().collect()));
		let (sender, messages) = mpsc::channel(64);
		let (updates, receiver) = mpsc::unbounded_channel();
		let (progress, progress_receiver) = mpsc::channel(64);
		let (output, output_receiver) = watch::channel(None);
		let (outcome, outcome_receiver) = watch::channel(None);
		let (status, _) = watch::channel(Status::default());
		let state =
			task::State::open(instance, arg, progress, output, outcome, status.clone()).await?;
		let update_sender = updates.clone();
		let task = Task::spawn(move |_| async move {
			state.run(messages, receiver, update_sender).await;
		});
		let session = Self {
			initial,
			outcome: outcome_receiver,
			output: output_receiver,
			sender,
			status,
			task: Arc::new(task),
			updates,
		};
		Ok((session, ReceiverStream::new(progress_receiver).boxed()))
	}

	async fn start_request(
		&self,
		arg: ClientRequestArg,
	) -> tg::Result<BoxFuture<'static, tg::Result<Option<ServerResponseOutput>>>> {
		if self.closed() {
			return Ok(futures::future::ready(self.error().map_or(Ok(None), Err)).boxed());
		}
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(task::Message::Request { arg, sender })
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the process connection closed"))??;
		let status = self.status.subscribe();
		let future = async move {
			let mut response = response;
			match (&mut response.receiver).await {
				Ok(result) => result.map(Some),
				Err(_) => status.borrow().error.clone().map_or(Ok(None), Err),
			}
		};
		Ok(future.boxed())
	}

	async fn ready(&self) {
		self.sender.send(task::Message::Ready).await.ok();
	}

	fn closed(&self) -> bool {
		self.status.borrow().closed
	}

	fn error(&self) -> Option<tg::Error> {
		self.status.borrow().error.clone()
	}

	fn arg(&self) -> tg::Result<Arg> {
		let output = self.output.borrow();
		let output = output
			.as_ref()
			.ok_or_else(|| tg::error!("the process was not selected"))?;
		let id = output
			.process
			.as_ref()
			.right()
			.cloned()
			.ok_or_else(|| tg::error!("expected a sandboxed process"))?;
		Ok(Arg {
			lease: output.lease.clone(),
			location: output.location.clone().map(Into::into),
			mode: Mode::Run,
			process: tg::Either::Right(id),
			reads: std::collections::BTreeMap::new(),
			sync: false,
			tokens: output.tokens.clone(),
		})
	}

	fn has_initial(&self, arg: &read::Arg) -> bool {
		self.initial
			.lock()
			.unwrap()
			.iter()
			.any(|initial| task::matches_read(initial, arg))
	}

	async fn close_initial(&self, stream: tg::process::stdio::Stream) {
		self.initial
			.lock()
			.unwrap()
			.retain(|arg| arg.streams != [stream]);
		self.sender
			.send(task::Message::CloseInitial(stream))
			.await
			.ok();
	}

	async fn wait(&self) -> tg::Result<tg::process::outcome::Data> {
		let mut receiver = self.outcome.clone();
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

	async fn detach(&self) -> tg::Result<()> {
		if !self.outcome.borrow().as_ref().is_some_and(Result::is_ok) {
			match self.start_request(ClientRequestArg::Detach).await?.await {
				Ok(Some(ServerResponseOutput::Detach)) => {},
				Ok(_) | Err(_) if self.outcome.borrow().as_ref().is_some_and(Result::is_ok) => {},
				Ok(_) => {
					return Err(tg::error!(
						"the process connection closed before the detach response"
					));
				},
				Err(error) => return Err(error),
			}
		}
		self.status.send_replace(Status {
			closed: true,
			error: Some(tg::error!("the process connection was detached")),
		});
		self.task.abort();
		Ok(())
	}

	async fn read(
		&self,
		arg: read::Arg,
		input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	) -> tg::Result<BoxStream<'static, tg::Result<read::ServerMessage>>> {
		let initial = {
			let mut initial = self.initial.lock().unwrap();
			initial
				.iter()
				.position(|initial| task::matches_read(initial, &arg))
				.map(|index| initial.remove(index))
				.is_some()
		};
		if self.closed() && !initial {
			return self
				.error()
				.map_or_else(|| Ok(stream::empty().boxed()), Err);
		}
		let (sender, receiver) = oneshot::channel();
		self.sender
			.send(task::Message::Read { arg, sender })
			.await
			.map_err(|_| tg::error!("the process connection closed"))?;
		let (id, receiver) = receiver
			.await
			.map_err(|_| tg::error!("the process connection closed"))??;
		let updates = self.updates.clone();
		let task = Task::spawn(move |_| async move {
			Self::read_task(updates, id, input).await;
		});
		let status = self.status.subscribe();
		let errors = stream::once(async move { status.borrow().error.clone().map(Err) })
			.filter_map(futures::future::ready);
		let ended = Arc::new(AtomicBool::new(false));
		let ended_stream = ended.clone();
		let output = ReceiverStream::new(receiver).inspect(move |result| {
			if matches!(result, Ok(read::ServerMessage::Response(_))) {
				ended_stream.store(true, Ordering::SeqCst);
			}
		});
		let guard = ReadGuard {
			ended,
			id,
			task: Some(task),
			updates: self.updates.clone(),
		};
		Ok(output.chain(errors).attach(guard).boxed())
	}

	async fn read_task(
		updates: mpsc::UnboundedSender<task::Update>,
		id: u64,
		mut input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	) {
		loop {
			let message = tokio::select! {
				message = input.next() => message,
				() = updates.closed() => return,
			};
			let Some(message) = message else {
				updates.send(task::Update::Close(id)).ok();
				return;
			};
			let ended = matches!(message, Ok(read::ClientMessage::Ack) | Err(_));
			if updates.send(task::Update::Read { id, message }).is_err() || ended {
				return;
			}
		}
	}
}

impl Drop for Response {
	fn drop(&mut self) {
		self.updates.send(task::Update::Remove(self.id)).ok();
	}
}

impl Drop for ReadGuard {
	fn drop(&mut self) {
		if self.ended.load(Ordering::SeqCst) {
			self.task.take().unwrap().detach();
		} else {
			self.updates.send(task::Update::Close(self.id)).ok();
		}
	}
}
