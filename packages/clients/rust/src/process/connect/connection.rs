use {
	super::*,
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_futures::task::Task,
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
	instance: tg::instance::dynamic::Instance,
	session: Mutex<Session>,
}

#[derive(Clone)]
struct Session {
	outcome: watch::Receiver<Option<tg::Result<tg::process::outcome::Data>>>,
	output: watch::Receiver<Option<tg::process::spawn::Output>>,
	sender: mpsc::Sender<task::Message>,
	status: watch::Sender<Status>,
	task: Arc<Task<()>>,
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
			self.ensure_session(&mut session).await?;
			session.start_request(arg).await?
		};
		let output = response
			.await?
			.ok_or_else(|| tg::error!("the process connection closed before the response"))?;
		Ok(output)
	}

	async fn ensure_session(&self, session: &mut Session) -> tg::Result<()> {
		if self.detached() {
			return Err(tg::error!("the process connection was detached"));
		}
		if !session.closed() {
			return Ok(());
		}
		// Reopen the selected process.
		let arg = session.arg()?;
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
			self.ensure_session(&mut session).await?;
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
}

impl Session {
	async fn open<I: tg::Instance>(
		instance: &I,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		let config = instance.arg().process.connect;
		let (sender, messages) = mpsc::channel(config.message_capacity);
		let (update_sender, receiver) = mpsc::unbounded_channel();
		let (progress, progress_receiver) = mpsc::channel(config.event_capacity);
		let (output, output_receiver) = watch::channel(None);
		let (outcome, outcome_receiver) = watch::channel(None);
		let (status, _) = watch::channel(Status::default());
		let state = task::State::open(
			instance,
			arg,
			config,
			progress,
			output,
			outcome,
			status.clone(),
		)
		.await?;
		let task = Task::spawn(move |_| async move {
			state.run(messages, receiver, update_sender).await;
		});
		let session = Self {
			outcome: outcome_receiver,
			output: output_receiver,
			sender,
			status,
			task: Arc::new(task),
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
			lease: output.lease,
			location: output.location.clone().map(Into::into),
			mode: Mode::Run,
			nonce: None,
			open: Vec::new(),
			process: tg::Either::Right(id),
			push: false,
			tokens: output.tokens.clone(),
		})
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
}

impl Drop for Response {
	fn drop(&mut self) {
		self.updates.send(task::Update::Remove(self.id)).ok();
	}
}
