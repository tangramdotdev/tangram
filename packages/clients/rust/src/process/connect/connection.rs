use {
	super::{session::Session, *},
	crate::process::stdio::{read, write},
	futures::{FutureExt as _, TryStreamExt as _, future::BoxFuture, stream},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tokio::sync::Mutex,
};

#[derive(Clone)]
pub struct Connection {
	inner: Arc<Inner>,
}

struct Inner {
	detached: AtomicBool,
	handle: tg::handle::dynamic::Handle,
	initial: Session,
	session: Mutex<Session>,
}

impl Connection {
	pub(crate) async fn open<H: tg::Handle>(
		handle: &H,
		arg: Arg,
	) -> tg::Result<(
		Self,
		BoxStream<'static, tg::Result<tg::progress::Event<tg::process::spawn::Output>>>,
	)> {
		let (session, progress) = Session::open(handle, arg).await?;
		let connection = Self::with_session(handle, session.clone());
		let progress = progress
			.then(move |event| {
				let session = session.clone();
				async move {
					if matches!(event, Ok(tg::progress::Event::Output(_))) {
						session.confirm().await;
					}
					event
				}
			})
			.boxed();
		Ok((connection, progress))
	}

	#[must_use]
	pub(super) fn with_session<H: tg::Handle>(handle: &H, session: Session) -> Self {
		let inner = Inner {
			detached: AtomicBool::new(false),
			handle: tg::handle::dynamic::Handle::new(handle.clone()),
			initial: session.clone(),
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
		let (next, progress) = Session::open(&self.inner.handle, arg).await?;
		progress
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("missing the connect output"))?;
		*session = next;
		Ok(())
	}

	pub(crate) async fn wait(&self) -> tg::Result<tg::process::wait::Output> {
		loop {
			let session = self.inner.session.lock().await.clone();
			session.confirm().await;
			match session.wait().await {
				Ok(output) => return Ok(output),
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
				.try_buffered(tg::process::stdio::flow::MAX_CHUNKS)
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
