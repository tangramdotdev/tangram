use {
	crate::Session,
	dashmap::DashMap,
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	std::{marker::PhantomData, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
};

pub(crate) trait Input<O> {
	fn kind(&self) -> InputKind<'_>;

	fn create_ack_message(id: String) -> O;
}

pub(crate) enum InputKind<'a> {
	Ack { id: &'a str },
	Message { id: Option<&'a str> },
}

pub(crate) trait Output {
	fn id(&self) -> Option<&str>;
}

pub(crate) struct Stream<I, O> {
	_marker: PhantomData<fn() -> O>,
	inner: BoxStream<'static, tg::Result<I>>,
	inbox: Arc<DashMap<String, ()>>,
	inbox_ttl: Duration,
	send_task: tokio::task::JoinHandle<()>,
	sender: Sender<I, O>,
}

pub(crate) struct Sender<I, O> {
	_marker: PhantomData<fn() -> I>,
	inner: tokio::sync::mpsc::Sender<O>,
	outbox: Arc<DashMap<String, O>>,
}

pub(crate) struct Options {
	pub retry: tangram_futures::retry::Options,
	pub timeout: Duration,
}

pub(crate) struct StreamOptions {
	pub inbox_ttl: Duration,
	pub retry: tangram_futures::retry::Options,
}

pub(crate) struct SendControlRequestArg<I, O, Response, ResponseFn, AckFn> {
	pub ack: AckFn,
	pub client_subject: String,
	pub options: Options,
	pub request: O,
	pub response: ResponseFn,
	pub server_subject: String,
	pub marker: PhantomData<fn() -> (I, Response)>,
}

pub(crate) fn id() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}

impl<I, O> Stream<I, O>
where
	I: Input<O> + Send + 'static,
	O: Output + Clone + Send + Sync + 'static,
{
	pub(crate) fn new(
		stream: BoxStream<'static, tg::Result<I>>,
		sender: tokio::sync::mpsc::Sender<O>,
		options: StreamOptions,
	) -> Self {
		let StreamOptions { inbox_ttl, retry } = options;
		let inbox = Arc::new(DashMap::new());
		let sender = Sender {
			inner: sender,
			outbox: Arc::new(DashMap::new()),
			_marker: PhantomData,
		};
		let send_task = tokio::spawn({
			let sender = sender.clone();
			async move {
				let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
				while retries.next().await.is_some() {
					for message in sender.messages() {
						if sender.send(message).await.is_err() {
							return;
						}
					}
				}
			}
		});
		Self {
			inner: stream,
			inbox,
			inbox_ttl,
			send_task,
			sender,
			_marker: PhantomData,
		}
	}

	pub(crate) async fn recv(&mut self) -> tg::Result<Option<I>> {
		loop {
			let Some(message) = self.inner.try_next().await? else {
				return Ok(None);
			};

			match message.kind() {
				InputKind::Ack { id } => {
					self.sender.remove(id);
					if self.inbox.contains_key(id) {
						let id = id.to_owned();
						let inbox = self.inbox.clone();
						let ttl = self.inbox_ttl;
						tokio::spawn(async move {
							tokio::time::sleep(ttl).await;
							inbox.remove(&id);
						});
					}
				},
				InputKind::Message { id } => {
					if let Some(id) = id.map(str::to_owned) {
						self.sender.send(I::create_ack_message(id.clone())).await?;
						if self.inbox.insert(id, ()).is_some() {
							continue;
						}
					}
					return Ok(Some(message));
				},
			}
		}
	}

	pub(crate) fn sender(&self) -> Sender<I, O> {
		self.sender.clone()
	}
}

impl<I, O> Drop for Stream<I, O> {
	fn drop(&mut self) {
		self.send_task.abort();
	}
}

impl<I, O> Clone for Sender<I, O> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
			outbox: self.outbox.clone(),
			_marker: PhantomData,
		}
	}
}

impl<I, O> Sender<I, O>
where
	O: Output + Clone + Send + Sync + 'static,
{
	pub(crate) async fn send(&self, message: O) -> tg::Result<()> {
		if let Some(id) = message.id() {
			self.outbox.insert(id.to_owned(), message.clone());
		}
		self.inner
			.send(message)
			.await
			.map_err(|_| tg::error!("failed to send the control message"))?;
		Ok(())
	}

	pub(crate) fn remove(&self, id: &str) {
		self.outbox.remove(id);
	}

	fn messages(&self) -> Vec<O> {
		self.outbox
			.iter()
			.map(|entry| entry.value().clone())
			.collect()
	}
}

pub(crate) fn stream_options() -> StreamOptions {
	StreamOptions {
		inbox_ttl: Duration::from_mins(1),
		retry: tangram_futures::retry::Options {
			backoff: Duration::from_secs(1),
			jitter: Duration::ZERO,
			max_delay: Duration::from_secs(1),
			max_retries: u64::MAX,
		},
	}
}

impl Session {
	pub(crate) async fn send_control_request<I, O, Response>(
		&self,
		arg: SendControlRequestArg<
			I,
			O,
			Response,
			impl Fn(I) -> tg::Result<Option<(String, Response)>> + Clone,
			impl Fn(String) -> O + Clone,
		>,
	) -> tg::Result<Response>
	where
		I: Payload,
		O: Clone + Payload,
	{
		let SendControlRequestArg {
			ack,
			client_subject,
			options,
			request,
			response,
			server_subject,
			marker: _,
		} = arg;
		let server = self.server.clone();
		let Options { retry, timeout } = options;

		let responses = server
			.messenger
			.subscribe::<I>(client_subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the response"))?;
		let mut responses = std::pin::pin!(responses);

		let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
		loop {
			if retries.next().await.is_none() {
				return Err(tg::error!("timed out waiting for the response"));
			}

			server
				.messenger
				.publish(server_subject.clone(), request.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the request"))?;

			let result = tokio::time::timeout(timeout, async {
				loop {
					let message = responses
						.next()
						.await
						.ok_or_else(|| tg::error!("the response stream ended"))?
						.map_err(|source| tg::error!(!source, "failed to receive the response"))?;
					let Some((id, response)) = response(message.payload)? else {
						continue;
					};
					let ack = ack(id);
					server
						.messenger
						.publish(server_subject.clone(), ack)
						.await
						.inspect_err(|error| {
							tracing::error!(%error, "failed to ack the response");
						})
						.ok();
					return Ok::<_, tg::Error>(response);
				}
			})
			.await;

			match result {
				Ok(Ok(response)) => return Ok(response),
				Ok(Err(error)) => return Err(error),
				Err(_) => (),
			}
		}
	}
}
