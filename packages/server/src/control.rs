use {
	crate::Server,
	dashmap::DashMap,
	futures::{StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::BoxStream},
	std::{
		marker::PhantomData,
		pin::Pin,
		sync::Arc,
		task::{Context, Poll},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
};

#[cfg(test)]
mod tests;

// Acknowledgements confirm receipt only. Requests remain pending until a response; reconnects replay them with the same IDs. Responses retry until acknowledged.
pub(crate) trait Input<O> {
	fn kind(&self) -> InputKind<'_>;

	fn create_ack_message(id: String) -> O;

	fn priority(&self) -> Priority {
		Priority::High
	}
}

pub(crate) enum InputKind<'a> {
	Ack { id: &'a str },
	Message { id: Option<&'a str> },
	Response { id: &'a str },
}

pub(crate) trait Output {
	fn id(&self) -> Option<&str>;

	fn is_request(&self) -> bool {
		false
	}
}

pub(crate) struct Stream<I, O> {
	inbox: Arc<DashMap<String, ()>>,
	inbox_ttl: Duration,
	inner: BoxStream<'static, tg::Result<tg::control::Event<I>>>,
	send_tasks: [tokio::task::JoinHandle<()>; 2],
	sender: Sender<I, O>,
}

pub(crate) struct Sender<I, O> {
	inbox: Arc<DashMap<String, ()>>,
	inner_high: tokio::sync::mpsc::Sender<O>,
	inner_low: tokio::sync::mpsc::Sender<O>,
	notify: Arc<tokio::sync::Notify>,
	outbox: Arc<DashMap<String, OutboxEntry<O>>>,
	outbox_ttl: Option<Duration>,
	responses: Arc<DashMap<String, tokio::sync::oneshot::Sender<I>>>,
}

pub(crate) struct Response<I, O> {
	id: String,
	receiver: tokio::sync::oneshot::Receiver<I>,
	sender: Sender<I, O>,
}

#[derive(Clone)]
struct OutboxEntry<O> {
	acknowledged: bool,
	message: O,
	priority: Priority,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Priority {
	High,
	Low,
}

pub(crate) struct Options {
	pub retry: tangram_futures::retry::Options,
	pub timeout: Duration,
}

pub(crate) struct StreamOptions {
	pub inbox_ttl: Duration,
	pub outbox_ttl: Option<Duration>,
	pub retry: tangram_futures::retry::Options,
}

pub(crate) struct SendControlRequestArg<I, O, Response, AckFn, IsAckFn, ResponseFn> {
	pub ack: AckFn,
	pub client_subject: String,
	pub is_ack: IsAckFn,
	pub marker: PhantomData<fn() -> (I, Response)>,
	pub options: Options,
	pub request: O,
	pub response: ResponseFn,
	pub server_subject: String,
}

enum ReceiveControlRequestOutput<Response> {
	Ack,
	Response { id: String, response: Response },
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
		Self::new_with_priorities(stream, sender.clone(), sender, options)
	}

	pub(crate) fn new_with_priorities(
		stream: BoxStream<'static, tg::Result<I>>,
		sender_high: tokio::sync::mpsc::Sender<O>,
		sender_low: tokio::sync::mpsc::Sender<O>,
		options: StreamOptions,
	) -> Self {
		let stream = stream.map_ok(tg::control::Event::Message).boxed();
		Self::new_reconnecting_with_priorities(stream, sender_high, sender_low, options)
	}

	pub(crate) fn new_reconnecting(
		stream: BoxStream<'static, tg::Result<tg::control::Event<I>>>,
		sender: tokio::sync::mpsc::Sender<O>,
		options: StreamOptions,
	) -> Self {
		Self::new_reconnecting_with_priorities(stream, sender.clone(), sender, options)
	}

	pub(crate) fn new_reconnecting_with_priorities(
		stream: BoxStream<'static, tg::Result<tg::control::Event<I>>>,
		sender_high: tokio::sync::mpsc::Sender<O>,
		sender_low: tokio::sync::mpsc::Sender<O>,
		options: StreamOptions,
	) -> Self {
		let StreamOptions {
			inbox_ttl,
			outbox_ttl,
			retry,
		} = options;
		let inbox = Arc::new(DashMap::new());
		let sender = Sender {
			inbox: inbox.clone(),
			inner_high: sender_high,
			inner_low: sender_low,
			notify: Arc::new(tokio::sync::Notify::new()),
			outbox: Arc::new(DashMap::new()),
			outbox_ttl,
			responses: Arc::new(DashMap::new()),
		};
		let send_tasks = [Priority::High, Priority::Low].map(|priority| {
			let retry = retry.clone();
			let sender = sender.clone();
			tokio::spawn(async move {
				let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
				while retries.next().await.is_some() {
					for message in sender.messages(priority) {
						if sender.send_inner(message, priority).await.is_err() {
							return;
						}
					}
				}
			})
		});
		Self {
			inbox,
			inbox_ttl,
			inner: stream,
			send_tasks,
			sender,
		}
	}

	pub(crate) async fn recv_with_ack(&mut self) -> tg::Result<Option<I>> {
		loop {
			let Some(message) = self.recv_without_ack().await? else {
				return Ok(None);
			};
			match message.kind() {
				InputKind::Ack { .. } => {},
				InputKind::Message { id } => {
					if let Some(id) = id {
						let priority = self.input_priority(&message);
						self.acknowledge_with_priority(id.to_owned(), priority)
							.await?;
					}
					return Ok(Some(message));
				},
				InputKind::Response { id } => {
					let id = id.to_owned();
					let priority = self.input_priority(&message);
					self.sender
						.send_with_priority(I::create_ack_message(id.clone()), priority)
						.await?;
					self.sender.remove(&id);
					if let Some((_, sender)) = self.sender.responses.remove(&id) {
						sender.send(message).ok();
					} else {
						return Ok(Some(message));
					}
				},
			}
		}
	}

	pub(crate) async fn recv_without_ack(&mut self) -> tg::Result<Option<I>> {
		loop {
			let Some(event) = self.inner.try_next().await? else {
				return Ok(None);
			};
			let message = match event {
				tg::control::Event::Message(message) => message,
				tg::control::Event::Reconnect => {
					// Receipt acknowledgements apply only to the previous connection.
					for mut entry in self.sender.outbox.iter_mut() {
						entry.acknowledged = false;
					}
					continue;
				},
			};
			match message.kind() {
				InputKind::Ack { id } => {
					self.sender.acknowledge(id);
					if self.inbox.contains_key(id) {
						let id = id.to_owned();
						let inbox = self.inbox.clone();
						let ttl = self.inbox_ttl;
						tokio::spawn(async move {
							tokio::time::sleep(ttl).await;
							inbox.remove(&id);
						});
					}
					return Ok(Some(message));
				},
				InputKind::Message { id } => {
					let Some(id) = id else {
						return Ok(Some(message));
					};
					if !self.inbox.contains_key(id) {
						return Ok(Some(message));
					}
					let priority = self.input_priority(&message);
					self.sender
						.send_with_priority(I::create_ack_message(id.to_owned()), priority)
						.await?;
				},
				InputKind::Response { id } => {
					if !self.sender.responses.contains_key(id) {
						self.sender.remove(id);
					}
					return Ok(Some(message));
				},
			}
		}
	}

	pub(crate) async fn acknowledge(&mut self, id: String) -> tg::Result<()> {
		self.acknowledge_with_priority(id, Priority::High).await
	}

	pub(crate) async fn acknowledge_with_priority(
		&mut self,
		id: String,
		priority: Priority,
	) -> tg::Result<()> {
		self.sender
			.send_with_priority(I::create_ack_message(id.clone()), priority)
			.await?;
		self.inbox.insert(id, ());

		Ok(())
	}

	fn input_priority(&self, message: &I) -> Priority {
		match message.kind() {
			InputKind::Ack { .. } | InputKind::Message { id: None } => message.priority(),
			InputKind::Message { id: Some(id) } | InputKind::Response { id } => self
				.sender
				.priority(id)
				.unwrap_or_else(|| message.priority()),
		}
	}

	pub(crate) fn sender(&self) -> Sender<I, O> {
		self.sender.clone()
	}
}

impl<I, O> Drop for Stream<I, O> {
	fn drop(&mut self) {
		self.sender.responses.clear();
		for task in &self.send_tasks {
			task.abort();
		}
	}
}

impl<I, O> Clone for Sender<I, O> {
	fn clone(&self) -> Self {
		Self {
			inbox: self.inbox.clone(),
			inner_high: self.inner_high.clone(),
			inner_low: self.inner_low.clone(),
			notify: self.notify.clone(),
			outbox: self.outbox.clone(),
			outbox_ttl: self.outbox_ttl,
			responses: self.responses.clone(),
		}
	}
}

impl<I, O> Sender<I, O>
where
	I: Send + 'static,
	O: Output + Clone + Send + Sync + 'static,
{
	pub(crate) async fn send(&self, message: O) -> tg::Result<()> {
		self.send_with_priority(message, Priority::High).await
	}

	pub(crate) async fn send_low(&self, message: O) -> tg::Result<()> {
		self.send_with_priority(message, Priority::Low).await
	}

	async fn send_with_priority(&self, message: O, priority: Priority) -> tg::Result<()> {
		let id = message.id().map(str::to_owned);
		if let Some(id) = &id {
			let entry = OutboxEntry {
				acknowledged: false,
				message: message.clone(),
				priority,
			};
			let previous = self.outbox.insert(id.clone(), entry);
			if previous.is_none()
				&& let Some(ttl) = self.outbox_ttl
			{
				let id = id.clone();
				let inbox = self.inbox.clone();
				let notify = self.notify.clone();
				let outbox = self.outbox.clone();
				tokio::spawn(async move {
					tokio::time::sleep(ttl).await;
					if outbox.remove(&id).is_some() {
						inbox.remove(&id);
						if outbox.is_empty() {
							notify.notify_waiters();
						}
					}
				});
			}
		}
		self.send_inner(message, priority).await?;
		Ok(())
	}

	fn send_inner(
		&self,
		message: O,
		priority: Priority,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let id = message.id().map(str::to_owned);
		let sender = match priority {
			Priority::High => &self.inner_high,
			Priority::Low => &self.inner_low,
		};
		sender.send(message).map_err(move |_| {
			if let Some(id) = id {
				self.remove(&id);
			}
			tg::error!("failed to send the control message")
		})
	}

	pub(crate) async fn request(
		&self,
		message: O,
		priority: Priority,
	) -> tg::Result<Response<I, O>> {
		let id = message
			.id()
			.filter(|_| message.is_request())
			.ok_or_else(|| tg::error!("expected a control request"))?
			.to_owned();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.responses.insert(id.clone(), sender);
		let response = Response {
			id,
			receiver,
			sender: self.clone(),
		};
		self.send_with_priority(message, priority).await?;
		Ok(response)
	}

	fn acknowledge(&self, id: &str) {
		if let Some(mut entry) = self.outbox.get_mut(id)
			&& entry.message.is_request()
		{
			entry.acknowledged = true;
			return;
		}
		self.remove(id);
	}

	pub(crate) fn remove(&self, id: &str) {
		if self.outbox.remove(id).is_some() && self.outbox.is_empty() {
			self.notify.notify_waiters();
		}
	}

	pub(crate) async fn wait_for_empty(&self) {
		loop {
			let notified = self.notify.notified();
			if self.outbox.is_empty() {
				return;
			}
			notified.await;
		}
	}

	fn priority(&self, id: &str) -> Option<Priority> {
		self.outbox.get(id).map(|entry| entry.priority)
	}

	fn messages(&self, priority: Priority) -> Vec<O> {
		self.outbox
			.iter()
			.filter(|entry| !entry.value().acknowledged && entry.value().priority == priority)
			.map(|entry| entry.value().message.clone())
			.collect()
	}
}

impl<I, O> Future for Response<I, O> {
	type Output = Result<I, tokio::sync::oneshot::error::RecvError>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		Pin::new(&mut self.receiver).poll(cx)
	}
}

impl<I, O> Drop for Response<I, O> {
	fn drop(&mut self) {
		self.sender.responses.remove(&self.id);
		if self.sender.outbox.remove(&self.id).is_some() && self.sender.outbox.is_empty() {
			self.sender.notify.notify_waiters();
		}
	}
}

pub(crate) fn priority_stream<T>(
	receiver_high: tokio::sync::mpsc::Receiver<T>,
	receiver_low: tokio::sync::mpsc::Receiver<T>,
) -> BoxStream<'static, T>
where
	T: Send + 'static,
{
	let stream_high = tokio_stream::wrappers::ReceiverStream::new(receiver_high);
	let stream_low = tokio_stream::wrappers::ReceiverStream::new(receiver_low);
	let stream = futures::stream::select_with_strategy(stream_high, stream_low, |(): &mut ()| {
		futures::stream::PollNext::Left
	});

	stream.boxed()
}

pub(crate) fn stream_options() -> StreamOptions {
	StreamOptions {
		inbox_ttl: Duration::from_mins(1),
		outbox_ttl: None,
		retry: tangram_futures::retry::Options {
			backoff: Duration::from_secs(1),
			jitter: Duration::ZERO,
			max_delay: Duration::from_secs(1),
			max_retries: u64::MAX,
		},
	}
}

impl Server {
	pub(crate) async fn send_control_request<I, O, Response>(
		&self,
		arg: SendControlRequestArg<
			I,
			O,
			Response,
			impl Fn(String) -> O + Clone,
			impl Fn(&I) -> bool + Clone,
			impl Fn(I) -> tg::Result<Option<(String, Response)>> + Clone,
		>,
	) -> tg::Result<Response>
	where
		I: Payload,
		O: Clone + Payload,
	{
		let SendControlRequestArg {
			ack,
			client_subject,
			is_ack,
			marker: _,
			options,
			request,
			response,
			server_subject,
		} = arg;
		let server = self.clone();
		let Options { retry, timeout } = options;

		let responses = server
			.messenger
			.subscribe::<I>(client_subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the response"))?;
		let mut responses = std::pin::pin!(responses);

		let mut acknowledged = false;
		let mut retries = std::pin::pin!(tangram_futures::retry::stream(retry));
		loop {
			if !acknowledged {
				if retries.next().await.is_none() {
					return Err(tg::error!(
						"timed out waiting for the request acknowledgement"
					));
				}

				server
					.messenger
					.publish(server_subject.clone(), request.clone())
					.await
					.map_err(|source| tg::error!(!source, "failed to publish the request"))?;
			}

			let receive = async {
				loop {
					let message = responses
						.next()
						.await
						.ok_or_else(|| tg::error!("the response stream ended"))?
						.map_err(|source| tg::error!(!source, "failed to receive the response"))?;
					if is_ack(&message.payload) {
						return Ok::<_, tg::Error>(ReceiveControlRequestOutput::Ack);
					}
					let Some((id, response)) = response(message.payload)? else {
						continue;
					};
					return Ok(ReceiveControlRequestOutput::Response { id, response });
				}
			};
			let result = tokio::time::timeout(timeout, receive).await;

			match result {
				Ok(Ok(ReceiveControlRequestOutput::Ack)) => acknowledged = true,
				Ok(Ok(ReceiveControlRequestOutput::Response { id, response })) => {
					let ack = ack(id);
					server
						.messenger
						.publish(server_subject.clone(), ack)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to acknowledge the control response")
						})?;

					return Ok(response);
				},
				Ok(Err(error)) => return Err(error),
				Err(_) => {
					crate::checkpoint!(
						server,
						"control.request.timeout",
						subject = server_subject.clone(),
					)
					.await;
				},
			}
		}
	}
}
