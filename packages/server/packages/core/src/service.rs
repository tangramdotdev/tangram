use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{
		fmt::{Debug, Display},
		sync::{Arc, RwLock},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::{Messenger as _, Payload},
};

#[cfg(test)]
mod tests;

/// A kind of server that registers in the index and serves requests on its own subject, such as an indexer or a scheduler.
pub trait Kind: Send + Sync + 'static {
	type Arg: Clone
		+ Debug
		+ tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>
		+ Send
		+ Sync
		+ 'static;

	type Id: Clone + Display + Send + Sync + 'static;

	type Output: Clone
		+ Debug
		+ tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>
		+ Send
		+ Sync
		+ 'static;

	/// The name of the kind, such as `indexer`.
	const NAME: &'static str;

	/// Get the IDs of the registered servers of this kind that are available.
	fn available(server: &Server) -> impl Future<Output = tg::Result<Vec<Self::Id>>> + Send;
}

/// Handle the requests that an endpoint receives.
pub trait Handler<K: Kind>: Send {
	type Accepted: Send;

	/// Accept a request, or return `None` to reply that the server is unavailable.
	fn accept(&mut self, request: &Request<K::Arg>) -> Option<Self::Accepted>;

	/// Handle an accepted request, which responds through the sender.
	fn handle(
		&mut self,
		request: Request<K::Arg>,
		accepted: Self::Accepted,
		sender: Sender<K>,
	) -> impl Future<Output = tg::Result<()>> + Send;

	/// Stop handling the accepted requests after the endpoint stops receiving them, so that their senders are dropped.
	fn shutdown(&mut self) -> impl Future<Output = ()> + Send {
		async {}
	}
}

pub type Sender<K> =
	crate::control::Sender<ServerMessage<<K as Kind>::Arg>, ClientMessage<<K as Kind>::Output>>;

/// The available servers of a kind.
pub struct Cache<K: Kind>(Arc<RwLock<Vec<K::Id>>>);

/// A subscription to a server's requests.
pub struct Endpoint<K: Kind> {
	control: crate::control::Stream<ServerMessage<K::Arg>, ClientMessage<K::Output>>,
	receiver: tokio::sync::mpsc::Receiver<ClientMessage<K::Output>>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum ClientMessage<O> {
	#[tangram_serialize(id = 0)]
	Ack(Ack),

	#[tangram_serialize(id = 1)]
	Response(Response<O>),

	#[tangram_serialize(id = 2)]
	Unavailable(Ack),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum ServerMessage<A> {
	#[tangram_serialize(id = 0)]
	Ack(Ack),

	#[tangram_serialize(id = 1)]
	Request(Request<A>),
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Ack {
	#[tangram_serialize(id = 0)]
	pub id: String,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Request<A> {
	#[tangram_serialize(id = 0)]
	pub arg: A,

	#[tangram_serialize(id = 1)]
	pub id: String,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Response<O> {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,

	#[tangram_serialize(id = 1)]
	pub id: String,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub output: Option<O>,
}

/// Select a server, and send the request to it until it answers, resending it with the same request ID after the response timeout in case the server restarted after accepting it. Move to another server only when the server replies that it is unavailable, which means that it did not accept the request, because a server that may have accepted the request must not receive it again through another server.
pub async fn retry<K, T, F, Fut>(
	cache: &Cache<K>,
	server: &Server,
	options: &tangram_futures::retry::Options,
	response_timeout: Duration,
	start: u64,
	f: F,
) -> tg::Result<(K::Id, T)>
where
	K: Kind,
	F: Fn(K::Id, String) -> Fut,
	Fut: Future<Output = tg::Result<Option<T>>>,
{
	let mut selection = start;
	let mut refresh = false;
	let mut retries = std::pin::pin!(tangram_futures::retry::stream(options.clone()));
	loop {
		// Select a server.
		let id = match cache.select(server, selection, refresh).await {
			Ok(id) => id,
			Err(error) => {
				if retries.next().await.is_none() {
					return Err(error);
				}
				refresh = true;
				continue;
			},
		};

		// Send the request to the server with one request ID until it answers, so that the server deduplicates a resend.
		let request_id = crate::control::id();
		let output = attempt(&mut retries, response_timeout, || {
			f(id.clone(), request_id.clone())
		})
		.await?;
		if let Some(output) = output {
			return Ok((id, output));
		}

		// Move to the next server, because this server did not accept the request.
		if retries.next().await.is_none() {
			let name = K::NAME;
			return Err(tg::error!(%id, "the {name} is unavailable"));
		}
		selection = selection.wrapping_add(1);
		refresh = true;
	}
}

/// Send the request to a known server with one request ID until it answers and accepts the request, resending it after the response timeout in case the server restarted after accepting it, because no other server can handle it.
pub async fn retry_to<T, F, Fut>(
	options: &tangram_futures::retry::Options,
	response_timeout: Duration,
	f: F,
) -> tg::Result<T>
where
	F: Fn(String) -> Fut,
	Fut: Future<Output = tg::Result<Option<T>>>,
{
	let mut retries = std::pin::pin!(tangram_futures::retry::stream(options.clone()));
	let request_id = crate::control::id();
	loop {
		let output = attempt(&mut retries, response_timeout, || f(request_id.clone())).await?;
		if let Some(output) = output {
			return Ok(output);
		}
		if retries.next().await.is_none() {
			return Err(tg::error!("the server is unavailable"));
		}
	}
}

/// Get a random starting point for server selection.
#[must_use]
pub fn start() -> u64 {
	rand::random()
}

/// Send a request to a server, returning `None` if the server is unavailable.
pub async fn send<K: Kind>(
	server: &Server,
	id: &K::Id,
	request_id: &str,
	arg: K::Arg,
	options: crate::control::Options,
) -> tg::Result<Option<tg::Result<K::Output>>> {
	let request = ServerMessage::Request(Request {
		arg,
		id: request_id.to_owned(),
	});
	let arg = crate::control::SendControlRequestArg {
		ack: |id| ServerMessage::Ack(Ack { id }),
		client_subject: client_subject::<K>(request_id),
		hold: None,
		is_ack: |message: &ClientMessage<K::Output>| matches!(message, ClientMessage::Ack(_)),
		is_not_holder: |_| None,
		marker: std::marker::PhantomData,
		options,
		request,
		response: |message: ClientMessage<K::Output>| match message {
			ClientMessage::Ack(_) => Ok(None),
			ClientMessage::Response(message) => {
				let name = K::NAME;
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error).map_err(|source| {
						tg::error!(!source, "failed to deserialize the {name} error")
					})?;
					return Ok(Some((message.id, Some(Err(error)))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing {name} response output"));
				};
				Ok(Some((message.id, Some(Ok(output)))))
			},
			ClientMessage::Unavailable(message) => Ok(Some((message.id, None))),
		},
		server_subject: server_subject::<K>(id),
	};
	let output = server.request_control(arg).boxed().await?;

	Ok(output)
}

#[must_use]
pub fn client_subject<K: Kind>(request_id: &str) -> String {
	format!("{}s.client.{request_id}", K::NAME)
}

#[must_use]
pub fn server_subject<K: Kind>(id: &K::Id) -> String {
	format!("{}s.{id}.server", K::NAME)
}

/// Run the operation until it answers, retrying with the same arguments on errors and after the response timeout.
async fn attempt<T, F, Fut>(
	retries: &mut std::pin::Pin<&mut impl futures::Stream<Item = ()>>,
	response_timeout: Duration,
	f: F,
) -> tg::Result<Option<T>>
where
	F: Fn() -> Fut,
	Fut: Future<Output = tg::Result<Option<T>>>,
{
	loop {
		let result = tokio::time::timeout(response_timeout, f())
			.await
			.map_err(|source| tg::error!(!source, "timed out waiting for the response"))
			.and_then(std::convert::identity);
		match result {
			Ok(output) => return Ok(output),
			Err(error) => {
				if retries.next().await.is_none() {
					return Err(error);
				}
			},
		}
	}
}

impl<K: Kind> Cache<K> {
	/// Refresh the cache periodically in multi-server mode, where other servers register and unregister.
	pub async fn task(&self, server: &Server, poll_interval: Duration, stopper: Stopper) {
		if !server.config.advanced.multi_server {
			return;
		}
		let mut interval = tokio::time::interval(poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				_ = interval.tick() => {
					if let Err(error) = self.refresh(server).await {
						let name = K::NAME;
						tracing::error!(error = %error.trace(), "failed to refresh the {name} cache");
					}
				},
			}
		}
	}

	pub async fn refresh(&self, server: &Server) -> tg::Result<()> {
		let ids = K::available(server).await?;
		self.replace(ids);

		Ok(())
	}

	/// Select an available server, refreshing the cache first if requested or if it is empty.
	pub async fn select(&self, server: &Server, index: u64, refresh: bool) -> tg::Result<K::Id> {
		let mut ids = self.available();
		if refresh || ids.is_empty() {
			self.refresh(server).await?;
			ids = self.available();
		}
		let len = u64::try_from(ids.len()).unwrap();
		if len == 0 {
			let name = K::NAME;
			return Err(tg::error!("no {name}s are available"));
		}
		let index = usize::try_from(index % len).unwrap();
		let id = ids.swap_remove(index);

		Ok(id)
	}

	#[must_use]
	pub fn available(&self) -> Vec<K::Id> {
		self.0.read().unwrap().clone()
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.read().unwrap().is_empty()
	}

	pub fn replace(&self, ids: Vec<K::Id>) {
		*self.0.write().unwrap() = ids;
	}
}

impl<K: Kind> Endpoint<K> {
	/// Subscribe to a server's requests, so that no request sent after the subscription is lost.
	pub async fn subscribe(
		server: &Server,
		id: &K::Id,
		channel_capacity: usize,
		response_ttl: Duration,
	) -> tg::Result<Self> {
		let name = K::NAME;
		let messages = server
			.messenger
			.subscribe::<ServerMessage<K::Arg>>(server_subject::<K>(id))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the {name} requests"))?
			.map_err(move |source| tg::error!(!source, "failed to receive a {name} message"))
			.map_ok(|message| message.payload)
			.boxed();
		let (sender, receiver) = tokio::sync::mpsc::channel(channel_capacity);
		let mut options = crate::control::stream_options(&server.config.control);
		options.outbox_ttl = Some(response_ttl);
		let control = crate::control::Stream::new(messages, sender, options);
		let endpoint = Self { control, receiver };

		Ok(endpoint)
	}

	#[must_use]
	pub fn sender(&self) -> Sender<K> {
		self.control.sender()
	}

	/// Handle requests until the stop, and publish the responses and acks.
	pub async fn run<H: Handler<K>>(
		self,
		server: &Server,
		mut handler: H,
		concurrency: usize,
		stopper: Stopper,
	) -> tg::Result<()> {
		let Self {
			control,
			mut receiver,
		} = self;

		// Handle the requests while publishing the client messages.
		let result = {
			let requests = std::pin::pin!(Self::handle_requests(control, &mut handler, stopper));
			let messages = std::pin::pin!(Self::publish_client_messages(
				server,
				&mut receiver,
				concurrency
			));
			let (result, messages) = match future::select(requests, messages).await {
				future::Either::Left((result, messages)) => (result, Some(messages)),
				future::Either::Right(((), requests)) => (requests.await, None),
			};
			if result.is_ok()
				&& let Some(messages) = messages
			{
				messages.await;
			}
			result
		};

		// Publish the acks and responses that are already queued if handling failed.
		if result.is_err() {
			while let Ok(message) = receiver.try_recv() {
				Self::publish_client_message(server, message).await;
			}
		}

		result
	}

	async fn handle_requests<H: Handler<K>>(
		mut control: crate::control::Stream<ServerMessage<K::Arg>, ClientMessage<K::Output>>,
		handler: &mut H,
		stopper: Stopper,
	) -> tg::Result<()> {
		let result = Self::handle_requests_inner(&mut control, handler, stopper).await;

		// Stop the handlers, which lets the client messages finish publishing.
		handler.shutdown().await;
		drop(control);

		result
	}

	async fn handle_requests_inner<H: Handler<K>>(
		control: &mut crate::control::Stream<ServerMessage<K::Arg>, ClientMessage<K::Output>>,
		handler: &mut H,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Accept each request once, and reply that the server is unavailable to each request that the handler does not accept.
		loop {
			let message = tokio::select! {
				biased;
				() = stopper.wait() => break,
				message = control.recv_without_ack() => message?,
			};
			let Some(message) = message else {
				let name = K::NAME;
				return Err(tg::error!("the {name} request stream ended"));
			};
			let ServerMessage::Request(request) = message else {
				continue;
			};
			let Some(accepted) = handler.accept(&request) else {
				let message = ClientMessage::Unavailable(Ack { id: request.id });
				control.sender().send(message).await?;
				continue;
			};
			control.acknowledge_now(request.id.clone());
			handler.handle(request, accepted, control.sender()).await?;
		}

		Ok(())
	}

	async fn publish_client_messages(
		server: &Server,
		receiver: &mut tokio::sync::mpsc::Receiver<ClientMessage<K::Output>>,
		concurrency: usize,
	) {
		futures::stream::poll_fn(|context| receiver.poll_recv(context))
			.for_each_concurrent(concurrency, |message| {
				Self::publish_client_message(server, message)
			})
			.await;
	}

	async fn publish_client_message(server: &Server, message: ClientMessage<K::Output>) {
		if let ClientMessage::Response(_) = &message {
			let checkpoint = format!("{}.response.publish", K::NAME);
			crate::checkpoint!(server, &checkpoint).await;
		}
		let subject = client_subject::<K>(message.id());
		if let Err(error) = server.messenger.publish(subject, message).await {
			let name = K::NAME;
			tracing::error!(%error, "failed to publish a {name} client message");
		}
	}
}

impl<O> ClientMessage<O> {
	fn id(&self) -> &str {
		match self {
			Self::Ack(ack) | Self::Unavailable(ack) => &ack.id,
			Self::Response(response) => &response.id,
		}
	}
}

impl<O> Response<O> {
	#[must_use]
	pub fn new(id: String, result: tg::Result<O>) -> Self {
		match result {
			Ok(output) => Self {
				error: None,
				id,
				output: Some(output),
			},
			Err(error) => Self {
				error: Some(error.to_data_or_id()),
				id,
				output: None,
			},
		}
	}
}

impl<K: Kind> Clone for Cache<K> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<K: Kind> Default for Cache<K> {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(Vec::new())))
	}
}

impl<A, O> crate::control::Input<ClientMessage<O>> for ServerMessage<A> {
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
		}
	}

	fn create_ack_message(id: String) -> ClientMessage<O> {
		ClientMessage::Ack(Ack { id })
	}
}

impl<O> crate::control::Output for ClientMessage<O> {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) | Self::Unavailable(_) => None,
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl<O> Payload for ClientMessage<O>
where
	O: tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>
		+ Send
		+ Sync
		+ 'static,
{
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}

impl<A> Payload for ServerMessage<A>
where
	A: tangram_serialize::Serialize
		+ for<'de> tangram_serialize::Deserialize<'de>
		+ Send
		+ Sync
		+ 'static,
{
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}
}
