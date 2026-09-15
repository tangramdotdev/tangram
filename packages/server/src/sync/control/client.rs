use {
	super::{ClientMessage, ServerMessage, client_subject, lease_subject, subject},
	crate::{Server, Session},
	futures::TryStreamExt as _,
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::{prelude::*, sync::control as protocol},
	tangram_futures::task::Stopper,
	tangram_messenger::Messenger as _,
	tokio::time::Instant,
};

#[derive(Default)]
pub(crate) struct Client {
	peers: Mutex<BTreeMap<String, Arc<Peer>>>,
}

struct Peer {
	sender: tokio::sync::mpsc::UnboundedSender<Command>,
}

pub(crate) struct Request {
	id: String,
	peer: Arc<Peer>,
	receiver: tokio::sync::watch::Receiver<Output>,
}

#[derive(Clone)]
pub(crate) enum Output {
	Pending { acknowledged: bool },
	Ready(tg::Result<protocol::GetServerResponseOutput>),
}

struct State {
	client: String,
	heartbeat_at: Instant,
	heartbeats: BTreeMap<String, Heartbeat>,
	last_heartbeat: Option<Instant>,
	lease: Option<Lease>,
	requests: BTreeMap<String, Pending>,
	subject: String,
}

struct Heartbeat {
	acknowledged: bool,
	request: protocol::ClientRequest,
	retry_at: Instant,
	sent_at: Instant,
}

struct Lease {
	deadline: Instant,
	id: String,
}

struct Pending {
	acknowledged: bool,
	arg: protocol::ClientRequestArg,
	deadline: Option<Instant>,
	retry_at: Instant,
	sender: tokio::sync::watch::Sender<Output>,
}

enum Command {
	Cancel(String),
	Request {
		arg: protocol::ClientRequestArg,
		id: String,
		sender: tokio::sync::watch::Sender<Output>,
	},
}

impl Client {
	pub fn request(
		&self,
		session: &Session,
		token: &tg::sync::Token,
		arg: protocol::ClientRequestArg,
	) -> Request {
		let peer = {
			let mut peers = self.peers.lock().unwrap();
			if let Some(peer) = peers
				.get(&token.body.id)
				.filter(|peer| !peer.sender.is_closed())
				.cloned()
			{
				peer
			} else {
				let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
				let peer = Arc::new(Peer { sender });
				let client = crate::control::id();
				let state = State {
					client,
					heartbeat_at: Instant::now(),
					heartbeats: BTreeMap::new(),
					last_heartbeat: None,
					lease: None,
					requests: BTreeMap::new(),
					subject: subject(token),
				};
				session
					.server
					.sync_control_tasks
					.spawn({
						let server = session.server.clone();
						move |stopper| async move {
							let result = server
								.sync_control_heartbeat_task(state, receiver, stopper)
								.await;
							if let Err(error) = &result {
								tracing::debug!(error = %error.trace(), "the sync heartbeat task failed");
							}
							result
						}
					})
					.detach();
				peers.insert(token.body.id.clone(), peer.clone());
				peer
			}
		};
		let id = crate::control::id();
		let (sender, receiver) = tokio::sync::watch::channel(Output::Pending {
			acknowledged: false,
		});
		let command = Command::Request {
			arg,
			id: id.clone(),
			sender,
		};
		peer.sender.send(command).ok();
		Request { id, peer, receiver }
	}
}

impl Request {
	pub fn output(&mut self) -> Output {
		self.receiver.borrow_and_update().clone()
	}

	pub async fn changed(&mut self) -> tg::Result<()> {
		self.receiver
			.changed()
			.await
			.map_err(|error| tg::error!(!error, "the sync heartbeat task ended"))?;
		Ok(())
	}
}

impl Drop for Request {
	fn drop(&mut self) {
		self.peer.sender.send(Command::Cancel(self.id.clone())).ok();
	}
}

impl Server {
	async fn sync_control_heartbeat_task(
		&self,
		mut state: State,
		mut commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
		stopper: Stopper,
	) -> tg::Result<()> {
		let messages = tokio::time::timeout(
			self.config.sync.control.request_timeout,
			self.messenger
				.subscribe::<ServerMessage>(client_subject(&state.subject, &state.client)),
		)
		.await
		.map_err(|error| {
			tg::error!(
				!error,
				"timed out subscribing to the sync control responses"
			)
		})?
		.map_err(|error| tg::error!(!error, "failed to subscribe to the sync control responses"))?;
		let mut messages = std::pin::pin!(messages);
		let config = &self.config.sync.control;
		let interval = std::cmp::min(config.heartbeat_interval, config.retry_interval);
		let mut tick = tokio::time::interval(interval);
		tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut closed_at = None;
		let mut stopping = false;
		crate::checkpoint!(
			self,
			"sync.control.heartbeat.start",
			client = %state.client,
			subject = %state.subject
		)
		.await;

		loop {
			tokio::select! {
				() = stopper.wait(), if !stopping => {
					stopping = true;
					closed_at = Some(Instant::now());
					for (_, request) in std::mem::take(&mut state.requests) {
						request.sender.send_replace(Output::Ready(Err(tg::error!(code = tg::error::Code::Cancellation, "the server is stopping"))));
					}
				},
				command = commands.recv(), if closed_at.is_none() => {
					match command {
						Some(Command::Cancel(id)) => {
							state.requests.remove(&id);
						},
						Some(Command::Request { arg, id, sender }) => {
							let request = Pending {
								acknowledged: false,
								arg,
								deadline: Some(Instant::now() + config.request_timeout),
								retry_at: Instant::now(),
								sender,
							};
							state.requests.insert(id.clone(), request);
							self.sync_control_send_request(&mut state, &id).await;
						},
						None => {
							closed_at = Some(Instant::now());
						},
					}
				},
				message = messages.try_next() => {
					let Some(message) = message.map_err(|error| tg::error!(!error, "failed to receive a sync control response"))? else {
						return Err(tg::error!("the sync control response stream ended"));
					};
					self.sync_control_client_message(&mut state, message.payload.0).await?;
				},
				_ = tick.tick() => {
					let now = Instant::now();
					state.heartbeats.retain(|_, heartbeat| now < heartbeat.sent_at + config.lease_ttl);
					if closed_at.is_none() && !state.requests.is_empty() {
						if state.lease.as_ref().is_some_and(|lease| now >= lease.deadline) {
							state.lease = None;
							self.sync_control_recover_requests(&mut state);
						}
						let expired = state.requests.iter().filter_map(|(id, request)| {
							request.deadline.filter(|deadline| now >= *deadline).map(|_| id.clone())
						}).collect::<Vec<_>>();
						for id in expired {
							let request = state.requests.remove(&id).unwrap();
							let error = tg::error!(code = tg::error::Code::HeartbeatExpiration, request = %id, sync = %state.subject, "failed to recover the sync request");
							request.sender.send_replace(Output::Ready(Err(error)));
						}
						if !state.requests.is_empty() && now >= state.heartbeat_at {
							let arg = protocol::ClientRequestArg::Heartbeat(protocol::HeartbeatClientRequestArg {});
							let request = protocol::ClientRequest {
								arg,
								client: state.client.clone(),
								id: crate::control::id(),
								lease: None,
							};
							let heartbeat = Heartbeat { acknowledged: false, request, retry_at: now, sent_at: now };
							state.heartbeats.insert(heartbeat.request.id.clone(), heartbeat);
							state.heartbeat_at = now + config.heartbeat_interval;
						}
						let future = async {
							for heartbeat in state.heartbeats.values_mut().filter(|heartbeat| !heartbeat.acknowledged && now >= heartbeat.retry_at) {
								heartbeat.retry_at = now + config.retry_interval;
								self.sync_control_client_publish(format!("{}.server", state.subject), protocol::ClientMessage::Request(heartbeat.request.clone())).await;
								crate::checkpoint!(self, "sync.control.heartbeat.request", client = %state.client, id = %heartbeat.request.id).await;
							}
						};
						tokio::time::timeout(interval, future).await.ok();
						self.sync_control_send_requests(&mut state).await;
					}
				},
			}
			if closed_at.is_some_and(|time| Instant::now() >= time + config.lease_ttl) {
				break;
			}
		}

		Ok(())
	}

	async fn sync_control_client_message(
		&self,
		state: &mut State,
		message: protocol::ServerMessage,
	) -> tg::Result<()> {
		match message {
			protocol::ServerMessage::Ack(ack) => {
				if let Some(heartbeat) = state.heartbeats.get_mut(&ack.id) {
					heartbeat.acknowledged = true;
				} else if state
					.lease
					.as_ref()
					.is_some_and(|lease| lease.id == ack.lease)
					&& let Some(request) = state.requests.get_mut(&ack.id)
				{
					request.acknowledged = true;
					request.deadline = None;
					request
						.sender
						.send_replace(Output::Pending { acknowledged: true });
					crate::checkpoint!(self, "sync.control.ack", id = %ack.id, lease = %ack.lease, node = %request.arg.node().unwrap()).await;
				}
			},
			protocol::ServerMessage::Response(response) => {
				if let Some(heartbeat) = state.heartbeats.remove(&response.id) {
					if let (None, Some(protocol::ServerResponseOutput::Heartbeat(output))) =
						(&response.error, &response.output)
						&& state
							.last_heartbeat
							.is_none_or(|time| heartbeat.sent_at >= time)
						&& let Some(deadline) = heartbeat.sent_at.checked_add(output.ttl)
						&& Instant::now() < deadline
						&& Instant::now() < heartbeat.sent_at + self.config.sync.control.lease_ttl
					{
						let changed = state
							.lease
							.as_ref()
							.is_none_or(|lease| lease.id != response.lease);
						state.last_heartbeat = Some(heartbeat.sent_at);
						state.lease = Some(Lease {
							deadline,
							id: response.lease.clone(),
						});
						if changed {
							self.sync_control_recover_requests(state);
							crate::checkpoint!(
								self,
								"sync.control.lease",
								client = %state.client,
								lease = %response.lease
							)
							.await;
							self.sync_control_send_requests(state).await;
						}
					}
				} else if let Some(request) = state.requests.get(&response.id) {
					// A terminal response from any lease can satisfy the idempotent request.
					let result = match (&response.error, &response.output, &request.arg) {
						(Some(error), None, _) => Some(Err(tg::Error::try_from(error.clone())?)),
						(
							None,
							Some(protocol::ServerResponseOutput::Get(
								protocol::GetServerResponseOutput::Object(output),
							)),
							protocol::ClientRequestArg::Get(protocol::GetClientRequestArg::Object(
								_,
							)),
						) => Some(Ok(protocol::GetServerResponseOutput::Object(
							output.clone(),
						))),
						(
							None,
							Some(protocol::ServerResponseOutput::Get(
								protocol::GetServerResponseOutput::Process(output),
							)),
							protocol::ClientRequestArg::Get(
								protocol::GetClientRequestArg::Process(_),
							),
						) => Some(Ok(protocol::GetServerResponseOutput::Process(
							output.clone(),
						))),
						_ => None,
					};
					if let Some(result) = result {
						let request = state.requests.remove(&response.id).unwrap();
						request.sender.send_replace(Output::Ready(result));
					} else {
						return Ok(());
					}
				}

				// Acknowledge duplicates and cancelled requests even after their local state is gone.
				let ack = protocol::ClientAck {
					id: response.id,
					lease: response.lease,
				};
				crate::checkpoint!(
					self,
					"sync.control.response",
					id = %ack.id,
					lease = %ack.lease
				)
				.await;
				self.sync_control_client_publish(
					lease_subject(&state.subject, &ack.lease),
					protocol::ClientMessage::Ack(ack),
				)
				.await;
			},
		}

		Ok(())
	}

	fn sync_control_recover_requests(&self, state: &mut State) {
		for request in state.requests.values_mut() {
			request.acknowledged = false;
			request.retry_at = Instant::now();
			request
				.deadline
				.get_or_insert_with(|| Instant::now() + self.config.sync.control.recovery_timeout);
			request.sender.send_replace(Output::Pending {
				acknowledged: false,
			});
		}
	}

	async fn sync_control_send_requests(&self, state: &mut State) {
		let now = Instant::now();
		let mut requests = state
			.requests
			.iter()
			.filter(|(_, request)| !request.acknowledged && now >= request.retry_at)
			.map(|(id, request)| (request.retry_at, id.clone()))
			.collect::<Vec<_>>();
		requests.sort_unstable();

		// Bound a batch so a slow messenger cannot prevent heartbeats and recovery deadlines from running.
		let timeout = std::cmp::min(
			self.config.sync.control.heartbeat_interval,
			self.config.sync.control.retry_interval,
		);
		let future = async {
			for (_, id) in requests {
				self.sync_control_send_request(state, &id).await;
			}
		};
		tokio::time::timeout(timeout, future).await.ok();
	}

	async fn sync_control_send_request(&self, state: &mut State, id: &str) {
		let Some(lease) = &state.lease else {
			return;
		};
		let pending = state.requests.get_mut(id).unwrap();
		pending.retry_at = Instant::now() + self.config.sync.control.retry_interval;
		let request = protocol::ClientRequest {
			arg: pending.arg.clone(),
			client: state.client.clone(),
			id: id.to_owned(),
			lease: Some(lease.id.clone()),
		};
		self.sync_control_client_publish(
			lease_subject(&state.subject, &lease.id),
			protocol::ClientMessage::Request(request),
		)
		.await;
		crate::checkpoint!(self, "sync.control.request", id = %id, lease = %lease.id, node = %pending.arg.node().unwrap()).await;
	}

	async fn sync_control_client_publish(&self, subject: String, message: protocol::ClientMessage) {
		let payload = ClientMessage(message);
		let result = tokio::time::timeout(
			self.config.sync.control.retry_interval,
			self.messenger.publish(subject, payload),
		)
		.await;
		match result {
			Ok(Ok(())) => {},
			Ok(Err(error)) => tracing::debug!(%error, "failed to publish a sync control message"),
			Err(_) => tracing::debug!("timed out publishing a sync control message"),
		}
	}
}
