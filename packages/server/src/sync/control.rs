use {
	crate::{Server, Session, sync::graph::Graph},
	futures::{TryStreamExt as _, stream},
	std::{
		collections::{BTreeMap, BTreeSet},
		sync::{Arc, Mutex},
	},
	tangram_client::{prelude::*, sync::control as protocol},
	tangram_futures::task::Stopper,
	tangram_messenger::Messenger as _,
	tokio::time::Instant,
};

mod client;
#[cfg(test)]
mod tests;

pub(crate) use self::client::{Client, Output};

#[derive(Clone)]
pub(super) struct Control {
	sender: tokio::sync::mpsc::UnboundedSender<Event>,
}

struct State {
	attempts: BTreeMap<String, Attempt>,
	clients: BTreeMap<String, String>,
	finished: Option<(Instant, tg::Result<()>)>,
	graph: Arc<Mutex<Graph>>,
	nodes: BTreeMap<tg::Id, BTreeSet<(String, String)>>,
	response_cursor: Option<(String, String)>,
}

struct Attempt {
	cancelled: BTreeSet<String>,
	client: String,
	expires_at: Instant,
	requests: BTreeMap<String, Request>,
}

struct Request {
	acknowledged_at: Option<Instant>,
	arg: protocol::ClientRequestArg,
	response: Option<protocol::ServerResponse>,
}

pub(super) enum Event {
	Finish(tg::Result<()>),
	Nodes(Vec<tg::Id>),
}

#[derive(Clone)]
pub(crate) struct ClientMessage(pub protocol::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub protocol::ServerMessage);

impl Control {
	pub fn finish(&self, result: tg::Result<()>) {
		self.sender.send(Event::Finish(result)).ok();
	}
}

impl Session {
	pub(super) fn spawn_sync_control_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		id: &tg::sync::Id,
	) -> Control {
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let control = Control { sender };
		graph
			.lock()
			.unwrap()
			.set_control(control.sender.downgrade());
		let subject = subject(id);
		let state = State {
			attempts: BTreeMap::new(),
			clients: BTreeMap::new(),
			finished: None,
			graph,
			nodes: BTreeMap::new(),
			response_cursor: None,
		};
		self.server
			.sync_control_tasks
			.spawn({
				let server = self.server.clone();
				move |stopper| async move {
					let result = server
						.sync_control_task(subject, state, receiver, stopper)
						.await;
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the sync control task failed");
					}
					result
				}
			})
			.detach();
		control
	}
}

impl Server {
	async fn sync_control_task(
		&self,
		subject: String,
		mut state: State,
		mut events: tokio::sync::mpsc::UnboundedReceiver<Event>,
		stopper: Stopper,
	) -> tg::Result<()> {
		crate::checkpoint!(self, "sync.control.subscribe").await;
		let heartbeat_messages = tokio::time::timeout(
			self.config.sync.control.request_timeout,
			self.messenger
				.subscribe::<ClientMessage>(heartbeat_subject(&subject)),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out subscribing to the sync heartbeats"))?
		.map_err(|error| tg::error!(!error, "failed to subscribe to the sync heartbeats"))?;
		let attempt_messages = tokio::time::timeout(
			self.config.sync.control.request_timeout,
			self.messenger
				.subscribe::<ClientMessage>(attempt_subject(&subject, "*")),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out subscribing to the sync attempt messages"))?
		.map_err(|error| tg::error!(!error, "failed to subscribe to the sync attempt messages"))?;
		let messages = stream::select(heartbeat_messages, attempt_messages);
		let mut messages = std::pin::pin!(messages);
		let config = &self.config.sync.control;
		let mut retry = tokio::time::interval(config.retry_interval);
		retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut events_closed = false;
		let mut stopping_at = None;

		loop {
			tokio::select! {
				() = stopper.wait(), if stopping_at.is_none() => {
					stopping_at = Some(Instant::now() + config.attempt_ttl);
					self.sync_control_finish(&subject, &mut state, Err(tg::error!(code = tg::error::Code::Cancellation, "the server is stopping"))).await;
				},
				event = events.recv(), if !events_closed => {
					match event {
						Some(Event::Finish(result)) => self.sync_control_finish(&subject, &mut state, result).await,
						Some(Event::Nodes(ids)) => {
							let mut responses = Vec::new();
							for id in ids {
								if let Some(requests) = state.nodes.remove(&id) {
									for (attempt, request) in requests {
										if Self::sync_control_create_response(&mut state, &attempt, &request) {
											responses.push((attempt, request));
										} else {
											state.nodes.entry(id.clone()).or_default().insert((attempt, request));
										}
									}
								}
							}
							self.sync_control_send_responses_inner(&subject, &mut state, responses).await;
						},
						None => {
							events_closed = true;
							self.sync_control_finish(&subject, &mut state, Err(tg::error!(code = tg::error::Code::Cancellation, "the sync was cancelled"))).await;
						},
					}
				},
				message = messages.try_next() => {
					let Some(message) = message.map_err(|error| tg::error!(!error, "failed to receive a sync control message"))? else {
						return Err(tg::error!("the sync control message stream ended"));
					};
					self.sync_control_message(&subject, &mut state, message).await?;
				},
				_ = retry.tick() => {
					self.sync_control_expire(&mut state);
					self.sync_control_send_responses(&subject, &mut state).await;
				},
			}
			let now = Instant::now();

			// A stopping server only needs to retain responses that have not been acknowledged.
			let drained = stopping_at.is_some()
				&& state.attempts.values().all(|attempt| {
					attempt
						.requests
						.values()
						.all(|request| request.acknowledged_at.is_some())
				});
			if stopping_at.is_some_and(|deadline| drained || now >= deadline)
				|| state.finished.as_ref().is_some_and(|(finished_at, _)| {
					now >= *finished_at + config.attempt_ttl && state.attempts.is_empty()
				}) {
				break;
			}
		}

		crate::checkpoint!(self, "sync.control.stopped", subject = %subject).await;

		Ok(())
	}

	async fn sync_control_message(
		&self,
		subject: &str,
		state: &mut State,
		message: tangram_messenger::Message<ClientMessage>,
	) -> tg::Result<()> {
		match message.payload.0 {
			protocol::ClientMessage::Ack(ack) => {
				if !self
					.messenger
					.matches_subject(&message.subject, attempt_subject(subject, &ack.attempt))
				{
					return Ok(());
				}
				if let Some(attempt) = state.attempts.get_mut(&ack.attempt)
					&& let Some(request) = attempt.requests.get_mut(&ack.id)
					&& request.response.is_some()
				{
					request.acknowledged_at.get_or_insert_with(Instant::now);
					crate::checkpoint!(
						self,
						"sync.control.response_ack",
						id = %ack.id,
						attempt = %ack.attempt
					)
					.await;
				}
			},
			protocol::ClientMessage::Cancel(cancel) => {
				if !self
					.messenger
					.matches_subject(&message.subject, attempt_subject(subject, &cancel.attempt))
				{
					return Ok(());
				}
				let Some(attempt) = state.attempts.get_mut(&cancel.attempt) else {
					return Ok(());
				};
				attempt.cancelled.insert(cancel.id.clone());
				if let Some(request) = attempt.requests.remove(&cancel.id)
					&& let Some(node) = request.arg.node()
					&& let Some(requests) = state.nodes.get_mut(&node)
				{
					requests.remove(&(cancel.attempt.clone(), cancel.id.clone()));
					if requests.is_empty() {
						state.nodes.remove(&node);
					}
				}
				crate::checkpoint!(self, "sync.control.cancel", id = %cancel.id, attempt = %cancel.attempt).await;
			},
			protocol::ClientMessage::Request(request) => {
				match &request.arg {
					protocol::ClientRequestArg::Get(_) => {
						let Some(attempt) = request.attempt.as_ref() else {
							return Ok(());
						};
						if !self
							.messenger
							.matches_subject(&message.subject, attempt_subject(subject, attempt))
							|| state.attempts.get(attempt).is_none_or(|attempt| {
								attempt.client != request.client
									|| Instant::now() >= attempt.expires_at
							}) {
							// Only a heartbeat may establish an attempt. The next heartbeat recovers a lost registration.
							return Ok(());
						}
						self.sync_control_get(subject, state, request).await;
					},
					protocol::ClientRequestArg::Heartbeat(_) => {
						if !self
							.messenger
							.matches_subject(&message.subject, heartbeat_subject(subject))
							|| request.attempt.is_some()
						{
							return Ok(());
						}
						self.sync_control_heartbeat(subject, state, request).await?;
					},
				}
			},
		}

		Ok(())
	}

	async fn sync_control_heartbeat(
		&self,
		subject: &str,
		state: &mut State,
		request: protocol::ClientRequest,
	) -> tg::Result<()> {
		let ttl = self.config.sync.control.attempt_ttl;
		if state
			.clients
			.get(&request.client)
			.is_some_and(|attempt| Instant::now() >= state.attempts[attempt].expires_at)
		{
			self.sync_control_expire(state);
		}
		let attempt = if let Some(attempt) = state.clients.get(&request.client) {
			attempt.clone()
		} else {
			// Leave a bounded window for callers racing with the end of the transfer.
			if state
				.finished
				.as_ref()
				.is_some_and(|(finished_at, _)| Instant::now() >= *finished_at + ttl)
			{
				return Ok(());
			}
			let mut bytes = [0; 16];
			aws_lc_rs::rand::fill(&mut bytes)
				.map_err(|error| tg::error!(!error, "failed to generate a sync attempt"))?;
			let id = tg::id::ENCODING.encode(&bytes);
			let attempt = Attempt {
				cancelled: BTreeSet::new(),
				client: request.client.clone(),
				expires_at: Instant::now() + ttl,
				requests: BTreeMap::new(),
			};
			state.attempts.insert(id.clone(), attempt);
			state.clients.insert(request.client.clone(), id.clone());
			id
		};
		let entry = state.attempts.get_mut(&attempt).unwrap();
		if let Some(previous) = entry.requests.get(&request.id) {
			if let Some(response) = &previous.response {
				self.sync_control_publish(
					subject,
					&entry.client,
					protocol::ServerMessage::Response(response.clone()),
				)
				.await;
			}
			return Ok(());
		}

		// A retransmitted heartbeat must not extend the attempt lifetime again.
		entry.expires_at = Instant::now() + ttl;
		let output = protocol::HeartbeatServerResponseOutput { ttl };
		let response = protocol::ServerResponse {
			attempt: attempt.clone(),
			error: None,
			id: request.id.clone(),
			output: Some(protocol::ServerResponseOutput::Heartbeat(output)),
		};
		let client = request.client.clone();
		let entry = Request {
			acknowledged_at: None,
			arg: request.arg,
			response: Some(response.clone()),
		};
		state
			.attempts
			.get_mut(&attempt)
			.unwrap()
			.requests
			.insert(response.id.clone(), entry);
		crate::checkpoint!(
			self,
			"sync.control.heartbeat.response",
			client = %client,
			id = %response.id,
			attempt = %attempt
		)
		.await;
		self.sync_control_publish(
			subject,
			&client,
			protocol::ServerMessage::Response(response),
		)
		.await;

		Ok(())
	}

	async fn sync_control_get(
		&self,
		subject: &str,
		state: &mut State,
		request: protocol::ClientRequest,
	) {
		let attempt = request.attempt.clone().unwrap();
		let id = request.id.clone();
		let client = request.client.clone();
		if state.attempts[&attempt].cancelled.contains(&id) {
			return;
		}
		if let Some(previous) = state.attempts[&attempt].requests.get(&id) {
			let message = match &previous.response {
				Some(response) => protocol::ServerMessage::Response(response.clone()),
				None => protocol::ServerMessage::Ack(protocol::ServerAck { attempt, id }),
			};
			self.sync_control_publish(subject, &client, message).await;
			return;
		}

		let node = request.arg.node().unwrap();
		let entry = Request {
			acknowledged_at: None,
			arg: request.arg,
			response: None,
		};
		state
			.attempts
			.get_mut(&attempt)
			.unwrap()
			.requests
			.insert(id.clone(), entry);
		if Self::sync_control_create_response(state, &attempt, &id) {
			self.sync_control_send_responses_inner(subject, state, vec![(attempt, id)])
				.await;
		} else {
			state
				.nodes
				.entry(node)
				.or_default()
				.insert((attempt.clone(), id.clone()));
			crate::checkpoint!(self, "sync.control.request.retain", id = %id, attempt = %attempt, node = %state.attempts[&attempt].requests[&id].arg.node().unwrap()).await;
			let ack = protocol::ServerAck { attempt, id };
			self.sync_control_publish(subject, &client, protocol::ServerMessage::Ack(ack))
				.await;
		}
	}

	async fn sync_control_finish(&self, subject: &str, state: &mut State, result: tg::Result<()>) {
		if state.finished.is_some() {
			return;
		}
		state.finished = Some((Instant::now(), result.clone()));
		let requests = std::mem::take(&mut state.nodes);
		for requests in requests.into_values() {
			for (attempt, id) in requests {
				Self::sync_control_create_response(state, &attempt, &id);
			}
		}
		self.sync_control_send_responses(subject, state).await;
		crate::checkpoint!(self, "sync.control.finish").await;
	}

	fn sync_control_create_response(state: &mut State, attempt: &str, id: &str) -> bool {
		let Some(request) = state
			.attempts
			.get(attempt)
			.and_then(|attempt| attempt.requests.get(id))
		else {
			return false;
		};
		if request.response.is_some() {
			return true;
		}
		let protocol::ClientRequestArg::Get(arg) = &request.arg else {
			return false;
		};
		let result = state
			.graph
			.lock()
			.unwrap()
			.try_get_node_local_control_output(arg);
		let result = match result {
			Ok(None) => {
				let Some((_, result)) = &state.finished else {
					return false;
				};
				result.clone().map(|()| None)
			},
			result => result,
		};
		let request = state
			.attempts
			.get_mut(attempt)
			.unwrap()
			.requests
			.get_mut(id)
			.unwrap();
		let (error, output) = match result {
			Ok(output) => (None, Some(protocol::ServerResponseOutput::Get(output))),
			Err(error) => {
				let error = tg::error!(!error, "the sync failed");
				let tg::Either::Left(error) = error.to_data_or_id() else {
					unreachable!()
				};
				(Some(error), None)
			},
		};
		let response = protocol::ServerResponse {
			attempt: attempt.to_owned(),
			error,
			id: id.to_owned(),
			output,
		};
		request.response = Some(response);
		true
	}

	async fn sync_control_send_responses(&self, subject: &str, state: &mut State) {
		let responses = state
			.attempts
			.iter()
			.flat_map(|(attempt_id, attempt)| {
				attempt.requests.iter().filter_map(|(id, request)| {
					(request.acknowledged_at.is_none() && request.response.is_some())
						.then_some((attempt_id.clone(), id.clone()))
				})
			})
			.collect::<Vec<_>>();
		self.sync_control_send_responses_inner(subject, state, responses)
			.await;
	}

	async fn sync_control_send_responses_inner(
		&self,
		subject: &str,
		state: &mut State,
		mut responses: Vec<(String, String)>,
	) {
		responses.sort_unstable();
		if let Some(cursor) = &state.response_cursor {
			let offset = responses.partition_point(|key| key <= cursor);
			responses.rotate_left(offset);
		}

		// Resume at the next response if a slow messenger exhausts this batch's time.
		let future = async {
			for (attempt, id) in responses {
				state.response_cursor = Some((attempt.clone(), id.clone()));
				let entry = &state.attempts[&attempt];
				let response = entry.requests[&id].response.as_ref().unwrap().clone();
				self.sync_control_publish(
					subject,
					&entry.client,
					protocol::ServerMessage::Response(response),
				)
				.await;
			}
		};
		tokio::time::timeout(self.config.sync.control.retry_interval, future)
			.await
			.ok();
	}

	fn sync_control_expire(&self, state: &mut State) {
		let now = Instant::now();
		let ttl = self.config.sync.control.attempt_ttl;
		state.attempts.retain(|_, attempt| {
			attempt
				.requests
				.retain(|_, request| request.acknowledged_at.is_none_or(|time| now < time + ttl));
			now < attempt.expires_at
		});
		state
			.clients
			.retain(|_, attempt| state.attempts.contains_key(attempt));
		state.nodes.retain(|_, requests| {
			requests.retain(|(attempt, _)| state.attempts.contains_key(attempt));
			!requests.is_empty()
		});
	}

	async fn sync_control_publish(
		&self,
		subject: &str,
		client: &str,
		message: protocol::ServerMessage,
	) {
		let payload = ServerMessage(message);
		let result = tokio::time::timeout(
			self.config.sync.control.retry_interval,
			self.messenger
				.publish(client_subject(subject, client), payload),
		)
		.await;
		match result {
			Ok(Ok(())) => {},
			Ok(Err(error)) => tracing::debug!(%error, "failed to publish a sync control message"),
			Err(_) => tracing::debug!("timed out publishing a sync control message"),
		}
	}
}

impl tangram_messenger::Payload for ClientMessage {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(&self.0)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes)
			.map(Self)
			.map_err(tangram_messenger::Error::deserialization)
	}
}

impl tangram_messenger::Payload for ServerMessage {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(&self.0)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes)
			.map(Self)
			.map_err(tangram_messenger::Error::deserialization)
	}
}

pub(crate) fn subject(id: &tg::sync::Id) -> String {
	format!("syncs.{id}.control")
}

fn client_subject(subject: &str, client: &str) -> String {
	format!("{subject}.client.{client}")
}

fn attempt_subject(subject: &str, attempt: &str) -> String {
	format!("{subject}.attempts.{attempt}.server")
}

fn heartbeat_subject(subject: &str) -> String {
	format!("{subject}.server")
}
