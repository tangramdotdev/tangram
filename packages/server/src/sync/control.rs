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

pub(crate) use self::client::{Client, Output};

#[derive(Clone)]
pub(super) struct Control {
	sender: tokio::sync::mpsc::UnboundedSender<Event>,
}

struct State {
	arg: tg::sync::Arg,
	clients: BTreeMap<String, String>,
	finished: Option<(Instant, tg::Result<()>)>,
	graph: Arc<Mutex<Graph>>,
	leases: BTreeMap<String, Lease>,
	nodes: BTreeMap<tg::Id, BTreeSet<(String, String)>>,
	response_cursor: Option<(String, String)>,
}

struct Lease {
	client: String,
	expires_at: Instant,
	requests: BTreeMap<String, Request>,
}

struct Request {
	acknowledged_at: Option<Instant>,
	arg: protocol::ClientRequestArg,
	response: Option<protocol::ServerResponse>,
}

enum Event {
	Finish(tg::Result<()>),
	Nodes(Vec<tg::Id>),
}

#[derive(Clone)]
pub(crate) struct ClientMessage(pub protocol::ClientMessage);

#[derive(Clone)]
pub(crate) struct ServerMessage(pub protocol::ServerMessage);

impl Control {
	pub fn nodes(&self, ids: impl IntoIterator<Item = tg::Id>) -> tg::Result<()> {
		self.sender
			.send(Event::Nodes(ids.into_iter().collect()))
			.map_err(|error| tg::error!(!error, "failed to notify the sync control task"))?;
		Ok(())
	}

	pub fn finish(&self, result: tg::Result<()>) {
		self.sender.send(Event::Finish(result)).ok();
	}
}

impl Session {
	pub(super) async fn sync_control_respond_to_nodes(
		&self,
		state: &crate::sync::get::State,
		ids: impl IntoIterator<Item = tg::Id>,
	) -> tg::Result<()> {
		if let Some(control) = &state.control {
			control.nodes(ids)?;
		}
		Ok(())
	}

	pub(super) fn spawn_sync_control_task(
		&self,
		arg: tg::sync::Arg,
		graph: Arc<Mutex<Graph>>,
		token: &tg::sync::Token,
	) -> Control {
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let subject = subject(token);
		let state = State {
			arg,
			clients: BTreeMap::new(),
			finished: None,
			graph,
			leases: BTreeMap::new(),
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
		Control { sender }
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
				.subscribe::<ClientMessage>(format!("{subject}.server")),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out subscribing to the sync heartbeats"))?
		.map_err(|error| tg::error!(!error, "failed to subscribe to the sync heartbeats"))?;
		let lease_messages = tokio::time::timeout(
			self.config.sync.control.request_timeout,
			self.messenger
				.subscribe::<ClientMessage>(format!("{subject}.leases.*.server")),
		)
		.await
		.map_err(|error| tg::error!(!error, "timed out subscribing to the sync lease messages"))?
		.map_err(|error| tg::error!(!error, "failed to subscribe to the sync lease messages"))?;
		let messages = stream::select(heartbeat_messages, lease_messages);
		let mut messages = std::pin::pin!(messages);
		let config = &self.config.sync.control;
		let mut retry = tokio::time::interval(config.retry_interval);
		retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut events_closed = false;
		let mut stopping_at = None;

		loop {
			tokio::select! {
				() = stopper.wait(), if stopping_at.is_none() => {
					stopping_at = Some(Instant::now() + config.lease_ttl);
					self.sync_control_finish(&subject, &mut state, Err(tg::error!(code = tg::error::Code::Cancellation, "the server is stopping"))).await;
				},
				event = events.recv(), if !events_closed => {
					match event {
						Some(Event::Finish(result)) => self.sync_control_finish(&subject, &mut state, result).await,
						Some(Event::Nodes(ids)) => {
							let mut responses = Vec::new();
							for id in ids {
								if let Some(requests) = state.nodes.remove(&id) {
									for (lease, request) in requests {
										Self::sync_control_create_response(&mut state, &lease, &request, Ok(true));
										responses.push((lease, request));
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
				&& state.leases.values().all(|lease| {
					lease
						.requests
						.values()
						.all(|request| request.acknowledged_at.is_some())
				});
			if stopping_at.is_some_and(|deadline| drained || now >= deadline)
				|| state.finished.as_ref().is_some_and(|(finished_at, _)| {
					now >= *finished_at + config.lease_ttl && state.leases.is_empty()
				}) {
				break;
			}
		}

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
				if message.subject != lease_subject(subject, &ack.lease) {
					return Ok(());
				}
				if let Some(lease) = state.leases.get_mut(&ack.lease)
					&& let Some(request) = lease.requests.get_mut(&ack.id)
					&& request.response.is_some()
				{
					request.acknowledged_at.get_or_insert_with(Instant::now);
					crate::checkpoint!(
						self,
						"sync.control.response_ack",
						id = %ack.id,
						lease = %ack.lease
					)
					.await;
				}
			},
			protocol::ClientMessage::Request(request) => {
				match &request.arg {
					protocol::ClientRequestArg::Get(_) => {
						let Some(lease) = request.lease.as_ref() else {
							return Ok(());
						};
						if message.subject != lease_subject(subject, lease)
							|| state.leases.get(lease).is_none_or(|lease| {
								lease.client != request.client || Instant::now() >= lease.expires_at
							}) {
							// Only a heartbeat may establish a lease. The next heartbeat recovers a lost registration.
							return Ok(());
						}
						self.sync_control_get(subject, state, request).await;
					},
					protocol::ClientRequestArg::Heartbeat(_) => {
						if message.subject != format!("{subject}.server") || request.lease.is_some()
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
		let ttl = self.config.sync.control.lease_ttl;
		if state
			.clients
			.get(&request.client)
			.is_some_and(|lease| Instant::now() >= state.leases[lease].expires_at)
		{
			self.sync_control_expire(state);
		}
		let lease = if let Some(lease) = state.clients.get(&request.client) {
			lease.clone()
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
				.map_err(|error| tg::error!(!error, "failed to generate a sync lease"))?;
			let id = tg::id::ENCODING.encode(&bytes);
			let lease = Lease {
				client: request.client.clone(),
				expires_at: Instant::now() + ttl,
				requests: BTreeMap::new(),
			};
			state.leases.insert(id.clone(), lease);
			state.clients.insert(request.client.clone(), id.clone());
			id
		};
		let entry = state.leases.get_mut(&lease).unwrap();
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

		// A retransmitted heartbeat must not renew the lease again.
		entry.expires_at = Instant::now() + ttl;
		let output = protocol::HeartbeatServerResponseOutput { ttl };
		let response = protocol::ServerResponse {
			error: None,
			id: request.id.clone(),
			lease: lease.clone(),
			output: Some(protocol::ServerResponseOutput::Heartbeat(output)),
		};
		let client = request.client.clone();
		let entry = Request {
			acknowledged_at: None,
			arg: request.arg,
			response: Some(response.clone()),
		};
		state
			.leases
			.get_mut(&lease)
			.unwrap()
			.requests
			.insert(response.id.clone(), entry);
		crate::checkpoint!(
			self,
			"sync.control.heartbeat.response",
			client = %client,
			id = %response.id,
			lease = %lease
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
		let lease = request.lease.clone().unwrap();
		let id = request.id.clone();
		let client = request.client.clone();
		if let Some(previous) = state.leases[&lease].requests.get(&id) {
			let message = match &previous.response {
				Some(response) => protocol::ServerMessage::Response(response.clone()),
				None => protocol::ServerMessage::Ack(protocol::ServerAck { id, lease }),
			};
			self.sync_control_publish(subject, &client, message).await;
			return;
		}

		let node = request.arg.node().unwrap();
		let protocol::ClientRequestArg::Get(arg) = &request.arg else {
			unreachable!()
		};
		let carried = match arg {
			protocol::GetClientRequestArg::Object(_) => true,
			protocol::GetClientRequestArg::Process(process) => {
				(!process.children || state.arg.process_children)
					&& (!process.commands || state.arg.process_commands)
					&& (!process.errors || state.arg.process_errors)
					&& (!process.logs || state.arg.process_logs)
					&& (!process.outputs || state.arg.process_outputs)
			},
		};
		let stored = state.graph.lock().unwrap().is_node_stored(&node);
		let entry = Request {
			acknowledged_at: None,
			arg: request.arg,
			response: None,
		};
		state
			.leases
			.get_mut(&lease)
			.unwrap()
			.requests
			.insert(id.clone(), entry);
		if !carried {
			self.sync_control_respond(subject, state, &lease, &id, Ok(false))
				.await;
		} else if stored {
			self.sync_control_respond(subject, state, &lease, &id, Ok(true))
				.await;
		} else if let Some((_, result)) = &state.finished {
			let result = result.clone().map(|()| false);
			self.sync_control_respond(subject, state, &lease, &id, result)
				.await;
		} else {
			state
				.nodes
				.entry(node)
				.or_default()
				.insert((lease.clone(), id.clone()));
			crate::checkpoint!(self, "sync.control.request.retain", id = %id, lease = %lease, node = %state.leases[&lease].requests[&id].arg.node().unwrap()).await;
			let ack = protocol::ServerAck { id, lease };
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
		for (node, requests) in requests {
			let stored = state.graph.lock().unwrap().is_node_stored(&node);
			for (lease, id) in requests {
				let result = if stored {
					Ok(true)
				} else {
					result.clone().map(|()| false)
				};
				Self::sync_control_create_response(state, &lease, &id, result);
			}
		}
		self.sync_control_send_responses(subject, state).await;
		crate::checkpoint!(self, "sync.control.finish").await;
	}

	async fn sync_control_respond(
		&self,
		subject: &str,
		state: &mut State,
		lease: &str,
		id: &str,
		result: tg::Result<bool>,
	) {
		Self::sync_control_create_response(state, lease, id, result);
		let entry = &state.leases[lease];
		if let Some(response) = &entry.requests[id].response {
			self.sync_control_publish(
				subject,
				&entry.client,
				protocol::ServerMessage::Response(response.clone()),
			)
			.await;
		}
	}

	fn sync_control_create_response(
		state: &mut State,
		lease: &str,
		id: &str,
		result: tg::Result<bool>,
	) {
		let Some(node) = state
			.leases
			.get(lease)
			.and_then(|lease| lease.requests.get(id))
			.and_then(|request| request.arg.node())
		else {
			return;
		};
		let result = result.and_then(|stored| {
			state
				.graph
				.lock()
				.unwrap()
				.get_node_local_control_output(&node, stored)
		});
		let Some(entry) = state.leases.get_mut(lease) else {
			return;
		};
		let Some(request) = entry.requests.get_mut(id) else {
			return;
		};
		if request.response.is_some() {
			return;
		}
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
			error,
			id: id.to_owned(),
			lease: lease.to_owned(),
			output,
		};
		request.response = Some(response);
	}

	async fn sync_control_send_responses(&self, subject: &str, state: &mut State) {
		let responses = state
			.leases
			.iter()
			.flat_map(|(lease_id, lease)| {
				lease.requests.iter().filter_map(|(id, request)| {
					(request.acknowledged_at.is_none() && request.response.is_some())
						.then_some((lease_id.clone(), id.clone()))
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
			for (lease, id) in responses {
				state.response_cursor = Some((lease.clone(), id.clone()));
				let entry = &state.leases[&lease];
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
		let ttl = self.config.sync.control.lease_ttl;
		state.leases.retain(|_, lease| {
			lease
				.requests
				.retain(|_, request| request.acknowledged_at.is_none_or(|time| now < time + ttl));
			now < lease.expires_at
		});
		state
			.clients
			.retain(|_, lease| state.leases.contains_key(lease));
		state.nodes.retain(|_, requests| {
			requests.retain(|(lease, _)| state.leases.contains_key(lease));
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

pub(crate) fn subject(token: &tg::sync::Token) -> String {
	format!("syncs.{}.control", token.body.id)
}

fn client_subject(subject: &str, client: &str) -> String {
	format!("{subject}.client.{client}")
}

fn lease_subject(subject: &str, lease: &str) -> String {
	format!("{subject}.leases.{lease}.server")
}
