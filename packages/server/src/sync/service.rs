use {
	crate::{Server, Session, sync::graph::Graph},
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture},
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_messenger::service,
	tokio::time::Instant,
};

#[cfg(test)]
mod tests;

/// The handle with which a sync tells its service that it finished.
#[derive(Clone)]
pub(super) struct Handle {
	sender: tokio::sync::mpsc::UnboundedSender<Event>,
}

pub(super) enum Event {
	Finish(tg::Result<()>),
	Nodes(Vec<tg::Id>),
}

/// The protocol of a sync's service, which verifies nodes for other servers.
pub(crate) struct Protocol;

pub(crate) struct Request(pub tg::sync::service::Arg);

pub(crate) struct Response(pub tg::sync::service::Response);

/// Observes the sync service's events as checkpoints.
pub(crate) struct Observer {
	checkpoints: Option<Arc<crate::checkpoint::State>>,
}

/// A request that waits until the sync can answer it.
struct Retained {
	arg: tg::sync::service::Arg,
	reply: service::Reply<Protocol>,
}

impl Handle {
	pub fn finish(&self, result: tg::Result<()>) {
		self.sender.send(Event::Finish(result)).ok();
	}
}

impl Observer {
	pub fn new(checkpoints: Option<Arc<crate::checkpoint::State>>) -> Self {
		Self { checkpoints }
	}
}

impl Session {
	pub(super) fn spawn_sync_service_task(
		&self,
		graph: Arc<Mutex<Graph>>,
		id: &tg::sync::Id,
	) -> Handle {
		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		let handle = Handle { sender };
		graph.lock().unwrap().set_service(handle.sender.downgrade());
		let subject = subject(id);
		self.server
			.sync_service_tasks
			.spawn({
				let server = self.server.clone();
				move |stopper| async move {
					let result = server
						.sync_service_task(subject, graph, receiver, stopper)
						.await;
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "the sync service task failed");
					}
					result
				}
			})
			.detach();
		handle
	}
}

impl Server {
	/// Ask a sync's service to verify a node, waiting until the sync can answer.
	pub(crate) async fn request_sync_verification(
		&self,
		sync: &tg::sync::Id,
		arg: tg::sync::service::Arg,
	) -> tg::Result<Option<tg::sync::service::Output>> {
		let mut request = self
			.sync_clients
			.request(&subject(sync), Request(arg))
			.map_err(|source| tg::error!(!source, "failed to send the sync request"))?;
		let Response(response) = request.wait().await.map_err(|error| match error {
			service::Error::Stopped => {
				tg::error!(
					code = tg::error::Code::Cancellation,
					"the server is stopping"
				)
			},
			service::Error::Timeout => tg::error!(
				code = tg::error::Code::HeartbeatExpiration,
				%sync,
				"failed to recover the sync request"
			),
			error => tg::error!(!error, "the sync request failed"),
		})?;
		if let Some(error) = response.error {
			let error = tg::Error::try_from(error).unwrap_or_else(|source| {
				tg::error!(!source, "failed to deserialize the sync service error")
			});
			return Err(error);
		}
		Ok(response.output)
	}

	async fn sync_service_task(
		&self,
		subject: String,
		graph: Arc<Mutex<Graph>>,
		mut events: tokio::sync::mpsc::UnboundedReceiver<Event>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Start answering the sync's verification requests.
		crate::checkpoint!(self, "sync.service.subscribe").await;
		let options = self.config.sync.service;
		let observer = Arc::new(Observer::new(self.checkpoints.clone()));
		let mut server =
			service::serve::<Protocol, _>(&self.messenger, subject.clone(), options, observer)
				.await
				.map_err(|source| tg::error!(!source, "failed to start the sync service"))?;

		// Answer the requests as the sync's nodes arrive, retaining each until it can be answered.
		let mut finished = None::<(Instant, tg::Result<()>)>;
		let mut retained = BTreeMap::<tg::Id, Vec<Retained>>::new();
		let mut prune = tokio::time::interval(options.retry_interval);
		prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut events_closed = false;
		let mut stopping_at = None;
		loop {
			tokio::select! {
				() = stopper.wait(), if stopping_at.is_none() => {
					stopping_at = Some(Instant::now() + options.attempt_ttl);
					let error = tg::error!(code = tg::error::Code::Cancellation, "the server is stopping");
					self.sync_service_finish(&graph, &mut finished, &mut retained, Err(error)).await;
				},
				event = events.recv(), if !events_closed => {
					match event {
						Some(Event::Finish(result)) => {
							self.sync_service_finish(&graph, &mut finished, &mut retained, result).await;
						},
						Some(Event::Nodes(ids)) => {
							for id in ids {
								let Some(requests) = retained.remove(&id) else {
									continue;
								};
								let requests = self.sync_service_answer(&graph, finished.as_ref(), requests);
								if !requests.is_empty() {
									retained.insert(id, requests);
								}
							}
						},
						None => {
							events_closed = true;
							let error = tg::error!(code = tg::error::Code::Cancellation, "the sync was cancelled");
							self.sync_service_finish(&graph, &mut finished, &mut retained, Err(error)).await;
						},
					}
				},
				incoming = server.next() => {
					let Some(incoming) = incoming else {
						return Err(tg::error!("the sync service stopped"));
					};
					let Request(arg) = incoming.request;
					let node = arg.node.clone();
					let request = Retained { arg, reply: incoming.reply };
					let requests = self.sync_service_answer(&graph, finished.as_ref(), vec![request]);
					if let Some(request) = requests.into_iter().next() {
						let id = request.reply.id().to_owned();
						let attempt = request.reply.attempt().to_owned();
						crate::checkpoint!(self, "sync.service.request.retain", id = %id, attempt = %attempt, node = %node).await;
						request.reply.acknowledge().ok();
						retained.entry(node.clone()).or_default().push(request);
					}
				},
				_ = prune.tick() => {
					// Drop the retained requests whose clients cancelled them or stopped, and check whether to stop.
					retained.retain(|_, requests| {
						requests.retain(|request| !request.reply.is_cancelled());
						!requests.is_empty()
					});
				},
			}

			// Stop once a stopping server's responses are acknowledged, or once a finished sync has no attempts left.
			let now = Instant::now();
			let status = server.status();
			let drained = stopping_at.is_some() && status.unacknowledged == 0;
			if stopping_at.is_some_and(|deadline| drained || now >= deadline)
				|| finished.as_ref().is_some_and(|(finished_at, _)| {
					now >= *finished_at + options.attempt_ttl && status.attempts == 0
				}) {
				break;
			}
		}

		crate::checkpoint!(self, "sync.service.stopped", subject = %subject).await;

		Ok(())
	}

	/// Record the sync's result and answer every retained request with it.
	async fn sync_service_finish(
		&self,
		graph: &Mutex<Graph>,
		finished: &mut Option<(Instant, tg::Result<()>)>,
		retained: &mut BTreeMap<tg::Id, Vec<Retained>>,
		result: tg::Result<()>,
	) {
		if finished.is_some() {
			return;
		}
		*finished = Some((Instant::now(), result));
		for requests in std::mem::take(retained).into_values() {
			self.sync_service_answer(graph, finished.as_ref(), requests);
		}
		crate::checkpoint!(self, "sync.service.finish").await;
	}

	/// Answer the requests that the sync can answer, and return the rest.
	fn sync_service_answer(
		&self,
		graph: &Mutex<Graph>,
		finished: Option<&(Instant, tg::Result<()>)>,
		requests: Vec<Retained>,
	) -> Vec<Retained> {
		let mut remaining = Vec::new();
		for request in requests {
			if request.reply.is_cancelled() {
				continue;
			}
			let result =
				Self::sync_service_result(graph, finished.map(|(_, result)| result), &request.arg);
			let Some(result) = result else {
				remaining.push(request);
				continue;
			};
			let response = self.sync_service_response(&request.arg, result);
			request.reply.send(&Response(response)).ok();
		}
		remaining
	}

	/// Get the result of a request, or `None` if the sync cannot answer it yet.
	fn sync_service_result(
		graph: &Mutex<Graph>,
		finished: Option<&tg::Result<()>>,
		arg: &tg::sync::service::Arg,
	) -> Option<tg::Result<Option<tg::sync::service::Output>>> {
		let result = graph
			.lock()
			.unwrap()
			.try_verify_node_local_service_output(arg);
		match result {
			Ok(None) => finished.map(|result| result.clone().map(|()| None)),
			result => Some(result),
		}
	}

	/// Create the response to a request, with a token for the verified permissions.
	fn sync_service_response(
		&self,
		arg: &tg::sync::service::Arg,
		result: tg::Result<Option<tg::sync::service::Output>>,
	) -> tg::sync::service::Response {
		let result = result.and_then(|mut output| {
			if let Some(output) = &mut output {
				self.sync_service_add_token(arg, output)?;
			}
			Ok(output)
		});
		match result {
			Ok(output) => tg::sync::service::Response {
				error: None,
				output,
			},
			Err(error) => {
				let error = tg::error!(!error, "the sync failed");
				let tg::Either::Left(error) = error.to_data_or_id() else {
					unreachable!()
				};
				tg::sync::service::Response {
					error: Some(error),
					output: None,
				}
			},
		}
	}

	fn sync_service_add_token(
		&self,
		arg: &tg::sync::service::Arg,
		output: &mut tg::sync::service::Output,
	) -> tg::Result<()> {
		let permissions = output.permissions().iter().collect::<Vec<_>>();
		if permissions.is_empty() {
			return Ok(());
		}
		let expires_at = self.clock.unix_timestamp()?
			+ i64::try_from(self.config.sync.permission_time_to_live.as_secs()).unwrap();
		let body = tg::authorization::Body {
			expires_at,
			permissions,
			resource: arg.node.clone(),
		};
		match output {
			tg::sync::service::Output::Object(output) => output.tokens.push(body),
			tg::sync::service::Output::Process(output) => output.tokens.push(body),
		}
		Ok(())
	}
}

impl service::Protocol for Protocol {
	type Request = Request;
	type Response = Response;

	fn accepts(request: &Self::Request, response: &Self::Response) -> bool {
		response.0.error.is_some()
			|| response
				.0
				.output
				.as_ref()
				.is_none_or(|output| output.satisfies(&request.0))
	}
}

impl service::Observer<Protocol> for Observer {
	fn observe<'a>(&'a self, event: service::Event<'a, Protocol>) -> BoxFuture<'a, ()> {
		async move {
			let Some(checkpoints) = &self.checkpoints else {
				return;
			};
			let mut params = tg::checkpoint::Params::new();
			let mut insert = |key: &str, value: &str| {
				params.insert(key.to_owned(), value.to_owned().into());
			};
			let name = match event {
				service::Event::Acknowledged {
					attempt,
					id,
					request,
				} => {
					insert("attempt", attempt);
					insert("id", id);
					insert("node", &request.0.node.to_string());
					"sync.service.ack"
				},
				service::Event::Attempt { attempt, client } => {
					insert("attempt", attempt);
					insert("client", client);
					"sync.service.attempt"
				},
				service::Event::Cancelled { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"sync.service.cancel"
				},
				service::Event::HeartbeatResponded {
					attempt,
					client,
					id,
				} => {
					insert("attempt", attempt);
					insert("client", client);
					insert("id", id);
					"sync.service.heartbeat.response"
				},
				service::Event::HeartbeatSent { client, id } => {
					insert("client", client);
					insert("id", id);
					"sync.service.heartbeat.request"
				},
				service::Event::RequestSent {
					attempt,
					id,
					request,
				} => {
					if let Some(attempt) = attempt {
						insert("attempt", attempt);
					}
					insert("id", id);
					insert("node", &request.0.node.to_string());
					"sync.service.request"
				},
				service::Event::Responded { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"sync.service.response"
				},
				service::Event::ResponseAcknowledged { attempt, id } => {
					insert("attempt", attempt);
					insert("id", id);
					"sync.service.response_ack"
				},
				service::Event::Started { client, subject } => {
					insert("client", client);
					insert("subject", subject);
					"sync.service.heartbeat.start"
				},
			};
			checkpoints.hit(name, params).await;
		}
		.boxed()
	}
}

impl tangram_messenger::Payload for Request {
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

impl tangram_messenger::Payload for Response {
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
	format!("syncs.{id}.service")
}
