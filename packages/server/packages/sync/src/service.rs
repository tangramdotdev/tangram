use {
	crate::{Component, graph::Graph},
	futures::FutureExt as _,
	std::{
		collections::BTreeMap,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::{stream::TryExt as _, task::Stopper},
	tangram_server_core::{Server, Session},
	tangram_server_service as service,
	tokio::time::Instant,
};

#[cfg(test)]
mod tests;

/// The name of the sync service's checkpoints.
pub(crate) const NAME: &str = "sync.service";

/// The subject under which the sync services serve the subjects of their syncs.
pub(crate) const SUBJECT: &str = "syncs.service";

/// The handle with which a sync tells its service that it finished.
#[derive(Clone)]
pub(super) struct Handle {
	sender: tokio::sync::mpsc::UnboundedSender<tg::Result<()>>,
}

/// A request that waits until the sync can answer it.
struct Retained {
	arg: tg::sync::service::VerifyArg,
	reply: service::Reply<Protocol>,
}

/// The protocol of a sync's service, which verifies nodes for other servers.
pub(crate) struct Protocol;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum Request {
	#[tangram_serialize(id = 0)]
	Verify(tg::sync::service::VerifyArg),
}

#[derive(Clone)]
pub(crate) struct Response(pub tg::sync::service::Response);

impl Handle {
	pub fn finish(&self, result: tg::Result<()>) {
		self.sender.send(result).ok();
	}
}

impl Component {
	pub(super) fn spawn_sync_service_task(
		&self,
		session: &Session,
		graph: Arc<Mutex<Graph>>,
		id: &tg::sync::Id,
	) -> Handle {
		let (sender, finish) = tokio::sync::mpsc::unbounded_channel();
		let handle = Handle { sender };
		let (nodes_sender, nodes) = tokio::sync::mpsc::unbounded_channel();
		graph.lock().unwrap().set_changed(nodes_sender.downgrade());
		self.service_tasks
			.spawn({
				let component = self.clone();
				let id = id.clone();
				let server = session.server.clone();
				move |stopper| async move {
					let result = component
						.sync_service_task(&server, &id, graph, finish, nodes, stopper)
						.await;

					// Close the graph's node notifications once the service stops.
					drop(nodes_sender);
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

impl Component {
	/// Ask a sync's service to verify a node, over the local channel if it runs on this server, waiting until the sync can answer.
	pub(crate) async fn request_sync_verification(
		&self,
		sync: &tg::sync::Id,
		arg: tg::sync::service::VerifyArg,
	) -> tg::Result<Option<tg::sync::service::Output>> {
		let server = self.servers.get(sync).map(|server| server.value().clone());
		let result = self
			.client
			.request(sync, server.as_ref(), Request::Verify(arg))
			.await;
		let Response(response) = result.map_err(|error| match error {
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
		server: &Server,
		id: &tg::sync::Id,
		graph: Arc<Mutex<Graph>>,
		mut finish: tokio::sync::mpsc::UnboundedReceiver<tg::Result<()>>,
		mut nodes: tokio::sync::mpsc::UnboundedReceiver<Vec<tg::Id>>,
		stopper: Stopper,
	) -> tg::Result<()> {
		// Start answering the sync's verification requests, and register the service so that the requests on this server skip the messenger.
		tangram_server_core::checkpoint!(server, "sync.service.subscribe").await;
		let options = server.config.sync.service;
		let observer = Arc::new(observer(server.checkpoints.clone()));
		let service = service::Server::start(&server.messenger, SUBJECT, id, options, observer);
		self.servers.insert(id.clone(), service.clone());

		// Stop and unregister the service when this task ends, even if it panics or is aborted.
		let guard = scopeguard::guard((), {
			let component = self.clone();
			let id = id.clone();
			let service = service.clone();
			move |()| {
				service.stop();
				component
					.servers
					.remove_if(&id, |_, server| server.is_stopped());
			}
		});

		// Answer the requests as the sync's nodes arrive, retaining each until it can be answered.
		let mut finished = None::<tg::Result<()>>;
		let mut record = None;
		let mut recorded = false;
		let mut retained = BTreeMap::<tg::Id, Vec<Retained>>::new();
		let mut prune = tokio::time::interval(options.retry_interval);
		prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut finish_closed = false;
		let mut cancelled_at = None;
		let result = loop {
			tokio::select! {
				() = stopper.wait() => {
					let error = tg::error!(code = tg::error::Code::Cancellation, "the server is stopping");
					self.sync_service_finish(server, &graph, &mut finished, &mut retained, Err(error)).await;
					break Ok(());
				},
				result = finish.recv(), if !finish_closed => {
					let result = if let Some(result) = result {
						result
					} else {
						finish_closed = true;
						Err(tg::error!(code = tg::error::Code::Cancellation, "the sync was cancelled"))
					};
					// Reject the requests of a cancelled sync for one attempt TTL, instead of staying subscribed while requesters retry.
					if finished.is_none()
						&& cancelled_at.is_none()
						&& result.as_ref().err().is_some_and(error_is_cancellation)
					{
						cancelled_at = Some(Instant::now());
					}
					self.sync_service_finish(server, &graph, &mut finished, &mut retained, result).await;
					if record.is_none() && !recorded {
						record = Some(Self::sync_service_record(server.clone(), id.clone()).boxed());
					}
				},
				() = async { record.as_mut().unwrap().await }, if record.is_some() => {
					record = None;
					recorded = true;
				},
				Some(ids) = nodes.recv() => {
					for id in ids {
						let Some(requests) = retained.remove(&id) else {
							continue;
						};
						let requests = Self::sync_service_answer(server, &graph, finished.as_ref(), requests);
						if !requests.is_empty() {
							retained.insert(id, requests);
						}
					}
				},
				incoming = service.next() => {
					let Some((request, reply)) = incoming else {
						break Err(tg::error!("the sync service stopped"));
					};
					let Request::Verify(arg) = request;
					let node = arg.node.clone();
					let request = Retained { arg, reply };
					let requests = Self::sync_service_answer(server, &graph, finished.as_ref(), vec![request]);
					if let Some(request) = requests.into_iter().next() {
						tangram_server_core::checkpoint!(server, "sync.service.request.retain", node = %node).await;
						request.reply.acknowledge();
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

			// Stop once a cancelled sync has rejected requests for one attempt TTL, or once the index records that the sync finished and the sync has no attempts left.
			let now = Instant::now();
			let status = service.status();
			if cancelled_at.is_some_and(|cancelled_at| now >= cancelled_at + options.attempt_ttl)
				|| (recorded && status.attempts == 0)
			{
				break Ok(());
			}
		};

		// Stop the service, unregister it, and wait for its responses to be acknowledged.
		drop(guard);
		service.wait().await;
		let subject = format!("{SUBJECT}.{id}");
		tangram_server_core::checkpoint!(server, "sync.service.stopped", subject = %subject).await;

		result
	}

	/// Record the sync's result and answer every retained request with it.
	async fn sync_service_finish(
		&self,
		server: &Server,
		graph: &Mutex<Graph>,
		finished: &mut Option<tg::Result<()>>,
		retained: &mut BTreeMap<tg::Id, Vec<Retained>>,
		result: tg::Result<()>,
	) {
		if finished.is_some() {
			return;
		}
		*finished = Some(result);
		for requests in std::mem::take(retained).into_values() {
			Self::sync_service_answer(server, graph, finished.as_ref(), requests);
		}
		tangram_server_core::checkpoint!(server, "sync.service.finish").await;
	}

	/// Record in the index that the sync finished, after its permissions are indexed, so that verification does not ask the sync's service once it stops.
	async fn sync_service_record(server: Server, id: tg::sync::Id) {
		let result = async {
			// Wait for the sync's permissions to be indexed.
			let session = server.session(&server.context);
			Self::sync_service_await_indexing(&session).await?;

			// Record that the sync finished.
			let time_to_live = server.config.sync.permission_time_to_live.as_secs();
			let time_to_live = i64::try_from(time_to_live)
				.map_err(|source| tg::error!(!source, "invalid sync permission time to live"))?;
			let expires_at = server.clock.unix_timestamp() + time_to_live;
			let arg = tangram_index::sync::put::Arg {
				expires_at,
				id: id.clone(),
			};
			let arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSync(arg)],
			};
			server.components.indexer.index_batch(&server, arg).await?;

			// Wait for the record to be indexed.
			Self::sync_service_await_indexing(&session).await?;

			Ok::<_, tg::Error>(())
		}
		.await;
		if let Err(error) = result {
			tracing::error!(error = %error.trace(), %id, "failed to record that the sync finished");
		}
	}

	async fn sync_service_await_indexing(session: &Session) -> tg::Result<()> {
		session
			.server
			.components
			.indexer
			.index(session, tg::index::Arg::default())
			.await?
			.try_last()
			.await?;
		Ok(())
	}

	/// Answer the requests that the sync can answer, and return the rest.
	fn sync_service_answer(
		server: &Server,
		graph: &Mutex<Graph>,
		finished: Option<&tg::Result<()>>,
		requests: Vec<Retained>,
	) -> Vec<Retained> {
		let mut remaining = Vec::new();
		for request in requests {
			if request.reply.is_cancelled() {
				continue;
			}
			let result = Self::sync_service_result(graph, finished, &request.arg);
			let Some(result) = result else {
				remaining.push(request);
				continue;
			};
			let response = Self::sync_service_response(server, &request.arg, result);
			request.reply.send(&Response(response));
		}
		remaining
	}

	/// Get the result of a request, or `None` if the sync cannot answer it yet.
	fn sync_service_result(
		graph: &Mutex<Graph>,
		finished: Option<&tg::Result<()>>,
		arg: &tg::sync::service::VerifyArg,
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
		server: &Server,
		arg: &tg::sync::service::VerifyArg,
		result: tg::Result<Option<tg::sync::service::Output>>,
	) -> tg::sync::service::Response {
		let result = result.map(|mut output| {
			if let Some(output) = &mut output {
				Self::sync_service_add_token(server, arg, output);
			}
			output
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
		server: &Server,
		arg: &tg::sync::service::VerifyArg,
		output: &mut tg::sync::service::Output,
	) {
		let permissions = output.permissions().iter().collect::<Vec<_>>();
		if permissions.is_empty() {
			return;
		}
		let expires_at = server.clock.unix_timestamp()
			+ i64::try_from(server.config.sync.permission_time_to_live.as_secs()).unwrap();
		let body = tg::authorization::Body {
			expires_at,
			permissions,
			resource: arg.node.clone(),
		};
		match output {
			tg::sync::service::Output::Object(output) => output.tokens.push(body),
			tg::sync::service::Output::Process(output) => output.tokens.push(body),
		}
	}
}

impl service::Protocol for Protocol {
	type Request = Request;
	type Response = Response;

	fn accepts(request: &Self::Request, response: &Self::Response) -> bool {
		match request {
			Request::Verify(arg) => {
				response.0.error.is_some()
					|| response
						.0
						.output
						.as_ref()
						.is_none_or(|output| output.satisfies(arg))
			},
		}
	}
}

impl tangram_messenger::Payload for Request {
	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		tangram_serialize::to_vec(self)
			.map(Into::into)
			.map_err(tangram_messenger::Error::serialization)
	}

	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		tangram_serialize::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
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

fn error_is_cancellation(error: &tg::Error) -> bool {
	let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
	while let Some(error) = current {
		if error
			.downcast_ref::<tg::Error>()
			.is_some_and(|error| matches!(error.code(), Some(tg::error::Code::Cancellation)))
		{
			return true;
		}
		current = error.source();
	}

	false
}

/// Create the observer of the sync service's events, whose checkpoints carry the node of each request.
pub(crate) fn observer(
	checkpoints: Option<Arc<tangram_server_core::checkpoint::State>>,
) -> service::Checkpoints<Protocol> {
	service::Checkpoints::with_params(checkpoints, NAME, |request, params| match request {
		Request::Verify(arg) => {
			params.insert("node".to_owned(), arg.node.to_string().into());
		},
	})
}
