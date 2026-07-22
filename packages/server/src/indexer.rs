use {
	crate::{Server, Session},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream::FuturesUnordered,
	},
	num::ToPrimitive as _,
	std::{collections::BTreeMap, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_index::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
	tangram_object_store::Store as _,
};

type Barriers = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;
type Sender = crate::control::Sender<ServerMessage, ClientMessage>;

#[derive(Clone)]
struct Indexer {
	server: Server,
}

struct State {
	barriers: Barriers,
	database_outbox_id: Option<crate::database::outbox::Id>,
	object_outbox_id: Option<crate::object::outbox::Id>,
	requests: BTreeMap<String, IndexRequest>,
}

struct IndexRequest {
	state: IndexRequestState,
}

enum IndexRequestState {
	DatabaseOutbox,
	DatabaseOutboxPending,
	Finalizations { transaction_id: Option<u64> },
	ObjectOutbox,
	ObjectOutboxPending,
	Tasks,
	Updates { transaction_id: Option<u64> },
}

enum Event {
	Barrier(Vec<String>),
	Message(ServerMessage),
	Poll,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ClientMessage {
	Ack(Ack),
	Response(Response),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ServerMessage {
	Ack(Ack),
	Request(Request),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Ack {
	pub id: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Request {
	pub arg: RequestArg,
	pub id: String,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum RequestArg {
	Index,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Response {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ResponseOutput>,
}

#[derive(Clone, Debug, derive_more::TryUnwrap, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(crate) enum ResponseOutput {
	Index,
}

impl Server {
	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let indexer = Indexer {
			server: self.clone(),
		};
		let poll_interval = config.poll_interval;

		// Spawn the database outbox task.
		let database_outbox_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			let outbox = self.config.database.outbox().clone();
			let region = self.config.region.clone().unwrap_or_default();
			move |_| async move {
				indexer
					.database_outbox_task(&config, &outbox, &region)
					.await
			}
		});

		// Spawn the object outbox task.
		let object_outbox_task = Task::spawn({
			let config = config.clone();
			let outbox =
				(!self.config.advanced.single_process).then(|| self.config.object.outbox.clone());
			let indexer = indexer.clone();
			move |_| async move { indexer.object_outbox_task(&config, outbox.as_ref()).await }
		});

		// Spawn the update task.
		let update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move { indexer.update_task(&config).await }
		});

		// Spawn the request task.
		let request_task = Task::spawn({
			let indexer = indexer.clone();
			move |_| async move { indexer.request_task(poll_interval).await }
		});

		// Wait for the tasks.
		let database_outbox_future = async move {
			database_outbox_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the database outbox task panicked"))?
		};
		let object_outbox_future = async move {
			object_outbox_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the object outbox task panicked"))?
		};
		let update_future = async move {
			update_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the index update task panicked"))?
		};
		let request_future = async move {
			request_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the indexer request task panicked"))?
		};
		future::try_join4(
			database_outbox_future,
			object_outbox_future,
			update_future,
			request_future,
		)
		.await?;

		Ok(())
	}
}

impl Session {
	pub(crate) async fn send_indexer_request(
		&self,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		let id = crate::control::id();
		let request = ServerMessage::Request(Request {
			arg,
			id: id.clone(),
		});
		let options = self.indexer_message_options();
		self.send_control_request(crate::control::SendControlRequestArg {
			ack: |id| ServerMessage::Ack(Ack { id }),
			client_subject: Indexer::client_subject(&id),
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: ClientMessage| {
				let ClientMessage::Response(message) = message else {
					return Ok(None);
				};
				if let Some(error) = message.error {
					let error = tg::Error::try_from(error).map_err(|source| {
						tg::error!(!source, "failed to deserialize the indexer error")
					})?;
					return Ok(Some((message.id, Err(error))));
				}
				let Some(output) = message.output else {
					return Err(tg::error!("missing indexer response output"));
				};
				Ok(Some((message.id, Ok(output))))
			},
			server_subject: Indexer::server_subject(),
		})
		.await
	}

	fn indexer_message_options(&self) -> crate::control::Options {
		let config = self.server.config.indexer.clone().unwrap_or_default();
		crate::control::Options {
			retry: config.message_retry.into(),
			timeout: config.message_timeout,
		}
	}
}

impl Indexer {
	async fn database_outbox_task(
		&self,
		config: &crate::config::Indexer,
		outbox: &crate::config::DatabaseOutbox,
		region: &str,
	) -> tg::Result<()> {
		loop {
			match self.database_outbox_batch(config, outbox, region).await {
				Ok(0) => {
					tokio::time::sleep(config.poll_interval).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to service the database outbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn database_outbox_batch(
		&self,
		config: &crate::config::Indexer,
		outbox: &crate::config::DatabaseOutbox,
		region: &str,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::database::outbox::DequeueArg {
			batch_size: outbox.batch_size,
			partition_end: config.partition_end,
			partition_start: config.partition_start,
			region: region.to_owned(),
		};
		let items = self
			.server
			.database
			.dequeue_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the database outbox"))?;
		if items.is_empty() {
			return Ok(0);
		}

		// Deserialize and combine the index batches.
		let count = items.len();
		let mut arg = tangram_index::batch::Arg::default();
		let mut keys = Vec::with_capacity(count);
		for item in items {
			let item_arg = tangram_index::batch::Arg::deserialize(&item.payload)?;
			append_index_batch(&mut arg, item_arg);
			let key = crate::database::outbox::Key {
				id: item.id,
				partition: item.partition,
			};
			keys.push(key);
		}

		// Write the batch to the index before deleting the durable intents.
		self.server
			.index
			.batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index a database outbox batch"))?;
		let arg = crate::database::outbox::DeleteArg {
			keys,
			region: region.to_owned(),
		};
		self.server
			.database
			.delete_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete a database outbox batch"))?;

		Ok(count)
	}

	async fn object_outbox_task(
		&self,
		config: &crate::config::Indexer,
		outbox: Option<&crate::config::ObjectOutbox>,
	) -> tg::Result<()> {
		let Some(outbox) = outbox else {
			return future::pending().await;
		};
		loop {
			match self.object_outbox_batch(config, outbox).await {
				Ok(0) => {
					tokio::time::sleep(config.poll_interval).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to service the object outbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn object_outbox_batch(
		&self,
		config: &crate::config::Indexer,
		outbox: &crate::config::ObjectOutbox,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::object::outbox::DequeueArg {
			batch_size: outbox.batch_size,
			partition_end: config.partition_end,
			partition_start: config.partition_start,
		};
		let items = self
			.server
			.object_store
			.dequeue_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the object outbox"))?;
		if items.is_empty() {
			return Ok(0);
		}

		// Deserialize and combine the index batches.
		let count = items.len();
		let mut arg = tangram_index::batch::Arg::default();
		let mut keys = Vec::with_capacity(count);
		for item in items {
			let item_arg = tangram_index::batch::Arg::deserialize(&item.payload)?;
			append_index_batch(&mut arg, item_arg);
			let key = crate::object::outbox::Key {
				id: item.id,
				partition: item.partition,
			};
			keys.push(key);
		}

		// Write the batch to the index before deleting the durable intents.
		self.server
			.index
			.batch(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to index an object outbox batch"))?;
		let arg = crate::object::outbox::DeleteArg { keys };
		self.server
			.object_store
			.delete_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an object outbox batch"))?;

		Ok(count)
	}

	async fn request_task(&self, poll_interval: Duration) -> tg::Result<()> {
		loop {
			let result = self.request_task_inner(poll_interval).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the indexer request task failed");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn request_task_inner(&self, poll_interval: Duration) -> tg::Result<()> {
		let messages = self
			.server
			.messenger
			.subscribe::<ServerMessage>(Self::server_subject())
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to subscribe to the indexer request stream")
			})?
			.map_err(|source| tg::error!(!source, "failed to receive an indexer message"))
			.map_ok(|message| message.payload)
			.boxed();
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		let control =
			crate::control::Stream::new(messages, sender, crate::control::stream_options());
		let requests = self.handle_requests(control, poll_interval);
		let responses = self.publish_client_messages(receiver);
		future::try_join(requests, responses).await?;

		Ok(())
	}

	async fn handle_requests(
		&self,
		mut control: crate::control::Stream<ServerMessage, ClientMessage>,
		poll_interval: Duration,
	) -> tg::Result<()> {
		let mut interval = tokio::time::interval(poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut state = State::new();
		loop {
			let event = tokio::select! {
				barrier = state.barriers.next(), if !state.barriers.is_empty() => {
					Event::Barrier(barrier.unwrap())
				},
				message = control.recv() => {
					let message = message?
						.ok_or_else(|| tg::error!("the indexer request stream ended"))?;
					Event::Message(message)
				},
				_ = interval.tick(), if state.needs_poll() => Event::Poll,
			};
			match event {
				Event::Barrier(ids) => {
					state.handle_barrier(ids, !self.server.config.advanced.single_process);
					state.start_barrier(&self.server);
				},
				Event::Message(ServerMessage::Ack(_)) => unreachable!(),
				Event::Message(ServerMessage::Request(request)) => match request.arg {
					RequestArg::Index => {
						state.requests.insert(
							request.id,
							IndexRequest {
								state: IndexRequestState::Tasks,
							},
						);
						state.start_barrier(&self.server);
					},
				},
				Event::Poll => {
					let sender = control.sender();
					if let Err(error) = state.poll(&self.server, &sender).await {
						state.fail(&error, &sender);
					}
				},
			}
		}
	}

	async fn publish_client_messages(
		&self,
		mut receiver: tokio::sync::mpsc::Receiver<ClientMessage>,
	) -> tg::Result<()> {
		while let Some(message) = receiver.recv().await {
			let id = message.id().to_owned();
			let server = self.server.clone();
			tokio::spawn(async move {
				let result = server
					.messenger
					.publish(Self::client_subject(&id), message)
					.await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish an indexer client message");
				}
			});
		}

		Err(tg::error!("the indexer client message stream ended"))
	}

	async fn update_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let concurrency = config.concurrency.to_u64().unwrap();
		let partition_end = config.partition_end;
		let partition_start = config.partition_start;
		let partition_length = partition_end - partition_start;
		loop {
			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_length / concurrency;
				let extra = partition_length % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				let task_end = task_start + task_count;
				self.server
					.index
					.update_batch(config.batch_size, task_start, task_end)
			});
			let result = future::try_join_all(futures)
				.await
				.map(|counts| counts.into_iter().sum::<usize>());
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to index");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	fn client_subject(id: &str) -> String {
		format!("indexer.client.{id}")
	}

	fn server_subject() -> String {
		"indexer.server".to_owned()
	}
}

impl State {
	fn new() -> Self {
		Self {
			barriers: Barriers::new(),
			database_outbox_id: None,
			object_outbox_id: None,
			requests: BTreeMap::new(),
		}
	}

	async fn poll(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		// Wait for the object outbox.
		self.poll_object_outbox(server).await?;

		// Wait for the database outbox.
		self.poll_database_outbox(server).await?;

		// Wait for the process finalization queue.
		self.set_finalization_targets(server).await?;
		self.poll_finalizations(server).await?;

		// Wait for the index update queue.
		self.set_update_targets(server).await?;
		self.poll_updates(server, sender).await?;

		Ok(())
	}

	async fn poll_object_outbox(&mut self, server: &Server) -> tg::Result<()> {
		if server.config.advanced.single_process {
			return Ok(());
		}
		let config = &server.config.object.outbox;

		// Poll the active cohort.
		if let Some(id) = self.object_outbox_id {
			let arg = crate::object::outbox::TryGetIdArg {
				id: Some(id),
				partition_end: config.partition_total,
				partition_start: 0,
			};
			let id = server
				.object_store
				.try_get_outbox_id_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the object outbox"))?;
			if id.is_some() {
				return Ok(());
			}
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::ObjectOutboxPending) {
					request.state = IndexRequestState::DatabaseOutbox;
				}
			}
			self.object_outbox_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::ObjectOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::object::outbox::TryGetIdArg {
			id: None,
			partition_end: config.partition_total,
			partition_start: 0,
		};
		let id = server
			.object_store
			.try_get_outbox_id_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the object outbox"))?;
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::ObjectOutbox) {
				continue;
			}
			request.state = if id.is_some() {
				IndexRequestState::ObjectOutboxPending
			} else {
				IndexRequestState::DatabaseOutbox
			};
		}
		self.object_outbox_id = id;

		Ok(())
	}

	async fn poll_database_outbox(&mut self, server: &Server) -> tg::Result<()> {
		let config = server.config.database.outbox();
		let region = server.config.region.clone().unwrap_or_default();

		// Poll the active cohort.
		if let Some(id) = self.database_outbox_id {
			let arg = crate::database::outbox::TryGetIdArg {
				id: Some(id),
				partition_end: config.partition_total,
				partition_start: 0,
				region,
			};
			let id = server
				.database
				.try_get_outbox_id_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the database outbox"))?;
			if id.is_some() {
				return Ok(());
			}
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::DatabaseOutboxPending) {
					request.state = IndexRequestState::Finalizations {
						transaction_id: None,
					};
				}
			}
			self.database_outbox_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::DatabaseOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::database::outbox::TryGetIdArg {
			id: None,
			partition_end: config.partition_total,
			partition_start: 0,
			region,
		};
		let id = server
			.database
			.try_get_outbox_id_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the database outbox"))?;
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::DatabaseOutbox) {
				continue;
			}
			request.state = if id.is_some() {
				IndexRequestState::DatabaseOutboxPending
			} else {
				IndexRequestState::Finalizations {
					transaction_id: None,
				}
			};
		}
		self.database_outbox_id = id;

		Ok(())
	}

	async fn set_finalization_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Finalizations {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.requests.values_mut() {
			if let IndexRequestState::Finalizations {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_finalizations(&mut self, server: &Server) -> tg::Result<()> {
		let poll = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Finalizations {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldest = server
			.index
			.try_get_oldest_finalization_transaction_id(tangram_index::finalization::Kind::Process)
			.await?;
		for request in self.requests.values_mut() {
			let IndexRequestState::Finalizations {
				transaction_id: Some(transaction_id),
			} = request.state
			else {
				continue;
			};
			if oldest.is_none_or(|oldest| oldest > transaction_id) {
				request.state = IndexRequestState::Updates {
					transaction_id: None,
				};
			}
		}

		Ok(())
	}

	fn start_barrier(&mut self, server: &Server) {
		if !self.barriers.is_empty() {
			return;
		}
		let ids = self
			.requests
			.iter()
			.filter(|(_, request)| matches!(request.state, IndexRequestState::Tasks))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		if ids.is_empty() {
			return;
		}
		let server = server.clone();
		self.barriers.push(
			async move {
				server.remote_object_put_tasks.wait().await;
				server.index_tasks.wait().await;

				ids
			}
			.boxed(),
		);
	}

	fn handle_barrier(&mut self, ids: Vec<String>, object_outbox: bool) {
		for id in ids {
			let Some(request) = self.requests.get_mut(&id) else {
				continue;
			};
			if matches!(request.state, IndexRequestState::Tasks) {
				request.state = if object_outbox {
					IndexRequestState::ObjectOutbox
				} else {
					IndexRequestState::DatabaseOutbox
				};
			}
		}
	}

	async fn set_update_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Updates {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.requests.values_mut() {
			if let IndexRequestState::Updates {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_updates(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		let poll = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Updates {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldest = server.index.try_get_oldest_update_transaction_id().await?;
		let ids = self
			.requests
			.iter()
			.filter_map(|(id, request)| {
				let IndexRequestState::Updates {
					transaction_id: Some(transaction_id),
				} = request.state
				else {
					return None;
				};
				oldest
					.is_none_or(|oldest| oldest > transaction_id)
					.then(|| id.clone())
			})
			.collect::<Vec<_>>();
		for id in ids {
			self.requests.remove(&id);
			Self::send_response(id, Ok(ResponseOutput::Index), sender);
		}

		Ok(())
	}

	fn fail(&mut self, error: &tg::Error, sender: &Sender) {
		let error = error.to_string();
		self.database_outbox_id = None;
		self.object_outbox_id = None;
		let ids = std::mem::take(&mut self.requests).into_keys();
		for id in ids {
			Self::send_response(
				id,
				Err(tg::error!(error = %error, "failed to wait for indexing")),
				sender,
			);
		}
	}

	fn send_response(id: String, result: tg::Result<ResponseOutput>, sender: &Sender) {
		let response = match result {
			Ok(output) => Response {
				error: None,
				id,
				output: Some(output),
			},
			Err(error) => Response {
				error: Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				id,
				output: None,
			},
		};
		let sender = sender.clone();
		tokio::spawn(async move {
			if let Err(error) = sender.send(ClientMessage::Response(response)).await {
				tracing::error!(error = %error.trace(), "failed to send an indexer response");
			}
		});
	}

	fn needs_poll(&self) -> bool {
		self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::DatabaseOutbox
					| IndexRequestState::DatabaseOutboxPending
					| IndexRequestState::Finalizations { .. }
					| IndexRequestState::ObjectOutbox
					| IndexRequestState::ObjectOutboxPending
					| IndexRequestState::Updates { .. }
			)
		})
	}
}

impl ClientMessage {
	fn id(&self) -> &str {
		match self {
			Self::Ack(ack) => &ack.id,
			Self::Response(response) => &response.id,
		}
	}
}

impl crate::control::Input<ClientMessage> for ServerMessage {
	fn kind(&self) -> crate::control::InputKind<'_> {
		match self {
			Self::Ack(ack) => crate::control::InputKind::Ack { id: &ack.id },
			Self::Request(request) => crate::control::InputKind::Message {
				id: Some(&request.id),
			},
		}
	}

	fn create_ack_message(id: String) -> ClientMessage {
		ClientMessage::Ack(Ack { id })
	}
}

impl crate::control::Output for ClientMessage {
	fn id(&self) -> Option<&str> {
		match self {
			Self::Ack(_) => None,
			Self::Response(response) => Some(&response.id),
		}
	}
}

impl Payload for ClientMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}
}

impl Payload for ServerMessage {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error> {
		serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let bytes = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(bytes.into())
	}
}

fn append_index_batch(batch: &mut tangram_index::batch::Arg, arg: tangram_index::batch::Arg) {
	let tangram_index::batch::Arg {
		delete_grants,
		delete_group_members,
		delete_groups,
		delete_organization_members,
		delete_organizations,
		delete_sandboxes,
		delete_tags,
		delete_users,
		put_cache_entries,
		put_grants,
		put_group_members,
		put_groups,
		put_objects,
		put_organization_members,
		put_organizations,
		put_processes,
		put_runners,
		put_sandboxes,
		put_tags,
		put_users,
	} = arg;
	batch.delete_grants.extend(delete_grants);
	batch.delete_group_members.extend(delete_group_members);
	batch.delete_groups.extend(delete_groups);
	batch
		.delete_organization_members
		.extend(delete_organization_members);
	batch.delete_organizations.extend(delete_organizations);
	batch.delete_sandboxes.extend(delete_sandboxes);
	batch.delete_tags.extend(delete_tags);
	batch.delete_users.extend(delete_users);
	batch.put_cache_entries.extend(put_cache_entries);
	batch.put_grants.extend(put_grants);
	batch.put_group_members.extend(put_group_members);
	batch.put_groups.extend(put_groups);
	batch.put_objects.extend(put_objects);
	batch
		.put_organization_members
		.extend(put_organization_members);
	batch.put_organizations.extend(put_organizations);
	batch.put_processes.extend(put_processes);
	batch.put_runners.extend(put_runners);
	batch.put_sandboxes.extend(put_sandboxes);
	batch.put_tags.extend(put_tags);
	batch.put_users.extend(put_users);
}
