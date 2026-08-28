use {
	crate::{Server, Session},
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		pin::pin,
		time::Duration,
	},
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_index::prelude::*,
	tangram_messenger::{Messenger as _, Payload},
	tangram_store::Store as _,
	tokio_stream::wrappers::IntervalStream,
};

type Barriers = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;
type Sender = crate::control::Sender<ServerMessage, ClientMessage>;

#[derive(Clone)]
struct Indexer {
	server: Server,
}

struct NamedCheckout {
	id: tg::Id,
	specifier: tg::Specifier,
}

struct State {
	barriers: Barriers,
	database_index_outbox_batch_id: Option<crate::database::index::outbox::BatchId>,
	object_index_outbox_batch_id: Option<crate::store::object::index::outbox::batch::Id>,
	requests: BTreeMap<String, IndexRequest>,
}

struct IndexRequest {
	state: IndexRequestState,
}

enum IndexRequestState {
	DatabaseIndexOutbox,
	DatabaseIndexOutboxPending,
	LogCompactions { transaction_id: Option<u64> },
	ObjectIndexOutbox,
	ObjectIndexOutboxPending,
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
		let usage_enabled = self.config.usage.enabled;

		// Spawn the database index outbox task.
		let database_index_outbox_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			let outbox = self.config.database.index_outbox().clone();
			let region = self.config.region.clone().unwrap_or_default();
			move |_| async move {
				indexer
					.database_index_outbox_task(&config, &outbox, &region)
					.await
			}
		});

		// Spawn the object archive outbox task.
		let object_archive_outbox_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			let outbox = self
				.archive
				.as_ref()
				.map(|_| self.config.object.archive_outbox.clone());
			move |_| async move {
				indexer
					.object_archive_outbox_task(&config, outbox.as_ref())
					.await
			}
		});

		// Spawn the object index outbox task.
		let object_index_outbox_task = Task::spawn({
			let config = config.clone();
			let outbox = (!self.config.advanced.single_process)
				.then(|| self.config.object.index_outbox.clone());
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.object_index_outbox_task(&config, outbox.as_ref())
					.await
			}
		});

		// Spawn the log compaction task.
		let log_compaction_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				if !config.log_compaction.enabled {
					return future::pending().await;
				}
				indexer
					.log_compaction_task(
						&config.log_compaction,
						config.partition_start,
						config.partition_end,
					)
					.await
			}
		});

		// Spawn the usage aggregation task.
		let usage_aggregation_task =
			(usage_enabled && config.usage.aggregation.enabled).then(|| {
				let config = config.clone();
				let indexer = indexer.clone();
				Task::spawn(move |_| async move {
					indexer
						.usage_aggregation_task(
							&config.usage.aggregation,
							config.partition_start,
							config.partition_end,
						)
						.await
				})
			});

		// Spawn the grant update task.
		let grant_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Grant,
						&config.updates.grants,
						config.partition_start,
						config.partition_end,
					)
					.await
			}
		});

		// Spawn the node update task.
		let node_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Node,
						&config.updates.nodes,
						config.partition_start,
						config.partition_end,
					)
					.await
			}
		});

		// Spawn the storage update task.
		let storage_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Storage,
						&config.usage.storage,
						config.partition_start,
						config.partition_end,
					)
					.await
			}
		});

		// Spawn the request task.
		let request_task = Task::spawn({
			let indexer = indexer.clone();
			move |_| async move { indexer.request_task(poll_interval).await }
		});

		// Wait for the tasks.
		let database_index_outbox_future = async move {
			database_index_outbox_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the database index outbox task panicked"))?
		};
		let object_archive_outbox_future = async move {
			object_archive_outbox_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the object archive outbox task panicked"))?
		};
		let object_index_outbox_future = async move {
			object_index_outbox_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the object index outbox task panicked"))?
		};
		let log_compaction_future = async move {
			log_compaction_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the indexer log compaction task panicked"))?
		};
		let usage_aggregation_future = async move {
			let Some(usage_aggregation_task) = usage_aggregation_task else {
				return future::pending().await;
			};
			usage_aggregation_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the usage aggregation task panicked"))?
		};
		let grant_update_future = async move {
			grant_update_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the grant index update task panicked"))?
		};
		let node_update_future = async move {
			node_update_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the node index update task panicked"))?
		};
		let storage_update_future = async move {
			storage_update_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the storage index update task panicked"))?
		};
		let update_future = async move {
			future::try_join3(
				grant_update_future,
				node_update_future,
				storage_update_future,
			)
			.await?;

			Ok(())
		};
		let queue_future = async move {
			future::try_join3(
				log_compaction_future,
				update_future,
				usage_aggregation_future,
			)
			.await?;

			Ok(())
		};
		let request_future = async move {
			request_task
				.wait()
				.await
				.map_err(|error| tg::error!(!error, "the indexer request task panicked"))?
		};
		let outbox_future = async move {
			future::try_join3(
				database_index_outbox_future,
				object_archive_outbox_future,
				object_index_outbox_future,
			)
			.await?;

			Ok(())
		};
		future::try_join3(outbox_future, queue_future, request_future).await?;

		Ok(())
	}

	pub(crate) fn spawn_publish_log_compaction_notification_task(&self) {
		let subject = log_compaction_subject();
		tokio::spawn({
			let server = self.clone();
			async move {
				if let Err(error) = server.messenger.publish(subject, ()).await {
					tracing::error!(%error, "failed to publish a log compaction notification");
				}
			}
		});
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
			is_ack: |message: &ClientMessage| matches!(message, ClientMessage::Ack(_)),
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
		let config = self.server.config.indexer.clone();
		crate::control::Options {
			retry: config.message_retry.into(),
			timeout: config.message_timeout,
		}
	}
}

impl Indexer {
	async fn usage_aggregation_task(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let concurrency = config.concurrency.to_u64().unwrap();
		let partition_length = partition_end - partition_start;
		let futures = (0..config.concurrency).filter_map(|task_index| {
			let task_index = task_index.to_u64().unwrap();
			let partitions_per_task = partition_length / concurrency;
			let extra = partition_length % concurrency;
			let task_start =
				partition_start + task_index * partitions_per_task + task_index.min(extra);
			let task_count = partitions_per_task + u64::from(task_index < extra);
			let task_end = task_start + task_count;
			(task_count > 0)
				.then(|| self.usage_aggregation_task_inner(config, task_start, task_end))
		});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn usage_aggregation_task_inner(
		&self,
		config: &crate::config::IndexerUsageAggregation,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		loop {
			crate::checkpoint!(self.server, "indexer.usage.aggregation.batch").await;
			let now = self.server.clock.now()?;
			let arg = tangram_index::usage::aggregate::Arg {
				batch_size: config.batch_size,
				now,
				partition_end,
				partition_start,
			};
			match self.server.index.aggregate_usage(arg).await {
				Ok(output) if output.count == 0 => {
					tokio::time::sleep(config.poll_interval).await;
				},
				Ok(_) => {},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to aggregate usage");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn database_index_outbox_task(
		&self,
		config: &crate::config::Indexer,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
	) -> tg::Result<()> {
		let wakeup_interval = config.database_index_outbox_wakeup_interval;
		loop {
			let result = self
				.database_index_outbox_task_inner(outbox, region, wakeup_interval)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to service the database index outbox");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn database_index_outbox_task_inner(
		&self,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		let subject = database_index_outbox_subject();
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to database index outbox notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			let count = self.database_index_outbox_batch(outbox, region).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the database index outbox wakeup stream ended"));
			}
		}
	}

	async fn database_index_outbox_batch(
		&self,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::database::index::outbox::DequeueArg {
			batch_size: outbox.batch_size,
			region: region.to_owned(),
		};
		let entries = self
			.server
			.database
			.dequeue_index_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the database index outbox"))?;
		if entries.is_empty() {
			return Ok(0);
		}

		// Deserialize the index batches.
		let count = entries.len();
		let batch = entries.last().unwrap().batch;
		let mut args = Vec::with_capacity(count);
		for entry in entries {
			let arg = tangram_index::batch::Arg::deserialize(&entry.payload)?;
			args.push(arg);
		}

		// Submit each outbox entry sequentially to preserve transaction order.
		for arg in args {
			if !self.server.named_checkout_maintenance_enabled()
				|| !Self::database_index_outbox_batch_contains_named_node_mutation(&arg)
			{
				self.server.index.batch(arg).await.map_err(|error| {
					tg::error!(!error, "failed to index a database index outbox batch")
				})?;
				continue;
			}
			crate::checkpoint!(self.server, "indexer.database_index_outbox.named_node").await;
			let guard = self.server.checkout_lock.acquire().await?;
			if !self.server.named_checkout_maintenance_enabled() {
				self.server.index.batch(arg).await.map_err(|error| {
					tg::error!(!error, "failed to index a database index outbox batch")
				})?;
				continue;
			}
			let (arg, mut invalidations) = self.prepare_database_index_outbox_batch(arg).await?;
			invalidations
				.sort_by_key(|checkout| std::cmp::Reverse(checkout.specifier.components().count()));
			for checkout in invalidations {
				self.server
					.remove_named_checkout_entry_with_lock(
						&guard,
						&checkout.id,
						&checkout.specifier,
					)
					.await?;
			}
			self.server.index.batch(arg).await.map_err(|error| {
				tg::error!(!error, "failed to index a database index outbox batch")
			})?;
		}
		let arg = crate::database::index::outbox::DeleteArg {
			batch,
			region: region.to_owned(),
		};
		self.server
			.database
			.delete_index_outbox(arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete a database index outbox batch")
			})?;

		Ok(count)
	}

	fn database_index_outbox_batch_contains_named_node_mutation(
		arg: &tangram_index::batch::Arg,
	) -> bool {
		arg.items
			.iter()
			.any(|item| Self::database_index_outbox_named_node_id(item).is_some())
	}

	fn database_index_outbox_named_node_id(item: &tangram_index::batch::Item) -> Option<tg::Id> {
		let id = match item {
			tangram_index::batch::Item::DeleteGroup(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteOrganization(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteTag(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteUser(id) => id.clone().into(),
			tangram_index::batch::Item::PutGroup(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutOrganization(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutTag(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutUser(arg) => arg.id.clone().into(),
			_ => return None,
		};

		Some(id)
	}

	async fn prepare_database_index_outbox_batch(
		&self,
		arg: tangram_index::batch::Arg,
	) -> tg::Result<(tangram_index::batch::Arg, Vec<NamedCheckout>)> {
		let ids = arg
			.items
			.iter()
			.filter_map(Self::database_index_outbox_named_node_id)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		let nodes = self.server.try_get_named_checkout_nodes(&ids).await?;
		let mut invalidations = BTreeMap::<tg::Id, tg::Specifier>::new();
		let mut items = Vec::with_capacity(arg.items.len());
		for item in arg.items {
			let invalidation = match &item {
				tangram_index::batch::Item::DeleteGroup(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteOrganization(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteTag(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteUser(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutGroup(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.parent != arg.parent || node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutOrganization(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutTag(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| {
							node.parent != arg.parent
								|| node.specifier != arg.specifier
								|| node.target.as_ref() != Some(&arg.target)
						})
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutUser(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				_ => None,
			};
			if let Some((id, specifier)) = invalidation {
				items.push(tangram_index::batch::Item::DeleteCheckout(id.clone()));
				invalidations.insert(id, specifier);
			}
			items.push(item);
		}
		let invalidations = invalidations
			.into_iter()
			.map(|(id, specifier)| NamedCheckout { id, specifier })
			.collect();
		let arg = tangram_index::batch::Arg { items };

		Ok((arg, invalidations))
	}

	async fn object_archive_outbox_task(
		&self,
		config: &crate::config::Indexer,
		outbox: Option<&crate::config::ObjectArchiveOutbox>,
	) -> tg::Result<()> {
		let Some(outbox) = outbox else {
			return future::pending().await;
		};
		let wakeup_interval = config.object_archive_outbox_wakeup_interval;
		future::try_join_all(
			(config.partition_start..config.partition_end).map(|partition| {
				self.object_archive_outbox_partition_task(outbox, partition, wakeup_interval)
			}),
		)
		.await?;

		Ok(())
	}

	async fn object_archive_outbox_partition_task(
		&self,
		outbox: &crate::config::ObjectArchiveOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		loop {
			let result = self
				.object_archive_outbox_partition_task_inner(outbox, partition, wakeup_interval)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), %partition, "failed to service the object archive outbox");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn object_archive_outbox_partition_task_inner(
		&self,
		outbox: &crate::config::ObjectArchiveOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		let subject = object_archive_outbox_subject(partition);
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					%subject,
					"failed to subscribe to object archive outbox notifications"
				)
			})?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			let count = self.object_archive_outbox_batch(outbox, partition).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the object archive outbox wakeup stream ended"));
			}
		}
	}

	async fn object_archive_outbox_batch(
		&self,
		outbox: &crate::config::ObjectArchiveOutbox,
		partition: u64,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::store::object::archive::outbox::dequeue::Arg {
			batch_size: outbox.batch_size,
			partition_end: partition + 1,
			partition_start: partition,
		};
		let entries = self
			.server
			.store
			.dequeue_object_archive_outbox_entries(arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to dequeue object archive outbox entries")
			})?;
		if entries.is_empty() {
			return Ok(0);
		}
		let count = entries.len();

		// Archive the objects.
		let results = future::join_all(entries.into_iter().map(|entry| async move {
			let result = self.object_archive_outbox_entry(&entry).await;
			(entry, result)
		}))
		.await;
		let mut completed = Vec::new();
		let mut error = None;
		for (entry, result) in results {
			match result {
				Ok(Some(stored_at)) => {
					let entry = crate::store::object::archive::outbox::Entry { stored_at, ..entry };
					completed.push(entry);
				},
				Ok(None) => {},
				Err(current) => {
					error.get_or_insert(current);
				},
			}
		}

		// Delete the completed entries.
		if !completed.is_empty() {
			let arg = crate::store::object::archive::outbox::delete::Arg { entries: completed };
			self.server
				.store
				.delete_object_archive_outbox_entries(arg)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to delete object archive outbox entries")
				})?;
		}
		if let Some(error) = error {
			return Err(error);
		}

		Ok(count)
	}

	async fn object_archive_outbox_entry(
		&self,
		entry: &crate::store::object::archive::outbox::Entry,
	) -> tg::Result<Option<i64>> {
		let arg = crate::store::object::get::Arg {
			id: entry.id.clone(),
		};
		let output = self.server.store.try_get_object(arg).await.map_err(
			|error| tg::error!(!error, id = %entry.id, "failed to get an object from the store"),
		)?;
		let Some(object) = output.object else {
			return Ok(None);
		};
		if object.stored_at < entry.stored_at {
			return Ok(None);
		}
		let stored_at = object.stored_at;
		let Some(bytes) = object.bytes else {
			return Ok(None);
		};
		let Some(archive) = &self.server.archive else {
			return Err(tg::error!("the archive is unavailable"));
		};
		let arg = tangram_archive::object::put::Arg {
			bytes: bytes.into_owned().into(),
			id: entry.id.clone(),
			stored_at,
		};
		archive.put_object(arg).await.map_err(
			|error| tg::error!(!error, id = %entry.id, "failed to put an object in the archive"),
		)?;

		Ok(Some(stored_at))
	}

	async fn object_index_outbox_task(
		&self,
		config: &crate::config::Indexer,
		outbox: Option<&crate::config::ObjectIndexOutbox>,
	) -> tg::Result<()> {
		let Some(outbox) = outbox else {
			return future::pending().await;
		};
		let wakeup_interval = config.object_index_outbox_wakeup_interval;
		future::try_join_all(
			(config.partition_start..config.partition_end).map(|partition| {
				self.object_index_outbox_partition_task(outbox, partition, wakeup_interval)
			}),
		)
		.await?;

		Ok(())
	}

	async fn object_index_outbox_partition_task(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		loop {
			let result = self
				.object_index_outbox_partition_task_inner(outbox, partition, wakeup_interval)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), %partition, "failed to service the object index outbox");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn object_index_outbox_partition_task_inner(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		let subject = object_index_outbox_subject(partition);
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to object index outbox notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			let count = self.object_index_outbox_batch(outbox, partition).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the object index outbox wakeup stream ended"));
			}
		}
	}

	async fn object_index_outbox_batch(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::store::object::index::outbox::fragment::dequeue::Arg {
			batch_size: outbox.batch_size,
			partition_end: partition + 1,
			partition_start: partition,
		};
		let fragments = self
			.server
			.store
			.dequeue_object_index_outbox_fragments(arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to dequeue the object index outbox fragments"
				)
			})?;
		if fragments.is_empty() {
			return Ok(0);
		}

		// Combine the fragments into batches.
		let count = fragments.len();
		let mut batches = BTreeMap::<_, Vec<_>>::new();
		let mut keys = Vec::with_capacity(count);
		for fragment in fragments {
			let arg = tangram_index::batch::Arg::deserialize(&fragment.payload)?;
			batches
				.entry(fragment.batch)
				.or_default()
				.push((fragment.index, arg));
			let key = crate::store::object::index::outbox::fragment::Key {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			};
			keys.push(key);
		}

		// Submit the batches concurrently and each batch's fragments sequentially.
		future::try_join_all(batches.into_values().map(|mut fragments| async move {
			fragments.sort_unstable_by_key(|(index, _)| *index);
			for (_, arg) in fragments {
				crate::checkpoint!(self.server, "index.batch").await;
				self.server.index.batch(arg).await?;
			}
			Ok::<_, tg::Error>(())
		}))
		.await
		.map_err(|error| tg::error!(!error, "failed to index an object index outbox batch"))?;
		let arg = crate::store::object::index::outbox::fragment::delete::Arg { fragments: keys };
		self.server
			.store
			.delete_object_index_outbox_fragments(arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete object index outbox fragments")
			})?;

		Ok(count)
	}

	async fn log_compaction_task(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let concurrency = config.concurrency.to_u64().unwrap();
		let partition_length = partition_end - partition_start;
		let futures = (0..config.concurrency).filter_map(|task_index| {
			let task_index = task_index.to_u64().unwrap();
			let partitions_per_task = partition_length / concurrency;
			let extra = partition_length % concurrency;
			let task_start =
				partition_start + task_index * partitions_per_task + task_index.min(extra);
			let task_count = partitions_per_task + u64::from(task_index < extra);
			let task_end = task_start + task_count;
			(task_count > 0)
				.then(|| self.log_compaction_partition_task(config, task_start, task_end))
		});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn log_compaction_partition_task(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		loop {
			let result = self
				.log_compaction_partition_task_inner(config, partition_start, partition_end)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to compact logs");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn log_compaction_partition_task_inner(
		&self,
		config: &crate::config::IndexerLogCompaction,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let subject = log_compaction_subject();
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to log compaction notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(config.wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			crate::checkpoint!(self.server, "indexer.log_compaction.batch").await;
			let entries = self
				.server
				.index
				.log_compaction_batch(config.batch_size, partition_start, partition_end)
				.await
				.map_err(|error| tg::error!(!error, "failed to read log compactions"))?;
			if entries.is_empty() {
				if wakeups.next().await.is_none() {
					return Err(tg::error!("the log compaction wakeup stream ended"));
				}
				continue;
			}
			self.compact_logs(&entries).boxed().await?;
		}
	}

	async fn compact_logs(&self, entries: &[tangram_index::log::Entry]) -> tg::Result<()> {
		for entry in entries {
			self.compact_log(entry).boxed().await?;
		}

		Ok(())
	}

	async fn compact_log(&self, entry: &tangram_index::log::Entry) -> tg::Result<()> {
		let process = &entry.process;
		let session = self.server.session(&self.server.context);
		session
			.compact_process_log(process)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to compact the process log"))?;
		self.server
			.index
			.complete_log_compaction(entry)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to complete the log compaction"),
			)?;

		Ok(())
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
				message = control.recv_with_ack() => {
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
						let id = request.id.clone();
						let entry = IndexRequest {
							state: IndexRequestState::Tasks,
						};
						state.requests.insert(request.id, entry);
						crate::checkpoint!(self.server, "indexer.request.receive", request = id,)
							.await;
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

	async fn update_task(
		&self,
		kind: tangram_index::update::Kind,
		config: &crate::config::IndexerUpdate,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let concurrency = config.concurrency.to_u64().unwrap();
		let partition_length = partition_end - partition_start;
		let checkpoint = match kind {
			tangram_index::update::Kind::Grant => "indexer.update.grant.batch",
			tangram_index::update::Kind::Node => "indexer.update.node.batch",
			tangram_index::update::Kind::Storage => "indexer.update.storage.batch",
		};
		loop {
			crate::checkpoint!(self.server, checkpoint).await;
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
					.update_batch(kind, config.batch_size, task_start, task_end)
			});
			let result = future::try_join_all(futures).await.map(|outputs| {
				outputs.into_iter().fold(
					tangram_index::update::Output::default(),
					|mut output, next| {
						output.merge(next);
						output
					},
				)
			});
			match result {
				Ok(output) if output.count == 0 => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(output) => {
					for process in output.processes_with_depth_exceeded {
						self.spawn_finish_process_for_max_depth_task(process);
					}
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to index");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	fn spawn_finish_process_for_max_depth_task(&self, process: tg::process::Id) {
		let indexer = self.clone();
		tokio::spawn(async move {
			if let Err(error) = indexer.finish_process_for_max_depth(&process).await {
				tracing::error!(
					error = %error.trace(),
					%process,
					"failed to finish the process that exceeded the maximum depth"
				);
			}
		});
	}

	async fn finish_process_for_max_depth(&self, id: &tg::process::Id) -> tg::Result<()> {
		let error = tg::error::Data {
			message: Some("maximum depth exceeded".into()),
			..Default::default()
		};
		let request = tg::process::control::ServerRequestArg::Finish(
			tg::process::control::FinishServerRequestArg {
				error: Some(error),
				exit: 1,
			},
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: Duration::from_secs(10),
		};
		let session = self.server.session(&self.server.context);
		let wait_future = session
			.try_wait_process_future(id, tg::process::wait::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let finish_future = session.send_process_control_request(id, request, options);
		let mut wait_future = pin!(wait_future);
		let mut finish_future = pin!(finish_future);
		tokio::select! {
			output = &mut wait_future => {
				let output = output
					.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
				if output.is_none() {
					return Err(tg::error!(%id, "the process wait ended without output"));
				}
			},
			response = &mut finish_future => {
				let response = response
					.map_err(
						|error| tg::error!(!error, %id, "failed to send the finish process control request"),
					)?
					.map_err(
						|error| tg::error!(!error, %id, "the finish process control request failed"),
					)?;
				response
					.try_unwrap_finish()
					.map_err(|_| tg::error!(%id, "expected a finish response"))?;
			},
		}

		Ok(())
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
			database_index_outbox_batch_id: None,
			object_index_outbox_batch_id: None,
			requests: BTreeMap::new(),
		}
	}

	async fn poll(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		// Wait for the object index outbox.
		self.poll_object_index_outbox(server).await?;

		// Wait for the database index outbox.
		self.poll_database_index_outbox(server).await?;

		// Wait for the log compaction queue.
		self.set_log_compaction_targets(server).await?;
		self.poll_log_compactions(server).await?;

		// Wait for the index update queue.
		self.set_update_targets(server).await?;
		self.poll_updates(server, sender).await?;

		Ok(())
	}

	async fn poll_object_index_outbox(&mut self, server: &Server) -> tg::Result<()> {
		if server.config.advanced.single_process {
			return Ok(());
		}
		let config = &server.config.object.index_outbox;

		// Poll the active cohort.
		if let Some(batch) = self.object_index_outbox_batch_id {
			let arg = crate::store::object::index::outbox::batch::get::Arg {
				batch: Some(batch),
				partition_end: config.partition_total,
				partition_start: 0,
			};
			let batch = server
				.store
				.try_get_object_index_outbox_batch_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the object index outbox"))?;
			if batch.is_some() {
				return Ok(());
			}
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::ObjectIndexOutboxPending) {
					request.state = IndexRequestState::DatabaseIndexOutbox;
				}
			}
			self.object_index_outbox_batch_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::ObjectIndexOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::store::object::index::outbox::batch::get::Arg {
			batch: None,
			partition_end: config.partition_total,
			partition_start: 0,
		};
		let batch = server
			.store
			.try_get_object_index_outbox_batch_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the object index outbox"))?;
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::ObjectIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				IndexRequestState::ObjectIndexOutboxPending
			} else {
				IndexRequestState::DatabaseIndexOutbox
			};
		}
		self.object_index_outbox_batch_id = batch;

		Ok(())
	}

	async fn poll_database_index_outbox(&mut self, server: &Server) -> tg::Result<()> {
		let region = server.config.region.clone().unwrap_or_default();

		// Poll the active cohort.
		if let Some(batch) = self.database_index_outbox_batch_id {
			let arg = crate::database::index::outbox::TryGetBatchArg {
				batch: Some(batch),
				region,
			};
			let batch = server
				.database
				.try_get_index_outbox_batch_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the database index outbox"))?;
			if batch.is_some() {
				return Ok(());
			}
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::DatabaseIndexOutboxPending) {
					request.state = if server.config.indexer.log_compaction.enabled {
						IndexRequestState::LogCompactions {
							transaction_id: None,
						}
					} else {
						IndexRequestState::Updates {
							transaction_id: None,
						}
					};
				}
			}
			self.database_index_outbox_batch_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::DatabaseIndexOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::database::index::outbox::TryGetBatchArg {
			batch: None,
			region,
		};
		let batch = server
			.database
			.try_get_index_outbox_batch_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the database index outbox"))?;
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::DatabaseIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				IndexRequestState::DatabaseIndexOutboxPending
			} else if server.config.indexer.log_compaction.enabled {
				IndexRequestState::LogCompactions {
					transaction_id: None,
				}
			} else {
				IndexRequestState::Updates {
					transaction_id: None,
				}
			};
		}
		self.database_index_outbox_batch_id = batch;

		Ok(())
	}

	async fn set_log_compaction_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::LogCompactions {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.requests.values_mut() {
			if let IndexRequestState::LogCompactions {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_log_compactions(&mut self, server: &Server) -> tg::Result<()> {
		let poll = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::LogCompactions {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldest = server
			.index
			.try_get_oldest_log_compaction_transaction_id()
			.await?;
		for request in self.requests.values_mut() {
			let IndexRequestState::LogCompactions {
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
				let request = ids.first().unwrap().clone();
				crate::checkpoint!(server, "indexer.request.barrier", request,).await;
				server.remote_object_put_tasks.wait().await;
				server.index_tasks.wait().await;

				ids
			}
			.boxed(),
		);
	}

	fn handle_barrier(&mut self, ids: Vec<String>, object_index_outbox: bool) {
		for id in ids {
			let Some(request) = self.requests.get_mut(&id) else {
				continue;
			};
			if matches!(request.state, IndexRequestState::Tasks) {
				request.state = if object_index_outbox {
					IndexRequestState::ObjectIndexOutbox
				} else {
					IndexRequestState::DatabaseIndexOutbox
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
		let oldests = future::try_join3(
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Grant),
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Node),
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Storage),
		)
		.await?;
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
				[oldests.0, oldests.1, oldests.2]
					.into_iter()
					.all(|oldest| oldest.is_none_or(|oldest| oldest > transaction_id))
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
		self.database_index_outbox_batch_id = None;
		self.object_index_outbox_batch_id = None;
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
				IndexRequestState::DatabaseIndexOutbox
					| IndexRequestState::DatabaseIndexOutboxPending
					| IndexRequestState::LogCompactions { .. }
					| IndexRequestState::ObjectIndexOutbox
					| IndexRequestState::ObjectIndexOutboxPending
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

pub(crate) fn database_index_outbox_subject() -> String {
	"database.index.outbox".to_owned()
}

pub(crate) fn log_compaction_subject() -> String {
	"index.log_compaction".to_owned()
}

pub(crate) fn object_archive_outbox_subject(partition: u64) -> String {
	format!("stores.object.archive.outbox.{partition}")
}

pub(crate) fn object_index_outbox_subject(partition: u64) -> String {
	format!("stores.object.index.outbox.{partition}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn database_index_outbox_subject_has_no_partition() {
		assert_eq!(database_index_outbox_subject(), "database.index.outbox");
	}

	#[test]
	fn log_compaction_subject_has_no_partition() {
		assert_eq!(log_compaction_subject(), "index.log_compaction");
	}

	#[test]
	fn object_archive_outbox_subject_includes_the_partition() {
		assert_eq!(
			object_archive_outbox_subject(42),
			"stores.object.archive.outbox.42"
		);
	}

	#[test]
	fn object_index_outbox_subject_includes_the_partition() {
		assert_eq!(
			object_index_outbox_subject(42),
			"stores.object.index.outbox.42"
		);
	}
}
