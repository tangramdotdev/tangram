use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Shared as SharedTask, Stopper, Task},
	tangram_index::Index as _,
};

mod cache;
mod cleaning;
mod compaction;
mod database;
mod object;
mod partition;
mod queue;
mod request;
mod stripe;
mod update;
mod usage;
mod wait;

pub(crate) use {
	cache::Cache,
	cleaning::CleanBatchArg,
	database::database_index_outbox_subject,
	request::{ArchiveRequestArg, IndexRequestArg, RequestArg},
};

pub(crate) const RETRY_OPTIONS: tangram_futures::retry::Options = tangram_futures::retry::Options {
	backoff: std::time::Duration::from_secs(1),
	jitter: std::time::Duration::ZERO,
	max_delay: std::time::Duration::from_secs(1),
	max_retries: u64::MAX,
};

#[derive(Clone)]
struct Indexer {
	id: Option<tg::indexer::Id>,
	server: Server,
}

struct ShutdownArgs<'a> {
	changed: &'a tokio::sync::Notify,
	checkpoints: &'a queue::CheckpointSender,
	index_sender: &'a queue::IndexMessageSender,
	state: &'a Mutex<State>,
	tasks: Tasks,
}

struct State {
	available: bool,
	limits: request::limits::Limits,
	queues: queue::Queues,
	writes: usize,
}

struct Tasks {
	archive_queue: SharedTask<tg::Result<()>>,
	archive_sequence_reservations: SharedTask<tg::Result<()>>,
	batch_expiration: SharedTask<tg::Result<()>>,
	cleaning: Task<tg::Result<()>>,
	database_index_outbox: Task<tg::Result<()>>,
	grant_update: Task<tg::Result<()>>,
	index_queue: SharedTask<tg::Result<()>>,
	index_sequence_reservations: SharedTask<tg::Result<()>>,
	log_compaction: Task<tg::Result<()>>,
	node_update: Task<tg::Result<()>>,
	object_cache: Task<tg::Result<()>>,
	queue_checkpoints: SharedTask<tg::Result<()>>,
	queue_completions: SharedTask<tg::Result<()>>,
	request: SharedTask<tg::Result<()>>,
	storage_update: Task<tg::Result<()>>,
	stripe_cleanup: Task<tg::Result<()>>,
	usage_aggregation: Task<tg::Result<()>>,
	usage_expiration: Task<tg::Result<()>>,
	wait: SharedTask<tg::Result<()>>,
}

impl Server {
	pub(crate) async fn indexer_task(
		&self,
		config: &crate::config::Indexer,
		stopper: Stopper,
	) -> tg::Result<()> {
		let id =
			if self.config.advanced.single_process {
				None
			} else {
				Some(config.id.clone().ok_or_else(|| {
					tg::error!("the indexer ID is required in multi-process mode")
				})?)
			};
		let indexer_state = if let Some(id) = &id {
			let arg = tangram_index::indexer::get::Arg { id: id.clone() };
			Some(
				if let Some(mut indexer) = self.index.try_get_indexer(arg).await? {
					indexer.available = false;
					let arg = tangram_index::indexer::put::Arg {
						indexer: indexer.clone(),
					};
					self.index.put_indexer(arg).await?;
					indexer
				} else {
					let indexer = tangram_index::indexer::Indexer::new(id.clone());
					let arg = tangram_index::indexer::put::Arg {
						indexer: indexer.clone(),
					};
					self.index.put_indexer(arg).await?;
					indexer
				},
			)
		} else {
			None
		};
		let (completion_sender, completion_receiver) = tokio::sync::mpsc::unbounded_channel();
		let (archive_sender, archive_receiver) =
			tokio::sync::mpsc::channel(self.config.object.archive_queue.concurrency);
		let (index_sender, index_receiver) =
			tokio::sync::mpsc::channel(self.config.object.index_queue.concurrency);
		let indexer = Indexer {
			id,
			server: self.clone(),
		};
		let archive_queue_task = SharedTask::spawn({
			let completion_sender = completion_sender.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.archive_queue_task(archive_receiver, completion_sender, stopper)
					.await
			}
		});
		let index_queue_task = SharedTask::spawn({
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.index_queue_task(index_receiver, completion_sender, stopper)
					.await
			}
		});
		let queues = if let Some(state) = &indexer_state {
			let mut queues = queue::Queues::new(state);
			queues
				.recover(&indexer, &archive_sender, &index_sender)
				.await?;
			queues.reserve_initial_sequences(&indexer).await?;
			queues
		} else {
			queue::Queues::empty()
		};
		// Create the shared queue state after recovery.
		let state = State {
			available: false,
			limits: request::limits::Limits::default(),
			queues,
			writes: 0,
		};
		let state = Arc::new(Mutex::new(state));
		let changed = Arc::new(tokio::sync::Notify::new());
		let (checkpoint_sender, checkpoint_receiver) = tokio::sync::mpsc::unbounded_channel();

		// Spawn the archive sequence reservations task.
		let archive_sequence_reservations_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let changed = changed.clone();
			let state = state.clone();
			move |stopper| async move {
				if indexer.id.is_none() {
					stopper.wait().await;
					return Ok(());
				}
				tokio::select! {
					result = indexer.sequence_reservations_task(&state, &changed, queue::Kind::Archive) => result,
					() = stopper.wait() => Ok(()),
				}
			}
		});

		// Spawn the batch expiration task.
		let batch_expiration_task = SharedTask::spawn({
			let index_sender = index_sender.clone();
			let indexer = indexer.clone();
			let changed = changed.clone();
			let state = state.clone();
			move |stopper| async move {
				if indexer.id.is_none() {
					stopper.wait().await;
					return Ok(());
				}
				tokio::select! {
					result = indexer.batch_expiration_task(&index_sender, &state, &changed) => result,
					() = stopper.wait() => Ok(()),
				}
			}
		});

		// Spawn the index sequence reservations task.
		let index_sequence_reservations_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let changed = changed.clone();
			let state = state.clone();
			move |stopper| async move {
				if indexer.id.is_none() {
					stopper.wait().await;
					return Ok(());
				}
				tokio::select! {
					result = indexer.sequence_reservations_task(&state, &changed, queue::Kind::Index) => result,
					() = stopper.wait() => Ok(()),
				}
			}
		});

		// Spawn the queue checkpoints task.
		let queue_checkpoints_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let state = state.clone();
			move |stopper| async move {
				if indexer.id.is_none() {
					stopper.wait().await;
					return Ok(());
				}
				tokio::select! {
					result = indexer.queue_checkpoints_task(&state, checkpoint_receiver) => result,
					() = stopper.wait() => Ok(()),
				}
			}
		});

		// Spawn the queue completions task.
		let queue_completions_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let changed = changed.clone();
			let state = state.clone();
			move |stopper| async move {
				if indexer.id.is_none() {
					stopper.wait().await;
					return Ok(());
				}
				tokio::select! {
					result = Indexer::queue_completions_task(&state, &changed, completion_receiver) => result,
					() = stopper.wait() => Ok(()),
				}
			}
		});

		let usage_enabled = self.config.usage.enabled;

		// Spawn the cleaning task.
		let cleaning_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				if !config.cleaning.enabled {
					stopper.wait().await;
					return Ok(());
				}
				indexer
					.cleaning_task(
						&config.cleaning,
						config.cleaning.partitions.start,
						config.cleaning.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the database index outbox task.
		let database_index_outbox_task = Task::spawn({
			let indexer = indexer.clone();
			let outbox = self.config.database.index_outbox().clone();
			let region = self.config.region.clone().unwrap_or_default();
			move |stopper| async move {
				indexer
					.database_index_outbox_task(&outbox, &region, &stopper)
					.await
			}
		});

		// Spawn the object cache task.
		let object_cache_task = Task::spawn({
			let cache = self.config.object.cache.clone();
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.object_cache_task(&config.object_cache_partitions, cache.as_ref(), &stopper)
					.await
			}
		});

		// Spawn the log compaction task.
		let log_compaction_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				if !config.log_compaction.enabled {
					stopper.wait().await;
					return Ok(());
				}
				indexer
					.log_compaction_task(
						&config.log_compaction,
						config.log_compaction.partitions.start,
						config.log_compaction.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the Stripe cleanup task.
		let stripe_cleanup_task = Task::spawn({
			let indexer = indexer.clone();
			move |stopper| async move {
				if !indexer.server.is_primary_region() {
					stopper.wait().await;
					return Ok(());
				}
				indexer.stripe_cleanup_task(&stopper).await
			}
		});

		// Spawn the usage aggregation task.
		let usage_aggregation_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				if !usage_enabled || !config.usage.aggregation.enabled {
					stopper.wait().await;
					return Ok(());
				}
				indexer
					.usage_aggregation_task(
						&config.usage.aggregation,
						config.usage_partitions.start,
						config.usage_partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the usage expiration task.
		let usage_expiration_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				if !usage_enabled || !config.usage.expiration.enabled {
					stopper.wait().await;
					return Ok(());
				}
				indexer
					.usage_expiration_task(
						&config.usage.expiration,
						config.usage_partitions.start,
						config.usage_partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the grant update task.
		let grant_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Grant,
						&config.updates.grants,
						config.updates.grants.partitions.start,
						config.updates.grants.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the node update task.
		let node_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Node,
						&config.updates.nodes,
						config.updates.nodes.partitions.start,
						config.updates.nodes.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the storage update task.
		let storage_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Storage,
						&config.updates.storage,
						config.updates.storage.partitions.start,
						config.updates.storage.partitions.end,
						&stopper,
					)
					.await
			}
		});

		let (wait_sender, wait_receiver) =
			tokio::sync::mpsc::channel(config.request.wait_concurrency);

		// Spawn the wait task.
		let wait_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let state = state.clone();
			move |stopper| async move { indexer.wait_task(&state, wait_receiver, stopper).await }
		});

		// Spawn the request task.
		let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
		let request_task = tangram_futures::task::Shared::spawn({
			let wait_sender = wait_sender.clone();
			let index_sender = index_sender.clone();
			let indexer = indexer.clone();
			let changed = changed.clone();
			let state = state.clone();
			move |stopper| async move {
				let args = request::RequestTaskArgs {
					archive_sender,
					changed,
					index_sender,
					ready: ready_sender,
					state,
					stopper,
					wait_sender,
				};
				indexer.request_task(args).await
			}
		});

		// Make the indexer available after recovery and subscription.
		ready_receiver
			.await
			.map_err(|_| tg::error!("the indexer request task stopped before becoming ready"))?;
		if indexer.id.is_some() {
			state.lock().unwrap().available = true;
			changed.notify_waiters();
			indexer.update_availability(true).await?;
			if let Err(error) = self.refresh_indexer_cache().await {
				tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
			}
		}

		let tasks = Tasks {
			archive_queue: archive_queue_task,
			archive_sequence_reservations: archive_sequence_reservations_task,
			batch_expiration: batch_expiration_task,
			cleaning: cleaning_task,
			database_index_outbox: database_index_outbox_task,
			grant_update: grant_update_task,
			index_queue: index_queue_task,
			index_sequence_reservations: index_sequence_reservations_task,
			log_compaction: log_compaction_task,
			node_update: node_update_task,
			object_cache: object_cache_task,
			queue_checkpoints: queue_checkpoints_task,
			queue_completions: queue_completions_task,
			request: request_task,
			storage_update: storage_update_task,
			stripe_cleanup: stripe_cleanup_task,
			usage_aggregation: usage_aggregation_task,
			usage_expiration: usage_expiration_task,
			wait: wait_task,
		};

		// Monitor the request and queue tasks at the indexer level.
		let mut monitored = [
			("archive queue", tasks.archive_queue.clone()),
			("index queue", tasks.index_queue.clone()),
			("request", tasks.request.clone()),
			("wait", tasks.wait.clone()),
			(
				"archive sequence reservations",
				tasks.archive_sequence_reservations.clone(),
			),
			("batch expiration", tasks.batch_expiration.clone()),
			(
				"index sequence reservations",
				tasks.index_sequence_reservations.clone(),
			),
			("queue checkpoints", tasks.queue_checkpoints.clone()),
			("queue completions", tasks.queue_completions.clone()),
		]
		.into_iter()
		.map(|(name, task)| async move { (name, task.wait().await) }.boxed())
		.collect::<FuturesUnordered<_>>();
		tokio::select! {
			() = stopper.wait() => {},
			completion = monitored.next() => {
				let (name, result) = completion.unwrap();
				let error = match result {
					Ok(Ok(())) => tg::error!(task = %name, "an indexer task stopped unexpectedly"),
					Ok(Err(error)) => tg::error!(!error, task = %name, "an indexer task failed"),
					Err(error) => tg::error!(!error, task = %name, "an indexer task panicked"),
				};
				if indexer.id.is_some() {
					let _ = tokio::time::timeout(config.request.timeout, indexer.update_availability(false)).await;
				}
				return Err(error);
			},
		}
		let args = ShutdownArgs {
			changed: &changed,
			checkpoints: &checkpoint_sender,
			index_sender: &index_sender,
			state: &state,
			tasks,
		};
		let shutdown = indexer.shutdown(args);
		tokio::pin!(shutdown);
		let result = loop {
			tokio::select! {
				result = &mut shutdown => break result,
				Some((name, result)) = monitored.next() => match result {
					Ok(Ok(())) => {},
					Ok(Err(error)) => break Err(tg::error!(!error, task = %name, "an indexer task failed during shutdown")),
					Err(error) => break Err(tg::error!(!error, task = %name, "an indexer task panicked during shutdown")),
				},
			}
		};
		if result.is_err() && indexer.id.is_some() {
			let _ =
				tokio::time::timeout(config.request.timeout, indexer.update_availability(false))
					.await;
		}
		result?;

		Ok(())
	}
}

impl Indexer {
	#[must_use]
	fn id(&self) -> &tg::indexer::Id {
		self.id
			.as_ref()
			.expect("a queue operation requires an indexer ID")
	}

	async fn shutdown(&self, args: ShutdownArgs<'_>) -> tg::Result<()> {
		let ShutdownArgs {
			changed,
			checkpoints,
			index_sender,
			state,
			tasks,
		} = args;
		let Tasks {
			archive_queue,
			archive_sequence_reservations,
			batch_expiration,
			cleaning,
			database_index_outbox,
			grant_update,
			index_queue,
			index_sequence_reservations,
			log_compaction,
			node_update,
			object_cache,
			queue_checkpoints,
			queue_completions,
			request,
			storage_update,
			stripe_cleanup,
			usage_aggregation,
			usage_expiration,
			wait,
		} = tasks;

		// Stop accepting queue requests.
		if self.id.is_some() {
			state.lock().unwrap().available = false;
			changed.notify_waiters();
			self.update_availability_with_retry(false).await?;
			if let Err(error) = self.server.refresh_indexer_cache().await {
				tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
			}
		}

		// Finish the current operations without exhausting the partitions.
		cleaning.stop();
		database_index_outbox.stop();
		grant_update.stop();
		log_compaction.stop();
		node_update.stop();
		object_cache.stop();
		storage_update.stop();
		stripe_cleanup.stop();
		usage_aggregation.stop();
		usage_expiration.stop();
		for (name, task) in [
			("cleaning", cleaning),
			("database index outbox", database_index_outbox),
			("grant update", grant_update),
			("log compaction", log_compaction),
			("node update", node_update),
			("object cache", object_cache),
			("storage update", storage_update),
			("Stripe cleanup", stripe_cleanup),
			("usage aggregation", usage_aggregation),
			("usage expiration", usage_expiration),
		] {
			wait_for_stopped_task(name, task).await?;
		}

		// Finish the local producers before draining the private queues.
		self.server.remote_object_put_tasks.wait().await;
		self.server.index_tasks.wait().await;
		self.server.archive_tasks.wait().await;
		if self.id.is_some() {
			Self::drain_queues(state, changed, index_sender).await?;
		}

		// Deletion certifies that the private queues are drained; shared work remains durable.
		if self.id.is_some() {
			self.checkpoint_queues_with_retry(checkpoints).await?;
			self.delete_with_retry().await?;
		}
		request.stop();
		request
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the indexer request task panicked"))??;
		wait.stop();
		wait.wait()
			.await
			.map_err(|error| tg::error!(!error, "the indexer wait task panicked"))??;
		// Stop queue support tasks before closing the queue task channels.
		for (name, task) in [
			(
				"archive sequence reservations",
				archive_sequence_reservations,
			),
			("batch expiration", batch_expiration),
			("index sequence reservations", index_sequence_reservations),
			("queue checkpoints", queue_checkpoints),
			("queue completions", queue_completions),
		] {
			task.stop();
			task.wait()
				.await
				.map_err(|error| tg::error!(!error, task = %name, "an indexer task panicked"))??;
		}
		for (name, task) in [
			("archive queue", archive_queue),
			("index queue", index_queue),
		] {
			task.stop();
			task.wait()
				.await
				.map_err(|error| tg::error!(!error, task = %name, "an indexer task panicked"))??;
		}

		Ok(())
	}

	async fn update_availability_with_retry(&self, available: bool) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			match self.update_availability(available).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to update the indexer availability");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn update_availability(&self, available: bool) -> tg::Result<()> {
		let arg = tangram_index::indexer::update::Arg {
			id: self.id().clone(),
			value: tangram_index::indexer::update::Value::Available(available),
		};
		self.server.index.update_indexer(arg).await
	}

	async fn delete_with_retry(&self) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let arg = tangram_index::indexer::delete::Arg {
				id: self.id().clone(),
			};
			match self.server.index.delete_indexer(arg).await {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to delete the indexer");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}
}

async fn wait_for_stopped_task(name: &'static str, task: Task<tg::Result<()>>) -> tg::Result<()> {
	task.wait()
		.await
		.map_err(|error| tg::error!(!error, task = %name, "an indexer task panicked"))?
		.map_err(|error| tg::error!(!error, task = %name, "an indexer task failed"))?;
	Ok(())
}
