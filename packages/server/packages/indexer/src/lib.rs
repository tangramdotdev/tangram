use {
	futures::{
		FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream,
		stream::FuturesUnordered,
	},
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_futures::task::{Shared as SharedTask, Stopper, Task},
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::{Server, Session, indexer::CleanBatchArg},
};

mod archive;
mod billing;
mod cache;
mod capture;
mod cleaning;
mod database;
mod index;
mod log;
mod object;
mod partition;
mod request;
mod update;
mod usage;
mod wait;

pub mod queue;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub(crate) index_wait_receiver: Mutex<Option<index::wait::Receiver>>,
	pub(crate) index_wait_sender: index::wait::Sender,
	pub(crate) index_wait_task: Mutex<Option<Task<()>>>,
	pub(crate) indexer_cache_task: Mutex<Option<Task<()>>>,
	pub(crate) indexer_task: Mutex<Option<Task<()>>>,
	pub(crate) indexers: cache::Cache,
}

#[derive(Clone)]
pub struct Indexer {
	component: Component,
	id: Option<tg::indexer::Id>,
	server: Server,
}

struct ShutdownArgs<'a> {
	changed: &'a tokio::sync::Notify,
	checkpoints: &'a queue::CheckpointSender,
	index_sender: &'a queue::IndexMessageSender,
	state: &'a Mutex<IndexerState>,
	tasks: Tasks,
}

pub struct IndexerState {
	available: bool,
	limits: request::limits::Limits,
	queues: queue::Queues,
	writes: usize,
}

struct Tasks {
	archive_queue: SharedTask<tg::Result<()>>,
	archive_sequence_reservations: SharedTask<tg::Result<()>>,
	batch_expiration: SharedTask<tg::Result<()>>,
	billing_cleanup: Task<tg::Result<()>>,
	permission_capture: SharedTask<tg::Result<()>>,
	cleaning: Task<tg::Result<()>>,
	database_index_queue: Task<tg::Result<()>>,
	permission_update: Task<tg::Result<()>>,
	index_queue: SharedTask<tg::Result<()>>,
	index_sequence_reservations: SharedTask<tg::Result<()>>,
	log_cache: Task<tg::Result<()>>,
	storage_and_metadata_update: Task<tg::Result<()>>,
	object_cache: Task<tg::Result<()>>,
	queue_checkpoints: SharedTask<tg::Result<()>>,
	queue_completions: SharedTask<tg::Result<()>>,
	request: SharedTask<tg::Result<()>>,
	usage_update: Task<tg::Result<()>>,
	usage_aggregation: Task<tg::Result<()>>,
	usage_expiration: Task<tg::Result<()>>,
	wait: SharedTask<tg::Result<()>>,
}

impl Component {
	#[must_use]
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> Self {
		// Create the index wait channel.
		let (index_wait_sender, index_wait_receiver) =
			tokio::sync::mpsc::channel(arg.config.indexer.request.wait_concurrency);
		let index_wait_receiver = Mutex::new(Some(index_wait_receiver));

		// Create the indexer cache.
		let indexers = cache::Cache::default();

		// Create the task handles.
		let index_wait_task = Mutex::new(None);
		let indexer_cache_task = Mutex::new(None);
		let indexer_task = Mutex::new(None);

		// Create the state.
		let state = State {
			index_wait_receiver,
			index_wait_sender,
			index_wait_task,
			indexer_cache_task,
			indexer_task,
			indexers,
		};

		Self(Arc::new(state))
	}

	fn spawn_index_wait_task(&self, server: &Server, receiver: index::wait::Receiver) -> Task<()> {
		let component = self.clone();
		let server = server.clone();
		Task::spawn(move |stopper| async move {
			component.index_wait_task(&server, receiver, stopper).await;
		})
	}

	fn spawn_indexer_cache_task(&self, server: &Server) -> Task<()> {
		let poll_interval = server.config.indexer.cache.poll_interval;
		let component = self.clone();
		let server = server.clone();
		Task::spawn(move |stopper| async move {
			component
				.indexer_cache_task(&server, poll_interval, stopper)
				.await;
		})
	}

	fn spawn_indexer_task(&self, server: &Server) -> Option<Task<()>> {
		if !server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Indexer)
		{
			return None;
		}
		let config = server.config.indexer.clone();
		let component = self.clone();
		let server = server.clone();
		let task = Task::spawn(|stopper| async move {
			let result = component.indexer_task(&server, &config, stopper).await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "the indexer failed; exiting for crash recovery");
				std::process::exit(1);
			}
		});

		Some(task)
	}

	pub(crate) async fn indexer_task(
		&self,
		server: &Server,
		config: &tangram_server_core::config::Indexer,
		stopper: Stopper,
	) -> tg::Result<()> {
		let id = if server.config.advanced.multi_server {
			Some(
				config
					.id
					.clone()
					.ok_or_else(|| tg::error!("the indexer ID is required in multi-server mode"))?,
			)
		} else {
			None
		};
		let indexer_state = if let Some(id) = &id {
			let arg = tangram_index::indexer::get::Arg { id: id.clone() };
			Some(
				if let Some(mut indexer) = server.index.try_get_indexer(arg).await? {
					indexer.available = false;
					let arg = tangram_index::indexer::put::Arg {
						indexer: indexer.clone(),
					};
					server.index.put_indexer(arg).await?;
					indexer
				} else {
					let indexer = tangram_index::indexer::Indexer::new(id.clone());
					let arg = tangram_index::indexer::put::Arg {
						indexer: indexer.clone(),
					};
					server.index.put_indexer(arg).await?;
					indexer
				},
			)
		} else {
			None
		};
		let (completion_sender, completion_receiver) = tokio::sync::mpsc::unbounded_channel();
		let (archive_sender, archive_receiver) =
			tokio::sync::mpsc::channel(server.config.object.archive_queue.concurrency);
		let (index_sender, index_receiver) =
			tokio::sync::mpsc::channel(server.config.object.index_queue.concurrency);
		let indexer = Indexer {
			component: self.clone(),
			id,
			server: server.clone(),
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
		let state = IndexerState {
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

		let usage_enabled = server.config.usage.enabled;

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

		// Service capture separately so its verification can await indexing.
		let permission_capture_task = SharedTask::spawn({
			let indexer = indexer.clone();
			let config = config.clone();
			move |stopper| async move {
				if !config.permission_capture.enabled {
					stopper.wait().await;
					return Ok(());
				}
				indexer
					.permission_capture_task(
						&config.permission_capture,
						config.updates.permissions.partitions.start,
						config.updates.permissions.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the database index queue task.
		let database_index_queue_task = Task::spawn({
			let indexer = indexer.clone();
			let queue = server.config.database.index_queue().clone();
			let region = server.config.region.clone().unwrap_or_default();
			move |stopper| async move {
				indexer
					.database_index_queue_task(&queue, &region, &stopper)
					.await
			}
		});

		// Spawn the object cache task.
		let object_cache_task = Task::spawn({
			let cache = server.config.object.cache.clone();
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.object_cache_task(&config.object_cache_partitions, cache.as_ref(), &stopper)
					.await
			}
		});

		// Spawn the log cache cleanup task.
		let log_cache_task = Task::spawn({
			let config = config.log_cache.clone();
			let indexer = indexer.clone();
			move |stopper| async move { indexer.log_cache_task(&config, &stopper).await }
		});

		// Spawn the billing cleanup task.
		let billing_cleanup_task = Task::spawn({
			let indexer = indexer.clone();
			move |stopper| async move {
				if !indexer.server.is_primary_region() {
					stopper.wait().await;
					return Ok(());
				}
				indexer.billing_cleanup_task(&stopper).await
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

		// Spawn the permission update task.
		let permission_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Permission,
						&config.updates.permissions,
						config.updates.permissions.partitions.start,
						config.updates.permissions.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the storage and metadata update task.
		let storage_and_metadata_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::StorageAndMetadata,
						&config.updates.storage_and_metadata,
						config.updates.storage_and_metadata.partitions.start,
						config.updates.storage_and_metadata.partitions.end,
						&stopper,
					)
					.await
			}
		});

		// Spawn the usage update task.
		let usage_update_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.update_task(
						tangram_index::update::Kind::Usage,
						&config.updates.usage,
						config.updates.usage.partitions.start,
						config.updates.usage.partitions.end,
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
			let changed = changed.clone();
			move |stopper| async move {
				indexer
					.wait_task(&state, &changed, wait_receiver, stopper)
					.await
			}
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
			if let Err(error) = self.refresh_indexer_cache(server).await {
				tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
			}
		}

		let tasks = Tasks {
			archive_queue: archive_queue_task,
			archive_sequence_reservations: archive_sequence_reservations_task,
			batch_expiration: batch_expiration_task,
			billing_cleanup: billing_cleanup_task,
			permission_capture: permission_capture_task,
			cleaning: cleaning_task,
			database_index_queue: database_index_queue_task,
			permission_update: permission_update_task,
			index_queue: index_queue_task,
			index_sequence_reservations: index_sequence_reservations_task,
			log_cache: log_cache_task,
			storage_and_metadata_update: storage_and_metadata_update_task,
			object_cache: object_cache_task,
			queue_checkpoints: queue_checkpoints_task,
			queue_completions: queue_completions_task,
			request: request_task,
			usage_update: usage_update_task,
			usage_aggregation: usage_aggregation_task,
			usage_expiration: usage_expiration_task,
			wait: wait_task,
		};

		// Monitor the request and queue tasks at the indexer level.
		let mut monitored = [
			("archive queue", tasks.archive_queue.clone()),
			("permission capture", tasks.permission_capture.clone()),
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["index"]) => self.index_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl Indexer {
	#[must_use]
	fn id(&self) -> &tg::indexer::Id {
		self.id
			.as_ref()
			.expect("a queue operation requires an indexer ID")
	}

	#[must_use]
	fn retry_options(&self) -> tangram_futures::retry::Options {
		self.server.config.indexer.retry.clone().into()
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
			billing_cleanup,
			permission_capture,
			cleaning,
			database_index_queue,
			permission_update,
			index_queue,
			index_sequence_reservations,
			log_cache,
			storage_and_metadata_update,
			object_cache,
			queue_checkpoints,
			queue_completions,
			request,
			usage_update,
			usage_aggregation,
			usage_expiration,
			wait,
		} = tasks;

		// Cancel capture before stopping the index requests it may be awaiting.
		permission_capture.stop();
		permission_capture
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the capture task panicked"))??;

		// Stop accepting queue requests.
		if self.id.is_some() {
			state.lock().unwrap().available = false;
			changed.notify_waiters();
			self.update_availability_with_retry(false).await?;
			if let Err(error) = self.component.refresh_indexer_cache(&self.server).await {
				tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
			}
		}

		// Finish the current operations without exhausting the partitions.
		cleaning.stop();
		database_index_queue.stop();
		permission_update.stop();
		log_cache.stop();
		storage_and_metadata_update.stop();
		object_cache.stop();
		usage_update.stop();
		billing_cleanup.stop();
		usage_aggregation.stop();
		usage_expiration.stop();
		for (name, task) in [
			("cleaning", cleaning),
			("database index queue", database_index_queue),
			("permission update", permission_update),
			("log cache", log_cache),
			("storage and metadata update", storage_and_metadata_update),
			("object cache", object_cache),
			("usage update", usage_update),
			("billing cleanup", billing_cleanup),
			("usage aggregation", usage_aggregation),
			("usage expiration", usage_expiration),
		] {
			wait_for_stopped_task(name, task).await?;
		}

		// Finish the local producers before draining the private queues.
		self.server
			.components
			.object
			.wait_remote_object_put_tasks()
			.await;
		self.server.index_tasks.wait().await;
		self.server.components.object.wait_archive_tasks().await;
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
		tangram_futures::retry(&self.retry_options(), || async {
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
		tangram_futures::retry(&self.retry_options(), || async {
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

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn start<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			// Spawn the shared index wait task.
			let receiver = self
				.index_wait_receiver
				.lock()
				.unwrap()
				.take()
				.ok_or_else(|| tg::error!("the indexer component was already started"))?;
			let task = self.spawn_index_wait_task(server, receiver);
			self.index_wait_task.lock().unwrap().replace(task);

			// Spawn the indexer cache task.
			let task = self.spawn_indexer_cache_task(server);
			self.indexer_cache_task.lock().unwrap().replace(task);

			// Spawn the indexer task.
			let task = self.spawn_indexer_task(server);
			*self.indexer_task.lock().unwrap() = task;

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the index wait task after its callers have stopped.
			let task = self.index_wait_task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the index wait task panicked");
				}
			}

			// Stop the indexer task.
			let task = self.indexer_task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the index task panicked");
				}
				tracing::trace!("indexer task");
			}

			// Stop the indexer cache task.
			let task = self.indexer_cache_task.lock().unwrap().take();
			if let Some(task) = task {
				task.stop();
				let result = task.wait().await;
				if let Err(error) = result
					&& !error.is_cancelled()
				{
					tracing::error!(?error, "the indexer cache task panicked");
				}
			}
		}
		.boxed()
	}

	fn abort(&self) {
		let task = self.index_wait_task.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
		let task = self.indexer_task.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
		let task = self.indexer_cache_task.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
	}
}

impl tangram_server_core::indexer::Component for Component {
	fn index<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>>> {
		self.index(session).boxed()
	}

	fn index_batch<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		arg: tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.index_batch(server, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}

	fn clean_batch<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		arg: CleanBatchArg,
	) -> BoxFuture<'a, tg::Result<tangram_index::clean::Output>> {
		self.clean_batch(server, arg).boxed()
	}

	fn enqueue_database_index_queue_with_transaction<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		arg: &'a tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), tangram_server_core::database::Error>>> {
		Self::enqueue_database_index_queue_with_transaction(server, transaction, arg).boxed()
	}

	fn enqueue_object_archive<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		object: tg::object::Id,
		put: [u8; 16],
	) -> BoxFuture<'a, tg::Result<()>> {
		self.enqueue_object_archive(server, object, put).boxed()
	}

	fn spawn_publish_database_index_queue_notification_task(
		&self,
		server: &tangram_server_core::Server,
	) {
		Self::spawn_publish_database_index_queue_notification_task(server);
	}
}

async fn wait_for_stopped_task(name: &'static str, task: Task<tg::Result<()>>) -> tg::Result<()> {
	task.wait()
		.await
		.map_err(|error| tg::error!(!error, task = %name, "an indexer task panicked"))?
		.map_err(|error| tg::error!(!error, task = %name, "an indexer task failed"))?;
	Ok(())
}
