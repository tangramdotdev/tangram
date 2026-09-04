use {
	crate::Server,
	futures::future,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_store::Store as _,
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

pub(crate) use {
	cache::Cache,
	cleaning::CleanBatchArg,
	database::database_index_outbox_subject,
	request::{ArchiveRequestArg, IndexRequestArg, RequestArg},
};

const RETRY_OPTIONS: tangram_futures::retry::Options = tangram_futures::retry::Options {
	backoff: std::time::Duration::from_secs(1),
	jitter: std::time::Duration::ZERO,
	max_delay: std::time::Duration::from_secs(1),
	max_retries: u64::MAX,
};

#[derive(Clone)]
struct Indexer {
	command_sender: tokio::sync::mpsc::UnboundedSender<request::Command>,
	id: tg::indexer::Id,
	server: Server,
}

struct Tasks {
	cleaning: Task<tg::Result<()>>,
	database_index_outbox: Task<tg::Result<()>>,
	grant_update: Task<tg::Result<()>>,
	log_compaction: Task<tg::Result<()>>,
	node_update: Task<tg::Result<()>>,
	object_cache: Task<tg::Result<()>>,
	queue: Task<tg::Result<()>>,
	request: Task<tg::Result<()>>,
	storage_update: Task<tg::Result<()>>,
	stripe_cleanup: Task<tg::Result<()>>,
	usage_aggregation: Task<tg::Result<()>>,
	usage_expiration: Task<tg::Result<()>>,
}

impl Server {
	pub(crate) async fn indexer_task(
		&self,
		config: &crate::config::Indexer,
		stopper: Stopper,
	) -> tg::Result<()> {
		let id = config.id.clone().unwrap_or_else(tg::indexer::Id::new);
		let arg = crate::store::indexer::get::Arg { id: id.clone() };
		let indexer_state = if let Some(mut indexer) = self.store.try_get_indexer(arg).await? {
			indexer.available = false;
			let arg = crate::store::indexer::put::Arg {
				indexer: indexer.clone(),
			};
			self.store.put_indexer(arg).await?;
			indexer
		} else {
			let indexer = crate::store::indexer::Indexer::new(id.clone());
			let arg = crate::store::indexer::put::Arg {
				indexer: indexer.clone(),
			};
			self.store.put_indexer(arg).await?;
			indexer
		};
		let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
		let (completion_sender, completion_receiver) = tokio::sync::mpsc::unbounded_channel();
		let (queue_sender, queue_receiver) = tokio::sync::mpsc::unbounded_channel();
		let indexer = Indexer {
			command_sender,
			id,
			server: self.clone(),
		};
		let queue_task = Task::spawn({
			let indexer = indexer.clone();
			move |stopper| async move {
				indexer
					.queue_task(queue_receiver, completion_sender, stopper)
					.await
			}
		});
		let mut queues = queue::Queues::new(&indexer_state);
		queues.recover(&indexer, &queue_sender).await?;
		queues.reserve_initial_sequences(&indexer).await?;
		let usage_enabled = self.config.usage.enabled;

		// Spawn the cleaning task.
		let cleaning_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				if !config.cleaning.enabled {
					return future::pending().await;
				}
				indexer
					.cleaning_task(
						&config.cleaning,
						config.partitions.start,
						config.partitions.end,
					)
					.await
			}
		});

		// Spawn the database index outbox task.
		let database_index_outbox_task = Task::spawn({
			let indexer = indexer.clone();
			let outbox = self.config.database.index_outbox().clone();
			let region = self.config.region.clone().unwrap_or_default();
			move |_| async move { indexer.database_index_outbox_task(&outbox, &region).await }
		});

		// Spawn the object cache task.
		let object_cache_task = Task::spawn({
			let cache = self.config.object.cache.clone();
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.object_cache_task(&config.partitions, cache.as_ref())
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
						config.partitions.start,
						config.partitions.end,
					)
					.await
			}
		});

		// Spawn the Stripe cleanup task.
		let stripe_cleanup_task = Task::spawn({
			let indexer = indexer.clone();
			move |_| async move {
				if !indexer.server.is_primary_region() {
					return future::pending().await;
				}
				indexer.stripe_cleanup_task().await
			}
		});

		// Spawn the usage aggregation task.
		let usage_aggregation_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				if !usage_enabled || !config.usage.aggregation.enabled {
					return future::pending().await;
				}
				indexer
					.usage_aggregation_task(
						&config.usage.aggregation,
						config.partitions.start,
						config.partitions.end,
					)
					.await
			}
		});

		// Spawn the usage expiration task.
		let usage_expiration_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move {
				if !usage_enabled || !config.usage.expiration.enabled {
					return future::pending().await;
				}
				indexer
					.usage_expiration_task(
						&config.usage.expiration,
						config.partitions.start,
						config.partitions.end,
					)
					.await
			}
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
						config.partitions.start,
						config.partitions.end,
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
						config.partitions.start,
						config.partitions.end,
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
						&config.updates.storage,
						config.partitions.start,
						config.partitions.end,
					)
					.await
			}
		});

		// Spawn the request task.
		let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
		let request_task = Task::spawn({
			let indexer = indexer.clone();
			let poll_interval = config.request.poll_interval;
			let inputs = request::Inputs {
				command_receiver,
				completion_receiver,
				queue_sender,
				queues,
			};
			move |stopper| async move {
				indexer
					.request_task(inputs, poll_interval, ready_sender, stopper)
					.await
			}
		});

		// Make the indexer available after recovery and subscription.
		ready_receiver
			.await
			.map_err(|_| tg::error!("the indexer request task stopped before becoming ready"))?;
		indexer.set_queue_requests_enabled(true).await?;
		indexer.update_availability(true).await?;
		if let Err(error) = self.refresh_indexer_cache().await {
			tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
		}

		// Wait for shutdown.
		stopper.wait().await;
		let tasks = Tasks {
			cleaning: cleaning_task,
			database_index_outbox: database_index_outbox_task,
			grant_update: grant_update_task,
			log_compaction: log_compaction_task,
			node_update: node_update_task,
			object_cache: object_cache_task,
			queue: queue_task,
			request: request_task,
			storage_update: storage_update_task,
			stripe_cleanup: stripe_cleanup_task,
			usage_aggregation: usage_aggregation_task,
			usage_expiration: usage_expiration_task,
		};
		indexer.shutdown(tasks).await?;

		Ok(())
	}
}

impl Indexer {
	async fn shutdown(&self, tasks: Tasks) -> tg::Result<()> {
		let Tasks {
			cleaning,
			database_index_outbox,
			grant_update,
			log_compaction,
			node_update,
			object_cache,
			queue,
			request,
			storage_update,
			stripe_cleanup,
			usage_aggregation,
			usage_expiration,
		} = tasks;

		// Stop accepting queue work and wait for in-flight queue writes.
		self.set_queue_requests_enabled(false).await?;
		self.update_availability_with_retry(false).await?;
		if let Err(error) = self.server.refresh_indexer_cache().await {
			tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
		}
		// Finish all of the indexer's work.
		self.drain_queues().await?;
		self.wait_for_indexing_with_retry().await?;
		self.checkpoint_queues_with_retry().await?;

		// Delete the row only after it certifies that all work is finished.
		self.delete_with_retry().await?;
		request.stop();
		wait_for_stopped_task("request", request).await;
		queue.stop();
		wait_for_stopped_task("object queue", queue).await;

		// Stop the remaining tasks.
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
			task.abort();
			wait_for_stopped_task(name, task).await;
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
		let arg = crate::store::indexer::update::Arg {
			id: self.id.clone(),
			value: crate::store::indexer::update::Value::Available(available),
		};
		self.server.store.update_indexer(arg).await
	}

	async fn wait_for_indexing_with_retry(&self) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let result = self
				.server
				.send_indexer_request(&self.id, RequestArg::Wait)
				.await;
			let result = match result {
				Ok(Ok(output)) => output
					.try_unwrap_wait()
					.map_err(|_| tg::error!("expected a wait response")),
				Ok(Err(error)) | Err(error) => Err(error),
			};
			match result {
				Ok(()) => Ok(ControlFlow::Break(())),
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to wait for indexing during indexer shutdown");

					Ok(ControlFlow::Continue(error))
				},
			}
		})
		.await?;

		Ok(())
	}

	async fn delete_with_retry(&self) -> tg::Result<()> {
		tangram_futures::retry(&RETRY_OPTIONS, || async {
			let arg = crate::store::indexer::delete::Arg {
				id: self.id.clone(),
			};
			match self.server.store.delete_indexer(arg).await {
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

async fn wait_for_stopped_task(name: &'static str, task: Task<tg::Result<()>>) {
	match task.wait().await {
		Ok(Ok(())) => {},
		Ok(Err(error)) => {
			tracing::error!(error = %error.trace(), task = name, "an indexer task failed");
		},
		Err(error) => {
			if !error.is_cancelled() {
				tracing::error!(?error, task = name, "an indexer task panicked");
			}
		},
	}
}
