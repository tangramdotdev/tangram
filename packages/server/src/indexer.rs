use {
	crate::Server,
	futures::{FutureExt as _, future},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

mod cleaning;
mod compaction;
mod database;
mod object;
mod partition;
mod request;
mod stripe;
mod update;
mod usage;

pub(crate) use {
	cleaning::CleanBatchArg,
	database::database_index_outbox_subject,
	object::{object_archive_outbox_subject, object_index_outbox_subject},
	request::RequestArg,
};

#[derive(Clone)]
struct Indexer {
	server: Server,
}

impl Server {
	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let indexer = Indexer {
			server: self.clone(),
		};
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
					.object_archive_outbox_task(&config.partitions, outbox.as_ref())
					.await
			}
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

		// Spawn the object index outbox task.
		let object_index_outbox_task = Task::spawn({
			let config = config.clone();
			let outbox = (!self.config.advanced.single_process)
				.then(|| self.config.object.index_outbox.clone());
			let indexer = indexer.clone();
			move |_| async move {
				indexer
					.object_index_outbox_task(&config.partitions, outbox.as_ref())
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
				if !config.usage.expiration.enabled {
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
		let request_task = Task::spawn({
			let config = config.clone();
			let indexer = indexer.clone();
			move |_| async move { indexer.request_task(&config.request).await }
		});

		// Wait for the tasks independently.
		future::join_all([
			wait_for_task("cleaning", cleaning_task).boxed(),
			wait_for_task("database index outbox", database_index_outbox_task).boxed(),
			wait_for_task("grant update", grant_update_task).boxed(),
			wait_for_task("log compaction", log_compaction_task).boxed(),
			wait_for_task("node update", node_update_task).boxed(),
			wait_for_task("object archive outbox", object_archive_outbox_task).boxed(),
			wait_for_task("object cache", object_cache_task).boxed(),
			wait_for_task("object index outbox", object_index_outbox_task).boxed(),
			wait_for_task("request", request_task).boxed(),
			wait_for_task("storage update", storage_update_task).boxed(),
			wait_for_task("Stripe cleanup", stripe_cleanup_task).boxed(),
			wait_for_task("usage aggregation", usage_aggregation_task).boxed(),
			wait_for_task("usage expiration", usage_expiration_task).boxed(),
		])
		.await;

		Ok(())
	}
}

async fn wait_for_task(name: &'static str, task: Task<tg::Result<()>>) {
	match task.wait().await {
		Ok(Ok(())) => {
			tracing::error!(task = name, "an indexer task exited unexpectedly");
		},
		Ok(Err(error)) => {
			tracing::error!(error = %error.trace(), task = name, "an indexer task failed");
		},
		Err(error) => {
			tracing::error!(?error, task = name, "an indexer task panicked");
		},
	}
	future::pending::<()>().await;
}
