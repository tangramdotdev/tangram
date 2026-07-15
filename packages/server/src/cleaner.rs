use {
	crate::{Server, database::Database, temp::Temp},
	futures::future,
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_object_store::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

pub(crate) struct CleanerTaskInnerArg {
	pub n: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_count: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}

impl Server {
	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		let partition_start = config.partition_start;
		let partition_count = config.partition_count;
		let concurrency = config.concurrency.to_u64().unwrap();
		loop {
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let object_time_to_live = self.config.object.time_to_live;
			let process_time_to_live = self.config.process.time_to_live;
			let sandbox_time_to_live = self.config.sandbox.time_to_live;
			let n = config.batch_size;

			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_count / concurrency;
				let extra = partition_count % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				self.cleaner_task_inner(CleanerTaskInnerArg {
					n,
					now,
					object_time_to_live,
					partition_count: task_count,
					partition_start: task_start,
					process_time_to_live,
					sandbox_time_to_live,
				})
			});

			match future::try_join_all(futures).await {
				Ok(outputs) => {
					if outputs.iter().all(|output| output.done) {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub(crate) async fn cleaner_task_inner(
		&self,
		arg: CleanerTaskInnerArg,
	) -> tg::Result<tangram_index::clean::Output> {
		let CleanerTaskInnerArg {
			n,
			now,
			object_time_to_live,
			partition_count,
			partition_start,
			process_time_to_live,
			sandbox_time_to_live,
		} = arg;
		let max_object_touched_at = now - object_time_to_live.as_secs().to_i64().unwrap();
		let max_process_touched_at = now - process_time_to_live.as_secs().to_i64().unwrap();
		let max_sandbox_touched_at = now - sandbox_time_to_live.as_secs().to_i64().unwrap();

		// Clean.
		let output = self
			.index
			.clean(tangram_index::clean::Arg {
				batch_size: n,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_count,
				partition_start,
			})
			.await?;

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = output.cache_entries.clone();
			move || {
				let temp = Temp::new(&server);
				let cache_path = server.cache_path();
				for artifact in &cache_entries {
					let path = cache_path.join(artifact.to_string());
					let temp_path = temp.path().join(artifact.to_string());
					std::fs::rename(&path, &temp_path).ok();
					tangram_util::fs::remove_sync(&temp_path).ok();

					for extension in [".tg.js", ".tg.ts"] {
						let path = cache_path.join(format!("{artifact}{extension}"));
						let temp_path = temp.path().join(format!("{artifact}{extension}"));
						std::fs::rename(&path, &temp_path).ok();
						tangram_util::fs::remove_sync(&temp_path).ok();
					}
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "the clean task panicked"))??;

		// Delete objects.
		let ttl = object_time_to_live.as_secs();
		let args = output
			.objects
			.iter()
			.cloned()
			.map(|id| crate::object::store::DeleteArg { id, now, ttl })
			.collect();
		self.object_store
			.delete_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete objects"))?;

		// Delete processes.
		self.clean_processes(&output.processes, max_process_touched_at)
			.await?;

		// Delete sandboxes.
		self.clean_sandboxes(&output.sandboxes, max_sandbox_touched_at)
			.await?;

		Ok(output)
	}

	async fn clean_processes(
		&self,
		processes: &[tg::process::Id],
		max_stored_at: i64,
	) -> tg::Result<()> {
		match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.clean_processes_postgres(process_store, processes, max_stored_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.clean_processes_sqlite(process_store, processes, max_stored_at)
					.await
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.clean_processes_turso(process_store, processes, max_stored_at)
					.await
			},
		}
	}

	async fn clean_sandboxes(
		&self,
		sandboxes: &[tg::sandbox::Id],
		max_finished_at: i64,
	) -> tg::Result<()> {
		match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.clean_sandboxes_postgres(process_store, sandboxes, max_finished_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.clean_sandboxes_sqlite(process_store, sandboxes, max_finished_at)
					.await
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.clean_sandboxes_turso(process_store, sandboxes, max_finished_at)
					.await
			},
		}
	}
}
