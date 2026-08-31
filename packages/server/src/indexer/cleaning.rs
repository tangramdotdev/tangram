use {
	super::{Indexer, partition},
	crate::{Server, temp::Temp},
	futures::future,
	num::ToPrimitive as _,
	std::{path::Path, time::Duration},
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_store::prelude::*,
};

#[derive(Clone, Copy, Debug)]
struct Capacity {
	available: u64,
	total: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum CleanMode {
	Capacity,

	#[default]
	Ttl,
}

pub(crate) struct CleanBatchArg {
	pub batch_size: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_end: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}

impl Indexer {
	pub(super) async fn cleaning_task(
		&self,
		config: &crate::config::IndexerCleaning,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let mut mode = CleanMode::default();
		loop {
			if let Some(config) = &config.capacity {
				match self.server.cleaning_capacity() {
					Ok(capacity) => {
						let previous_mode = mode;
						mode = mode.next(config, capacity);
						if mode != previous_mode {
							let available = capacity.available;
							let total = capacity.total;
							tracing::info!(available, ?mode, total, "the cleaning mode changed");
						}
					},
					Err(error) => {
						mode = CleanMode::Ttl;
						tracing::error!(error = %error.trace(), "failed to get the cleaning capacity");
					},
				}
			}

			let now = self.server.clock.unix_timestamp()?;
			let (object_time_to_live, process_time_to_live, sandbox_time_to_live) = match mode {
				CleanMode::Capacity => (Duration::ZERO, Duration::ZERO, Duration::ZERO),
				CleanMode::Ttl => (
					self.server.config.object.time_to_live,
					self.server.config.process.time_to_live,
					self.server.config.sandbox.time_to_live,
				),
			};
			let batch_size = config.batch_size;

			let futures = partition::ranges(partition_start, partition_end, config.concurrency)
				.map(|range| {
					self.server.clean_batch(CleanBatchArg {
						batch_size,
						now,
						object_time_to_live,
						partition_end: range.end,
						partition_start: range.start,
						process_time_to_live,
						sandbox_time_to_live,
					})
				});
			let results = future::join_all(futures).await;
			let mut done = true;
			let mut failed = false;
			for result in results {
				match result {
					Ok(output) => {
						done &= output.done;
						for process in output.processes {
							crate::checkpoint!(self.server, "cleaning.process.delete", process = %process)
								.await;
						}
					},
					Err(error) => {
						failed = true;
						tracing::error!(error = %error.trace(), "failed to clean an index partition");
					},
				}
			}
			if done || failed {
				tokio::time::sleep(config.poll_interval).await;
			}
		}
	}
}

impl Server {
	fn cleaning_capacity(&self) -> tg::Result<Capacity> {
		let status = rustix::fs::statvfs(&self.path).map_err(
			|error| tg::error!(!error, path = %self.path.display(), "failed to get the server filesystem capacity"),
		)?;
		let available = status.f_bavail.saturating_mul(status.f_frsize);
		let total = status.f_blocks.saturating_mul(status.f_frsize);
		let capacity = Capacity { available, total };

		Ok(capacity)
	}

	pub(crate) async fn clean_batch(
		&self,
		arg: CleanBatchArg,
	) -> tg::Result<tangram_index::clean::Output> {
		let CleanBatchArg {
			batch_size,
			now,
			object_time_to_live,
			partition_end,
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
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_end,
				partition_start,
			})
			.await?;

		// Prepare the side effects.
		let (artifacts, named): (Vec<_>, Vec<_>) = output
			.checkouts
			.iter()
			.cloned()
			.partition(|id| tg::artifact::Id::try_from(id.clone()).is_ok());
		let archive_args = output
			.objects
			.iter()
			.cloned()
			.map(|object| tangram_archive::object::delete::Arg {
				id: object.id,
				put: object.put,
			})
			.collect();
		let store_args = output
			.objects
			.iter()
			.cloned()
			.map(|object| crate::store::object::delete::Arg {
				id: object.id,
				put: object.put,
			})
			.collect();

		// Run every side effect independently.
		let delete_artifact_checkouts_future = self.delete_artifact_checkouts(artifacts);
		let delete_named_checkouts_future = self.delete_named_checkouts(named);
		let delete_archive_future = async {
			if let Some(archive) = &self.archive
				&& let Err(error) = archive.delete_object_batch(archive_args).await
			{
				let error = tg::error!(!error, "failed to delete objects from the archive");
				tracing::error!(error = %error.trace());
			}
		};
		let delete_store_future = async {
			if let Err(error) = self.store.delete_object_batch(store_args).await {
				let error = tg::error!(!error, "failed to delete objects from the store");
				tracing::error!(error = %error.trace());
			}
		};
		future::join4(
			delete_archive_future,
			delete_artifact_checkouts_future,
			delete_named_checkouts_future,
			delete_store_future,
		)
		.await;

		Ok(output)
	}

	async fn delete_artifact_checkouts(&self, artifacts: Vec<tg::Id>) {
		if artifacts.is_empty() || !self.checkouts_enabled() {
			return;
		}
		let result = tokio::task::spawn_blocking({
			let server = self.clone();
			move || {
				let temp = Temp::new(&server);
				let checkout_path = server.checkout_path();
				for artifact in artifacts {
					let path = checkout_path.join(artifact.to_string());
					let temp_path = temp.path().join(artifact.to_string());
					delete_checkout_path(&path, &temp_path);

					for extension in [".tg.js", ".tg.ts"] {
						let path = checkout_path.join(format!("{artifact}{extension}"));
						let temp_path = temp.path().join(format!("{artifact}{extension}"));
						delete_checkout_path(&path, &temp_path);
					}
				}
			}
		})
		.await;
		if let Err(error) = result {
			tracing::error!(?error, "the artifact checkout clean task panicked");
		}
	}

	async fn delete_named_checkouts(&self, named: Vec<tg::Id>) {
		if named.is_empty() || !self.named_checkout_maintenance_enabled() {
			return;
		}
		if let Err(error) = self.delete_named_checkouts_inner(named).await {
			tracing::error!(error = %error.trace(), "failed to delete named checkouts");
		}
	}

	async fn delete_named_checkouts_inner(&self, named: Vec<tg::Id>) -> tg::Result<()> {
		let guard = self.checkout_lock.acquire().await?;
		if !self.named_checkout_maintenance_enabled() {
			return Ok(());
		}
		let checkouts = self.index.try_get_checkouts(&named).await?;
		let specifiers = self.index.try_get_specifiers_for_ids(&named).await?;
		let mut entries = std::iter::zip(named, std::iter::zip(checkouts, specifiers))
			.filter_map(|(id, (checkout, specifier))| {
				(checkout.is_none()).then_some((id, specifier?))
			})
			.collect::<Vec<_>>();
		entries.sort_by_key(|(_, specifier)| std::cmp::Reverse(specifier.components().count()));
		for (id, specifier) in entries {
			if let Err(error) = self
				.remove_named_checkout_entry_with_lock(&guard, &id, &specifier)
				.await
			{
				tracing::error!(error = %error.trace(), %id, %specifier, "failed to delete a named checkout");
			}
		}

		Ok(())
	}
}

impl CleanMode {
	fn next(self, config: &crate::config::CapacityThreshold, capacity: Capacity) -> Self {
		match self {
			Self::Capacity if !config.should_stop(capacity.available, capacity.total) => {
				Self::Capacity
			},
			Self::Ttl if config.should_start(capacity.available, capacity.total) => Self::Capacity,
			Self::Capacity | Self::Ttl => Self::Ttl,
		}
	}
}

fn delete_checkout_path(path: &Path, temp_path: &Path) {
	match std::fs::rename(path, temp_path) {
		Ok(()) => {
			if let Err(error) = tangram_util::fs::remove_sync(temp_path) {
				tracing::error!(?error, path = %temp_path.display(), "failed to delete a checkout path");
			}
		},
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => {
			tracing::error!(?error, path = %path.display(), "failed to move a checkout path for deletion");
		},
	}
}
