use {
	super::Indexer,
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

pub(crate) struct CleanerTaskInnerArg {
	pub batch_size: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_end: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}

impl Indexer {
	pub(super) async fn cleaner_task(
		&self,
		config: &crate::config::IndexerCleaner,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<()> {
		let partition_length = partition_end - partition_start;
		let concurrency = config.concurrency.to_u64().unwrap();
		let mut mode = CleanMode::default();
		loop {
			if let Some(config) = &config.capacity {
				match self.server.cleaner_capacity() {
					Ok(capacity) => {
						let previous_mode = mode;
						mode = mode.next(config, capacity);
						if mode != previous_mode {
							let available = capacity.available;
							let total = capacity.total;
							tracing::info!(available, ?mode, total, "the cleaner mode changed");
						}
					},
					Err(error) => {
						mode = CleanMode::Ttl;
						tracing::error!(error = %error.trace(), "failed to get the cleaner capacity");
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

			let futures = (0..config.concurrency).filter_map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_length / concurrency;
				let extra = partition_length % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				let task_end = task_start + task_count;
				(task_count > 0).then(|| {
					self.server.cleaner_task_inner(CleanerTaskInnerArg {
						batch_size,
						now,
						object_time_to_live,
						partition_end: task_end,
						partition_start: task_start,
						process_time_to_live,
						sandbox_time_to_live,
					})
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
							crate::checkpoint!(self.server, "cleaner.process.delete", process = %process)
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
	fn cleaner_capacity(&self) -> tg::Result<Capacity> {
		let status = rustix::fs::statvfs(&self.path).map_err(
			|error| tg::error!(!error, path = %self.path.display(), "failed to get the server filesystem capacity"),
		)?;
		let available = status.f_bavail.saturating_mul(status.f_frsize);
		let total = status.f_blocks.saturating_mul(status.f_frsize);
		let capacity = Capacity { available, total };

		Ok(capacity)
	}

	pub(crate) async fn cleaner_task_inner(
		&self,
		arg: CleanerTaskInnerArg,
	) -> tg::Result<tangram_index::clean::Output> {
		let CleanerTaskInnerArg {
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
				touched_at: object.touched_at,
			})
			.collect();
		let store_args = output
			.objects
			.iter()
			.cloned()
			.map(|object| crate::store::object::delete::Arg {
				id: object.id,
				touched_at: object.touched_at,
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
	fn next(self, config: &crate::config::IndexerCleanerCapacity, capacity: Capacity) -> Self {
		match config {
			crate::config::IndexerCleanerCapacity::Filesystem(config) => {
				let available = if capacity.total == 0 {
					0.0
				} else {
					#[expect(
						clippy::cast_precision_loss,
						reason = "the ratio does not require integer precision"
					)]
					let available = capacity.available as f64 / capacity.total as f64;
					available
				};
				match self {
					Self::Capacity if available < config.target_available => Self::Capacity,
					Self::Ttl if available < config.minimum_available => Self::Capacity,
					Self::Capacity | Self::Ttl => Self::Ttl,
				}
			},
			crate::config::IndexerCleanerCapacity::Limit(config) => {
				let used = capacity.total.saturating_sub(capacity.available);
				match self {
					Self::Capacity if used > config.target_used => Self::Capacity,
					Self::Ttl if used > config.maximum_used => Self::Capacity,
					Self::Capacity | Self::Ttl => Self::Ttl,
				}
			},
		}
	}
}

pub(super) fn validate(
	config: &crate::config::IndexerCleaner,
	partition_start: u64,
	partition_end: u64,
) -> tg::Result<()> {
	if config.batch_size == 0 {
		return Err(tg::error!(
			"the cleaner batch size must be greater than zero"
		));
	}
	if config.concurrency == 0 {
		return Err(tg::error!(
			"the cleaner concurrency must be greater than zero"
		));
	}
	if partition_end <= partition_start {
		return Err(tg::error!(
			"the cleaner partition end must be greater than the partition start"
		));
	}
	if config.poll_interval.is_zero() {
		return Err(tg::error!(
			"the cleaner poll interval must be greater than zero"
		));
	}
	validate_capacity(config.capacity.as_ref())?;

	Ok(())
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

fn validate_capacity(config: Option<&crate::config::IndexerCleanerCapacity>) -> tg::Result<()> {
	match config {
		None => {},
		Some(crate::config::IndexerCleanerCapacity::Filesystem(config)) => {
			if !config.minimum_available.is_finite()
				|| !(0.0..=1.0).contains(&config.minimum_available)
			{
				return Err(tg::error!(
					"the cleaner minimum available capacity must be between zero and one"
				));
			}
			if !config.target_available.is_finite()
				|| !(0.0..=1.0).contains(&config.target_available)
			{
				return Err(tg::error!(
					"the cleaner target available capacity must be between zero and one"
				));
			}
			if config.target_available <= config.minimum_available {
				return Err(tg::error!(
					"the cleaner target available capacity must be greater than the minimum available capacity"
				));
			}
		},
		Some(crate::config::IndexerCleanerCapacity::Limit(config)) => {
			if config.maximum_used == 0 {
				return Err(tg::error!(
					"the cleaner maximum used capacity must be greater than zero"
				));
			}
			if config.target_used >= config.maximum_used {
				return Err(tg::error!(
					"the cleaner target used capacity must be less than the maximum used capacity"
				));
			}
		},
	}

	Ok(())
}
