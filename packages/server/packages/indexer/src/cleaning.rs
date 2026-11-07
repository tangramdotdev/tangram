use {
	super::{Indexer, partition},
	crate::Component,
	futures::future,
	num::ToPrimitive as _,
	std::{os::unix::fs::PermissionsExt as _, path::Path, time::Duration},
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::prelude::*,
	tangram_server_core::{Server, indexer::CleanBatchArg, temp::Temp},
};

#[derive(Clone, Copy, Debug)]
struct Capacity {
	available: u64,
	total: u64,
}

#[derive(Default)]
struct CheckoutOutput {
	bytes: u64,
	files: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum CleanMode {
	Capacity,

	#[default]
	Ttl,
}

impl Indexer {
	pub(super) async fn cleaning_task(
		&self,
		config: &tangram_server_core::config::IndexerCleaning,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		if partition_start == partition_end {
			stopper.wait().await;
			return Ok(());
		}
		let mut mode = CleanMode::default();
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			if let Some(config) = &config.capacity {
				match Component::cleaning_capacity(&self.server) {
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

			let now = self.server.clock.unix_timestamp();
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
					self.component.clean_batch(
						&self.server,
						CleanBatchArg {
							batch_size,
							now,
							object_time_to_live,
							partition_end: range.end,
							partition_start: range.start,
							process_time_to_live,
							sandbox_time_to_live,
						},
					)
				});
			let results = future::join_all(futures).await;
			let mut done = true;
			let mut failed = false;
			let mut objects = Vec::new();
			let mut processes = Vec::new();
			let mut sandboxes = Vec::new();
			for result in results {
				match result {
					Ok(output) => {
						done &= output.done;
						objects.extend(output.objects.into_iter().map(|object| object.id));
						processes.extend(output.processes);
						sandboxes.extend(output.sandboxes);
					},
					Err(error) => {
						failed = true;
						tracing::error!(error = %error.trace(), "failed to clean an index partition");
					},
				}
			}

			// Hit the deletion checkpoints in retention order, so that a sandbox precedes its processes and a process precedes its objects.
			for sandbox in sandboxes {
				tangram_server_core::checkpoint!(self.server, "cleaning.sandbox.delete", sandbox = %sandbox)
					.await;
			}
			for process in processes {
				tangram_server_core::checkpoint!(self.server, "cleaning.process.delete", process = %process)
					.await;
			}
			for object in objects {
				tangram_server_core::checkpoint!(self.server, "cleaning.object.delete", object = %object).await;
			}

			if done || failed {
				tokio::select! {
					() = stopper.wait() => return Ok(()),
					() = self.server.clock.sleep(config.poll_interval) => {},
				}
			}
		}
	}
}

impl Component {
	fn cleaning_capacity(server: &Server) -> tg::Result<Capacity> {
		let status = rustix::fs::statvfs(&server.path).map_err(
			|error| tg::error!(!error, path = %server.path.display(), "failed to get the server filesystem capacity"),
		)?;
		let available = status.f_bavail.saturating_mul(status.f_frsize);
		let total = status.f_blocks.saturating_mul(status.f_frsize);
		let capacity = Capacity { available, total };

		Ok(capacity)
	}

	pub(crate) async fn clean_batch(
		&self,
		server: &Server,
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
		let mut output = server
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

		// Remove the cleaned nodes from retained runner state.
		let runner = server.components.runner.state();
		for process in &output.processes {
			runner.remove_process(process);
		}
		for sandbox in &output.sandboxes {
			runner.remove_sandbox(sandbox);
		}

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
		let cache_args = output
			.objects
			.iter()
			.cloned()
			.map(|object| tangram_server_core::cache::object::delete::Arg {
				id: object.id,
				put: object.put,
			})
			.collect();

		// Run every side effect independently.
		let delete_artifact_checkouts_future = self.delete_artifact_checkouts(server, artifacts);
		let delete_named_checkouts_future = self.delete_named_checkouts(server, named);
		let delete_archive_future = async {
			if let Some(archive) = &server.archive
				&& let Err(error) = archive.delete_object_batch(archive_args).await
			{
				let error = tg::error!(!error, "failed to delete objects from the archive");
				tracing::error!(error = %error.trace());
			}
		};
		let delete_store_future = async {
			match self.delete_cached_objects(server, cache_args).await {
				Ok(bytes) => bytes,
				Err(error) => {
					let error = tg::error!(!error, "failed to delete objects from the cache");
					tracing::error!(error = %error.trace());
					0
				},
			}
		};
		let ((), checkouts, (), object_bytes) = future::join4(
			delete_archive_future,
			delete_artifact_checkouts_future,
			delete_named_checkouts_future,
			delete_store_future,
		)
		.await;
		output.checkout_bytes = checkouts.bytes;
		output.checkout_files = checkouts.files;
		output.object_bytes = object_bytes;

		Ok(output)
	}

	async fn delete_cached_objects(
		&self,
		server: &Server,
		args: Vec<tangram_server_core::cache::object::delete::Arg>,
	) -> tg::Result<u64> {
		if args.is_empty() {
			return Ok(0);
		}

		// Count the cached payloads that match the versions being deleted.
		let arg = tangram_server_core::cache::object::get::batch::Arg {
			bytes: true,
			ids: args.iter().map(|arg| arg.id.clone()).collect(),
		};
		let result = server.cache.try_get_object_batch(arg).await.map(|outputs| {
			std::iter::zip(&args, outputs)
				.filter_map(|(arg, output)| output.object.filter(|object| object.put == arg.put))
				.filter_map(|object| object.bytes)
				.map(|bytes| bytes.len().to_u64().unwrap())
				.sum()
		});

		// Delete the objects even if counting their payloads failed.
		server.cache.delete_object_batch(args).await?;
		let bytes = result
			.map_err(|error| tg::error!(!error, "failed to count the cached object bytes"))?;

		Ok(bytes)
	}

	async fn delete_artifact_checkouts(
		&self,
		server: &Server,
		artifacts: Vec<tg::Id>,
	) -> CheckoutOutput {
		if artifacts.is_empty() || !server.checkouts_enabled() {
			return CheckoutOutput::default();
		}
		let result = tokio::task::spawn_blocking({
			let server = server.clone();
			move || {
				let mut output = CheckoutOutput::default();
				let temp = Temp::new(&server);
				std::fs::create_dir(temp.path())?;
				let checkout_path = server.checkout_path();
				for artifact in artifacts {
					let path = checkout_path.join(artifact.to_string());
					let temp_path = temp.path().join(artifact.to_string());
					delete_checkout_path(&path, &temp_path, &mut output);

					for extension in [".tg.js", ".tg.ts"] {
						let path = checkout_path.join(format!("{artifact}{extension}"));
						let temp_path = temp.path().join(format!("{artifact}{extension}"));
						delete_checkout_path(&path, &temp_path, &mut output);
					}
				}

				Ok::<_, std::io::Error>(output)
			}
		})
		.await;
		match result {
			Ok(Ok(output)) => output,
			Ok(Err(error)) => {
				tracing::error!(?error, "failed to create the checkout cleanup directory");
				CheckoutOutput::default()
			},
			Err(error) => {
				tracing::error!(?error, "the artifact checkout clean task panicked");
				CheckoutOutput::default()
			},
		}
	}

	async fn delete_named_checkouts(&self, server: &Server, named: Vec<tg::Id>) {
		if named.is_empty()
			|| !server
				.components
				.checkout
				.named_checkout_maintenance_enabled(server)
		{
			return;
		}
		if let Err(error) = self.delete_named_checkouts_inner(server, named).await {
			tracing::error!(error = %error.trace(), "failed to delete named checkouts");
		}
	}

	async fn delete_named_checkouts_inner(
		&self,
		server: &Server,
		named: Vec<tg::Id>,
	) -> tg::Result<()> {
		let guard = server.components.checkout.lock().acquire().await;
		if !server
			.components
			.checkout
			.named_checkout_maintenance_enabled(server)
		{
			return Ok(());
		}
		let checkouts = server.index.try_get_checkouts(&named).await?;
		let specifiers = server.index.try_get_specifiers_for_ids(&named).await?;
		let mut entries = std::iter::zip(named, std::iter::zip(checkouts, specifiers))
			.filter_map(|(id, (checkout, specifier))| {
				(checkout.is_none()).then_some((id, specifier?))
			})
			.collect::<Vec<_>>();
		entries.sort_by_key(|(_, specifier)| std::cmp::Reverse(specifier.components().count()));
		for (id, specifier) in entries {
			if let Err(error) = server
				.components
				.checkout
				.remove_named_checkout_entry_with_lock(server, &guard, &id, &specifier)
				.await
			{
				tracing::error!(error = %error.trace(), %id, %specifier, "failed to delete a named checkout");
			}
		}

		Ok(())
	}
}

impl CleanMode {
	fn next(
		self,
		config: &tangram_server_core::config::CapacityThreshold,
		capacity: Capacity,
	) -> Self {
		match self {
			Self::Capacity if !config.should_stop(capacity.available, capacity.total) => {
				Self::Capacity
			},
			Self::Ttl if config.should_start(capacity.available, capacity.total) => Self::Capacity,
			Self::Capacity | Self::Ttl => Self::Ttl,
		}
	}
}

fn delete_checkout_path(path: &Path, temp_path: &Path, output: &mut CheckoutOutput) {
	let result = (|| {
		// Restore owner access so read-only directories can be moved on macOS.
		let metadata = std::fs::symlink_metadata(path)?;
		if metadata.is_dir() {
			let permissions =
				std::fs::Permissions::from_mode(metadata.permissions().mode() | 0o700);
			std::fs::set_permissions(path, permissions)?;
		}
		std::fs::rename(path, temp_path)?;
		Ok::<_, std::io::Error>(())
	})();
	match result {
		Ok(()) => {
			let result = tangram_util::fs::remove_sync_with_callback(temp_path, |metadata| {
				if metadata.is_file() {
					output.bytes += metadata.len();
					output.files += 1;
				}
			});
			if let Err(error) = result {
				tracing::error!(?error, path = %temp_path.display(), "failed to delete a checkout path");
			}
		},
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => {
			tracing::error!(?error, path = %path.display(), "failed to move a checkout path for deletion");
		},
	}
}
