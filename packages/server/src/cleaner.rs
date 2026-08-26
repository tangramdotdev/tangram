use {
	crate::{Server, temp::Temp},
	futures::{FutureExt as _, future},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
	tangram_store::prelude::*,
};

const STRIPE_WEBHOOK_TIME_TO_LIVE: Duration = Duration::from_hours(35 * 24);

pub(crate) struct CleanerTaskInnerArg {
	pub n: usize,
	pub now: i64,
	pub object_time_to_live: Duration,
	pub partition_end: u64,
	pub partition_start: u64,
	pub process_time_to_live: Duration,
	pub sandbox_time_to_live: Duration,
}

impl Server {
	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		if config.concurrency == 0 {
			return Err(tg::error!(
				"the cleaner concurrency must be greater than zero"
			));
		}
		if config.partition_end <= config.partition_start {
			return Err(tg::error!(
				"the cleaner partition end must be greater than the partition start"
			));
		}
		let partition_start = config.partition_start;
		let partition_end = config.partition_end;
		let partition_length = partition_end - partition_start;
		let concurrency = config.concurrency.to_u64().unwrap();
		loop {
			let now = self.clock.unix_timestamp()?;
			let object_time_to_live = self.config.object.time_to_live;
			let process_time_to_live = self.config.process.time_to_live;
			let sandbox_time_to_live = self.config.sandbox.time_to_live;
			let usage = self.config.usage;
			let usage_partition_total = self.index.usage_partition_total();
			let usage_partition_end = partition_end.min(usage_partition_total);
			let usage_partition_start = partition_start.min(usage_partition_total);
			let n = config.batch_size;

			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_length / concurrency;
				let extra = partition_length % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				let task_end = task_start + task_count;
				self.cleaner_task_inner(CleanerTaskInnerArg {
					n,
					now,
					object_time_to_live,
					partition_end: task_end,
					partition_start: task_start,
					process_time_to_live,
					sandbox_time_to_live,
				})
			});

			let clean_database_future = async {
				if self.is_primary_region() {
					self.clean_stripe_webhooks(now).await?;
				}

				Ok(())
			};
			let clean_index_future = future::try_join_all(futures);
			let clean_usage_future = self.index.clean_usage(tangram_index::usage::clean::Arg {
				batch_size: n,
				day_time_to_live: usage.day_time_to_live,
				delta_time_to_live: usage.delta_time_to_live,
				hour_time_to_live: usage.hour_time_to_live,
				month_time_to_live: usage.month_time_to_live,
				now: jiff::Timestamp::new(now, 0).unwrap(),
				partition_end: usage_partition_end,
				partition_start: usage_partition_start,
				week_time_to_live: usage.week_time_to_live,
			});
			match future::try_join3(
				clean_database_future,
				clean_index_future,
				clean_usage_future,
			)
			.await
			{
				Ok(((), outputs, usage_output)) => {
					for process in outputs.iter().flat_map(|output| &output.processes) {
						crate::checkpoint!(self, "cleaner.process.delete", process = %process)
							.await;
					}
					if usage_output.done && outputs.iter().all(|output| output.done) {
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

	async fn clean_stripe_webhooks(&self, now: i64) -> tg::Result<()> {
		let ttl = STRIPE_WEBHOOK_TIME_TO_LIVE.as_secs().to_i64().unwrap();
		let max_created_at = now.saturating_sub(ttl);
		self.database
			.run(|transaction| {
				async move {
					Self::clean_stripe_webhooks_with_transaction(transaction, max_created_at).await
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn clean_stripe_webhooks_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		max_created_at: i64,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from stripe_webhooks where created_at < {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![max_created_at])
			.await;
		crate::database::retry!(result, "failed to clean the Stripe webhook events");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn cleaner_task_inner(
		&self,
		arg: CleanerTaskInnerArg,
	) -> tg::Result<tangram_index::clean::Output> {
		let CleanerTaskInnerArg {
			n,
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
				batch_size: n,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_end,
				partition_start,
			})
			.await?;

		// Delete artifact checkouts.
		let (artifacts, named): (Vec<_>, Vec<_>) = output
			.checkouts
			.iter()
			.cloned()
			.partition(|id| tg::artifact::Id::try_from(id.clone()).is_ok());
		if self.checkouts_enabled() {
			tokio::task::spawn_blocking({
				let server = self.clone();
				move || {
					let temp = Temp::new(&server);
					let checkout_path = server.checkout_path();
					for artifact in artifacts {
						let path = checkout_path.join(artifact.to_string());
						let temp_path = temp.path().join(artifact.to_string());
						std::fs::rename(&path, &temp_path).ok();
						tangram_util::fs::remove_sync(&temp_path).ok();

						for extension in [".tg.js", ".tg.ts"] {
							let path = checkout_path.join(format!("{artifact}{extension}"));
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
		}

		// Delete named checkouts.
		if !named.is_empty() && self.named_checkout_maintenance_enabled() {
			let guard = self.checkout_lock.acquire().await?;
			if self.named_checkout_maintenance_enabled() {
				let checkouts = self.index.try_get_checkouts(&named).await?;
				let specifiers = self.index.try_get_specifiers_for_ids(&named).await?;
				let mut entries = std::iter::zip(named, std::iter::zip(checkouts, specifiers))
					.filter_map(|(id, (checkout, specifier))| {
						(checkout.is_none()).then_some((id, specifier?))
					})
					.collect::<Vec<_>>();
				entries.sort_by_key(|(_, specifier)| {
					std::cmp::Reverse(specifier.components().count())
				});
				for (id, specifier) in entries {
					self.remove_named_checkout_entry_with_lock(&guard, &id, &specifier)
						.await?;
				}
			}
		}

		// Delete objects.
		let ttl = object_time_to_live.as_secs();
		let args = output
			.objects
			.iter()
			.cloned()
			.map(|id| crate::store::object::delete::Arg { id, now, ttl })
			.collect();
		self.store
			.delete_object_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete objects"))?;

		Ok(output)
	}
}
