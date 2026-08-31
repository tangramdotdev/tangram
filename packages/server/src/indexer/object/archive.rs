use {
	super::super::Indexer,
	futures::{FutureExt as _, StreamExt as _, future, stream},
	std::{collections::BTreeMap, time::Duration},
	tangram_archive::Archive as _,
	tangram_client::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_store::Store as _,
	tokio_stream::wrappers::IntervalStream,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);

struct BatchOutput {
	count: usize,
}

struct GroupOutput {
	completed: Vec<crate::store::object::archive::outbox::Entry>,
}

struct Object {
	bytes: bytes::Bytes,
	entry: crate::store::object::archive::outbox::Entry,
}

impl Indexer {
	pub(in crate::indexer) async fn object_archive_outbox_task(
		&self,
		partitions: &crate::config::IndexerPartitions,
		outbox: Option<&crate::config::ObjectArchiveOutbox>,
	) -> tg::Result<()> {
		let Some(outbox) = outbox else {
			return future::pending().await;
		};
		let wakeup_interval = outbox.wakeup_interval;
		future::try_join_all((partitions.start..partitions.end).map(|partition| {
			self.object_archive_outbox_partition_task(outbox, partition, wakeup_interval)
		}))
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
				tokio::time::sleep(RETRY_INTERVAL).await;
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
			let output = self.object_archive_outbox_batch(outbox, partition).await?;
			if output.count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the object archive outbox wakeup stream ended"));
			}
		}
	}

	async fn object_archive_outbox_batch(
		&self,
		outbox: &crate::config::ObjectArchiveOutbox,
		partition: u64,
	) -> tg::Result<BatchOutput> {
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
			let output = BatchOutput { count: 0 };

			return Ok(output);
		}
		let count = entries.len();

		// Group entries for the same object.
		let mut groups = BTreeMap::<_, Vec<_>>::new();
		for entry in entries {
			groups.entry(entry.id.clone()).or_default().push(entry);
		}

		// Archive the objects.
		let results = future::join_all(groups.into_iter().map(|(id, entries)| async move {
			self.object_archive_outbox_group(&outbox.retry, &id, entries)
				.await
		}))
		.await;
		let mut completed = Vec::new();
		let mut error = None;
		for result in results {
			match result {
				Ok(output) => {
					completed.extend(output.completed);
				},
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

		let output = BatchOutput { count };

		Ok(output)
	}

	async fn object_archive_outbox_group(
		&self,
		retry: &crate::config::Retry,
		id: &tg::object::Id,
		entries: Vec<crate::store::object::archive::outbox::Entry>,
	) -> tg::Result<GroupOutput> {
		// Read every exact store row before completing its outbox entry.
		let results = future::try_join_all(entries.iter().cloned().map(|entry| async move {
			let object = self
				.try_wait_for_object_put(retry, id, entry.put)
				.await?
				.and_then(|object| object.bytes)
				.map(|bytes| Object {
					bytes: bytes.into_owned().into(),
					entry: entry.clone(),
				});
			if object.is_none() {
				tracing::error!(%id, put = ?entry.put, "discarding an object archive outbox entry because the object put is absent from the store");
			}

			Ok::<_, tg::Error>(object)
		}))
		.await?;
		let mut objects = results.into_iter().flatten().collect::<Vec<_>>();
		if objects.is_empty() {
			let output = GroupOutput { completed: entries };

			return Ok(output);
		}
		objects.sort_unstable_by_key(|object| object.entry.put);
		let object = objects.last().unwrap();

		// Archive the greatest available put.
		let Some(archive) = &self.server.archive else {
			return Err(tg::error!("the archive is unavailable"));
		};
		let arg = tangram_archive::object::put::Arg {
			bytes: object.bytes.clone(),
			id: id.clone(),
			put: object.entry.put,
		};
		archive
			.put_object(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put an object in the archive"))?;
		if let Some(config) = &self.server.config.object.cache {
			future::try_join_all(objects.iter().map(|object| async move {
				let arg = crate::store::object::cache::put::Arg {
					cache: uuid::Uuid::now_v7().into_bytes(),
					id: id.clone(),
					partition: rand::random_range(0..config.partition_total),
					put: object.entry.put,
				};
				self.server
					.store
					.put_object_cache_entry(arg)
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to put an object cache entry"))
			}))
			.await?;
		}
		let output = GroupOutput { completed: entries };

		Ok(output)
	}
}

pub(crate) fn object_archive_outbox_subject(partition: u64) -> String {
	format!("stores.object.archive.outbox.{partition}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn subject_includes_the_partition() {
		assert_eq!(
			object_archive_outbox_subject(42),
			"stores.object.archive.outbox.42"
		);
	}
}
