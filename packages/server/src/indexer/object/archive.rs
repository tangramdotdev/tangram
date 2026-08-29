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
				tokio::time::sleep(Duration::from_secs(1)).await;
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
			let count = self.object_archive_outbox_batch(outbox, partition).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the object archive outbox wakeup stream ended"));
			}
		}
	}

	async fn object_archive_outbox_batch(
		&self,
		outbox: &crate::config::ObjectArchiveOutbox,
		partition: u64,
	) -> tg::Result<usize> {
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
			return Ok(0);
		}
		let count = entries.len();

		// Group entries for the same object.
		let mut groups = BTreeMap::<_, Vec<_>>::new();
		for entry in entries {
			groups.entry(entry.id.clone()).or_default().push(entry);
		}

		// Archive the objects.
		let results = future::join_all(groups.into_iter().map(|(id, entries)| async move {
			let stored_at = entries.iter().map(|entry| entry.stored_at).max().unwrap();
			let result = self.object_archive_outbox_object(&id, stored_at).await;
			(entries, result)
		}))
		.await;
		let mut completed = Vec::new();
		let mut error = None;
		for (entries, result) in results {
			match result {
				Ok(true) => completed.extend(entries),
				Ok(false) => {},
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

		Ok(count)
	}

	async fn object_archive_outbox_object(
		&self,
		id: &tg::object::Id,
		stored_at: i64,
	) -> tg::Result<bool> {
		let arg = crate::store::object::get::Arg { id: id.clone() };
		let output =
			self.server.store.try_get_object(arg).await.map_err(
				|error| tg::error!(!error, %id, "failed to get an object from the store"),
			)?;
		let Some(object) = output.object else {
			return Ok(false);
		};
		if object.stored_at < stored_at {
			return Ok(false);
		}
		let stored_at = object.stored_at;
		let Some(bytes) = object.bytes else {
			return Ok(false);
		};
		let Some(archive) = &self.server.archive else {
			return Err(tg::error!("the archive is unavailable"));
		};
		let arg = tangram_archive::object::put::Arg {
			bytes: bytes.into_owned().into(),
			id: id.clone(),
			stored_at,
		};
		archive
			.put_object(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put an object in the archive"))?;
		if let Some(cache) = &self.server.config.object.cache {
			let partition = rand::random_range(0..cache.partition_total);
			let arg = crate::store::object::cache::put::Arg {
				id: id.clone(),
				partition,
				stored_at,
			};
			self.server
				.store
				.put_object_cache_entry(arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to put an object cache entry"))?;
		}

		Ok(true)
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
