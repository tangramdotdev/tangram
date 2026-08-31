use {
	super::super::Indexer,
	futures::{FutureExt as _, StreamExt as _, future, stream},
	std::{
		collections::{BTreeMap, BTreeSet},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tangram_store::Store as _,
	tokio_stream::wrappers::IntervalStream,
};

struct Fragment {
	arg: tangram_index::batch::Arg,
	key: crate::store::object::index::outbox::fragment::Key,
}

impl Indexer {
	pub(in crate::indexer) async fn object_index_outbox_task(
		&self,
		partitions: &crate::config::IndexerPartitions,
		outbox: Option<&crate::config::ObjectIndexOutbox>,
	) -> tg::Result<()> {
		let Some(outbox) = outbox else {
			return future::pending().await;
		};
		let wakeup_interval = outbox.wakeup_interval;
		future::try_join_all((partitions.start..partitions.end).map(|partition| {
			self.object_index_outbox_partition_task(outbox, partition, wakeup_interval)
		}))
		.await?;

		Ok(())
	}

	async fn object_index_outbox_partition_task(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		loop {
			let result = self
				.object_index_outbox_partition_task_inner(outbox, partition, wakeup_interval)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), %partition, "failed to service the object index outbox");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn object_index_outbox_partition_task_inner(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		let subject = object_index_outbox_subject(partition);
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to object index outbox notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			let count = self.object_index_outbox_batch(outbox, partition).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the object index outbox wakeup stream ended"));
			}
		}
	}

	async fn object_index_outbox_batch(
		&self,
		outbox: &crate::config::ObjectIndexOutbox,
		partition: u64,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::store::object::index::outbox::fragment::dequeue::Arg {
			batch_size: outbox.batch_size,
			partition_end: partition + 1,
			partition_start: partition,
		};
		let fragments = self
			.server
			.store
			.dequeue_object_index_outbox_fragments(arg)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					"failed to dequeue the object index outbox fragments"
				)
			})?;
		if fragments.is_empty() {
			return Ok(0);
		}

		// Combine the fragments into batches.
		let count = fragments.len();
		let mut batches = BTreeMap::<_, Vec<_>>::new();
		for fragment in fragments {
			let arg = tangram_index::batch::Arg::deserialize(&fragment.payload)?;
			let key = crate::store::object::index::outbox::fragment::Key {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			};
			let fragment = Fragment { arg, key };
			batches.entry(key.batch).or_default().push(fragment);
		}

		// Submit the batches concurrently and each batch's fragments sequentially.
		let results = future::join_all(batches.into_iter().map(|(batch, fragments)| async move {
			self.object_index_outbox_group(&outbox.retry, batch, fragments)
				.await
		}))
		.await;
		let mut error = None;
		for result in results {
			if let Err(current) = result {
				error.get_or_insert(current);
			}
		}
		if let Some(error) = error {
			return Err(tg::error!(
				!error,
				"failed to index an object index outbox batch"
			));
		}

		Ok(count)
	}

	async fn object_index_outbox_group(
		&self,
		retry: &crate::config::Retry,
		batch: crate::store::object::index::outbox::batch::Id,
		mut fragments: Vec<Fragment>,
	) -> tg::Result<()> {
		// Wait for every exact object put referenced by these fragments.
		let puts = fragments
			.iter()
			.flat_map(|fragment| &fragment.arg.items)
			.filter_map(|item| match item {
				tangram_index::batch::Item::PutObject(arg) => Some((arg.id.clone(), arg.put)),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let results = future::try_join_all(puts.into_iter().map(|(id, put)| async move {
			let contains = self.wait_for_object_put(retry, &id, put).await?;

			Ok::<_, tg::Error>((id, put, contains))
		}))
		.await?;
		let missing = results
			.into_iter()
			.filter(|(_, _, exists)| !exists)
			.collect::<Vec<_>>();
		if let Some((id, put, _)) = missing.first() {
			let partition = fragments.first().unwrap().key.partition;
			tracing::error!(%id, ?put, missing_count = missing.len(), "discarding an object index outbox batch because an object put is absent from the store");
			let arg = crate::store::object::index::outbox::batch::delete::Arg {
				id: batch,
				partition,
			};
			self.server
				.store
				.delete_object_index_outbox_batch(arg)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to delete an object index outbox batch")
				})?;

			return Ok(());
		}

		// Index the fragments in order.
		fragments.sort_unstable_by_key(|fragment| fragment.key.index);
		for fragment in &fragments {
			crate::checkpoint!(self.server, "index.batch").await;
			self.server.index.batch(fragment.arg.clone()).await?;
		}

		// Delete the indexed fragments.
		let fragments = fragments.into_iter().map(|fragment| fragment.key).collect();
		let arg = crate::store::object::index::outbox::fragment::delete::Arg { fragments };
		self.server
			.store
			.delete_object_index_outbox_fragments(arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete object index outbox fragments")
			})?;

		Ok(())
	}
}

pub(crate) fn object_index_outbox_subject(partition: u64) -> String {
	format!("stores.object.index.outbox.{partition}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn subject_includes_the_partition() {
		assert_eq!(
			object_index_outbox_subject(42),
			"stores.object.index.outbox.42"
		);
	}
}
