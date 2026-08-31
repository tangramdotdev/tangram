use {
	super::super::Indexer,
	futures::{FutureExt as _, StreamExt as _, stream},
	std::{
		collections::{BTreeMap, BTreeSet},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
	tokio_stream::wrappers::IntervalStream,
};

struct NamedCheckout {
	id: tg::Id,
	specifier: tg::Specifier,
}

impl Indexer {
	pub(in crate::indexer) async fn database_index_outbox_task(
		&self,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
	) -> tg::Result<()> {
		let wakeup_interval = outbox.wakeup_interval;
		loop {
			let result = self
				.database_index_outbox_task_inner(outbox, region, wakeup_interval)
				.await;
			if let Err(error) = result {
				tracing::error!(error = %error.trace(), "failed to service the database index outbox");
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn database_index_outbox_task_inner(
		&self,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
		wakeup_interval: Duration,
	) -> tg::Result<()> {
		let subject = database_index_outbox_subject();
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject.clone())
			.await
			.map_err(
				|error| tg::error!(!error, %subject, "failed to subscribe to database index outbox notifications"),
			)?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(wakeup_interval))
			.skip(1)
			.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let mut wakeups = wakeups.boxed();
		loop {
			while wakeups.next().now_or_never().flatten().is_some() {}
			let count = self.database_index_outbox_batch(outbox, region).await?;
			if count == 0 && wakeups.next().await.is_none() {
				return Err(tg::error!("the database index outbox wakeup stream ended"));
			}
		}
	}

	async fn database_index_outbox_batch(
		&self,
		outbox: &crate::config::DatabaseIndexOutbox,
		region: &str,
	) -> tg::Result<usize> {
		// Dequeue a batch.
		let arg = crate::database::index::outbox::DequeueArg {
			batch_size: outbox.batch_size,
			region: region.to_owned(),
		};
		let entries = self
			.server
			.database
			.dequeue_index_outbox(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the database index outbox"))?;
		if entries.is_empty() {
			return Ok(0);
		}

		// Deserialize the index batches.
		let count = entries.len();
		let batch = entries.last().unwrap().batch;
		let mut args = Vec::with_capacity(count);
		for entry in entries {
			let arg = tangram_index::batch::Arg::deserialize(&entry.payload)?;
			args.push(arg);
		}

		// Submit each outbox entry sequentially to preserve transaction order.
		for arg in args {
			if !self.server.named_checkout_maintenance_enabled()
				|| !Self::database_index_outbox_batch_contains_named_node_mutation(&arg)
			{
				self.server.index.batch(arg).await.map_err(|error| {
					tg::error!(!error, "failed to index a database index outbox batch")
				})?;
				continue;
			}
			crate::checkpoint!(self.server, "indexer.database_index_outbox.named_node").await;
			let guard = self.server.checkout_lock.acquire().await?;
			if !self.server.named_checkout_maintenance_enabled() {
				self.server.index.batch(arg).await.map_err(|error| {
					tg::error!(!error, "failed to index a database index outbox batch")
				})?;
				continue;
			}
			let (arg, mut invalidations) = self.prepare_database_index_outbox_batch(arg).await?;
			invalidations
				.sort_by_key(|checkout| std::cmp::Reverse(checkout.specifier.components().count()));
			for checkout in invalidations {
				self.server
					.remove_named_checkout_entry_with_lock(
						&guard,
						&checkout.id,
						&checkout.specifier,
					)
					.await?;
			}
			self.server.index.batch(arg).await.map_err(|error| {
				tg::error!(!error, "failed to index a database index outbox batch")
			})?;
		}
		let arg = crate::database::index::outbox::DeleteArg {
			batch,
			region: region.to_owned(),
		};
		self.server
			.database
			.delete_index_outbox(arg)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete a database index outbox batch")
			})?;

		Ok(count)
	}

	fn database_index_outbox_batch_contains_named_node_mutation(
		arg: &tangram_index::batch::Arg,
	) -> bool {
		arg.items
			.iter()
			.any(|item| Self::database_index_outbox_named_node_id(item).is_some())
	}

	fn database_index_outbox_named_node_id(item: &tangram_index::batch::Item) -> Option<tg::Id> {
		let id = match item {
			tangram_index::batch::Item::DeleteGroup(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteOrganization(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteTag(id) => id.clone().into(),
			tangram_index::batch::Item::DeleteUser(id) => id.clone().into(),
			tangram_index::batch::Item::PutGroup(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutOrganization(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutTag(arg) => arg.id.clone().into(),
			tangram_index::batch::Item::PutUser(arg) => arg.id.clone().into(),
			_ => return None,
		};

		Some(id)
	}

	async fn prepare_database_index_outbox_batch(
		&self,
		arg: tangram_index::batch::Arg,
	) -> tg::Result<(tangram_index::batch::Arg, Vec<NamedCheckout>)> {
		let ids = arg
			.items
			.iter()
			.filter_map(Self::database_index_outbox_named_node_id)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		let nodes = self.server.try_get_named_checkout_nodes(&ids).await?;
		let mut invalidations = BTreeMap::<tg::Id, tg::Specifier>::new();
		let mut items = Vec::with_capacity(arg.items.len());
		for item in arg.items {
			let invalidation = match &item {
				tangram_index::batch::Item::DeleteGroup(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteOrganization(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteTag(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::DeleteUser(id) => {
					let id = tg::Id::from(id.clone());
					nodes.get(&id).map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutGroup(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.parent != arg.parent || node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutOrganization(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutTag(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| {
							node.parent != arg.parent
								|| node.specifier != arg.specifier
								|| node.target.as_ref() != Some(&arg.target)
						})
						.map(|node| (id, node.specifier.clone()))
				},
				tangram_index::batch::Item::PutUser(arg) => {
					let id = tg::Id::from(arg.id.clone());
					nodes
						.get(&id)
						.filter(|node| node.specifier != arg.specifier)
						.map(|node| (id, node.specifier.clone()))
				},
				_ => None,
			};
			if let Some((id, specifier)) = invalidation {
				items.push(tangram_index::batch::Item::DeleteCheckout(id.clone()));
				invalidations.insert(id, specifier);
			}
			items.push(item);
		}
		let invalidations = invalidations
			.into_iter()
			.map(|(id, specifier)| NamedCheckout { id, specifier })
			.collect();
		let arg = tangram_index::batch::Arg { items };

		Ok((arg, invalidations))
	}
}

pub(crate) fn database_index_outbox_subject() -> String {
	"database.index.outbox".to_owned()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn subject_has_no_partition() {
		assert_eq!(database_index_outbox_subject(), "database.index.outbox");
	}
}
