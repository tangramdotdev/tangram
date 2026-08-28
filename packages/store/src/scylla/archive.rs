use {
	super::Store, crate::object::archive::outbox, futures::FutureExt as _, num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_object_archive_outbox_entries(
		&self,
		arg: outbox::delete::Arg,
	) -> tg::Result<()> {
		futures::future::try_join_all(arg.entries.into_iter().map(|entry| async move {
			let timestamp = super::object_timestamp(entry.stored_at)?;
			let partition = entry
				.partition
				.to_i64()
				.ok_or_else(|| tg::error!("the object archive outbox partition exceeded an i64"))?;
			let id = entry.id.to_bytes();
			let params = (timestamp, partition, id.as_ref());
			self.session
				.execute_unpaged(&self.statements.delete_object_archive_outbox_entry, params)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to delete an object archive outbox entry")
				})?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}

	pub async fn dequeue_object_archive_outbox_entries(
		&self,
		arg: outbox::dequeue::Arg,
	) -> tg::Result<Vec<outbox::Entry>> {
		let partitions = partitions(arg.partition_start, arg.partition_end)?;
		if partitions.is_empty() || arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let limit = arg
			.batch_size
			.to_i32()
			.ok_or_else(|| tg::error!("the object archive outbox batch size exceeded an i32"))?;
		let params = (&partitions, limit);
		let result = self
			.session
			.execute_unpaged(
				&self.statements.dequeue_object_archive_outbox_entries,
				params,
			)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue object archive outbox entries"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get object archive outbox rows"))?;

		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			partition: i64,
			stored_at: i64,
		}

		let entries = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate object archive outbox rows"))?
			.map(|result| {
				let row = result.map_err(|error| {
					tg::error!(!error, "failed to get an object archive outbox row")
				})?;
				let id = tg::object::Id::from_slice(row.id)?;
				let partition = row.partition.to_u64().ok_or_else(|| {
					tg::error!("the object archive outbox partition was negative")
				})?;
				let entry = outbox::Entry {
					id,
					partition,
					stored_at: row.stored_at,
				};
				Ok(entry)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(entries)
	}

	pub async fn put_object_archive_outbox_entries(&self, arg: outbox::put::Arg) -> tg::Result<()> {
		futures::future::try_join_all(arg.entries.into_iter().map(|entry| async move {
			let timestamp = super::object_timestamp(entry.stored_at)?;
			let partition = entry
				.partition
				.to_i64()
				.ok_or_else(|| tg::error!("the object archive outbox partition exceeded an i64"))?;
			let id = entry.id.to_bytes();
			let params = (id.as_ref(), partition, entry.stored_at, timestamp);
			self.session
				.execute_unpaged(&self.statements.put_object_archive_outbox_entry, params)
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to write an entry to the object archive outbox"
					)
				})?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}
}

fn partitions(partition_start: u64, partition_end: u64) -> tg::Result<Vec<i64>> {
	(partition_start..partition_end)
		.map(|partition| {
			partition
				.to_i64()
				.ok_or_else(|| tg::error!("the object archive outbox partition exceeded an i64"))
		})
		.collect()
}
