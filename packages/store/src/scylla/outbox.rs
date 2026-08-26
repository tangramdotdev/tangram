use {
	super::Store,
	crate::outbox::{batch, fragment},
	futures::FutureExt as _,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_outbox_fragments(&self, arg: fragment::delete::Arg) -> tg::Result<()> {
		futures::future::try_join_all(arg.fragments.into_iter().map(|fragment| async move {
			let partition = fragment
				.partition
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))?;
			let index = fragment
				.index
				.value()
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox fragment index exceeded an i64"))?;
			let batch = fragment.batch.value();
			let params = (partition, batch.as_slice(), index);
			self.session
				.execute_unpaged(&self.statements.delete_outbox_fragment, params)
				.await
				.map_err(|error| tg::error!(!error, "failed to delete the outbox fragment"))?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}

	pub async fn dequeue_outbox_fragments(
		&self,
		arg: fragment::dequeue::Arg,
	) -> tg::Result<Vec<fragment::Fragment>> {
		let partitions = partitions(arg.partition_start, arg.partition_end)?;
		if partitions.is_empty() || arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let limit = arg
			.batch_size
			.to_i32()
			.ok_or_else(|| tg::error!("the outbox batch size exceeded an i32"))?;
		let params = (&partitions, limit);
		let result = self
			.session
			.execute_unpaged(&self.statements.dequeue_outbox_fragments, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the outbox fragments"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the outbox rows"))?;

		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			batch: &'a [u8],
			fragment: i64,
			partition: i64,
			payload: &'a [u8],
		}

		let fragments = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the outbox rows"))?
			.map(|result| {
				let row =
					result.map_err(|error| tg::error!(!error, "failed to get the outbox row"))?;
				let batch = row
					.batch
					.try_into()
					.map_err(|_| tg::error!("invalid outbox batch id length"))?;
				let index = row
					.fragment
					.to_u64()
					.ok_or_else(|| tg::error!("the outbox fragment index was negative"))?;
				let partition = row
					.partition
					.to_u64()
					.ok_or_else(|| tg::error!("the outbox partition was negative"))?;
				let fragment = fragment::Fragment {
					batch: batch::Id::new(batch),
					index: fragment::Index::new(index),
					partition,
					payload: bytes::Bytes::copy_from_slice(row.payload),
				};
				Ok(fragment)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(fragments)
	}

	pub async fn enqueue_outbox_batch(&self, arg: batch::enqueue::Arg) -> tg::Result<()> {
		let partition = arg
			.partition
			.to_i64()
			.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))?;
		let batch_id = arg.id.value();
		for (index, payload) in arg.fragments.into_iter().enumerate() {
			let index = index
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox fragment index exceeded an i64"))?;
			let params = (batch_id.as_slice(), index, partition, payload);
			self.session
				.execute_unpaged(&self.statements.enqueue_outbox_fragment, params)
				.await
				.map_err(|error| tg::error!(!error, "failed to enqueue the outbox fragment"))?;
		}

		Ok(())
	}

	pub async fn try_get_outbox_batch_at_or_before(
		&self,
		arg: batch::get::Arg,
	) -> tg::Result<Option<batch::Id>> {
		let partitions = partitions(arg.partition_start, arg.partition_end)?;
		if partitions.is_empty() {
			return Ok(None);
		}
		let result = if let Some(batch) = arg.batch {
			let batch = batch.value();
			let params = (&partitions, batch.as_slice());
			self.session
				.execute_unpaged(&self.statements.try_get_outbox_batch_at_or_before, params)
				.boxed()
				.await
		} else {
			let params = (&partitions,);
			self.session
				.execute_unpaged(&self.statements.try_get_outbox_batch, params)
				.boxed()
				.await
		}
		.map_err(|error| tg::error!(!error, "failed to get the outbox batch"))?
		.into_rows_result()
		.map_err(|error| tg::error!(!error, "failed to get the outbox rows"))?;
		let batch = result
			.maybe_first_row::<(Option<Vec<u8>>,)>()
			.map_err(|error| tg::error!(!error, "failed to get the outbox row"))?
			.and_then(|(batch,)| batch)
			.map(|batch| {
				batch
					.as_slice()
					.try_into()
					.map(batch::Id::new)
					.map_err(|_| tg::error!("invalid outbox batch id length"))
			})
			.transpose()?;

		Ok(batch)
	}
}

fn partitions(partition_start: u64, partition_end: u64) -> tg::Result<Vec<i64>> {
	(partition_start..partition_end)
		.map(|partition| {
			partition
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))
		})
		.collect()
}
