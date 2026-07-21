use {
	super::Store,
	crate::outbox::{DeleteArg, DequeueArg, EnqueueArg, Id, Item, TryGetIdArg},
	futures::FutureExt as _,
	num::ToPrimitive as _,
	scylla::value::CqlTimeuuid,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_outbox(&self, arg: DeleteArg) -> tg::Result<()> {
		futures::future::try_join_all(arg.keys.into_iter().map(|key| async move {
			let partition = key
				.partition
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))?;
			let id = CqlTimeuuid::from_u128(key.id.value());
			let params = (partition, id);
			self.session
				.execute_unpaged(&self.statements.delete_outbox, params)
				.await
				.map_err(|error| tg::error!(!error, "failed to delete the outbox item"))?;
			Ok::<_, tg::Error>(())
		}))
		.await?;

		Ok(())
	}

	pub async fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let partitions = partitions(arg.partition_start, arg.partition_count)?;
		if partitions.is_empty() || arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let limit = arg
			.batch_size
			.to_i32()
			.ok_or_else(|| tg::error!("the outbox batch size exceeded an i32"))?;
		let items = self
			.dequeue_outbox_with_statement(&partitions, limit, &self.statements.dequeue_outbox)
			.await?;
		if !items.is_empty() {
			return Ok(items);
		}
		let mut statement = self.statements.dequeue_outbox.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		self.dequeue_outbox_with_statement(&partitions, limit, &statement)
			.await
	}

	pub async fn enqueue_outbox(&self, arg: EnqueueArg) -> tg::Result<()> {
		let partition = arg
			.partition
			.to_i64()
			.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))?;
		let params = (partition, arg.payload);
		self.session
			.execute_unpaged(&self.statements.enqueue_outbox, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to enqueue the outbox item"))?;

		Ok(())
	}

	pub async fn try_get_outbox_id_at_or_before(&self, arg: TryGetIdArg) -> tg::Result<Option<Id>> {
		let partitions = partitions(arg.partition_start, arg.partition_count)?;
		if partitions.is_empty() {
			return Ok(None);
		}
		let result = if let Some(id) = arg.id {
			let params = (partitions, CqlTimeuuid::from_u128(id.value()));
			self.session
				.execute_unpaged(&self.statements.try_get_outbox_id_at_or_before, params)
				.boxed()
				.await
		} else {
			let params = (partitions,);
			self.session
				.execute_unpaged(&self.statements.try_get_outbox_id, params)
				.boxed()
				.await
		}
		.map_err(|error| tg::error!(!error, "failed to get the outbox id"))?
		.into_rows_result()
		.map_err(|error| tg::error!(!error, "failed to get the outbox rows"))?;
		let id = result
			.maybe_first_row::<(Option<CqlTimeuuid>,)>()
			.map_err(|error| tg::error!(!error, "failed to get the outbox row"))?
			.and_then(|(id,)| id)
			.map(|id| Id::new(id.as_u128()));

		Ok(id)
	}

	async fn dequeue_outbox_with_statement(
		&self,
		partitions: &[i64],
		limit: i32,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Vec<Item>> {
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: CqlTimeuuid,
			partition: i64,
			payload: &'a [u8],
		}

		let params = (partitions, limit);
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the outbox items"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the outbox rows"))?;
		let items = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the outbox rows"))?
			.map(|result| {
				let row =
					result.map_err(|error| tg::error!(!error, "failed to get the outbox row"))?;
				let partition = row
					.partition
					.to_u64()
					.ok_or_else(|| tg::error!("the outbox partition was negative"))?;
				let item = Item {
					id: Id::new(row.id.as_u128()),
					partition,
					payload: bytes::Bytes::copy_from_slice(row.payload),
				};
				Ok(item)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(items)
	}
}

fn partitions(partition_start: u64, partition_count: u64) -> tg::Result<Vec<i64>> {
	let partition_end = partition_start
		.checked_add(partition_count)
		.ok_or_else(|| tg::error!("the outbox partition range overflowed"))?;
	(partition_start..partition_end)
		.map(|partition| {
			partition
				.to_i64()
				.ok_or_else(|| tg::error!("the outbox partition exceeded an i64"))
		})
		.collect()
}
