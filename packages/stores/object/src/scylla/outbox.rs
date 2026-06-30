use {super::Store, bytes::Bytes, futures::FutureExt as _, tangram_client::prelude::*};

pub struct OutboxEntry {
	pub id: tg::object::Id,
	pub payload: Bytes,
}

impl Store {
	pub async fn enqueue_outbox_batch(
		&self,
		entries: Vec<(i64, tg::object::Id, i64, Vec<u8>)>,
	) -> tg::Result<()> {
		if entries.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(
			self.statements
				.enqueue_object_outbox
				.get_consistency()
				.unwrap(),
		);
		for _ in &entries {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.enqueue_object_outbox.clone(),
			));
		}
		let params = entries
			.iter()
			.map(|(partition, id, enqueued_at, payload)| {
				(*partition, id.to_bytes().to_vec(), *enqueued_at, payload.clone())
			})
			.collect::<Vec<_>>();
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the batch"))?;
		Ok(())
	}

	pub async fn dequeue_outbox(
		&self,
		partition: i64,
		limit: i32,
	) -> tg::Result<Vec<OutboxEntry>> {
		let entries = self
			.dequeue_outbox_with_statement(
				partition,
				limit,
				&self.statements.dequeue_object_outbox,
			)
			.await?;
		if !entries.is_empty() {
			return Ok(entries);
		}
		let mut statement = self.statements.dequeue_object_outbox.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		self.dequeue_outbox_with_statement(partition, limit, &statement)
			.await
	}

	async fn dequeue_outbox_with_statement(
		&self,
		partition: i64,
		limit: i32,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Vec<OutboxEntry>> {
		let params = (partition, limit);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			payload: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let entries = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the rows"))?
			.map(|result| {
				let row = result.map_err(|error| tg::error!(!error, "failed to get the row"))?;
				let id = tg::object::Id::from_slice(row.id)
					.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
				let payload = Bytes::copy_from_slice(row.payload);
				Ok(OutboxEntry { id, payload })
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(entries)
	}

	pub async fn delete_outbox(
		&self,
		partition: i64,
		ids: &[tg::object::Id],
	) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (partition, id_bytes);
		self.session
			.execute_unpaged(&self.statements.delete_object_outbox, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the outbox entries"))?;
		Ok(())
	}

	pub async fn outbox_has_pending(&self, partitions: Vec<i64>, before: i64) -> tg::Result<bool> {
		let params = (partitions, before);
		let result = self
			.session
			.execute_unpaged(&self.statements.outbox_pending, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let row = result
			.maybe_first_row::<(i64,)>()
			.map_err(|error| tg::error!(!error, "failed to get the row"))?;
		Ok(row.is_some())
	}
}
