use {
	super::Store,
	bytes::Bytes,
	futures::FutureExt as _,
	scylla::value::{CqlTimeuuid, CqlTimestamp, MaybeUnset},
	tangram_client::prelude::*,
};

pub struct OutboxItem {
	pub token: [u8; 16],
	pub id: tg::object::Id,
	pub grant: Option<Bytes>,
	pub metadata: Option<Bytes>,
}

impl Store {
	pub async fn enqueue_outbox_batch(
		&self,
		entries: Vec<(i64, tg::object::Id, Option<Vec<u8>>, Option<Vec<u8>>)>,
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
			.map(|(partition, id, grant, metadata)| {
				(
					*partition,
					id.to_bytes().to_vec(),
					grant
						.clone()
						.map_or(MaybeUnset::Unset, MaybeUnset::Set),
					metadata
						.clone()
						.map_or(MaybeUnset::Unset, MaybeUnset::Set),
				)
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
	) -> tg::Result<Vec<OutboxItem>> {
		let items = self
			.dequeue_outbox_with_statement(
				partition,
				limit,
				&self.statements.dequeue_object_outbox,
			)
			.await?;
		if !items.is_empty() {
			return Ok(items);
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
	) -> tg::Result<Vec<OutboxItem>> {
		let params = (partition, limit);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			token: CqlTimeuuid,
			id: &'a [u8],
			grant_payload: Option<&'a [u8]>,
			metadata_payload: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let items = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the rows"))?
			.map(|result| {
				let row = result.map_err(|error| tg::error!(!error, "failed to get the row"))?;
				let id = tg::object::Id::from_slice(row.id)
					.map_err(|error| tg::error!(!error, "failed to parse the id"))?;
				let grant = row.grant_payload.map(Bytes::copy_from_slice);
				let metadata = row.metadata_payload.map(Bytes::copy_from_slice);
				Ok(OutboxItem {
					token: *row.token.as_bytes(),
					id,
					grant,
					metadata,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(items)
	}

	pub async fn delete_outbox(&self, partition: i64, tokens: &[[u8; 16]]) -> tg::Result<()> {
		if tokens.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(
			self.statements
				.delete_object_outbox
				.get_consistency()
				.unwrap(),
		);
		for _ in tokens {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.delete_object_outbox.clone(),
			));
		}
		let params = tokens
			.iter()
			.map(|token| (partition, CqlTimeuuid::from_bytes(*token)))
			.collect::<Vec<_>>();
		self.session
			.batch(&batch, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the outbox entries"))?;
		Ok(())
	}

	pub async fn outbox_has_pending(&self, partitions: Vec<i64>, before: i64) -> tg::Result<bool> {
		let params = (partitions, CqlTimestamp(before));
		let result = self
			.session
			.execute_unpaged(&self.statements.outbox_pending, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the rows"))?;
		let row = result
			.maybe_first_row::<(CqlTimeuuid,)>()
			.map_err(|error| tg::error!(!error, "failed to get the row"))?;
		Ok(row.is_some())
	}
}
