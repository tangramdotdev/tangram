use {
	super::Index, indoc::indoc, num::ToPrimitive as _, rusqlite as sqlite,
	tangram_client::prelude::*, tangram_database::prelude::*,
};

impl Index {
	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let id = connection
			.with(move |connection, cache| {
				let statement = "select id from transaction_id;";
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let mut rows = statement
					.query([])
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let row = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to retrieve the row"))?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let id: i64 = row
					.get(0)
					.map_err(|source| tg::error!(!source, "failed to deserialize the column"))?;
				let id = id
					.to_u64()
					.ok_or_else(|| tg::error!("expected a valid transaction id"))?;
				Ok::<_, tg::Error>(id)
			})
			.await?;
		Ok(id)
	}

	pub async fn get_queue_size(&self, transaction_id: u64) -> tg::Result<u64> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let count = connection
			.with(move |connection, cache| {
				let statement = indoc!(
					"
						select
							(select count(*) from cache_entry_queue where transaction_id <= ?1) +
							(select count(*) from object_queue where transaction_id <= ?1) +
							(select count(*) from process_queue where transaction_id <= ?1) as count;
					"
				);
				let params = sqlite::params![
					transaction_id
						.to_i64()
						.ok_or_else(|| tg::error!("invalid transaction id"))?
				];
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let mut rows = statement
					.query(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let row = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to retrieve the row"))?
					.ok_or_else(|| tg::error!("expected a row"))?;
				let count: i64 = row
					.get(0)
					.map_err(|source| tg::error!(!source, "failed to deserialize the column"))?;
				let count = count
					.to_u64()
					.ok_or_else(|| tg::error!("expected a valid count"))?;
				Ok::<_, tg::Error>(count)
			})
			.await?;
		Ok(count)
	}
}
