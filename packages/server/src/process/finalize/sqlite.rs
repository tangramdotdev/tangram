use {
	crate::{Server, process::finalize::Entry},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Server {
	pub(crate) async fn try_finalizer_dequeue_batch_sqlite(
		&self,
		sandbox_store: &db::sqlite::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_finalizer_dequeue_batch_sqlite_sync(connection, batch_size)
			})
			.await
	}

	fn try_finalizer_dequeue_batch_sqlite_sync(
		connection: &mut sqlite::Connection,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let statement = indoc!(
			"
				select position, process
				from process_finalize_queue
				where status = 'created'
				order by position
				limit ?1;
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![i64::try_from(batch_size).unwrap()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut entries = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let position = row
				.get::<_, i64>(0)
				.map_err(|source| tg::error!(!source, "failed to get the position"))?;
			let process = row
				.get::<_, String>(1)
				.map_err(|source| tg::error!(!source, "failed to get the process"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
			entries.push(Entry { position, process });
		}
		drop(rows);
		drop(statement);
		if entries.is_empty() {
			return Ok(None);
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update process_finalize_queue
				set
					started_at = coalesce(started_at, ?1),
					status = 'started'
				where position = ?2;
			"
		);
		for entry in &entries {
			let n = transaction
				.execute(statement, sqlite::params![now, entry.position])
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			if n != 1 {
				return Err(tg::error!(
					"failed to claim the process finalize queue entry"
				));
			}
		}
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(Some(entries))
	}
}
