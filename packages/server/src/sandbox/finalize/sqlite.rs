use {
	crate::{Server, sandbox::finalize::Entry},
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_finalize_batch_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		db::sqlite::run!(
			process_store,
			[batch_size = batch_size],
			|transaction, _cache| {
				Self::try_dequeue_sandbox_finalize_batch_sqlite_sync(transaction, batch_size)
			}
		)
		.map_err(|error| tg::error!(!error, "failed to dequeue sandbox finalize entries"))
	}

	fn try_dequeue_sandbox_finalize_batch_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		batch_size: usize,
	) -> tg::Result<ControlFlow<Option<Vec<Entry>>, db::sqlite::Error>> {
		let statement = indoc!(
			"
				select position, sandbox
				from sandbox_finalize_queue
				where status = 'created'
				order by position
				limit ?1;
			"
		);
		let result = transaction
			.prepare(statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let mut rows = {
			let result = statement
				.query(sqlite::params![i64::try_from(batch_size).unwrap()])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement")
		};
		let mut entries = Vec::new();
		loop {
			let result = rows.next().map_err(db::sqlite::Error::from);
			let Some(row) = crate::database::retry!(result, "failed to execute the statement")
			else {
				break;
			};
			let result = row.get::<_, i64>(0).map_err(db::sqlite::Error::from);
			let position = crate::database::retry!(result, "failed to get the position");
			let result = row.get::<_, String>(1).map_err(db::sqlite::Error::from);
			let sandbox = crate::database::retry!(result, "failed to get the sandbox")
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
			entries.push(Entry { position, sandbox });
		}
		drop(rows);
		drop(statement);
		if entries.is_empty() {
			return Ok(ControlFlow::Break(None));
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update sandbox_finalize_queue
				set
					started_at = coalesce(started_at, ?1),
					status = 'started'
				where position = ?2;
			"
		);
		for entry in &entries {
			let result = transaction
				.execute(statement, sqlite::params![now, entry.position])
				.map_err(db::sqlite::Error::from);
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n != 1 {
				return Err(tg::error!(
					"failed to claim the sandbox finalize queue entry"
				));
			}
		}
		Ok(ControlFlow::Break(Some(entries)))
	}
}
