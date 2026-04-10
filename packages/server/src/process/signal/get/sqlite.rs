use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Server {
	pub(crate) async fn try_dequeue_process_signal_sqlite(
		&self,
		sandbox_store: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let id = id.clone();
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_dequeue_process_signal_sqlite_sync(connection, &id)
			})
			.await
	}

	fn try_dequeue_process_signal_sqlite_sync(
		connection: &mut sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let statement = indoc!(
			"
				select position, signal
				from process_signals
				where process = ?1
				order by position
				limit 1;
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let position = row
			.get::<_, i64>(0)
			.map_err(|source| tg::error!(!source, "failed to get the position"))?;
		let signal = row
			.get::<_, String>(1)
			.map_err(|source| tg::error!(!source, "failed to get the signal"))?
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the signal"))?;
		drop(rows);
		drop(statement);
		let statement = indoc!(
			"
				delete from process_signals
				where position = ?1;
			"
		);
		transaction
			.execute(statement, sqlite::params![position])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(Some(signal))
	}
}
