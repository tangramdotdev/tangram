use {
	crate::Session,
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn try_dequeue_process_signal_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let id = id.clone();
		process_store
			.run(move |transaction, _cache| {
				Self::try_dequeue_process_signal_sqlite_sync(transaction, &id)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the process signal"))
	}

	fn try_dequeue_process_signal_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<tg::process::Signal>, db::sqlite::Error>> {
		let statement = indoc!(
			"
				select position, signal
				from process_signals
				where process = ?1
				order by position
				limit 1;
			"
		);
		let result = transaction
			.prepare(statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let mut rows = {
			let result = statement
				.query(sqlite::params![id.to_string()])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement")
		};
		let result = rows.next().map_err(db::sqlite::Error::from);
		let Some(row) = crate::database::retry!(result, "failed to execute the statement") else {
			return Ok(ControlFlow::Break(None));
		};
		let result = row.get::<_, i64>(0).map_err(db::sqlite::Error::from);
		let position = crate::database::retry!(result, "failed to get the position");
		let result = row.get::<_, String>(1).map_err(db::sqlite::Error::from);
		let signal = crate::database::retry!(result, "failed to get the signal")
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the signal"))?;
		drop(rows);
		drop(statement);
		let statement = indoc!(
			"
				delete from process_signals
				where position = ?1;
			"
		);
		let result = transaction
			.execute(statement, sqlite::params![position])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(Some(signal)))
	}
}
