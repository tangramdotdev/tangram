use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Server {
	pub(crate) async fn clean_processes_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		processes: &[tg::process::Id],
		max_stored_at: i64,
	) -> tg::Result<()> {
		if processes.is_empty() {
			return Ok(());
		}

		let processes = processes
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		for process in processes {
			process_store
				.run(move |transaction, _cache| {
					Self::clean_processes_sqlite_sync(transaction, &process, max_stored_at)
				})
				.await
				.map_err(|error| tg::error!(!error, "failed to clean the process"))?;
		}

		Ok(())
	}

	fn clean_processes_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		process: &str,
		max_stored_at: i64,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
		let statement = indoc!(
			"
				delete from processes
				where id = ?1 and stored_at <= ?2;
			"
		);
		let result = transaction
			.execute(statement, sqlite::params![process, max_stored_at])
			.map_err(db::sqlite::Error::from);
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(()));
		}

		let statement = indoc!(
			"
				delete from process_children
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement, sqlite::params![process])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_finalize_queue
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement, sqlite::params![process])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}
}
