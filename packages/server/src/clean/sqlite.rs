use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
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
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::clean_processes_sqlite_sync(connection, &processes, max_stored_at)
			})
			.await
	}

	fn clean_processes_sqlite_sync(
		connection: &mut sqlite::Connection,
		processes: &[String],
		max_stored_at: i64,
	) -> tg::Result<()> {
		for process in processes {
			let transaction = connection
				.transaction()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

			let statement = indoc!(
				"
					delete from processes
					where id = ?1 and stored_at <= ?2;
				"
			);
			let n = transaction
				.execute(statement, sqlite::params![process, max_stored_at])
				.map_err(|source| tg::error!(!source, "failed to delete the process"))?;
			if n == 0 {
				continue;
			}

			let statement = indoc!(
				"
					delete from process_children
					where process = ?1;
				"
			);
			transaction
				.execute(statement, sqlite::params![process])
				.map_err(|source| tg::error!(!source, "failed to delete process_children"))?;

			let statement = indoc!(
				"
					delete from process_tokens
					where process = ?1;
				"
			);
			transaction
				.execute(statement, sqlite::params![process])
				.map_err(|source| tg::error!(!source, "failed to delete process_tokens"))?;

			let statement = indoc!(
				"
					delete from process_finalize_queue
					where process = ?1;
				"
			);
			transaction
				.execute(statement, sqlite::params![process])
				.map_err(|source| tg::error!(!source, "failed to delete process_finalize_queue"))?;

			let statement = indoc!(
				"
					delete from process_signals
					where process = ?1;
				"
			);
			transaction
				.execute(statement, sqlite::params![process])
				.map_err(|source| tg::error!(!source, "failed to delete process_signals"))?;

			let statement = indoc!(
				"
					delete from process_stdio
					where process = ?1;
				"
			);
			transaction
				.execute(statement, sqlite::params![process])
				.map_err(|source| tg::error!(!source, "failed to delete process_stdio"))?;

			transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		}

		Ok(())
	}
}
